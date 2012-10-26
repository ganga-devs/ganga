################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DiracFile.py,v 0.1 2012-16-25 15:40:00 idzhunov Exp $
################################################################################
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File.IOutputFile import IOutputFile
import copy, os
from GangaGaudi.Lib.Applications.GaudiUtils import shellEnv_cmd, shellEnvUpdate_cmd
import Ganga.Utility.Config
from Ganga.Utility.Config import getConfig
configDirac = Ganga.Utility.Config.getConfig('DIRAC')
configLHCb  = Ganga.Utility.Config.getConfig('LHCb' )
import fnmatch
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class DiracFile(IOutputFile):
    """todo DiracFile represents a class marking a file ...todo
    """
    _schema = Schema(Version(1,1), { 'namePattern'   : SimpleItem(defvalue="",doc='pattern of the file name'),
                                     'localDir'      : SimpleItem(defvalue="",doc='local dir where the file is stored, used from get and put methods'),    
                                     'joboutputdir'  : SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
                                     'locations'     : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
                                     'compressed'    : SimpleItem(defvalue=False,typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere'),
                                     'lfn'           : SimpleItem(defvalue="",typelist=['str'],doc='The logical file name'),
                                     'diracSE'       : SimpleItem(defvalue=[],typelist=['list'],doc='The dirac SE sites'),
                                     'guid'          : SimpleItem(defvalue=None,typelist=['str','type(None)'],doc='The files GUID'),
                                     'failureReason' : SimpleItem(defvalue="",doc='reason for the upload failure')
                                     })

#    _schema.datadict['lfn']=SimpleItem(defvalue="",typelist=['str'],doc='The logical file name')
#    _schema.datadict['diracSE']=SimpleItem(defvalue=[],typelist=['list'],doc='The dirac SE sites')
#    _schema.datadict['guid']=SimpleItem(defvalue=None,typelist=['str','type(None)'],doc='The files GUID')
#    _schema.version.major += 0
#    _schema.version.minor += 1
    _env=None

    _category = 'outputfiles'
    _name = "DiracFile"
    _exportmethods = [  "get", "getMetadata", 'remove', 'upload' ]
        
    def __init__(self,namePattern='',  localDir='', **kwds):
        """ name is the name of the output file that has to be written ...
        """
        super(DiracFile, self).__init__()
        self.namePattern = namePattern
        self.localDir = localDir
        self.locations = []

    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.namePattern = args[0]
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self.localDir = args[1]     

    def _attribute_filter__set__(self,name, value):
        if name == 'lfn':
            self.namePattern = os.path.split(value)[1]
        return value

    def __repr__(self):
        """Get the representation of the file."""

        return "DiracFile(namePattern='%s', lfn='%s')" % (self.namePattern, self.lfn)
    

    def setLocation(self):
        """
        """
        job = self.getJobObject()
        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            #logger.warning('Couldn\'t locate locations file so couldn\'t set the lfn info') ##seems to be called twice (only on Dirac backend... must check) so misleading when second one works??
            return

        postprocesslocations = open(postprocessLocationsPath, 'r')
        
        for line in postprocesslocations.readlines():
            if line.startswith('DiracFile'):
                names = line.split(':')[1].split('->')
                if names[0] == self.namePattern:
                    self.lfn = names[1]
                    self.diracSE = line.split(':')[2]
                    self.guid = line.split(':')[3]
        postprocesslocations.close()

    def _getEnv(self):
        if not self._env:
            self._env = copy.deepcopy(os.environ)
            shellEnvUpdate_cmd('. SetupProject.sh LHCbDirac', self._env)
        
    def remove(self):
        if self.lfn == "":
            raise GangaException('Can\'t remove a  file from DIRAC SE without an LFN.')
        self._getEnv()
        rc, stdout, stderr = shellEnv_cmd('dirac-dms-remove-lfn %s' % self.lfn, self._env)
        if not rc:
            self.lfn=""
            #self.namePattern +="-<REMOVED>"
            self.diracSE=[]
            self.guid=None
        return stdout
        
    def getMetadata(self):
        if self.lfn == "":
            raise GangaException('Can\'t obtain metadata with no LFN set.')
        self._getEnv()
        return shellEnv_cmd('dirac-dms-lfn-metadata %s' % self.lfn, self._env)[1]
        
    def get(self):
        """
        Retrieves locally all files matching this DiracFile object pattern
        """
##         from LogicalFile import get_result
        if not os.path.isdir(self.localDir):
            raise GangaException('%s is not a valid directory... ' % self.localDir)

        if self.lfn == "":
            raise GangaException('Can\'t download a file without an LFN.')
##         cmd = 'result = DiracCommands.getFile("%s","%s")' % (self.lfn,dir)
##         result = get_result(cmd,'Problem during download','Download error.')
##         #from PhysicalFile import PhysicalFile
##         #return GPIProxyObjectFactory(PhysicalFile(name=result['Value']))

        ## OTHER WAY... doesn't pollute the environment!
        ##caching the environment for future use.
        self._getEnv()
        return shellEnv_cmd('dirac-dms-get-file %s' % self.lfn, self._env, self.localDir)[1]
        #todo Alex      

    def upload(self):
        return self.put()
    
    def put(self):
        """
        this method will be called on the client
        """
        ## looks like will only need this for the interactive uploading of jobs.
        
##    def upload(self,lfn,diracSE,guid=None):
        'Upload PFN to LFC on SE "diracSE" w/ LFN "lfn".' 
        if self.namePattern == "":
            raise GangaException('Can\'t upload a file without a local file name.')
        if self.lfn == "":
            self.lfn = os.path.join(configDirac['DiracLFNBase'], os.path.split(self.namePattern)[1])


        sourceDir = ''

        #if used as a stand alone object
        if self._parent == None:
            if self.localDir == '':
                raise GangaException('localDir attribute is empty, don\'t know from which dir to take the file' )
            else:
                sourceDir = self.localDir
        else:
            sourceDir = self.joboutputdir




            #raise GangaException('Can\'t upload a file without a logical file name (LFN).')
        storage_elements = self.diracSE
        if not self.diracSE:
            storage_elements=configLHCb['DiracSpaceTokens']

        self._getEnv()
        for se in storage_elements:
            if self.guid:
                rc, stdout, stderr = shellEnv_cmd('dirac-dms-add-file %s %s %s %s' %(self.lfn, os.path.join(sourceDir,self.namePattern), se, guid), self._env)
            else:
                rc, stdout, stderr = shellEnv_cmd('dirac-dms-add-file %s %s %s' %(self.lfn, os.path.join(sourceDir,self.namePattern), se), self._env)

            if not rc:
                self.diracSE = [se]
                try:
                    import datetime
                    self.guid = eval(shellEnv_cmd('dirac-dms-lfn-metadata %s' % self.lfn, self._env)[1])['Successful'][self.lfn]['GUID']
                except:
                    self.guid = None
                return stdout
        self.failureReason = "Error in uploading file %s. : %s"% (self.namePattern,stdout)
        return self.failureReason

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        #todo Alex
        ## Might not even need to inject this code when running on the Dirac backend as use API to
        ## ensure that the output is sent to SE

        ## looks like only need this for non dirac backend, see above
        cmd = """
###INDENT###import os, sys, copy, glob
###INDENT###def shellEnvUpdate_cmd(cmd, environ=os.environ, cwdir=None):
###INDENT###    import subprocess, time, tempfile, pickle
###INDENT###    f = tempfile.NamedTemporaryFile(mode='w+b')
###INDENT###    fname = f.name
###INDENT###    f.close()
###INDENT###
###INDENT###    if not cmd.endswith(';'): cmd += ';'
###INDENT###    envdump  = 'import os, pickle;'
###INDENT###    envdump += 'f=open(\\\'%s\\\',\\\'w+b\\\');' % fname
###INDENT###    envdump += 'pickle.dump(os.environ,f);'
###INDENT###    envdump += 'f.close();'
###INDENT###    envdumpcommand = 'python -c \"%s\"' % envdump
###INDENT###    cmd += envdumpcommand
###INDENT###
###INDENT###    pipe = subprocess.Popen(cmd,
###INDENT###                            shell=True,
###INDENT###                            env=environ,
###INDENT###                            cwd=cwdir,
###INDENT###                            stdout=subprocess.PIPE,
###INDENT###                            stderr=subprocess.PIPE)
###INDENT###    stdout, stderr  = pipe.communicate()
###INDENT###    while pipe.poll() is None:
###INDENT###        time.sleep(0.5)
###INDENT###
###INDENT###    f = open(fname,'r+b')
###INDENT###    environ=environ.update(pickle.load(f))
###INDENT###    f.close()
###INDENT###    os.system('rm -f %s' % fname)
###INDENT###
###INDENT###    return pipe.returncode, stdout, stderr
###INDENT###
###INDENT###env = copy.deepcopy(os.environ)
###INDENT###if shellEnvUpdate_cmd('which dirac-dms-add-file' , env)[1].find('no dirac-dms-add-file in')>=0 or shellEnvUpdate_cmd('which dirac-dms-lfn-metadata' , env)[1].find('no dirac-dms-lfn-metadata in')>=0:
###INDENT###    if shellEnvUpdate_cmd('which SetupProject.sh' , env)[1].find('no SetupProject.sh in')<0:
###INDENT###        shellEnvUpdate_cmd('. SetupProject.sh LHCbDirac' , env)
###INDENT###    else:
###INDENT###        print \'ERROR: Could not find the SetupProject.sh script so dirac commands could not be set up\'
###INDENT###        sys.exit(1)
"""

        for f in outputFiles:
            if f.namePattern == "":
                logger.warning('Skipping dirac SE file %s as it\'s name attribute is not defined'% str(f))
                continue
            cmd += """
###INDENT###if os.path.exists('###NAME###'):
###INDENT###    for se in ###SE###:"""
            if f.namePattern in patternsToZip:
                cmd +="""
###INDENT###        if not shellEnvUpdate_cmd('dirac-dms-add-file ###LFN### ###NAME###.gz %s ###GUID###' % se, env)[0]:
###INDENT###            try:                
###INDENT###                import datetime
###INDENT###                guid = eval(shellEnvUpdate_cmd('dirac-dms-lfn-metadata ###LFN###', env)[1])['Successful']['###LFN###']['GUID']
###INDENT###                ###LOCATIONSFILE###.write('DiracFile:###NAME###.gz->###LFN###:%s:%s\n' % (se, guid))
###INDENT###            except:
###INDENT###                ###LOCATIONSFILE###.write('DiracFile:###NAME###.gz->###LFN###:%s:NotAvailable\n' % se)                
###INDENT###            break
"""
            else:
                cmd += """
###INDENT###        if not shellEnvUpdate_cmd('dirac-dms-add-file ###LFN### ###NAME### %s ###GUID###' % se, env)[0]:
###INDENT###            try:                
###INDENT###                import datetime
###INDENT###                guid = eval(shellEnvUpdate_cmd('dirac-dms-lfn-metadata ###LFN###', env)[1])['Successful']['###LFN###']['GUID']
###INDENT###                ###LOCATIONSFILE###.write('DiracFile:###NAME###->###LFN###:%s:%s\n' % (se, guid))
###INDENT###            except:
###INDENT###                ###LOCATIONSFILE###.write('DiracFile:###NAME###->###LFN###:%s:NotAvailable\n' % se)                
###INDENT###            break
"""
            # Set LFN here but when job comes back test which worked
            # by which in file, and remove appropriate failed ones
            if f.lfn == "":
                f.lfn=os.path.join(configDirac['DiracLFNBase'], os.path.split(f.namePattern)[1])
            cmd = cmd.replace('###LFN###',  f.lfn         )
            cmd = cmd.replace('###NAME###', f.namePattern )
            if f.diracSE:
                cmd = cmd.replace('###SE###',   str(f.diracSE))                
            else:
                cmd = cmd.replace('###SE###',   str(configLHCb['DiracSpaceTokens']))
            if f.guid:
                cmd = cmd.replace('###GUID###', f.guid )
            else:
                cmd = cmd.replace('###GUID###', '' )
                

        cmd = cmd.replace('###INDENT###',indent)
        cmd = cmd.replace('###LOCATIONSFILE###',postProcessLocationsFP)

        return cmd

# add DiracFile objects to the configuration scope (i.e. it will be possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile


