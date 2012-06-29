################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DiracFile.py,v 0.1 2012-16-25 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File.OutputSandboxFile import OutputSandboxFile

import fnmatch 

class DiracFile(OutputSandboxFile):
    """todo DiracFile represents a class marking a file ...todo
    """
    _schema = OutputSandboxFile._schema.inherit_copy()
#    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file'),
#                                    'joboutputdir': SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
#                                    'locations' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
#                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')
#                                        })

    _schema.datadict['lfn']=SimpleItem(defvalue="",typelist=['str'],doc='The logical file name')
    _schema.datadict['diracSE']=SimpleItem(defvalue=[],typelist=['str'],doc='The dirac SE sites')
    _schema.datadict['guid']=SimpleItem(defvalue=None,typelist=['str','type(None)'],doc='The files GUID')
    _schema.version.major += 0
    _schema.version.minor += 1

    _category = 'outputfiles'
    _name = "DiracFile"
    _exportmethods = [  "get", "setLocation" ]
        
    def __init__(self,name='', **kwds):
        """ name is the name of the output file that has to be written ...
        """
        super(DiracFile, self).__init__(name, **kwds)
        self.locations = []

    def __construct__(self,args):
        super(DiracFile,self).__construct__(args)

            
    def __repr__(self):
        """Get the representation of the file."""

        return "DiracFile(name='%s', lfn='%s')" % (self.name, self.lfn)
    

    def setLocation(self, location):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        if location not in self.locations:
            self.locations.append(location)
        
    def get(self, dir='.'):
        """
        Retrieves locally all files matching this DiracFile object pattern
        """
        import os
        from LogicalFile import get_result
        dir=os.path.abspath(expandfilename(dir))
        if not os.path.isdir(dir)
            print "%s is not a valid directory.... " % dir
            return
        if self.lfn == "":
            raise GangaException('Can\'t download a file without an LFN.')
        cmd = 'result = DiracCommands.getFile("%s","%s")' % (self.lfn,dir)
        result = get_result(cmd,'Problem during download','Download error.')
        #from PhysicalFile import PhysicalFile
        #return GPIProxyObjectFactory(PhysicalFile(name=result['Value']))

        ## OTHER WAY... doesn't pollute the environment!
        import copy
        from GangaGaudi.Lib.Application.GaudiUtils import shellEnvUpdate_cmd
        env = copy.deepcopy(os.environ)
        shellEnvUpdate_cmd('SetupProject LHCbDirac',           env, dir)
        shellEnvUpdate_cmd('dirac-dms-get-file %s' % self.lfn, env, dir)        
        #todo Alex      

    def put(self):
        """
        this method will be called on the client
        """
        ## looks like will only need this for the interactive uploading of jobs.
        
##    def upload(self,lfn,diracSE,guid=None):
        'Upload PFN to LFC on SE "diracSE" w/ LFN "lfn".' 
        from LogicalFile import get_result
        if self.name == "":
            raise GangaException('Can\'t upload a file without a local file name.')
        if self.lfn == "":
            raise GangaException('Can\'t upload a file without a logical file name (LFN).')
        if not self.diracSE:
            raise GangaException('Please specify a dirac SE')
            
        if self.guid is None:
            cmd = 'result = DiracCommands.addFile("%s","%s","%s",None)' % \
                  (self.lfn,self.name,self.diracSE)
        else:
            cmd = 'result = DiracCommands.addFile("%s","%s","%s","%s")' % \
                  (self.lfn,self.name,self.diracSE,self.guid)
        result = get_result(cmd,'Problem w/ upload','Error uploading file.')
  #      from LogicalFile import LogicalFile
  #      return GPIProxyObjectFactory(LogicalFile(name=lfn))
        
        #todo Alex

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        #todo Alex
        ## Might not even need to inject this code when running on the Dirac backend as use API to
        ## ensure that the output is sent to SE

        ## looks like only need this for non dirac backend, see above
        cmd = """
###INDENT###import os, copy
###INDENT###def shellEnvUpdate_cmd(cmd, environ=os.environ, cwdir=None):
###INDENT###    import subprocess, time, tempfile, pickle
###INDENT###    f = tempfile.NamedTemporaryFile(mode='w+b')
###INDENT###    fname = f.name
###INDENT###    f.close()

###INDENT###    if not cmd.endswith(';'): cmd += ';'
###INDENT###    envdump  = 'import os, pickle;'
###INDENT###    envdump += 'f=open(\'%s\',\'w+b\');' % fname
###INDENT###    envdump += 'pickle.dump(os.environ,f);'
###INDENT###    envdump += 'f.close();'
###INDENT###    envdumpcommand = 'python -c \"%s\"' % envdump
###INDENT###    cmd += envdumpcommand

###INDENT###    pipe = subprocess.Popen(cmd,
###INDENT###                            shell=True,
###INDENT###                            env=environ,
###INDENT###                            cwd=cwdir,
###INDENT###                            stdout=subprocess.PIPE,
###INDENT###                            stderr=subprocess.PIPE)
###INDENT###    stdout, stderr  = pipe.communicate()
###INDENT###    while pipe.poll() is None:
###INDENT###        time.sleep(0.5)

###INDENT###    f = open(fname,'r+b')
###INDENT###    environ=environ.update(pickle.load(f))
###INDENT###    f.close()
###INDENT###    os.system('rm -f %s' % fname)

###INDENT###    return pipe.returncode, stdout, stderr


###INDENT###shellEnvUpdate_cmd('SetupProject LHCbDirac' ,os.environ)
"""

        for f in outputFiles:
            if f.name == "":
                logger.warning('Skipping dirac SE file %s as it\'s name attribute is not defined'% str(f))
                continue
            cmd += """
###INDENT###if os.path.exists(###NAME###):
###INDENT###    if not shellEnvUpdate_cmd('dirac-dms-add-file ###LFN### ###NAME### ###SE### ###GUID###')[0]:
###INDENT###        ###LOCATIONSFILE###.write(###LFN###)
"""
            # Set LFN here but when job comes back test which worked
            # by which in file, and remove appropriate failed ones
            if f.lfn == "":
                f.lfn=base + f.name
            if f.diracSE =="":
                f.diracSE=default
            cmd.replace('###LFN###',  f.lfn    )
            cmd.replace('###NAME###', f.name   )
            cmd.replace('###SE###',   f.diracSE)
            cmd.replace('###GUID###', f.guid   )

        cmd.replace('###INDENT###',indent)
        cmd.replace('###LOCATIONSFILE###',postProcessLocationsFP)

        return cmd

# add DiracFile objects to the configuration scope (i.e. it will be possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile


