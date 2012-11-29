from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File.IOutputFile import IOutputFile
import copy, os, datetime
from GangaGaudi.Lib.Applications.GaudiUtils import shellEnv_cmd, shellEnvUpdate_cmd
import Ganga.Utility.Config
from Ganga.Utility.Config import getConfig
configDirac = Ganga.Utility.Config.getConfig('DIRAC')
#configLHCb  = Ganga.Utility.Config.getConfig('LHCb' )
import fnmatch,subprocess
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class DiracFile(IOutputFile):
    """
    File stored on a DIRAC storage element
    """
    _schema = Schema(Version(1,1), { 'namePattern'   : SimpleItem(defvalue="",doc='pattern of the file name'),
                                     'localDir'      : SimpleItem(defvalue="",doc='local dir where the file is stored, used from get and put methods'),    
                                     'joboutputdir'  : SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
                                     'locations'     : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
                                     'compressed'    : SimpleItem(defvalue=False,typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere'),
                                     'lfn'           : SimpleItem(defvalue="",typelist=['str'],doc='The logical file name'),
#                                     'diracSE'       : SimpleItem(defvalue=[],typelist=['str'],sequence=1,hidden=1,doc='The dirac SE sites to try to upload to'),
                                     'guid'          : SimpleItem(defvalue='',typelist=['str'],doc='The files GUID'),
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
    _exportmethods = [  "get", "getMetadata", 'remove', "replicate", 'upload' ]
        
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
        if name == 'localDir':
            return expandfilename(value)
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
                names = line.split(':::')[1].split('->')
                if names[0] == self.namePattern:
                    if names[1] == '###FAILED###':
                        self.failureReason = line.split(':::')[2]
                    else:
                        self.lfn = names[1]
                        # self.diracSE= line.split(':')[2]
                        self.locations= line.split(':::')[2]
                        self.guid = line.split(':::')[3].replace('\n','')
                
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
            self.locations=[]
            #self.diracSE=[]
            self.guid=''
        return stdout
        
    def getMetadata(self):
        if self.lfn == "":
            raise GangaException('Can\'t obtain metadata with no LFN set.')
        self._getEnv()
        ret =  eval(shellEnv_cmd('dirac-dms-lfn-metadata %s' % self.lfn, self._env)[1])
        try: self.guid = ret['Successful'][self.lfn]['GUID']
        except: pass
        return ret
        
    def getGUID(self):
        if self.guid: return self.guid
        self.getMetadata()
        return self.guid
    
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
        r=shellEnv_cmd('dirac-dms-get-file %s' % self.lfn, self._env, self.localDir)[1]
        self.namePattern = os.path.split(self.lfn)[1]
        self.getMetadata()
        return r
        #todo Alex      

    def replicate(self, destSE):
        if not self.locations:
            raise GangaException('Can\'t replicate a file if it isn\'t already on a DIRAC SE, upload it first')
        if self.lfn == '':
            raise GangaException('Must supply an lfn to replicate')
        rc, stdout, stderr = shellEnv_cmd('dirac-dms-replicate-lfn %s %s %s' % (self.lfn, destSE, self.locations[0]),
                                          self._env)

        if 'Successful' in eval(stdout) and self.lfn in eval(stdout)['Successful']:
            self.locations.append(destSE)
        return stdout
         

    def upload(self):#, SEs=[]):
        """
        Try to upload file sequentially to storage elements in SEs.

        File will be uploaded to the first SE that the upload command succeeds for.
        """
        #self.diracSE = SEs
        #r = self.put()
        #self.diracSE = []
        #return r
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
            from uuid import uuid4
            self.lfn = os.path.join(configDirac['DiracLFNBase'], str(uuid4()),os.path.split(self.namePattern)[1])


        sourceDir = ''

        #if used as a stand alone object
        if self._parent == None:
            if self.localDir == '':
                raise GangaException('localDir attribute is empty, don\'t know from which dir to take the file' )
            else:
                sourceDir = self.localDir
        else:
            sourceDir = self.joboutputdir


        if not os.path.exists(os.path.join(sourceDir,self.namePattern)):
            raise GangaException('File "%s" must exist!'% os.path.join(sourceDir,self.namePattern))

            #raise GangaException('Can\'t upload a file without a logical file name (LFN).')
##         storage_elements = self.diracSE
##         if not self.diracSE:
        storage_elements=configDirac['DiracSpaceTokens']

        self._getEnv()
        stderr=''
        stdout=''
        for se in storage_elements:
            if self.guid:
                rc, stdout, stderr = shellEnv_cmd('dirac-dms-add-file %s %s %s %s' %(self.lfn, os.path.join(sourceDir,self.namePattern), se, guid), self._env)
            else:
                rc, stdout, stderr = shellEnv_cmd('dirac-dms-add-file %s %s %s' %(self.lfn, os.path.join(sourceDir,self.namePattern), se), self._env)

            if 'Successful' in eval(stdout) and self.lfn not in eval(stdout)['Successful']: continue
#            if rc: continue

#            self.diracSE = [se]
            self.locations = [se]
            try:
                import datetime
                self.guid = self.getGUID()
            except:
                self.guid = None
            return stdout
        self.failureReason = "Error in uploading file %s. : %s"% (self.namePattern,stdout)
        return self.failureReason

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
##         cmd = """
## ###INDENT###def run_command(cmd):
## ###INDENT###    import os, subprocess        
## ###INDENT###    pipe = subprocess.Popen(cmd,
## ###INDENT###                            shell=True,
## ###INDENT###                            env=os.environ,
## ###INDENT###                            cwd=os.getcwd(),
## ###INDENT###                            stdout=subprocess.PIPE,
## ###INDENT###                            stderr=subprocess.PIPE)
## ###INDENT###    stdout, stderr = pipe.communicate()
## ###INDENT###    return pipe.returncode, stdout, stderr
## ###INDENT###
## ###INDENT####def zip_files(output_files, patterns_to_zip):
## ###INDENT####    import gzip
## ###INDENT####    zipped_files = []
## ###INDENT####    files_to_zip =[]
## ###INDENT####    for p in patterns_to_zip: files_to_zip.extend(glob.glob(p))
## ###INDENT####    for file in files_to_zip:
## ###INDENT####        if os.path.exists(os.path.join(os.getcwd(),file)):
## ###INDENT####            f_in = open(file_name, 'rb')
## ###INDENT####            f_out = gzip.open(file_name+'.gz', 'wb')
## ###INDENT####            f_out.writelines(f_in)
## ###INDENT####            f_out.close()
## ###INDENT####            f_in.close()
## ###INDENT####        else: errorfile.write('Could not zip file %s as it was not found' % file)
## ###INDENT####        zipped_files.append(file_name+'.gz')
## ###INDENT####    return list(set(output_files).difference(set(files_to_zip)).update(set(zipped_files)))
## ###INDENT###
## ###INDENT###
## ###INDENT####for file in zip_files(###OUTPUTFILES###, ###ZIPFILES###):
## ###INDENT###for file, lfn, guid in ###OUTPUTFILES###:
## ###INDENT###    if not os.path.exists(os.path.join(os.getcwd(),file)):
## ###INDENT###        ###LOCATIONSFILE###.write('DiracFile:%s->###FAILED###:File \\'%s\\' didn\\'t exist:NotAvailable\\n' % (file, file))
## ###INDENT###        continue
## ###INDENT###    for se in ###SE###:
## ###INDENT###        rc, stdout, stderr = run_command('. SetupProject.sh LHCbDirac >/dev/null && dirac-dms-add-file %s %s %s %s' % (lfn, file, se, guid))
## ###INDENT###        print stdout
## ###INDENT###        print stderr    
## ###INDENT###        if stdout.find('Successful') >=0  and stdout.find(lfn) >=0:
## ###INDENT###            try:
## ###INDENT###                import datetime
## ###INDENT###                id = eval(run_command('. SetupProject.sh LHCbDirac >/dev/null && dirac-dms-lfn-metadata %s' % lfn)[1])['Successful'][lfn]['GUID']
## ###INDENT###                ###LOCATIONSFILE###.write('DiracFile:%s->%s:%s:%s\\n' % (file, lfn, se, id))
## ###INDENT###            except:
## ###INDENT###                ###LOCATIONSFILE###.write('DiracFile:%s->%s:%s:NotAvailable\\n' % (file, lfn, se))                
## ###INDENT###            break
## ###INDENT###        if se == ###SE###[-1]: ###LOCATIONSFILE###.write('DiracFile:%s->###FAILED###:File \\'%s\\' could not be uploaded to any SE:NotAvailable\\n' % (file, file))
## """
        #todo Alex
        ## Might not even need to inject this code when running on the Dirac backend as use API to
        ## ensure that the output is sent to SE

        ## looks like only need this for non dirac backend, see above
        cmd = """
###INDENT###def run_command(cmd):
###INDENT###    import os, subprocess        
###INDENT###    pipe = subprocess.Popen(cmd,
###INDENT###                            shell=True,
###INDENT###                            env=os.environ,
###INDENT###                            cwd=os.getcwd(),
###INDENT###                            stdout=subprocess.PIPE,
###INDENT###                            stderr=subprocess.PIPE)
###INDENT###    stdout, stderr = pipe.communicate()
###INDENT###    return pipe.returncode, stdout, stderr
###INDENT###
###INDENT####def zip_files(output_files, patterns_to_zip):
###INDENT####    import gzip
###INDENT####    zipped_files = []
###INDENT####    files_to_zip =[]
###INDENT####    for p in patterns_to_zip: files_to_zip.extend(glob.glob(p))
###INDENT####    for file in files_to_zip:
###INDENT####        if os.path.exists(os.path.join(os.getcwd(),file)):
###INDENT####            f_in = open(file_name, 'rb')
###INDENT####            f_out = gzip.open(file_name+'.gz', 'wb')
###INDENT####            f_out.writelines(f_in)
###INDENT####            f_out.close()
###INDENT####            f_in.close()
###INDENT####        else: errorfile.write('Could not zip file %s as it was not found' % file)
###INDENT####        zipped_files.append(file_name+'.gz')
###INDENT####    return list(set(output_files).difference(set(files_to_zip)).update(set(zipped_files)))
###INDENT###
###INDENT###outputfiles = ###OUTPUTFILES###
###INDENT###storage_elements = ###SE###
###INDENT###
###INDENT####for file in zip_files(###OUTPUTFILES###, ###ZIPFILES###):
###INDENT###for file, lfn, guid in outputfiles:
###INDENT###    if not os.path.exists(os.path.join(os.getcwd(),file)):
###INDENT###        ###LOCATIONSFILE###.write('DiracFile:::%s->###FAILED###:::File \\'%s\\' didn\\'t exist:::NotAvailable\\n' % (file, file))
###INDENT###        continue
###INDENT###    for se in storage_elements:
###INDENT###        try:
###INDENT###            retcode, stdout, stderr = run_command('###SETUP###dirac-dms-add-file %s %s %s %s' % (lfn, file, se, guid))
###INDENT###        except Exception,x:
###INDENT###            ###LOCATIONSFILE###.write('DiracFile:::%s->###FAILED###:::Exception running command \\'%s\\' - %s:::NotAvailable\\n' % (file,'###SETUP###dirac-dms-add-file %s %s %s %s' % (lfn, file, se, guid),x))
###INDENT###        if stdout.find('Successful') >=0  and stdout.find(lfn) >=0:
###INDENT###            try:
###INDENT###                import datetime
###INDENT###                id = eval(run_command('###SETUP###dirac-dms-lfn-metadata %s' % lfn)[1])['Successful'][lfn]['GUID']
###INDENT###                ###LOCATIONSFILE###.write('DiracFile:::%s->%s:::%s:::%s\\n' % (file, lfn, se, id))
###INDENT###            except:
###INDENT###                ###LOCATIONSFILE###.write('DiracFile:::%s->%s:::%s:::NotAvailable\\n' % (file, lfn, se))                
###INDENT###            break
###INDENT###        if se == storage_elements[-1]: ###LOCATIONSFILE###.write('DiracFile:::%s->###FAILED###:::File \\'%s\\' could not be uploaded to any SE (%s,%s):::NotAvailable\\n' % (file, file, stdout, stderr))
"""
        import uuid
        uid = str(uuid.uuid4())
        output_nps   = [file.namePattern for file in outputFiles]
        output_lfns  = [os.path.join(configDirac['DiracLFNBase'], uid, file.namePattern) for file in outputFiles if file.lfn==''] +\
                       [file.lfn for file in outputFiles if file.lfn != '']
        output_guids = [file.guid for file in outputFiles]
        cmd = cmd.replace('###OUTPUTFILES###', str(zip(output_nps, output_lfns, output_guids)) )
        cmd = cmd.replace('###SE###',   str(configDirac['DiracSpaceTokens']))
        cmd = cmd.replace('###INDENT###',indent)
        cmd = cmd.replace('###LOCATIONSFILE###',postProcessLocationsFP)
        if self._parent and self._parent.backend._name=='Dirac':
            cmd = cmd.replace('###SETUP###','')
        else:
            cmd = cmd.replace('###SETUP###','. SetupProject.sh LHCbDirac &>/dev/null && ')

        return cmd

# add DiracFile objects to the configuration scope (i.e. it will be possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile


