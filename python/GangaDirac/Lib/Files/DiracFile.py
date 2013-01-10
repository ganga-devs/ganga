from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File.IOutputFile import IOutputFile
import copy, os, datetime, hashlib
from GangaGaudi.Lib.Applications.GaudiUtils import shellEnv_cmd, shellEnvUpdate_cmd
import Ganga.Utility.Config
from Ganga.Utility.Config import getConfig
configDirac = Ganga.Utility.Config.getConfig('DIRAC')
#configLHCb  = Ganga.Utility.Config.getConfig('LHCb' )
import fnmatch,subprocess,re
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
logger = Ganga.Utility.logging.getLogger()
regex = re.compile('[*?\[\]]')

class DiracFile(IOutputFile):
    """
    File stored on a DIRAC storage element
    """
    _schema = Schema(Version(1,1), { 'namePattern'   : SimpleItem(defvalue="",doc='pattern of the file name'),
                                     'localDir'      : SimpleItem(defvalue="",copyable=0,doc='local dir where the file is stored, used from get and put methods'),    
#                                     'joboutputdir'  : SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
                                     'locations'     : SimpleItem(defvalue=[],copyable=0,typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
                                     'compressed'    : SimpleItem(defvalue=False,typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere'),
                                     'lfn'           : SimpleItem(defvalue='',copyable=0,typelist=['str'],doc='The logical file name'),
#                                     'diracSE'       : SimpleItem(defvalue=[],typelist=['str'],sequence=1,hidden=1,doc='The dirac SE sites to try to upload to'),
                                     'guid'          : SimpleItem(defvalue='',copyable=0,typelist=['str'],doc='The files GUID'),
                                     'subfiles'      : ComponentItem(category='outputfiles',defvalue=[], hidden=1, typelist=['GangaDirac.Lib.Files.DiracFile'], sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),
                                     'failureReason' : SimpleItem(defvalue="",copyable=0,doc='reason for the upload failure')
                                     })

#    _schema.datadict['lfn']=SimpleItem(defvalue="",typelist=['str'],doc='The logical file name')
#    _schema.datadict['diracSE']=SimpleItem(defvalue=[],typelist=['list'],doc='The dirac SE sites')
#    _schema.datadict['guid']=SimpleItem(defvalue=None,typelist=['str','type(None)'],doc='The files GUID')
#    _schema.version.major += 0
#    _schema.version.minor += 1
    _env=None

    _category = 'outputfiles'
    _name = "DiracFile"
    _exportmethods = [  "get", "getMetadata", 'remove', "replicate", 'put']#, 'upload' ]
        
    def __init__(self, namePattern='',  localDir='', **kwds):
        """ name is the name of the output file that has to be written ...
        """
        super(DiracFile, self).__init__()
        self.namePattern = namePattern
        self.localDir = localDir
        self.locations = []
##COULD MAKE os.getcwd THE DEFAULT LOCALDIR, ALSO COULD CONSTRUCT FROM LFN
    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.namePattern = args[0]
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self.localDir = args[1]    

    def _attribute_filter__set__(self,name, value):
        if name == 'lfn':
            self.namePattern = os.path.basename(value)
        if name == 'localDir':
            return expandfilename(value)
        return value

    def __repr__(self):
        """Get the representation of the file."""

        return "DiracFile(namePattern='%s', lfn='%s')" % (self.namePattern, self.lfn)
    

    def setLocation(self):
        """
        """
        def dirac_line_processor(line, dirac_file):
            tokens = line.strip().split(':::')
            pattern   = tokens[1].split('->')[0].split('&&')[0]
            name      = tokens[1].split('->')[0].split('&&')[1]
            lfn       = tokens[1].split('->')[1]
#            pattern   = ''
#            if '&&' in name:
#                pattern = name.split('&&')[0]
#                name    = name.split('&&')[1]

            if pattern == dirac_file.namePattern:
                d=DiracFile(namePattern=name)
                dirac_file.subfiles.append(GPIProxyObjectFactory(d))
                dirac_line_processor(line, d)
            elif name == dirac_file.namePattern:
                if lfn == '###FAILED###':
                    logger.error("Failed to upload file '%s' to Dirac" % name)
                    dirac_file.failureReason = tokens[2]
                    return
                dirac_file.lfn       = lfn
                dirac_file.locations = tokens[2]
                dirac_file.guid      = tokens[3]
            else:
                logger.error("Could't decipher the outputfiles location entry!")
                logger.error("Neither '%s' nor '%s' match the namePattern attribute of '%s'" % (pattern, name, dirac_file.namePattern))
                dirac_file.failureReason = "Could't decipher the outputfiles location entry!"
                
  
            
        job = self.getJobObject()
        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            #logger.warning("Couldn\'t locate the output locations file so couldn't determine the lfn info") ##seems to be called twice (only on Dirac backend... must check) so misleading when second one works??
            return

        postprocesslocations = open(postprocessLocationsPath, 'r')
        
        for line in postprocesslocations.readlines():
            if line.startswith('DiracFile'):
                dirac_line_processor(line, self)
                        
        postprocesslocations.close()

    def _getEnv(self):
        if not self._env:
            self._env = copy.deepcopy(os.environ)
            shellEnvUpdate_cmd('. SetupProject.sh LHCbDirac', self._env)
        
    def remove(self):
        """
        Remove this lfn and all replicas from DIRAC LFC/SEs
        """
        if self.lfn == "":
            raise GangaException('Can\'t remove a  file from DIRAC SE without an LFN.')
        self._getEnv()
        logger.info('Removing file %s' % self.lfn)
        rc, stdout, stderr = shellEnv_cmd('dirac-dms-remove-lfn %s' % self.lfn, self._env)
        if stdout.find("'Successful': {'%s'" % self.lfn) >=0:
            self.lfn=""
            self.locations=[]
            self.guid=''
            return
        logger.error("Error in removing file '%s' : %s" % (self.lfn, stdout))
        return stdout
        
    def getMetadata(self):
        """
        Get Metadata associated with this files lfn. This method will also
        try to automatically set the files guid attribute.
        """
        if self.lfn == "":
            raise GangaException('Can\'t obtain metadata with no LFN set.')

        def inTokens(element):
            return element in configDirac['DiracSpaceTokens']

        self._getEnv()
        ret  =  eval(shellEnv_cmd('dirac-dms-lfn-metadata %s' % self.lfn, self._env)[1])
        reps =  filter(inTokens,shellEnv_cmd('dirac-dms-lfn-replicas %s' % self.lfn, self._env)[1].split(' '))
        try:
            if self.guid != ret['Successful'][self.lfn]['GUID']:
                self.guid = ret['Successful'][self.lfn]['GUID']
        except: pass
        try:
            if self.locations != reps:
                self.locations = reps
                ret['Successful'][self.lfn].update({'replicas': self.locations})
        except: pass
        return ret
          
    def get(self):
        """
        Retrieves locally the file matching this DiracFile object pattern
        """
        to_location = self.localDir
        if not os.path.isdir(self.localDir):
            if self._parent is not None and os.path.isdir(self.getJobObject().outputdir):
                to_location = self.getJobObject().outputdir
            else:
                raise GangaException('"%s" is not a valid directory... Please set the localDir attribute' % self.localDir)

        if self.lfn == "":
            raise GangaException('Can\'t download a file without an LFN.')

        self._getEnv()
        r=shellEnv_cmd('dirac-dms-get-file %s' % self.lfn, self._env, to_location)[1]
        self.namePattern = os.path.split(self.lfn)[1]
        self.getMetadata()
        return r

    def replicate(self, destSE):
        """
        Replicate this file from self.locations[0] to destSE
        """
        if not self.locations:
            raise GangaException('Can\'t replicate a file if it isn\'t already on a DIRAC SE, upload it first')
        if self.lfn == '':
            raise GangaException('Must supply an lfn to replicate')
        self._getEnv()
        logger.info("Replicating file %s to %s" % (self.lfn, destSE))
        rc, stdout, stderr = shellEnv_cmd('dirac-dms-replicate-lfn %s %s %s' % (self.lfn, destSE, self.locations[0]),
                                          self._env)
        if stdout.find("'Successful': {'%s'" % self.lfn) >=0:
            # if 'Successful' in eval(stdout) and self.lfn in eval(stdout)['Successful']:
            self.locations.append(destSE)
            return
        logger.error("Error in replicating file '%s' : %s" % (self.lfn, stdout))
        return stdout
         

#    def upload(self):
#        """
#        Try to upload file sequentially to storage elements defined in configDirac['DiracSpaceTokens'].
#
#        File will be uploaded to the first SE that the upload command succeeds for.
#        Return value will be either the stdout from the dirac upload command if not
#        using the wildcard characters '*?[]' in the namePattern. If the wildcard characters
#        are used then the return value will be a list containing newly created DiracFile
#        objects which were the result of glob-ing the wildcards. The objects in this list
#        will have been uploaded or had their failureReason attribute populated if the
#        upload failed.
#        """
#        return self.put()
    
    def put(self):
        """
        Try to upload file sequentially to storage elements defined in configDirac['DiracSpaceTokens'].

        File will be uploaded to the first SE that the upload command succeeds for.
        Return value will be either the stdout from the dirac upload command if not
        using the wildcard characters '*?[]' in the namePattern. If the wildcard characters
        are used then the return value will be a list containing newly created DiracFile
        objects which were the result of glob-ing the wildcards. The objects in this list
        will have been uploaded or had their failureReason attribute populated if the
        upload failed.
        """
        ## looks like will only need this for the interactive uploading of jobs.
        
        if self.namePattern == "":
            raise GangaException('Can\'t upload a file without a local file name.')

        sourceDir = self.localDir
        if self._parent != None: # attached to a job, use the joboutputdir
            sourceDir = self.getJobObject().outputdir
        if not os.path.isdir(sourceDir):
            raise GangaException('localDir attribute is not a valid dir, don\'t know from which dir to take the file' )

        if regex.search(self.namePattern) is not None:
            if self.lfn != "":
                logger.warning("Cannot specify a single lfn for a wildcard namePattern")
                logger.warning("LFN will be generated automatically")
                self.lfn=""
            if self.guid != "":
                logger.warning("Cannot specify a single guid for a wildcard namePattern")
                logger.warning("GUID will be generated automatically")
                self.guid=""
 
        import glob, uuid
        lfn_base =  os.path.join(configDirac['DiracLFNBase'], str(uuid.uuid4()))
        storage_elements=configDirac['DiracSpaceTokens']
 
        self._getEnv()
        
        outputFiles=GangaList()
        for file in glob.glob(os.path.join(sourceDir, self.namePattern)):
            if not os.path.exists(file):
                raise GangaException('File "%s" must exist!'% os.path.join(sourceDir, self.namePattern))
            lfn = self.lfn
            guid = self.guid
            if lfn == "":
                lfn = os.path.join(lfn_base, os.path.basename(file))
            if guid == "":
                md5 = hashlib.md5(lfn).hexdigest()
                guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()# conforming to DIRAC GUID hex md5 8-4-4-4-12
            
            d=DiracFile()
            d.namePattern = os.path.basename(file)
            d.localDir    = sourceDir
            stderr=''
            stdout=''
            logger.info('Uploading file %s' % file)
            for se in storage_elements:
                rc, stdout, stderr = shellEnv_cmd('dirac-dms-add-file %s %s %s %s' %(lfn, file, se, guid), self._env)
                if stdout.find("'Successful': {'%s'" % lfn) >=0:
                    if regex.search(self.namePattern) is not None:
                        d.lfn = lfn
                        d.locations = [se]
                        d.guid = guid
                        outputFiles.append(GPIProxyObjectFactory(d))
                        break
                    else:
                        self.lfn = lfn
                        self.locations = [se]
                        self.guid = guid
                        return
            else:
                logger.error(failureReason = "Error in uploading file %s. : %s"% (os.path.basename(file), stdout))
                if regex.search(self.namePattern) is not None:
                    d.failureReason =  failureReason
                    outputFiles.append(GPIProxyObjectFactory(d))
                else:
                    self.failureReason = failureReason
                    return stdout
        return GPIProxyObjectFactory(outputFiles)

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """

        def wildcard_script(namePattern, lfnBase):
            return """
###INDENT###for f in glob.glob('###NAME_PATTERN###'):
###INDENT###    wildcard_lfn = os.path.join('###LFN_BASE###', os.path.basename(f))
###INDENT###    md5 = hashlib.md5(wildcard_lfn).hexdigest()
###INDENT###    wildcard_guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()
###INDENT###    uploadFile(os.path.basename(f), wildcard_lfn, wildcard_guid, storage_elements, '###NAME_PATTERN###')
""".replace('###NAME_PATTERN###', namePattern).replace('###LFN_BASE###', lfnBase)

        script = """
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
###INDENT###def uploadFile(file, lfn, guid, SEs, wildcard=''):
###INDENT###    import os, datetime
###INDENT###    if not os.path.exists(os.path.join(os.getcwd(),file)):
###INDENT###        ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' didn't exist:::NotAvailable\\n" % (wildcard, file, file))
###INDENT###        return
###INDENT###    stdout=''
###INDENT###    stderr=''
###INDENT###    for se in SEs:
###INDENT###        try:
###INDENT###            retcode, stdout, stderr = run_command('###SETUP###dirac-dms-add-file %s %s %s %s' % (lfn, file, se, guid))
###INDENT###        except Exception,x:
###INDENT###            ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::Exception running command '%s' - %s:::NotAvailable\\n" % (wildcard, file,'###SETUP###dirac-dms-add-file %s %s %s %s' % (lfn, file, se, guid),x))
###INDENT###        if stdout.find(\"'Successful': {'%s'\" % lfn) >=0:
###INDENT###            try:
###INDENT###                id = eval(run_command('###SETUP###dirac-dms-lfn-metadata %s' % lfn)[1])['Successful'][lfn]['GUID']
###INDENT###                ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->%s:::%s:::%s\\n" % (wildcard, file, lfn, se, id))
###INDENT###            except:
###INDENT###                ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->%s:::%s:::NotAvailable\\n" % (wildcard, file, lfn, se))                
###INDENT###            return
###INDENT###    ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' could not be uploaded to any SE (%s,%s):::NotAvailable\\n" % (wildcard, file, file, stdout, stderr))
###INDENT###
###INDENT###storage_elements = ###STORAGE_ELEMENTS###
###INDENT###import os, glob, hashlib
"""

        import uuid
        lfn_base = os.path.join(configDirac['DiracLFNBase'], str(uuid.uuid4()))
        for file in outputFiles:
            if regex.search(file.namePattern) is not None:
                script+= wildcard_script(file.namePattern, lfn_base)
            else:
                lfn = file.lfn
                guid = file.guid
                if file.lfn=='':
                    lfn = os.path.join(lfn_base, file.namePattern)
                if file.guid == '':
                    md5 = hashlib.md5(lfn).hexdigest()
                    guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()

                script+='###INDENT###uploadFile("%s", "%s", "%s", storage_elements)' % (file.namePattern, lfn, guid)

        script = script.replace('###STORAGE_ELEMENTS###', str(configDirac['DiracSpaceTokens']))
        script = script.replace('###INDENT###',           indent)
        script = script.replace('###LOCATIONSFILE###',    postProcessLocationsFP)
        if self._parent and self._parent.backend._name=='Dirac':
            script = script.replace('###SETUP###', '')
        else:
            script = script.replace('###SETUP###', '. SetupProject.sh LHCbDirac &>/dev/null && ')

        return script

# add DiracFile objects to the configuration scope (i.e. it will be possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile


