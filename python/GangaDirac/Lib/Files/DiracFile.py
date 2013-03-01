import copy, os, datetime, hashlib, re
from Ganga.GPIDev.Base.Proxy                  import GPIProxyObjectFactory
from Ganga.GPIDev.Lib.GangaList.GangaList     import GangaList
from Ganga.GPIDev.Schema                      import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Lib.File.IOutputFile        import IOutputFile
from Ganga.GPIDev.Lib.Job.Job                 import Job
from Ganga.Core.exceptions                    import GangaException
from Ganga.Utility.files                      import expandfilename
from GangaDirac.Lib.Utilities.DiracUtilities  import getDiracEnv
from GangaDirac.Lib.Utilities.smartsubprocess import runcmd
from GangaDirac.Lib.Backends.DiracBase        import dirac_ganga_server
from Ganga.Utility.Config                     import getConfig
from Ganga.Utility.logging                    import getLogger
configDirac = getConfig('DIRAC')
logger      = getLogger()
regex       = re.compile('[*?\[\]]')

def upload_ok(lfn):
    import datetime
    retcode, stdout, stderr = dirac_ganga_server.execute('dirac-dms-lfn-metadata %s' % lfn, shell=True)
    try:
        r = eval(stdout)
    except: pass
    if type(r) == dict:
        if r.get('Successful', False) and type(r.get('Successful', False)) == dict:
            return lfn in r['Successful']
    return stdout.find("'Successful': {'%s'" % lfn) >=0

class DiracFile(IOutputFile):
    """
    File stored on a DIRAC storage element
    """
    _schema = Schema(Version(1,1), { 'namePattern'   : SimpleItem(defvalue="",doc='pattern of the file name'),
                                     'localDir'      : SimpleItem(defvalue="",copyable=1,
                                                                  doc='local dir where the file is stored, used from get and put methods'),
                                     'locations'     : SimpleItem(defvalue=[],copyable=1,typelist=['str'],sequence=1,
                                                                  doc="list of SE locations where the outputfiles are uploaded"),
                                     'compressed'    : SimpleItem(defvalue=False,typelist=['bool'],protected=0,
                                                                  doc='wheather the output file should be compressed before sending somewhere'),
                                     'lfn'           : SimpleItem(defvalue='',copyable=1,typelist=['str'],
                                                                  doc='return the logical file name/set the logical file name to use if not '\
                                                                      'using wildcards in namePattern'),
                                     'guid'          : SimpleItem(defvalue='',copyable=1,typelist=['str'],
                                                                  doc='return the GUID/set the GUID to use if not using wildcards in the namePattern.'),
                                     'subfiles'      : ComponentItem(category='outputfiles',defvalue=[], hidden=1, sequence=1, copyable=0,
                                                                     typelist=['GangaDirac.Lib.Files.DiracFile'],
                                                                     doc="collected files from the wildcard namePattern"),
                                     'failureReason' : SimpleItem(defvalue="", copyable=1, doc='reason for the upload failure')
                                     })

    _env=None

    _category = 'outputfiles'
    _name = "DiracFile"
    _exportmethods = [  "get", "getMetadata", 'remove', "replicate", 'put']
        
    def __init__(self, namePattern='', localDir='', lfn='', **kwds):
        """ name is the name of the output file that has to be written ...
        """
        super(DiracFile, self).__init__()
        self.namePattern = namePattern
        self.lfn         = lfn
        self.localDir    = localDir
        self.locations   = []
##COULD MAKE os.getcwd THE DEFAULT LOCALDIR
    def __construct__(self,args):
        if   len(args) == 1 and type(args[0]) == type(''):
            self.namePattern = args[0]
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self.localDir    = args[1]
        elif len(args) == 3 and type(args[0]) == type('') and type(args[1]) == type('') and type(args[2]) == type(''):
            self.namePattern = args[0]
            self.localDir    = args[1]
            self.lfn         = args[2]

    def _attribute_filter__set__(self,name, value):
        if name == 'lfn':
            self.namePattern = os.path.basename(value)
        if name == 'localDir':
            return expandfilename(value)
        return value

    def _on_attribute__set__(self, obj_type, attrib_name):
        r = copy.deepcopy(self)
        if isinstance(obj_type, Job) and attrib_name == 'outputfiles':
            r.lfn=''
            r.guid=''
            r.locations=[]
            r.localDir=''
            r.failureReason=''
        return r

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

            if pattern == name:
                logger.error("Failed to parse outputfile data for file '%s'" % name)
                return True
            if pattern == dirac_file.namePattern:
                d=DiracFile(namePattern=name)
                d.compressed = dirac_file.compressed
                dirac_file.subfiles.append(GPIProxyObjectFactory(d))
                dirac_line_processor(line, d)
            elif name == dirac_file.namePattern:
                if lfn == '###FAILED###':
                    dirac_file.failureReason = tokens[2]
                    logger.error("Failed to upload file '%s' to Dirac: %s" % (name, dirac_file.failureReason))
                    return True
                dirac_file.lfn       = lfn
                dirac_file.locations = tokens[2]
                dirac_file.guid      = tokens[3]
            else:
                return False
#                logger.error("Could't decipher the outputfiles location entry! %s" % line.strip())
#                logger.error("Neither '%s' nor '%s' match the namePattern attribute of '%s'" % (pattern, name, dirac_file.namePattern))
#                dirac_file.failureReason = "Could't decipher the outputfiles location entry!"
            return True
  
            
        job = self.getJobObject()
        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            #logger.warning("Couldn\'t locate the output locations file so couldn't determine the lfn info") ##seems to be called twice (only on Dirac backend... must check) so misleading when second one works??
            return

        postprocesslocations = open(postprocessLocationsPath, 'r')
        
        for line in postprocesslocations.readlines():
            if line.startswith('DiracFile'):
                 if dirac_line_processor(line, self) and regex.search(self.namePattern) is None:
                     break
                        
        postprocesslocations.close()

    def _getEnv(self):
        if not self._env:
            self._env=getDiracEnv()
        return self._env

    def _auto_remove(self):
        """
        Remove called when job is removed as long as config option allows
        """
        if self.lfn!='':
            dirac_ganga_server.execute_nonblocking('dirac-dms-remove-lfn %s' % self.lfn, shell=True, priority = 6)

    def remove(self):
        """
        Remove this lfn and all replicas from DIRAC LFC/SEs
        """
        if self.lfn == "":
            raise GangaException('Can\'t remove a  file from DIRAC SE without an LFN.')
        logger.info('Removing file %s' % self.lfn)
        rc, stdout, stderr = runcmd('dirac-dms-remove-lfn %s' % self.lfn, env=self._getEnv())
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

        # NEW WAY TO GET LOCATIONS
        #def SE_getter(element):
        #    return element.split(':',1)[0].strip()   
        #map(SE_getter, runcmd('dirac-dms-lfn-replicas %s' % self.lfn, env=self._getEnv()).stdout.splitlines()[2:])
        def inTokens(element):
            return element in configDirac['DiracSpaceTokens']

        ret  =  eval(runcmd('dirac-dms-lfn-metadata %s' % self.lfn, env=self._getEnv()).stdout)
        reps =  filter(inTokens, runcmd('dirac-dms-lfn-replicas %s' % self.lfn, env=self._getEnv()).stdout.split(' '))
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

        logger.info("Getting file %s" % self.lfn)
        rc, stdout, stderr=runcmd('dirac-dms-get-file %s' % self.lfn, env=self._getEnv(), cwd=to_location)
        if stdout.find("'Successful': {'%s'" % self.lfn) >=0:
            if self.namePattern=="":
                name = os.path.basename(self.lfn)
                if self.compressed:
                    name = name[:-3]
                self.namePattern = name
        
            if self.guid =="" or not self.locations:
                self.getMetadata()
            return
        logger.error("Error in getting file '%s' : %s" % (self.lfn, stdout))
        return stdout

    def replicate(self, destSE):
        """
        Replicate this file from self.locations[0] to destSE
        """
        if not self.locations:
            raise GangaException('Can\'t replicate a file if it isn\'t already on a DIRAC SE, upload it first')
        if self.lfn == '':
            raise GangaException('Must supply an lfn to replicate')

        logger.info("Replicating file %s to %s" % (self.lfn, destSE))
        rc, stdout, stderr = runcmd('dirac-dms-replicate-lfn %s %s %s' % (self.lfn, destSE, self.locations[0]),
                                          env=self._getEnv())
        if stdout.find("'Successful': {'%s'" % self.lfn) >=0:
            # if 'Successful' in eval(stdout) and self.lfn in eval(stdout)['Successful']:
            self.locations.append(destSE)
            return
        logger.error("Error in replicating file '%s' : %s" % (self.lfn, stdout))
        return stdout
         

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
        if self._parent != None and os.path.isdir(self.getJobObject().outputdir): # attached to a job, use the joboutputdir
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
       
        outputFiles=GangaList()
        for file in glob.glob(os.path.join(sourceDir, self.namePattern)):
            name = file
            if self.compressed:
                os.system('gzip -c %s > %s.gz' % (name,name))
                name+='.gz'
            if not os.path.exists(name):
                raise GangaException('File "%s" must exist!'% os.path.join(sourceDir, name))
            lfn = self.lfn
            guid = self.guid
            if lfn == "":
                lfn = os.path.join(lfn_base, os.path.basename(name))
            if guid == "":
                md5 = hashlib.md5(lfn).hexdigest()
                guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()# conforming to DIRAC GUID hex md5 8-4-4-4-12
            
            d=DiracFile()
            d.namePattern = os.path.basename(file)
            d.compressed  = self.compressed
            d.localDir    = sourceDir
            stderr=''
            stdout=''
            logger.info('Uploading file %s' % name)
            for se in storage_elements:
                rc, stdout, stderr = runcmd('dirac-dms-add-file %s %s %s %s' %(lfn, name, se, guid), env=self._getEnv())
                if stdout.find("'Successful': {'%s'" % lfn) >=0:
                    if self.compressed: os.system('rm -f %s'% name)
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
                if self.compressed: os.system('rm -f %s'% name)
                failureReason = "Error in uploading file %s : %s"% (os.path.basename(name), stdout)
                logger.error(failureReason)
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

        def wildcard_script(namePattern, lfnBase, compressed, wildCard):
            return """
###INDENT###for f in glob.glob('###NAME_PATTERN###'):
###INDENT###    label = f
###INDENT###    if ###COMPRESSED###: label=label[:-3]
###INDENT###    wildcard_lfn = os.path.join('###LFN_BASE###', os.path.basename(f))
###INDENT###    md5 = hashlib.md5(wildcard_lfn).hexdigest()
###INDENT###    wildcard_guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()
###INDENT###    uploadFile(os.path.basename(f), wildcard_lfn, wildcard_guid, storage_elements, label, '###WILD_CARD###')
""".replace('###NAME_PATTERN###', namePattern).replace('###LFN_BASE###', lfnBase).replace('###COMPRESSED###', compressed).replace('###WILD_CARD###', wildCard)

        script = """
###INDENT###import os
###INDENT###dirac_env_setup = ###DIRAC_ENV###
###INDENT###def run_command(cmd, env=None):
###INDENT###    import os, subprocess        
###INDENT###    pipe = subprocess.Popen(cmd,
###INDENT###                            shell=True,
###INDENT###                            env=env,
###INDENT###                            cwd=os.getcwd(),
###INDENT###                            stdout=subprocess.PIPE,
###INDENT###                            stderr=subprocess.PIPE)
###INDENT###    stdout, stderr = pipe.communicate()
###INDENT###    return pipe.returncode, stdout, stderr
###INDENT###
###INDENT###def upload_ok(lfn):
###INDENT###    import datetime
###INDENT###    retcode, stdout, stderr = run_command('dirac-dms-lfn-metadata %s' % lfn, dirac_env_setup)
###INDENT###    try:
###INDENT###        r = eval(stdout)
###INDENT###    except: pass
###INDENT###    if type(r) == dict:
###INDENT###        if r.get('Successful', False) and type(r.get('Successful', False)) == dict:
###INDENT###            return lfn in r['Successful']
###INDENT###    return stdout.find("'Successful': {'%s'" % lfn) >=0
###INDENT###            
###INDENT###def uploadFile(file, lfn, guid, SEs, file_label, wildcard=''):
###INDENT###    import os, datetime
###INDENT###    if not os.path.exists(os.path.join(os.getcwd(),file)):
###INDENT###        ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' didn't exist:::NotAvailable\\n" % (wildcard, file_label, file))
###INDENT###        return
###INDENT###    stdout=''
###INDENT###    stderr=''
###INDENT###    errmsg=''
###INDENT###    for se in SEs:
###INDENT###        try:
###INDENT###            retcode, stdout, stderr = run_command('###ADD_COMMAND### %s %s %s %s' % (lfn, file, se, guid), dirac_env_setup)
###INDENT###        except Exception,x:
###INDENT###            ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::Exception running command '%s' - %s:::NotAvailable\\n" % (wildcard, file_label,'dirac-dms-add-file %s %s %s %s' % (lfn, file, se, guid),x))
###INDENT###            return
###INDENT###        if upload_ok(lfn):
###INDENT###            ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->%s:::%s:::%s\\n" % (wildcard, file_label, lfn, se, guid))
###INDENT###            return
###INDENT###        errmsg+="(%s,%s,%s)" % (se, stdout, stderr)
###INDENT###    ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' could not be uploaded to any SE (%s):::NotAvailable\\n" % (wildcard, file_label, file, errmsg))
###INDENT###
###INDENT###storage_elements = ###STORAGE_ELEMENTS###
###INDENT###import os, glob, hashlib
"""

        import uuid
        lfn_base = os.path.join(configDirac['DiracLFNBase'], str(uuid.uuid4()))
        for file in outputFiles:
            name = file.namePattern
            if file.namePattern in patternsToZip:
                name+='.gz'
            if regex.search(file.namePattern) is not None:
                script+= wildcard_script(name, lfn_base, str(file.namePattern in patternsToZip), file.namePattern)
            else:
                lfn = file.lfn
                guid = file.guid
                if file.lfn=='':
                    lfn = os.path.join(lfn_base, name)
                if file.guid == '':
                    md5 = hashlib.md5(lfn).hexdigest()
                    guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()

                script+='###INDENT###uploadFile("%s", "%s", "%s", storage_elements, "%s")\n' % (name, lfn, guid, file.namePattern)

        script = script.replace('###STORAGE_ELEMENTS###', str(configDirac['DiracSpaceTokens']))
        script = script.replace('###INDENT###',           indent)
        script = script.replace('###LOCATIONSFILE###',    postProcessLocationsFP)
        if self._parent and self._parent.backend._name=='Dirac':
            script = script.replace('###DIRAC_ENV###', 'os.environ')
            script = script.replace('###ADD_COMMAND###', 'dirac-dms-add-files') # necessary until we switch to new LHCbDirac
        else:
            script = script.replace('###DIRAC_ENV###', str(self._getEnv()))
            script = script.replace('###ADD_COMMAND###', 'dirac-dms-add-file')

#        if self._parent and self._parent.backend._name=='Dirac':
#            script = script.replace('###SETUP###', '')
#        elif self._parent and self._parent.backend._name=='Local' and self._parent.application._name=='Root' and self._parent.application.usepython:
#            # THIS WHOLE IF STATEMENT IS A HORRIBLE HACK AS THE LOCAL ROOT RTHANDLER OVERWRITES THE PYTHONPATH ENV VAR
#            # WITH A REDUCED PATH
#            extra_setup=''
##            if 'LHCBPROJECTPATH' in os.environ:
##                extra_setup+='export LHCBPROJECTPATH=%s && ' % os.environ['LHCBPROJECTPATH']
##            if 'CMTCONFIG' in os.environ:
##                 extra_setup+='export CMTCONFIG=%s && ' % os.environ['CMTCONFIG']
#            if 'PYTHONPATH' in os.environ:
#                 extra_setup+='export PYTHONPATH=%s && ' % os.environ['PYTHONPATH']
#            script = script.replace('###SETUP###', extra_setup + '. SetupProject.sh LHCbDirac &&')#&>/dev/null && ')          
#        else:
#            script = script.replace('###SETUP###', '. SetupProject.sh LHCbDirac &>/dev/null && ')

        return script

# add DiracFile objects to the configuration scope (i.e. it will be possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile


