import copy, os, datetime, hashlib, re
from Ganga.GPIDev.Base.Proxy                  import GPIProxyObjectFactory
from Ganga.GPIDev.Lib.GangaList.GangaList     import GangaList
from Ganga.GPIDev.Schema                      import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Lib.File.IGangaFile         import IGangaFile
from Ganga.GPIDev.Lib.Job.Job                 import Job
from Ganga.Core.exceptions                    import GangaException
from Ganga.Utility.files                      import expandfilename
from GangaDirac.Lib.Utilities.DiracUtilities  import getDiracEnv, execute
from Ganga.GPI                                import queues
from Ganga.Utility.Config                     import getConfig
from Ganga.Utility.logging                    import getLogger
configDirac = getConfig('DIRAC')
logger      = getLogger()
regex       = re.compile('[*?\[\]]')

#def upload_ok(lfn):
#    import datetime
#    retcode, stdout, stderr = dirac_ganga_server.execute('dirac-dms-lfn-metadata %s' % lfn, shell=True)
#    try:
#        r = eval(stdout)
#    except: pass
#    if type(r) == dict:
#        if r.get('Successful', False) and type(r.get('Successful', False)) == dict:
#            return lfn in r['Successful']
#    return stdout.find("'Successful': {'%s'" % lfn) >=0

class DiracFile(IGangaFile):
    """
    File stored on a DIRAC storage element
    """
    _schema = Schema(Version(1,1), { 'namePattern'   : SimpleItem(defvalue="", doc='pattern of the file name'),
                                     'localDir'      : SimpleItem(defvalue=None, copyable=1, typelist=['str','type(None)'],
                                                                  doc='local dir where the file is stored, used from get and put methods'),
                                     'locations'     : SimpleItem(defvalue=[], copyable=1, typelist=['str'], sequence=1,
                                                                  doc="list of SE locations where the outputfiles are uploaded"),
                                     'compressed'    : SimpleItem(defvalue=False, typelist=['bool'], protected=0,
                                                                  doc='wheather the output file should be compressed before sending somewhere'),
                                     'lfn'           : SimpleItem(defvalue='', copyable=1, typelist=['str'],
                                                                  doc='return the logical file name/set the logical file name to use if not '\
                                                                      'using wildcards in namePattern'),
                                     'remoteDir'     : SimpleItem(defvalue="", doc='remote directory where the LFN is to be placed within '\
                                                                      'the dirac base directory by the put method.'),
                                     'guid'          : SimpleItem(defvalue='', copyable=1, typelist=['str'],
                                                                  doc='return the GUID/set the GUID to use if not using wildcards in the namePattern.'),
                                     'subfiles'      : ComponentItem(category='gangafiles', defvalue=[], sequence=1, copyable=0,# hidden=1,
                                                                     typelist=['GangaDirac.Lib.Files.DiracFile'],
                                                                     doc="collected files from the wildcard namePattern"),
                                     'failureReason' : SimpleItem(defvalue="", protected=1, copyable=0, doc='reason for the upload failure')
                                     })

    _env=None

    _category = 'gangafiles'
    _name = "DiracFile"
    _exportmethods = [  "get", "getMetadata", "getReplicas", 'remove', "replicate", 'put']
        
    def __init__(self, namePattern='', localDir=None, lfn='', remoteDir=None, **kwds):
        """
        name is the name of the output file that has to be written ...
        """

        #logger.debug( "DiracFile" )

        super(DiracFile, self).__init__( **kwds )
        self.locations   = []

        if str(namePattern).upper()[0:4] == "LFN:" and lfn=='':
            self._setLFNnamePattern( _lfn = namePattern[4:], _namePattern = '' )
        else:
            self._setLFNnamePattern( _lfn = lfn, _namePattern = namePattern )

        if localDir is not None:
            self.localDir = expandfilename( localDir )
        if remoteDir is not None:
            self.remoteDir = remoteDir

    def __construct__( self, args ):
        logger.debug( "__construct__" )
        if len(args) == 1 and type(args[0]) == type(''):
            if str(str(args[0]).upper()[0:4]) == str("LFN:"):
                self._setLFNnamePattern( _lfn = args[0][4:], _namePattern="" )
            else:
                self._setLFNnamePattern( _lfn="", _namePattern = args[0] )
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self._setLFNnamePattern( _lfn = '', _namePattern = self.namePattern )
            self.localDir    = expandfilename(args[1])
        elif len(args) == 3 and type(args[0]) == type('') and type(args[1]) == type('') and type(args[2]) == type(''):
            self.namePattern = args[0]
            self.lfn         = args[2]
            self._setLFNnamePattern( _lfn = self.lfn, _namePattern = self.namePattern )
            self.localDir    = expandfilename(args[1])
        elif len(args) == 4 and type(args[0]) == type('') and type(args[1]) == type('')\
                and type(args[2]) == type('') and type(args[3]) == type(''):
            self.namePattern = args[0]
            self.lfn         = args[2]
            self._setLFNnamePattern( _lfn = lfn, _namePattern = namePattern )
            self.localDir    = expandfilename(args[1])
            self.remoteDir   = args[3]
        else:
             super( DiracFile, self ).__construct__( args )
        return

    def _attribute_filter__set__(self, name, value):

        #   Do some checking of the filenames in a subprocess
        if name == "lfn":
            self._setLFNnamePattern( _lfn = value, _namePattern = '' )
            return self.lfn
        elif name == 'namePattern':
            self._setLFNnamePattern( _lfn = '', _namePattern = value )
            return self.namePattern

        if name == 'localDir' and type(value) != type(None):
            return expandfilename(value)
        return value

    def _setLFNnamePattern( self, _lfn = "", _namePattern = "" ):

        logger.debug( "_lfn: %s" % _lfn )

        if _lfn != "" and _lfn is not None:
            if len(_lfn) > 3:
                if _lfn[0:4] == "LFN:":
                    _lfn = _lfn[4:]

        import os.path

        if _lfn != "" and _namePattern != "":
            self.lfn = _lfn
            self.remoteDir = os.path.dirname( _lfn )
            self.namePattern = os.path.basename( _namePattern )
            self.localDir = os.path.dirname( expandfilename(_namePattern) )

        if _lfn != "" and _namePattern == "":
            self.lfn = _lfn
            self.remoteDir = os.path.dirname( self.lfn )
            self.namePattern = os.path.basename( self.lfn )
            self.localDir = ""

        if _namePattern != "" and _lfn == "":
            self.namePattern = os.path.basename( _namePattern )
            self.localDir = os.path.dirname( expandfilename(_namePattern) )
            self.remoteDir = ""
            self.lfn = ""

    def _attribute_filter__get__(self, name ):

        # Attempt to spend too long loading un-needed objects into memory in order to read job status
        if name is 'lfn':
            #j = self.getJobObject()
            #if j:
            #    j.backend.getOutputDataLFNs()
            if not self.lfn:
                logger.warning( "Do NOT have an LFN, for file: %s" % self.namePattern )
                logger.warning( "If file exists try first using the method put()" )
            return object.__getattribute__(self, 'lfn')
        elif name is 'guid' or name is 'locations':
            if configDirac[ 'DiracFileAutoGet' ]:
                if self.guid is None or self.guid == '' or self.locations == []:
                    if self.lfn:
                        self.getMetadata()
                        return object.__getattribute__(self, 'guid')

        return object.__getattribute__(self, name )

#    def _on_attribute__set__(self, obj_type, attrib_name):
#        r = copy.deepcopy(self)
        #if isinstance(obj_type, Job) and attrib_name == 'outputfiles':
        #    r.lfn=''
        #    r.guid=''
        #    r.locations=[]
#        #    r.localDir=None
#        #    r.failureReason=''
#        return r

    def __repr__(self):
        """Get the representation of the file."""

        return "DiracFile(namePattern='%s', lfn='%s')" % (self.namePattern, self.lfn)

    def getSubFiles(self):
        """
        Returns the subfiles for this instance
        """
        if self.lfn == '':
            self.setLocation()
        return self.subfiles

    def setLocation(self):
        """
        """

        def dirac_line_processor(line, dirac_file, localPath):
            """
                Function to interperate the post processor lines.
                This returns False when everything went OK and True on an ERROR
            """
            logger.debug( "Calling dirac_line_processor" )
            tokens = line.strip().split(':::')
            logger.debug( "dirac_line_processor: %s" % tokens )
            pattern   = tokens[1].split('->')[0].split('&&')[0]
            name      = tokens[1].split('->')[0].split('&&')[1]
            lfn       = tokens[1].split('->')[1]
            guid      = tokens[3]
            try:
                locations = eval(tokens[2])
            except:
                locations = tokens[2]

            if pattern == name:
                logger.debug( "pattern == name" )
                logger.error("Failed to parse outputfile data for file '%s'" % name)
                return True

            #   This is the case that multiple files were requested
            if pattern == dirac_file.namePattern:
                logger.debug( "pattern == dirac_file.namePattern" )
                d =DiracFile( namePattern=name, lfn=lfn )
                d.compressed = dirac_file.compressed
                d.guid = guid
                d.locations = locations
                d.localDir = localPath
                dirac_file.subfiles.append( d )
                #dirac_line_processor(line, d)
                return False

            #   This is the case that an individual file was requested
            elif name == dirac_file.namePattern:
                logger.debug( "name == dirac_file.namePattern" )
                if lfn == '###FAILED###':
                    dirac_file.failureReason = tokens[2]
                    logger.error("Failed to upload file '%s' to Dirac: %s" % (name, dirac_file.failureReason))
                    return True
                dirac_file.lfn       = lfn
                dirac_file.locations = locations
                dirac_file.guid      = guid
                dirac_file.localDir  = localPath
                return False

            else:
                return False

            #logger.error("Could't decipher the outputfiles location entry! %s" % line.strip())
            #logger.error("Neither '%s' nor '%s' match the namePattern attribute of '%s'" % (pattern, name, dirac_file.namePattern))
            #dirac_file.failureReason = "Could't decipher the outputfiles location entry!"
            #return True
  
             
        job = self.getJobObject()
        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        #if not os.path.exists(postprocessLocationsPath):
        #    #logger.warning("Couldn\'t locate the output locations file so couldn't determine the lfn info") ##seems to be called twice (only on Dirac backend... must check) so misleading when second one works??
        #    return

        try:
            postprocesslocations = open(postprocessLocationsPath, 'r')
            self.subfiles = []
            for line in postprocesslocations.readlines():
                if line.startswith('DiracFile'):
                    if dirac_line_processor(line, self, postprocessLocationsPath) and regex.search(self.namePattern) is None:
                        logger.error( "Error processing line:\n%\nAND: namePattern: %s is NOT matched" % (str(line), str(self.namePattern) ) )
                        break

            postprocesslocations.close()
        except:
            pass

#    def _getEnv(self):
#        if not self._env:
#            self._env=getDiracEnv()
#        return self._env

    def _auto_remove(self):
        """
        Remove called when job is removed as long as config option allows
        """
        if self.lfn!='':
            queues.add( self.remove )

    def remove(self):
        """
        Remove this lfn and all replicas from DIRAC LFC/SEs
        """
        if self.lfn == "":
            raise GangaException('Can\'t remove a  file from DIRAC SE without an LFN.')
        logger.info('Removing file %s' % self.lfn)
        stdout = execute('removeFile("%s")' % self.lfn)
        if isinstance(stdout, dict) and stdout.get('OK', False) and self.lfn in stdout.get('Value', {'Successful': {}})['Successful']:
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
            self._optionallyUploadLocalFile()

        # eval again here as datatime not included in dirac_ganga_server
        r = execute('getMetadata("%s")' % self.lfn)
        try:
            ret  =  eval(r)
        except:
            ret = r
        reps =  self.getReplicas()
        if isinstance(ret,dict) and ret.get('OK', False) and self.lfn in ret.get('Value', {'Successful': {}})['Successful']:
            try:
                if self.guid != ret['Value']['Successful'][self.lfn]['GUID']:
                    self.guid = ret['Value']['Successful'][self.lfn]['GUID']
            except: pass
        if isinstance(reps,dict) and reps.get('OK', False) and self.lfn in reps.get('Value', {'Successful': {}})['Successful']:
            try:
                if self.locations != reps['Value']['Successful'][self.lfn].keys():
                    self.locations = reps['Value']['Successful'][self.lfn].keys()
                    ret['Value']['Successful'][self.lfn].update({'replicas': self.locations})
            except: pass

        return ret

    def _optionallyUploadLocalFile(self):

        if self.lfn != "":
            return

        if self.namePattern != "" and self.lfn == "":

            logger.info( "I have a local DiracFile, however you're requesting it's location on the grid" )
            logger.info( "Shall I upload it to the grid before I continue?" )
            decision = raw_input( '[y] / n:' )
            while not ( decision == 'y' or decision == '' ):
                decision = raw_input( '[y] / n:' )

            if decision == 'y' or decision == '':
                #upload namePattern to grid
                logger.debug( "Uploading the file first" )
                self.put()
            else:
                #do Nothing
                logger.debug( "Continuing without uploading file" )

            if self.lfn == "":
                raise GangaException('Uploading of namePattern: %s failed' % self.namePattern )

        if self.namePattern == "" and self.lfn == "":
            raise GangaException('Cannot do anything if I don\'t have an lfn or a namePattern!')

        return

    def getReplicas(self):

        reps =  execute('getReplicas("%s")' % self.lfn)

        return reps

    def get(self):
        """
        Retrieves locally the file matching this DiracFile object pattern
        """
        to_location = self.localDir
        if self.localDir is None:
            to_location = os.getcwd()
            if self._parent is not None and os.path.isdir(self.getJobObject().outputdir):
                to_location = self.getJobObject().outputdir

        if not os.path.isdir(to_location):
            raise GangaException('"%s" is not a valid directory... Please set the localDir attribute to a valid directory' % self.localDir)

        if self.lfn == "":
            raise GangaException('Can\'t download a file without an LFN.')

        logger.info("Getting file %s" % self.lfn)
        stdout = execute('getFile("%s", destDir="%s")' % (self.lfn, to_location))
        if isinstance(stdout, dict) and stdout.get('OK',False) and self.lfn in stdout.get('Value',{'Successful':{}})['Successful']:
            if self.namePattern=="":
                name = os.path.basename(self.lfn)
                if self.compressed:
                    name = name[:-3]
                self.namePattern = name
        
            if self.guid =="" or not self.locations:
                self.getMetadata()
            return
        logger.error("Error in getting file '%s' : %s" % (self.lfn, str(stdout)))
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
        stdout = execute('replicateFile("%s", "%s", "%s")' % (self.lfn, destSE, self.locations[0]))
        if isinstance(stdout, dict) and stdout.get('OK',False) and self.lfn in stdout.get('Value',{'Successful':{}})['Successful']:
            # if 'Successful' in eval(stdout) and self.lfn in eval(stdout)['Successful']:
            self.locations.append(destSE)
            return
        logger.error("Error in replicating file '%s' : %s" % (self.lfn, stdout))
        return stdout
         
    def processWildcardMatches(self):
        if regex.search(self.namePattern) is not None:
            raise Exception("No wildcards in inputfiles for DiracFile just yet. Dirac are exposing this in API soon.")


    def put(self, force=False ):
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

        if self.lfn != "" and force == False:
            logger.warning( "Warning you're about to 'put' this DiracFile: %s on the grid as it already has an lfn: %s" % (  self.namePattern, self.lfn ) )
            decision = raw_input( 'y / [n]:' )
            while not ( decision == 'y' or decision == '' ):
                decision = raw_input( 'y / [n]:' )

            if decision == 'y' or decision == '':
                pass
            else:
                return

        ## looks like will only need this for the interactive uploading of jobs.
        ## Also if any backend need dirac upload on client then when downloaded
        ## this will upload then delete the file.
        
        if self.namePattern == "":
            if self.lfn != '':
                logger.warning( "'Put'-ing a file with ONLY an existing LFN makes no sense!" )
            raise GangaException('Can\'t upload a file without a local file name.')

        sourceDir = self.localDir
        if self.localDir is None:
            sourceDir = os.getcwd()
            if self._parent != None and os.path.isdir(self.getJobObject().outputdir): # attached to a job, use the joboutputdir
                sourceDir = self.getJobObject().outputdir

        if not os.path.isdir(sourceDir):
            raise GangaException('localDir attribute is not a valid dir, don\'t know from which dir to take the file' )

        if regex.search(self.namePattern) is not None:
            if self.lfn != "":
                logger.warning("Cannot specify a single lfn for a wildcard namePattern")
                logger.warning("LFN will be generated automatically")
                self.lfn=""
#            if self.guid != "":
#                logger.warning("Cannot specify a single guid for a wildcard namePattern")
#                logger.warning("GUID will be generated automatically")
#                self.guid=""
 
        import glob, uuid
        if self.remoteDir == '':
            self.remoteDir = str(uuid.uuid4())
        if self.remoteDir[:4] == 'LFN:':
            lfn_base = self.remoteDir[4:]
        elif self.remoteDir[:5] == "/lhcb":
            lfn_base = self.remoteDir
        else:
            lfn_base = os.path.join( configDirac['DiracLFNBase'], self.remoteDir )
        storage_elements=configDirac['DiracSpaceTokens']

        outputFiles=GangaList()
        for file in glob.glob(os.path.join(sourceDir, self.namePattern)):
            name = file
    
            if not os.path.exists(name):
                if not self.compressed:
                    raise GangaException('Cannot upload file. File "%s" must exist!'% name)
                name+='.gz'
                if not os.path.exists(name):
                    raise GangaException('File "%s" must exist!'% name)
            else:
                if self.compressed:
                    os.system('gzip -c %s > %s.gz' % (name, name))
                    name+='.gz'
                    if not os.path.exists(name):
                        raise GangaException('File "%s" must exist!'% name)

            lfn = self.lfn
#            guid = self.guid
            if lfn == "":
                lfn = os.path.join(lfn_base, os.path.basename(name))
#           if guid == "":
#               md5 = hashlib.md5(open(name,'rb').read()).hexdigest()
#               guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()# conforming to DIRAC GUID hex md5 8-4-4-4-12
            
            d=DiracFile()
            d.namePattern = os.path.basename(file)
            d.compressed  = self.compressed
            d.localDir    = sourceDir
            stderr=''
            stdout=''
            logger.info('Uploading file %s' % name)
#            for se in storage_elements:
#                stdout = dirac_ganga_server.execute('uploadFile("%s", "%s", "%s")' %(lfn, name, se))
            stdout = execute('uploadFile("%s", "%s", %s)' %(lfn, name, str(storage_elements)))
            if type(stdout)==str: 
                logger.warning("Couldn't upload file '%s': %s"%(os.path.basename(name), stdout))##FIX this to run on a process so dont need to interpret the string stdout from process
                continue
            if stdout.get('OK', False) and lfn in stdout.get('Value',{'Successful':{}})['Successful']:
                if self.compressed or self._parent !=None: # when doing the two step upload delete the temp file
                    os.remove(name)
                # need another eval as datetime needs to be included.
                guid = stdout['Value']['Successful'][lfn].get('GUID','')#eval(dirac_ganga_server.execute('getMetadata("%s")'%lfn))['Value']['Successful'][lfn]['GUID']
                if regex.search(self.namePattern) is not None:
                    d.lfn = lfn
                    d.locations = stdout['Value']['Successful'][lfn].get('DiracSE','')#[se]
                    d.guid = guid
                    outputFiles.append(GPIProxyObjectFactory(d))
                    continue
                self.lfn = lfn
                self.locations = stdout['Value']['Successful'][lfn].get('DiracSE','')#[se]
                self.guid = guid
                return
            failureReason = "Error in uploading file %s : %s"% (os.path.basename(name), str(stdout))
            logger.error(failureReason)
            if regex.search(self.namePattern) is not None:
                d.failureReason =  failureReason
                outputFiles.append(GPIProxyObjectFactory(d))
                continue
            self.failureReason = failureReason
            return str(stdout)
        return GPIProxyObjectFactory(outputFiles)

    def getWNScriptDownloadCommand(self, indent):

        script = """\n

###INDENT###upload_script='''
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
dirac=Dirac()
import os
dirac.getFile('###LFN###', os.getcwd())
'''

###INDENT###import subprocess
###INDENT###dirac_env=###DIRAC_ENV###
###INDENT###subprocess.Popen('''python -c "import sys\nexec(sys.stdin.read())"''', shell=True, env=dirac_env, stdin=subprocess.PIPE).communicate(upload_script)
"""
        script = script.replace('###DIRAC_ENV###',"dict((tuple(line.strip().split('=',1)) for line in open('%s','r').readlines() if len(line.strip().split('=',1))==2))"%configDirac['DiracEnvFile'])

        script = script.replace('###INDENT###', indent)
        script = script.replace('###LFN###', self.lfn)
        return script 

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        def wildcard_script(namePattern, lfnBase, compressed):
            return """
###INDENT###import glob, hashlib
###INDENT###for f in glob.glob('###NAME_PATTERN###'):
###INDENT###    processes.append(uploadFile(os.path.basename(f), '###LFN_BASE###', ###COMPRESSED###, '###NAME_PATTERN###'))
""".replace('###NAME_PATTERN###', namePattern).replace('###LFN_BASE###', lfnBase).replace('###COMPRESSED###', str(compressed))

        script = """
###INDENT###import os, sys
###INDENT###dirac_env=###DIRAC_ENV###
###INDENT###processes=[]    
###INDENT###storage_elements = ###STORAGE_ELEMENTS###
###INDENT###           
###INDENT###def uploadFile(file_name, lfn_base, compress=False, wildcard=''):
###INDENT###    import sys, os, datetime, subprocess
###INDENT###    if not os.path.exists(os.path.join(os.getcwd(),file_name)):
###INDENT###        ###LOCATIONSFILE###.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' didn't exist:::NotAvailable\\n" % (wildcard, file_label, file_name))
###INDENT###        return
###INDENT###   
###INDENT###    upload_script='''
import os
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
dirac=Dirac()
errmsg=''
file_name='###UPLOAD_FILE###'
file_label=file_name
compressed = ###COMPRESSED###
if compressed:
    import gzip
    file_name += '.gz'
    f_in = open(file_label, 'rb')
    f_out = gzip.open(file_name, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
lfn= os.path.join('###LFN_BASE###', file_name)
wildcard='###WILDCARD###'
storage_elements=###SEs###

with open('###LOCATIONSFILE_NAME###','ab') as locationsfile:
    for se in storage_elements:
        try:
            result = dirac.addFile(lfn, file_name, se)
        except Exception,x:
            print 'Exception running dirac.addFile command:',x
            break
        if result.get('OK',False) and lfn in result.get('Value',{'Successful':{}})['Successful']:
            import datetime
            guid = dirac.getMetadata(lfn)['Value']['Successful'][lfn]['GUID']
            locationsfile.write("DiracFile:::%s&&%s->%s:::['%s']:::%s\\\\n" % (wildcard, file_label, lfn, se, guid))
            break
        errmsg+="(%s, %s), " % (se, result)
    else:
        locationsfile.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' could not be uploaded to any SE (%s):::NotAvailable\\\\n" % (wildcard, file_label, file_name, errmsg))
        print 'Could not upload file %s' % file_name
'''
###INDENT###    upload_script = upload_script.replace('###UPLOAD_FILE###',        file_name)
###INDENT###    upload_script = upload_script.replace('###LFN_BASE###',           lfn_base)
###INDENT###    upload_script = upload_script.replace('###COMPRESSED###',         str(compress))
###INDENT###    upload_script = upload_script.replace('###WILDCARD###',           wildcard)
###INDENT###    upload_script = upload_script.replace('###SEs###',                str(storage_elements))
###INDENT###    upload_script = upload_script.replace('###LOCATIONSFILE_NAME###', ###LOCATIONSFILE###.name)
###INDENT###
###INDENT###    inread, inwrite = os.pipe()
###INDENT###    p = subprocess.Popen('''python -c "import os\\nos.close(%i)\\nexec(os.fdopen(%s,'rb'))"'''%(inwrite,inread), shell=True, env=dirac_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
###INDENT###    os.close(inread)
###INDENT###    with os.fdopen(inwrite, 'wb') as instream:
###INDENT###        instream.write(upload_script)
###INDENT###    return p
"""

        import uuid

        if self.remoteDir == '':
            self.remoteDir = str(uuid.uuid4())
        if self.remoteDir[:4] == 'LFN:':
            lfn_base = self.remoteDir
        else:
            lfn_base = os.path.join(configDirac['DiracLFNBase'], self.remoteDir )

        for file in outputFiles:
            if regex.search(file.namePattern) is not None:
                script+= wildcard_script(file.namePattern, lfn_base, str(file.namePattern in patternsToZip))
            else:
#                lfn = file.lfn
                #guid = file.guid
#                if file.lfn=='':
#                    lfn = os.path.join(lfn_base, name)
                #if file.guid == '':
                #    md5 = hashlib.md5(open(name,'rb').read()).hexdigest()
                #    guid = (md5[:8]+'-'+md5[8:12]+'-'+md5[12:16]+'-'+md5[16:20]+'-'+md5[20:]).upper()

                script+='###INDENT###processes.append(uploadFile("%s", "%s", %s))\n' % (file.namePattern, lfn_base, str(file.namePattern in patternsToZip))

        if self._parent and self._parent.backend._name!='Dirac':
            script = script.replace('###DIRAC_ENV###',"dict((tuple(line.strip().split('=',1)) for line in open('%s','r').readlines() if len(line.strip().split('=',1))==2))"%configDirac['DiracEnvFile'])
        else:
            script = script.replace('###DIRAC_ENV###',str(None))

        script +="""
###INDENT###for p in processes:
###INDENT###    #print p.communicate()
###INDENT###    output = p.communicate()[0]
###INDENT###    if output != '':
###INDENT###        print output
"""
        script = script.replace('###STORAGE_ELEMENTS###', str(configDirac['DiracSpaceTokens']))
        script = script.replace('###INDENT###',           indent)
        script = script.replace('###LOCATIONSFILE###',    postProcessLocationsFP)

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

