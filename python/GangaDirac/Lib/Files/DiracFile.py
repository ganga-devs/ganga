import copy
import os
import datetime
import inspect
import hashlib
import re
import os.path
import random
import glob
from Ganga.GPIDev.Base.Proxy import stripProxy, GPIProxyObjectFactory, isType, getName
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
from Ganga.GPIDev.Lib.Job.Job import Job
from Ganga.Core.exceptions import GangaException
from Ganga.Utility.files import expandfilename
from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv, execute
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
config = getConfig('Configuration')
configDirac = getConfig('DIRAC')
logger = getLogger()
regex = re.compile('[*?\[\]]')

global stored_list_of_sites
stored_list_of_sites = []


class DiracFile(IGangaFile):

    """
    File stored on a DIRAC storage element
    """
    _schema = Schema(Version(1, 1), {'namePattern': SimpleItem(defvalue="", doc='pattern of the file name'),
                                     'localDir': SimpleItem(defvalue=None, copyable=1, typelist=['str', 'type(None)'],
                                                            doc='local dir where the file is stored, used from get and put methods'),
                                     'locations': SimpleItem(defvalue=[], copyable=1, typelist=['str'], sequence=1,
                                                             doc="list of SE locations where the outputfiles are uploaded"),
                                     'compressed': SimpleItem(defvalue=False, typelist=['bool'], protected=0,
                                                              doc='wheather the output file should be compressed before sending somewhere'),
                                     'lfn': SimpleItem(defvalue='', copyable=1, typelist=['str'],
                                                       doc='return the logical file name/set the logical file name to use if not '
                                                       'using wildcards in namePattern'),
                                     'remoteDir': SimpleItem(defvalue="", doc='remote directory where the LFN is to be placed within '
                                                             'the dirac base directory by the put method.'),
                                     'guid': SimpleItem(defvalue='', copyable=1, typelist=['str'],
                                                        doc='return the GUID/set the GUID to use if not using wildcards in the namePattern.'),
                                     'subfiles': ComponentItem(category='gangafiles', defvalue=[], sequence=1, copyable=0,  # hidden=1,
                                                               typelist=['GangaDirac.Lib.Files.DiracFile'], doc="collected files from the wildcard namePattern"),
                                     'defaultSE': SimpleItem(defvalue='', copyable=1, doc="defaultSE where the file is to be accessed from or uploaded to"),
                                     'failureReason': SimpleItem(defvalue="", protected=1, copyable=0, doc='reason for the upload failure'),
    })

    _env = None

    _category = 'gangafiles'
    _name = "DiracFile"
    _exportmethods = ["get", "getMetadata", "getReplicas", 'getSubFiles', 'remove',
                      "replicate", 'put', 'locations', 'location', 'accessURL',
                      '_updateRemoteURLs', 'hasMatchedFiles']
    _remoteURLs = {}
    _storedReplicas = {}
    _have_copied = False

    def __init__(self, namePattern='', localDir=None, lfn='', remoteDir=None, **kwds):
        """
        name is the name of the output file that has to be written ...
        """

        super(DiracFile, self).__init__()
        self.locations = []

        self._setLFNnamePattern(lfn, namePattern)

        if localDir is not None:
            self.localDir = localDir
        if remoteDir is not None:
            self.remoteDir = remoteDir

    def __setattr__(self, attr, value):
        """
        This is an overloaded setter method to make sure that we're auto-expanding the filenames of files which exist.
        In the case we're assigning any other attributes the value is simply passed through
        Args:
            attr (str): This is the name of the attribute which we're assigning
            value (unknown): This is the value being assigned.
        """
        actual_value = value
        if attr == "namePattern":
            actual_value = os.path.basename(value)
            this_dir = os.path.dirname(value)
            if this_dir:
                super(DiracFile, self).__setattr__('localDir', this_dir)
        elif attr == 'localDir':
            if value:
                new_value = os.path.abspath(expandfilename(value))
                if os.path.exists(new_value):
                    actual_value = new_value
        elif attr == 'lfn':
            if value:
                this_name = os.path.basename(value)
                super(DiracFile, self).__setattr__('namePattern', this_name)
                super(DiracFile, self).__setattr__('remoteDir', os.path.dirname(value))
        elif attr == 'remoteDir':
            if value:
                this_lfn = os.path.join(value, self.namePattern)
                super(DiracFile, self).__setattr__('lfn', this_lfn)

        super(DiracFile, self).__setattr__(attr, actual_value)

    def _attribute_filter__set__(self, name, value):

        if value != "" and value is not None:
            #   Do some checking of the filenames in a subprocess
            if name == 'lfn':
                self.namePattern = os.path.basename(value)
                self.remoteDir = os.path.dirname(value)
                return value

            elif name == 'namePattern':
                self.localDir = os.path.dirname(value)
                return os.path.basename(value)

            elif name == 'localDir':
                return expandfilename(value)

        return value

    def locations(self):

        return self.locations

    def _setLFNnamePattern(self, lfn="", namePattern=""):

        if self.defaultSE != "":
            ## TODO REPLACE THIS WITH IN LIST OF VONAMES KNOWN
            # Check for /lhcb/some/path or /gridpp/some/path
            if namePattern.split(os.pathsep)[0] == self.defaultSE \
                or (len(namePattern) > 3 and namePattern[0:4].upper() == "LFN:"\
                    or len(namePattern.split(os.pathsep)) > 1 and namePattern.split(os.pathsep)[1] == self.defaultSE):
                # Check for LFN:/gridpp/some/path or others...
                lfn = namePattern
                namePattern = ""

        if lfn:
            if len(lfn) > 3 and lfn[0:4].upper() == "LFN:":
                lfn = lfn[4:]
        elif namePattern:
            if len(namePattern) > 3 and namePattern[0:4].upper() == 'LFN:':
                lfn = namePattern[4:]

        if lfn != "" and namePattern != "":
            self.lfn = lfn
            self.namePattern = namePattern

        elif lfn != "" and namePattern == "":
            self.lfn = lfn

        elif namePattern != "" and lfn == "":
            self.namePattern = namePattern

    def _attribute_filter__get__(self, name):

        # Attempt to spend too long loading un-needed objects into memory in
        # order to read job status
        if name is 'lfn':
            if self.lfn == "":
                logger.warning("Do NOT have an LFN, for file: %s" % self.namePattern)
                logger.warning("If file exists locally try first using the method put()")
            return object.__getattribute__(self, 'lfn')
        elif name in ['guid', 'locations']:
            if configDirac['DiracFileAutoGet']:
                if name is 'guid':
                    if self.guid is None or self.guid == '':
                        if self.lfn != "":
                            self.getMetadata()
                            return object.__getattribute__(self, 'guid')
                if name is 'locations':
                    if self.locations == []:
                        if self.lfn != "":
                            self.getMetadata()
                            return object.__getattribute__(self, 'locations')

        return object.__getattribute__(self, name)

    def __repr__(self):
        """Get the representation of the file."""

        return "DiracFile(namePattern='%s', lfn='%s', localDir='%s')" % (self.namePattern, self.lfn, self.localDir)

    def getSubFiles(self):
        """
        Returns the subfiles for this instance
        """
        if self.lfn == '':
            self.setLocation()
        return self.subfiles

    def dirac_line_processor(self, line, dirac_file, localPath):
        """
            Function to interperate the post processor lines.
            This returns False when everything went OK and True on an ERROR
        """
        logger.debug("Calling dirac_line_processor")
        tokens = line.strip().split(':::')
        logger.debug("dirac_line_processor: %s" % tokens)
        pattern = tokens[1].split('->')[0].split('&&')[0]
        name = tokens[1].split('->')[0].split('&&')[1]
        lfn = tokens[1].split('->')[1]
        guid = tokens[3]
        try:
            locations = eval(tokens[2])
        except Exception as err:
            logger.debug("line_process err: %s" % err)
            locations = tokens[2]

        if pattern == name:
            logger.debug("pattern == name")
            logger.error("Failed to parse outputfile data for file '%s'" % name)
            return True

        #   This is the case that multiple files were requested
        if pattern == dirac_file.namePattern:
            logger.debug("pattern == dirac_file.namePattern")
            d = DiracFile(namePattern=name, lfn=lfn)
            d.compressed = dirac_file.compressed
            d.guid = guid
            d.locations = locations
            d.localDir = localPath
            dirac_file.subfiles.append(d)
            #dirac_line_processor(line, d)
            return False

        #   This is the case that an individual file was requested
        elif name == dirac_file.namePattern:
            logger.debug("name == dirac_file.namePattern")
            if lfn == '###FAILED###':
                dirac_file.failureReason = tokens[2]
                logger.error("Failed to upload file '%s' to Dirac: %s" % (name, dirac_file.failureReason))
                return True
            dirac_file.lfn = lfn
            dirac_file.locations = locations
            dirac_file.guid = guid
            dirac_file.localDir = localPath
            return False

        else:
            logger.debug("False")
            return False

    def setLocation(self):
        """
        """

        logger.debug("DiracFile: setLocation")

        if not stripProxy(self).getJobObject():
            logger.error("No job assocaited with DiracFile: %s" % str(self))
            return

        job = self.getJobObject()
        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])

        postprocesslocations = None

        try:
            postprocesslocations = open(postprocessLocationsPath, 'r')
            self.subfiles = []
            ## NB remember only do this once at it leaves the 'cursor' at the end of the file - rcurrie
            all_lines = postprocesslocations.readlines()
            logger.debug("lines:\n%s" % all_lines)
            for line in all_lines:
                logger.debug("This line: %s" % line)
                if line.startswith('DiracFile'):
                    if self.dirac_line_processor(line, self, os.path.dirname(postprocessLocationsPath)) and regex.search(self.namePattern) is None:
                        logger.error("Error processing line:\n%s\nAND: namePattern: %s is NOT matched" % (str(line), str(self.namePattern)))
                    else:
                        logger.debug("Parsed the Line")
                else:
                    logger.debug("Skipping the Line")

        except Exception as err:
            logger.warning("unexpected Error: %s" % str(err))
        finally:
            if postprocesslocations is not None:
                postprocesslocations.close()

    def _auto_remove(self):
        """
        Remove called when job is removed as long as config option allows
        """
        if self.lfn != '':
            self.remove()

    def remove(self):
        """
        Remove this lfn and all replicas from DIRAC LFC/SEs
        """
        if self.lfn == "":
            raise GangaException('Can\'t remove a  file from DIRAC SE without an LFN.')
        logger.info('Removing file %s' % self.lfn)
        stdout = execute('removeFile("%s")' % self.lfn)
        if isinstance(stdout, dict) and stdout.get('OK', False) and self.lfn in stdout.get('Value', {'Successful': {}})['Successful']:
            self.lfn = ""
            self.locations = []
            self.guid = ''
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
            ret = eval(r)
        except:
            ret = r
        if isinstance(ret, dict) and ret.get('OK', False) and self.lfn in ret.get('Value', {'Successful': {}})['Successful']:
            try:
                if self.guid != ret['Value']['Successful'][self.lfn]['GUID']:
                    self.guid = ret['Value']['Successful'][self.lfn]['GUID']
            except:
                pass

        try:
            reps = self.getReplicas()
            ret['Value']['Successful'][self.lfn].update({'replicas': self.locations})
        except:
            pass

        return ret

    def _optionallyUploadLocalFile(self):
        """
        """

        if self.lfn != "":
            return

        if self.namePattern != "" and self.lfn == "":

            logger.info("I have a local DiracFile, however you're requesting it's location on the grid")
            logger.info("Shall I upload it to the grid before I continue?")
            decision = raw_input('[y] / n:')
            while not (decision.lower() in ['y', 'n'] or decision.lower() == ''):
                decision = raw_input('[y] / n:')

            if decision.lower() in ['y', '']:
                # upload namePattern to grid
                logger.debug("Uploading the file first")
                self.put()
            elif decision == 'n':
                logger.debug("Not uploading now")
                return
            else:
                # do Nothing
                logger.debug("Continuing without uploading file")

            if self.lfn == "":
                raise GangaException('Uploading of namePattern: %s failed' % self.namePattern)

        if self.namePattern == "" and self.lfn == "":
            raise GangaException('Cannot do anything if I don\'t have an lfn or a namePattern!')

        return

    def getReplicas(self, forceRefresh=False):
        """
        Get the list of all SE where this file has a replica
        This relies on an internally stored list of replicas, (SE and  unless forceRefresh = True
        """

        if self.lfn == '':
            self._optionallyUploadLocalFile()
        if self.lfn == '':
            raise GangaException("Can't find replicas for file which has no LFN!")

        these_replicas = None

        if len(self.subfiles) != 0:

            allReplicas = []
            for i in self.subfiles:
                allReplicas.append(i.getReplicas())

            these_replicas = allReplicas

        else:
            # deep copy just before wer change it incase we're pointing to the
            # data stored in original from a copy
            if self._have_copied:
                self._storedReplicas = copy.deepcopy(self._storedReplicas)
            if (self._storedReplicas == {} and len(self.subfiles) == 0) or forceRefresh:

                self._storedReplicas = execute('getReplicas("%s")' % self.lfn)
                if self._storedReplicas.get('OK', False) is True:
                    try:
                        self._storedReplicas = self._storedReplicas['Value']['Successful']
                    except Exception as err:
                        logger.error("Unknown Error: %s" % str(err))
                        raise err
                else:
                    logger.error("Couldn't find replicas for: %s" % str(self.lfn))
                    raise GangaException("Couldn't find replicas for: %s" % str(self.lfn))
                logger.debug("getReplicas: %s" % str(self._storedReplicas))

                if self.lfn in self._storedReplicas:
                    self._updateRemoteURLs(self._storedReplicas)

                    these_replicas = [self._storedReplicas[self.lfn]]
                else:
                    these_replicas = {}
            elif self._storedReplicas != {}:
                these_replicas = [self._storedReplicas[self.lfn]]

        return these_replicas

    def _updateRemoteURLs(self, reps):
        """
        Internal function used for storing all replica information about this LFN at different sites
        """
        if len(self.subfiles) != 0:
            for i in self.subfiles:
                i._updateRemoteURLs(reps)
        else:
            if self.lfn not in reps:
                return
            if self.locations != reps[self.lfn].keys():
                self.locations = reps[self.lfn].keys()
            #logger.debug( "locations: %s" % str( self.locations ) )
            # deep copy just before wer change it incase we're pointing to the
            # data stored in original from a copy
            if self._have_copied:
                self._remoteURLs = copy.deepcopy(self._remoteURLs)
            for site in self.locations:
                #logger.debug( "site: %s" % str( site ) )
                self._remoteURLs[site] = reps[self.lfn][site]
                #logger.debug("Adding _remoteURLs[site]: %s" % str(self._remoteURLs[site]))

    def location(self):
        """
        Return a list of LFN locations for this DiracFile
        """
        if len(self.subfiles) == 0:
            if self.lfn == "":
                self._optionallyUploadLocalFile()
            else:
                return [self.lfn]
        else:
            # 1 LFN per DiracFile
            LFNS = []
            for this_file in self.subfiles:
                these_LFNs = this_file.location()
                for this_url in these_LFNs:
                    LFNs.append(this_url)
            return LFNs

    def accessURL(self, thisSE=''):
        """
        Attempt to find an accessURL which corresponds to the specified SE. If no SE is specified then
        return a random one from all the replicas. 
        """
        _accessURLs = []
        if len(self.subfiles) == 0:
          self.getReplicas()
          # If the SE isn't specified return a random choice.
          if thisSE == '':
            this_SE = random.choice(self.locations)
          # If the SE is specified and we got a URL for a replica there, use it.
          elif thisSE in self.locations:
            this_SE = thisSE
          # If the specified SE doesn't have a replica then return another one at random.
          else:
             logger.warning('No replica at specified SE for the LFN %s, here is a URL for another replica' % self.lfn)
             this_SE = random.choice(self.locations) 
          myurl = execute('getAccessURL("%s" , "%s")' % (self.lfn, this_SE))
          this_accessURL = myurl['Value']['Successful'][self.lfn]
          _accessURLs.append(this_accessURL)
        else:
          # For all subfiles request the accessURL, 1 URL per LFN
          for i in self.subfiles:
            _accessURLs.append(i.accessURL(thisSE)[0])
        return _accessURLs

    def get(self, localPath=''):
        """
        Retrieves locally the file matching this DiracFile object pattern.
        If localPath is specified
        """
        if localPath == '':
            to_location = self.localDir

            if self.localDir is None:
                #to_location = os.getcwd()
                if self._parent is not None and os.path.isdir(self.getJobObject().outputdir):
                    to_location = self.getJobObject().outputdir
                else:
                    to_location = os.getcwd()
        else:
            to_location = localPath

        self.localDir = to_location

        if not os.path.isdir(to_location):
            raise GangaException(
                '"%s" is not a valid directory... Please set the localDir attribute to a valid directory' % self.localDir)

        if self.lfn == "":
            raise GangaException('Can\'t download a file without an LFN.')

        logger.info("Getting file %s" % self.lfn)
        stdout = execute('getFile("%s", destDir="%s")' % (self.lfn, to_location))
        if isinstance(stdout, dict) and stdout.get('OK', False) and self.lfn in stdout.get('Value', {'Successful': {}})['Successful']:
            if self.namePattern == "":
                name = os.path.basename(self.lfn)
                if self.compressed:
                    name = name[:-3]
                self.namePattern = name

            if self.guid == "" or not self.locations:
                self.getMetadata()
            return
        logger.error("Error in getting file '%s' : %s" % (self.lfn, str(stdout)))
        return stdout

    def replicate(self, destSE, sourceSE=''):
        """
        Replicate an LFN to another SE

        Args:
            destSE (str): the SE to replicate the file to
            sourceSE (str): the se to use as a cource for the file
        """

        if not self.lfn:
            raise GangaException('Must supply an lfn to replicate')

        logger.info("Replicating file %s to %s" % (self.lfn, destSE))
        stdout = execute('replicateFile("%s", "%s", "%s")' % (self.lfn, destSE, sourceSE))
        if isinstance(stdout, dict) and stdout.get('OK', False) and self.lfn in stdout.get('Value', {'Successful': {}})['Successful']:
            if destSE not in self.locations:
                self.locations.append(destSE)
            return
        logger.error("Error in replicating file '%s' : %s" % (self.lfn, stdout))
        return stdout

    def processWildcardMatches(self):
        if regex.search(self.namePattern) is not None:
            raise Exception(
                "No wildcards in inputfiles for DiracFile just yet. Dirac are exposing this in API soon.")

    def put(self, lfn='', force=False, uploadSE="", replicate=False):
        """
        Try to upload file sequentially to storage elements defined in configDirac['allDiracSE'].
        File will be uploaded to the first SE that the upload command succeeds for.

        The file is uploaded to the SE described by the DiracFile.defaultSE attribute

        Alternatively, the user can specify an uploadSE which contains an SE
        which the file is to be uploaded to.

        If the user wants to replicate this file(s) across all SE then they should state replicate = True.

        Return value will be either the stdout from the dirac upload command if not
        using the wildcard characters '*?[]' in the namePattern.
        If the wildcard characters are used then the return value will be a list containing
        newly created DiracFile objects which were the result of glob-ing the wildcards.

        The objects in this list will have been uploaded or had their failureReason attribute populated if the
        upload failed.
        """

        if self.lfn != "" and force == False and lfn == '':
            logger.warning("Warning you're about to 'put' this DiracFile: %s on the grid as it already has an lfn: %s" % (self.namePattern, self.lfn))
            decision = raw_input('y / [n]:')
            while not (decision.lower() in ['y', 'n'] or decision.lower() == ''):
                decision = raw_input('y / [n]:')

            if decision.lower() == 'y':
                pass
            else:
                return

        if (lfn != '' and self.lfn != '') and force == False:
            logger.warning("Warning you're attempting to put this DiracFile: %s" % self.namePattern)
            logger.warning("It currently has an LFN associated with it: %s" % self.lfn)
            logger.warning("Do you want to continue and attempt to upload to: %s" % lfn)
            decision = raw_input('y / [n]:')
            while not (decision.lower() in ['y', 'n', '']):
                decision = raw_input('y / [n]:')

            if decision.lower() == 'y':
                pass
            else:
                return

        if lfn and os.path.basename(lfn) != self.namePattern:
            logger.warning("Changing namePattern from: '%s' to '%s' during put operation" % (self.namePattern, os.path.basename(lfn)))

        if lfn:
            self.lfn = lfn

        # looks like will only need this for the interactive uploading of jobs.
        # Also if any backend need dirac upload on client then when downloaded
        # this will upload then delete the file.

        if self.namePattern == "":
            if self.lfn != '':
                logger.warning("'Put'-ing a file with ONLY an existing LFN makes no sense!")
            raise GangaException('Can\'t upload a file without a local file name.')

        sourceDir = self.localDir
        if self.localDir is None:
            sourceDir = os.getcwd()
            # attached to a job, use the joboutputdir
            if self._parent != None and os.path.isdir(self.getJobObject().outputdir):
                sourceDir = self.getJobObject().outputdir

        if not os.path.isdir(sourceDir):
            raise GangaException('localDir attribute is not a valid dir, don\'t know from which dir to take the file')

        if regex.search(self.namePattern) is not None:
            if self.lfn != "":
                logger.warning("Cannot specify a single lfn for a wildcard namePattern")
                logger.warning("LFN will be generated automatically")
                self.lfn = ""

        if not self.remoteDir:
            try:
                job = self.getJobObject()
                lfn_folder = os.path.join("GangaUploadedFiles", "GangaJob_%s" % job.getFQID('.'))
            except AssertionError:
                t = datetime.datetime.now()
                this_date = t.strftime("%H.%M_%A_%d_%B_%Y")
                lfn_folder = os.path.join("GangaUploadedFiles", 'GangaFiles_%s' % this_date)
            self.lfn = os.path.join(DiracFile.diracLFNBase(), lfn_folder, self.namePattern)

        if self.remoteDir[:4] == 'LFN:':
            lfn_base = self.remoteDir[4:]
        else:
            lfn_base = self.remoteDir

        if uploadSE == "":
            if self.defaultSE != "":
                storage_elements = [self.defaultSE]
            else:
                if configDirac['allDiracSE']:
                    storage_elements = [random.choice(configDirac['allDiracSE'])]
                else:
                    raise GangaException("Can't upload a file without a valid defaultSE or storageSE, please provide one")
        elif isinstance(uploadSE, list):
            storage_elements = uploadSE
        else:
            storage_elements = [uploadSE]

        outputFiles = GangaList()
        for this_file in glob.glob(os.path.join(sourceDir, self.namePattern)):
            name = this_file

            if not os.path.exists(name):
                if not self.compressed:
                    raise GangaException('Cannot upload file. File "%s" must exist!' % name)
                name += '.gz'
                if not os.path.exists(name):
                    raise GangaException('File "%s" must exist!' % name)
            else:
                if self.compressed:
                    os.system('gzip -c %s > %s.gz' % (name, name))
                    name += '.gz'
                    if not os.path.exists(name):
                        raise GangaException('File "%s" must exist!' % name)

            if lfn == "":
                lfn = os.path.join(lfn_base, os.path.basename(name))

            #lfn = os.path.join(os.path.dirname(self.lfn), this_file)

            d = DiracFile()
            d.namePattern = os.path.basename(name)
            d.compressed = self.compressed
            d.localDir = sourceDir
            stderr = ''
            stdout = ''
            logger.debug('Uploading file \'%s\' to \'%s\' as \'%s\'' % (name, storage_elements[0], lfn))
            logger.debug('execute: uploadFile("%s", "%s", %s)' % (lfn, name, str([storage_elements[0]])))
            stdout = execute('uploadFile("%s", "%s", %s)' % (lfn, name, str([storage_elements[0]])))
            if type(stdout) == str:
                logger.warning("Couldn't upload file '%s': \'%s\'" % (os.path.basename(name), stdout))
                continue
            if stdout.get('OK', False) and lfn in stdout.get('Value', {'Successful': {}})['Successful']:
                # when doing the two step upload delete the temp file
                if self.compressed or self._parent != None:
                    os.remove(name)
                # need another eval as datetime needs to be included.
                guid = stdout['Value']['Successful'][lfn].get('GUID', '')
                if regex.search(self.namePattern) is not None:
                    d.lfn = lfn
                    d.remoteDir = os.path.dirname(lfn)
                    d.locations = stdout['Value']['Successful'][lfn].get('allDiracSE', '')
                    d.guid = guid
                    outputFiles.append(GPIProxyObjectFactory(d))
                    continue
                else:
                    self.lfn = lfn
                    self.remoteDir = os.path.dirname(lfn)
                    self.locations = stdout['Value']['Successful'][lfn].get('allDiracSE', '')
                    self.guid = guid
                # return ## WHY?
            else:
                failureReason = "Error in uploading file %s : %s" % (os.path.basename(name), str(stdout))
                logger.error(failureReason)
                if regex.search(self.namePattern) is not None:
                    d.failureReason = failureReason
                    outputFiles.append(GPIProxyObjectFactory(d))
                    continue
                self.failureReason = failureReason
                return str(stdout)

        if replicate == True:

            if len(outputFiles) == 1 or len(outputFiles) == 0:
                storage_elements.pop(0)
                for se in storage_elements:
                    self.replicate(se)
            else:
                storage_elements.pop(0)
                for this_file in outputFiles:
                    for se in storage_elements:
                        this_file.replicate(se)

        if len(outputFiles) > 0:
            return outputFiles
        else:
            outputFiles.append(self)
            return outputFiles

    def getWNScriptDownloadCommand(self, indent):

        script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), 'downloadScript.py')

        from Ganga.GPIDev.Lib.File import FileUtils
        download_script = FileUtils.loadScript(script_location, '')

        script = """\n
download_script='''\n###DOWNLOAD_SCRIPT###'''
import subprocess
dirac_env=###DIRAC_ENV###
subprocess.Popen('''python -c "import sys\nexec(sys.stdin.read())"''', shell=True, env=dirac_env, stdin=subprocess.PIPE).communicate(download_script)
"""
        script = '\n'.join([ str(indent+str(line)) for line in script.split('\n')])

        replace_dict = {'###DOWNLOAD_SCRIPT###' : download_script,
                        '###DIRAC_ENV###' : self._getDiracEnvStr(),
                        '###LFN###' : self.lfn}

        for k, v in replace_dict.iteritems():
            script = script.replace(str(k), str(v))

        return script

    def _getDiracEnvStr(self):
        diracEnv = str(getDiracEnv())
        return diracEnv

    def _WN_wildcard_script(self, namePattern, lfnBase, compressed):
        wildcard_str =  """
for f in glob.glob('###NAME_PATTERN###'):
    processes.append(uploadFile(os.path.basename(f), '###LFN_BASE###', ###COMPRESSED###, '###NAME_PATTERN###'))
"""
        from Ganga.GPIDev.Lib.File import FileUtils
        wildcard_str = FileUtils.indentScript(wildcard_str, '###INDENT###')

        replace_dict = { '###NAME_PATTERN###' : namePattern,
                         '###LFN_BASE###' : lfnBase,
                         '###COMPRESSED###' : compressed }

        for k, v in replace_dict.iteritems():
            wildcard_str = wildcard_str.replace(str(k), str(v))

        return wildcard_str

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """

        script_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        script_location = os.path.join( script_path, 'uploadScript.py')

        from Ganga.GPIDev.Lib.File import FileUtils
        upload_script = FileUtils.loadScript(script_location, '')

        WNscript_location = os.path.join( script_path, 'WNInjectTemplate.py' )
        script = FileUtils.loadScript(WNscript_location, '')

        if not self.remoteDir:
            try:
                job = self.getJobObject()
                lfn_folder = os.path.join("GangaUploadedFiles", "GangaJob_%s" % job.getFQID('.'))
            except AssertionError:
                t = datetime.datetime.now()
                this_date = t.strftime("%H.%M_%A_%d_%B_%Y")
                lfn_folder = os.path.join("GangaUploadedFiles", 'GangaFiles_%s' % this_date)
            self.lfn = os.path.join(DiracFile.diracLFNBase(), lfn_folder, self.namePattern)

        if self.remoteDir == '':
            self.remoteDir = DiracFile.diracLFNBase()

        if self.remoteDir[:4] == 'LFN:':
            lfn_base = self.remoteDir[4:]
        else:
            lfn_base = self.remoteDir

        for this_file in outputFiles:
            isCompressed = this_file.namePattern in patternsToZip

            if not regex.search(this_file.namePattern) is None:
                script += self._WN_wildcard_script(this_file.namePattern, lfn_base, str(isCompressed))
            else:
                script += '###INDENT###print("Uploading: %s as: %s")\n' % (this_file.namePattern, str(os.path.join(lfn_base, this_file.namePattern)))
                script += '###INDENT###processes.append(uploadFile("%s", "%s", %s))\n' % (this_file.namePattern, lfn_base, str(isCompressed))


        if stripProxy(self)._parent is not None and stripProxy(self).getJobObject() and getName(stripProxy(self).getJobObject().backend) != 'Dirac':
            script_env = self._getDiracEnvStr()
        else:
            script_env = str(None)

        script = '\n'.join([str('###INDENT###' + str(line)) for line in script.split('\n')])

        replace_dict = {'###UPLOAD_SCRIPT###' : upload_script,
                        '###STORAGE_ELEMENTS###' : str(configDirac['allDiracSE']),
                        '###INDENT###' : indent,
                        '###LOCATIONSFILE###' : postProcessLocationsFP,
                        '###DIRAC_ENV###' : script_env}

        for k, v in replace_dict.iteritems():
            script = script.replace(str(k), str(v))

        return script

    def hasMatchedFiles(self):

        if self.lfn != "" and self.namePattern != "":
            if self.namePattern == os.path.basename(self.lfn):
                return True
            else:
                logger.error("LFN doesn't match namePattern for file: %s" % str(self.namePattern))
                return False
        elif len(self.subfiles) > 0 and regex.search(self.namePattern) is not None:
            return True
        else:
            logger.error("Failed to Match file:\n%s" % str(self))
            return False

    @staticmethod
    def diracLFNBase():
        """
        Compute a sensible default LFN base name
        If ``DiracLFNBase`` has been defined, use that.
        Otherwise, construct one from the user name and the user VO
        """
        if configDirac['DiracLFNBase']:
            return configDirac['DiracLFNBase']
        return '/{0}/user/{1}/{2}'.format(configDirac['userVO'], config['user'][0], config['user'])

# add DiracFile objects to the configuration scope (i.e. it will be
# possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile

from Ganga.Runtime.GPIexport import exportToGPI
exportToGPI('GangaDirac', GangaList, 'Classes')

