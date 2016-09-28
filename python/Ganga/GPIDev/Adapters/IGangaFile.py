import os
import glob
import re
import shutil
import copy
from Ganga.Core.exceptions import GangaFileError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import getName
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.Utility.logging import getLogger
from fnmatch import fnmatch

logger = getLogger()
regex = re.compile('[*?\[\]]')


class IGangaFile(GangaObject):

    """IGangaFile represents base class for output files, such as MassStorageFile, LCGSEFile, DiracFile, LocalFile, etc 
    """
    _schema = Schema(Version(1, 1), {'namePattern': SimpleItem(defvalue="", doc='pattern of the file name')})
    _category = 'gangafiles'
    _name = 'IGangaFile'
    _hidden = 1

    def __init__(self):
        super(IGangaFile, self).__init__()

    def setLocation(self):
        """
        Sets the location of output files that were uploaded from the WN
        """
        raise NotImplementedError

    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        raise NotImplementedError

    def get(self):
        """
        Retrieves locally all files that were uploaded before that 
        Order of priority about where a file is going to be placed are:
            1) The localDir as defined in the schema. (Exceptions thrown if this doesn't exist)
            2) The Job outpudir of the parent job if the localDir is not defined.
            3) raise an exception if neither are defined correctly.
        """
        if self.localDir:
            if not os.path.isdir(self.localDir):
                msg = "Folder '%s' doesn't exist. Please construct this before 'get'-ing a file." % self.localDir
                raise GangaFileError(msg)
            to_location = self.localDir
        else:
            try:
                to_location = self.getJobObject().outputdir
            except AssertionError:
                msg = "%s: Failed to get file object. Please set the `localDir` parameter and try again. e.g. file.localDir=os.getcwd();file.get()" % getName(self)
                logger.debug("localDir value: %s" % self.localDir)
                logger.debug("parent: %s" % self._getParent())
                raise GangaFileError(msg)

        # FIXME CANNOT perform a remote globbing here in a nice way so have to just perform a copy when dealing with wildcards
        if not os.path.isfile(os.path.join(to_location, self.namePattern)):
            returnable = self.copyTo(to_location)
            if not self.localDir:
                self.localDir = to_location
            return returnable
        else:
            logger.debug("File: %s already exists, not performing copy" % (os.path.join(to_location, self.namePattern), ))
            return True


    def getSubFiles(self, process_wildcards=False):
        """
        Returns the sub files if wildcards are used
        """
        # should we process wildcards? Used for inputfiles
        if process_wildcards:
            self.processWildcardMatches()

        # if we have subfiles, return that
        if hasattr(self, 'subfiles'):
            return self.subfiles

        return []

    def getFilenameList(self):
        """
        Returns the filenames of all associated files through a common interface
        """
        raise NotImplementedError

    def getWNScriptDownloadCommand(self, indent):
        """
        Gets the command used to download already uploaded file
        """
        raise NotImplementedError

    def put(self):
        """
        Postprocesses (upload) output file to the desired destination from the client
        Order of priority of where the file is to be uploaded to:
            1) If a this is managed by a parent job then it's name is expanded according to the outputnameformat if the IGangaFile supports it
                then it's placed in an automatic folder based upon the base string with the correct name expansion
                i.e. baseDir / auto-expanded-filename
            2) If the remoteDir has been defined for this file object the file is uploaded to
                baseDir / remoteDir / auto-expanded-filename
            3) In the case the namePattern isn't auto-expanded
                baseDir / namePattern

        Order of priority as to where the file is taken to be on local storage:
            1) If the object is managed by a Job the job.outputdir is set to the localDir if None exists
            2) If the object has a localDir then this is taken to be the location the upload will attempt to upload the file from
            2) This will raise an exception if the file doesn't exist before attempting an upload
        """

        sourcePath = ''

        fileName = self.namePattern
        if self.compressed:
            fileName = '%s.gz' % self.namePattern

        if self._getParent() is not None:
            sourceDir = self.getJobObject().outputdir
        else:
            sourceDir = self.localDir

        if self.containsWildcards():

            output = True
            for this_file in glob.glob(os.path.join(sourceDir, fileName)):
            
                logger.info("Adding: %s" % (this_file))
                sub_file = copy.deepcopy(self)
                sub_file.namePattern = os.path.basename(this_file)
                sub_file.localDir = sourceDir
                output = output and sub_file.put()
                if sub_file.namePattern not in [file_.namePattern for file_ in self.subfiles]:
                    self.subfiles.append(sub_file)
                else:
                    for file_ in self.subfiles:
                        if file_.namePattern == sub_file.namePattern:
                            file_.localDir = sourceDir
                            break

            return output
        
        elif self.getSubFiles():

            output = True
            for sub_file in self.getSubFiles():
                output = output and sub_file.put()
            return output

        targetDir, targetFile = self.getOutputFilename()

        returnable = self.uploadTo(os.path.join(targetDir, targetFile))

        if returnable is True:
            self.namePattern = targetFile

        if hasattr(self, 'locations') and os.path.join(targetDir, targetFile) not in self.locations:
            self.locations.append(os.path.join(targetDir, targetFile))

        return True

    def getOutputFilename(self):
        """
        This method expands the otuput file name of a class which has implemented the 'outputfilenameformat' attribute
        Args:
            fileName(str): This is the basename of the inputfile which is copied to the output folder
        """

        if not self._getParent() or hasattr(self, 'outputfilenameformat') and self.outputfilenameformat:
            return '', self.namePattern

        jobfqid = self.getJobObject().fqid

        jobid = jobfqid
        subjobid = ''

        folderStructure = ''
        filenameStructure = self.namePattern

        if (jobfqid.find('.') > -1):
            jobid = jobfqid.split('.')[0]
            subjobid = jobfqid.split('.')[1]

        if not hasattr(self, 'outputfilenameformat') or (hasattr(self, 'outputfilenameformat') and not self.outputfilenameformat):
            # create jid/sjid directories
            folderStructure = jobid
            if subjobid != '':
                folderStructure = os.path.join(jobid, subjobid)
        else:
            filenameStructure = os.path.basename(self.outputfilenameformat)
            filenameStructure = filenameStructure.replace('{jid}', jobid)

            folderStructure = os.path.dirname(self.outputfilenameformat)
            folderStructure = folderStructure.replace('{jid}', jobid)

            if subjobid != '':
                filenameStructure = filenameStructure.replace('{sjid}', subjobid)
                folderStructure = folderStructure.replace('{sjid}', subjobid)
       
        return (folderStructure, filenameStructure)

    def uploadTo(self, targetPath):
        """
        This method only cares about uploading the file to the correct location given as 'targetPath'
        Args:
            targetPath (str): This is the _absolute_ target where the file managed by this class is uploaded to
        """
        raise NotImplementedError

    def copyTo(self, targetPath):
        """
        Copy a the file to the local storage using the appropriate file-transfer mechanism
        This will raise an exception if targetPath isn't set to something sensible.
        Args:
            targetPath (str): Target path where the file is to copied to
        """
        if not isinstance(targetPath, str) and targetPath:
            raise GangaException("Cannot perform a copyTo with no given targetPath!")
        if self.containsWildcards() and os.path.isfile(os.path.join(self.localDir, self.namePattern)):

            if not os.path.isfile(os.path.join(targetPath, self.namePattern)):
                shutil.copy(os.path.join(self.localDir, self.namePattern), os.path.join(targetPath, self.namePattern))
            else:
                logger.debug("Already found file: %s" % os.path.join(targetPath, self.namePattern))
                
            return True

        # Again, cannot perform a remote glob here so have to ignore wildcards
        else:
            return self.internalCopyTo(targetPath)

    def internalCopyTo(self, targetPath):
        """
        Internal method for implementing the actual copy mechanism for each IGangaFile
        Args:
             targetPath (str): Target path where the file is to copied to
        """
        raise NotImplementedError

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        raise NotImplementedError

    def processWildcardMatches(self):
        """
        If namePattern contains a wildcard, populate the subfiles property
        """
        raise NotImplementedError

    def _auto_remove(self):
        """
        Remove called when job is removed as long as config option allows
        """
        self.remove()

    def _readonly(self):
        return False

    def _list_get__match__(self, to_match):
        if isinstance(to_match, str):
            return fnmatch(self.namePattern, to_match)
        # Note: type(DiracFile) = ObjectMetaclass
        # type(ObjectMetaclass) = type
        # hence checking against a class type not an instance
        if isinstance(type(to_match), type):
            return issubclass(self.__class__, to_match)
        return to_match == self

    @staticmethod
    def execSyscmdSubprocess(cmd):

        import subprocess

        exitcode = -999
        mystdout = ''
        mystderr = ''

        try:
            child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (mystdout, mystderr) = child.communicate()
            exitcode = child.returncode
        finally:
            pass

        return (exitcode, mystdout, mystderr)

    def remove(self):
        """
        Objects should implement something to overload this!
        """
        raise NotImplementedError

    def accessURL(self):
        """
        Return the URL including the protocol used to access a file on a certain storage element
        """
        raise NotImplementedError

    def hasMatchedFiles(self):
        """
        Return if this file has got valid matched files. Default implementation checks for
        subfiles and locations
        """

        # check for subfiles
        if (hasattr(self, 'subfiles') and len(self.subfiles) > 0):
            # we have subfiles so we must have actual files associated
            return True

        # check for locations
        if (hasattr(self, 'locations') and len(self.locations) > 0):
            return True

        return False

    def containsWildcards(self):
        """
        Return if the name has got wildcard characters
        """
        if regex.search(self.namePattern) is not None:
            return True

        return False

    def cleanUpClient(self):
        """
        This method cleans up the client space after performing a put of a file after a job has completed
        """

        # For all other file types (not LocalFile) The file in the outputdir is temporary waiting for Ganga to pass it to the storage solution
        job = self.getJobObject()

        for f in glob.glob(os.path.join(job.outputdir, self.namePattern)):
            try:
                os.remove(f)
            except OSError as err:
                if err.errno != errno.ENOENT:
                    logger.error('failed to remove temporary/intermediary file: %s' % f)
                    logger.debug("Err: %s" % err)
                    raise err

