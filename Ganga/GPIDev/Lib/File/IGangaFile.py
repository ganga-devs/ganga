from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from fnmatch import fnmatch

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
        """
        raise NotImplementedError

    def getSubFiles(self):
        """
        Returns the name of a file object throgh a common interface
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
        pass

    def _readonly(self):
        return False

    def _list_get__match__(self, to_match):
        if type(to_match) == str:
            return fnmatch(self.namePattern, to_match)
        ## Note: type(DiracFile) = ObjectMetaclass
        ##       type(ObjectMetaclass) = type
        ## hence checking against a class type not an instance
        if type(type(to_match)) == type:
             return issubclass(self.__class__, to_match)
        return to_match == self

    def execSyscmdSubprocess(self, cmd):

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

