################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IOutputFile.py,v 0.1 2012-09-28 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class IOutputFile(GangaObject):
    """IOutputFile represents base class for output files, such as MassStorageFile, LCGStorageElementFile, DiracFile, OutputSandboxFile, etc 
    """
    _schema = Schema(Version(1,1), {})
    _category = 'outputfiles'
    _name = 'IOutputFile'
        
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

    def _readonly(self):
        return False

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
