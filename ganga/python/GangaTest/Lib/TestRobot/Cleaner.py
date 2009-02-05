#Clean

from GangaRobot.Framework.Action import IAction
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig
import os, shutil
from os.path import join
from GangaRobot.Framework.exceptions import *

logger = getLogger()

class Cleaner(IAction):
    """
    Cleaner IAction implementation
    """
    
    def execute(self, runid):
        #Clean up
        self._getconfig()
        logger.info("Starting Cleanup")
        self._clean()
        
    def _getconfig(self):
        config = getConfig('Configuration')
        self.releasedir = join(config['gangadir'],"Releases")
        #config = getConfig('IndependantTest')
        #self.ReleaseNumber = config['ReleaseNo']
        
    def _clean(self):
        #Removes the installed files
        installdir = join(self.releasedir,"install")
        logger.info("Removing install directory %s" % str(installdir))
        try:
            shutil.rmtree(installdir)
        except:
            raise GangaRobotContinueError("Failed to remove install directory",str(installdir))
        externaldir = join(self.releasedir,"external")
        logger.info("Removing external directory %s " % str(externaldir))
        try:
            shutil.rmtree(externaldir)
        except:
            raise GangaRobotContinueError("Failed to remove external directory",str(externaldir))
        tarballdir = join(self.releasedir,"tarball")
        logger.info("Removing tarball directory %s"  %str(tarballdir))
        try:
            shutil.rmtree(tarballdir)
        except:
            raise GangaRobotContinueError("Failed to remove tarball directory",str(tarballdir))
        """    
        jobdir = join(self.releasedir,"Jobs")
        logger.info("cleaning job directory %s"  %str(jobdir))
        try:
            shutil.rmtree(jobdir)
        except:
            raise GangaRobotContinueError("Failed to remove job directory",str(jobdir))
        """    
        
