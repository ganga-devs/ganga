""" Implementation of the Ganga Robot IAction class
to install ganga on a remote machine
"""

from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from Ganga.Utility.logging import getLogger
import Ganga.Utility.Config
from GangaRobot.Framework.exceptions import *

import os, urllib, datetime, time
from os.path import join

logger = getLogger()

class Installer(IAction):

    """ Ganga Install implementation

    has:

    > Overridden 'execute' method which does:
    >> Retrieve config
    >> Runs the ganga install script, with the config options

    """

    downloadURL = ''
    VersionNumber = ''
    VersionTime = ''
    InstallPath = ''

    def execute(self, runid):
        """
        Call the class methods in turn
        """
        #raise GangaRobotBreakError("Testing checking time",ValueError)
        Datalist = []
        
        self._getConfigInfo()

        logger.info("Running install script")
        self._Install()
        #update last download time


    def _getConfigInfo(self):
        """ Gets the config info from the ganga config file """
        config = Ganga.Utility.Config.getConfig('TestRobot')     
        self.InstallPath = config['InstallPath']
        self.downloadURL = config['ReleasePath']        
        self.VersionNumber = config['VersionNumber']

    def _Install(self):

        """ Calls the ganga installation script """
        try: 
            f, inf = urllib.urlretrieve("http://ganga.web.cern.ch/ganga/download/ganga-install")
        except IOError, e:
            raise GangaRobotBreakError(e, IOError)
            
        config = Ganga.Utility.Config.getConfig('IndependantTest')

        cmd = "python "+f+" --prefix="+self.InstallPath+" --extern=GangaTest "+self.VersionNumber
        logger.warning("Executing command: '%s'",cmd)
        try:
            os.system(cmd)
            logger.info('ganga installed')

        except Exception, e:
            logger.error('ganga installation failed')
            raise GangaRobotBreakError(Exception, e) # breaks the run and starts again
            
