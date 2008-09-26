#Check for grid proxy
from GangaRobot.Framework.Action import IAction
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig
import os, datetime, time
from GangaRobot.Framework.exceptions import *
#from Ganga.GPIDev.Credentials.GridProxy import GridProxy
from Ganga.GPIDev.Credentials import getCredential

logger = getLogger()

class ProxyChecker(IAction):
    """
    ProxyChecker IAction implementation
    
    Checks for grid proxy valid long enough to run tests.....
    """
    
    def execute(self, runid):
        # check for grid proxy
        
        GPcred = getCredential(name = 'GridProxy', create = 'False')

        timeleft = float(GPcred.timeleft("seconds"))

        # Get maxumin time tests allowed to take
        config = getConfig('TestRobot')
        MaxTestTime = config['JobTimeOut']
        
        if ( timeleft < MaxTestTime ):
            raise GangaRobotBreakError("Grid Proxy valid for %8.0f seconds but %d might be required to finish testing. Breaking." % (timeleft, MaxTestTime), Exception)
            
        
        
