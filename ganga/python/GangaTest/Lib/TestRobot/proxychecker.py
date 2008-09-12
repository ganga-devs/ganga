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

        timeleft = GPcred.timeleftInHMS()
        timeleftAsDT = datetime.datetime(*(time.strptime(timeleft,"%H:%M:%S")[0:6]))
        # Get maxumin time tests allowed to take
        config = getConfig('IndependantTest')
        MaxTestTime = config['TestLength']
        
        if ( timeleftAsDT > MaxTestTime ):
            pass
        else:
            try:
                GPcred.renew()
            except:
                #email to say oops, not valid grid proxy forl ong enough
                raise GangaRobotBreakError("Grid Proxy not valid for long enough, breaking", Exception)
       
            
        
        
