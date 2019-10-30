#Bail out when loading this module as it is not python3 compliant
from GangaCore.Core.exceptions import PluginError
raise PluginError("The GangaNA62 module has not been upgraded for python 3. The last python 2 Ganga version is 7.1.15 . Please contact the ganga devs to discuss updating this module.")

import os
import GangaCore.Utility.logging
import GangaCore.Utility.Config

def standardSetup():

   import PACKAGE
   PACKAGE.standardSetup()


def loadPlugins( config = {} ):
   import Lib.Applications
   import Lib.Tasks
   import Lib.Requirements
   
    #import Lib.Backends
    #import Lib.Applications
    #import Lib.LHCbDataset
    #import Lib.Mergers
    #import Lib.RTHandlers
    #import Lib.Splitters
#    import Lib.DIRAC
    #import Lib.Tasks

#from GangaCore.GPIDev.Credentials_old import getCredential
#proxy = getCredential('GridProxy', '')
