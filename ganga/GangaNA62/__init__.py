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
