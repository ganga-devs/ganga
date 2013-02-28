import os
import Ganga.Utility.logging
import Ganga.Utility.Config

def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return

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

#from Ganga.GPIDev.Credentials import getCredential
#proxy = getCredential('GridProxy', '')
