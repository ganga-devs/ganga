import os
import Ganga.Utility.logging
import Ganga.Utility.Config

def standardSetup():

   from . import PACKAGE
   PACKAGE.standardSetup()


def loadPlugins( config = {} ):
   from . import Lib.Applications
   from . import Lib.Tasks
   from . import Lib.Requirements
   
    #import Lib.Backends
    #import Lib.Applications
    #import Lib.LHCbDataset
    #import Lib.Mergers
    #import Lib.RTHandlers
    #import Lib.Splitters
#    import Lib.DIRAC
    #import Lib.Tasks

#from Ganga.GPIDev.Credentials_old import getCredential
#proxy = getCredential('GridProxy', '')
