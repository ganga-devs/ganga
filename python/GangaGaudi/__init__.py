import os
import Ganga.Utility.logging
import Ganga.Utility.Config
configLHCb=Ganga.Utility.Config.makeConfig('GAUDI','Generic GAUDI based parameters')

dscrpt = 'The command used to make a CMT application.'
configLHCb.addOption('make_cmd','make',dscrpt)

dscrpt = 'Levels below InstallArea/[<platform>]/python to decend when looking for .py files to include'
configLHCb.addOption('pyFileCollectionDepth',2,dscrpt)

def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE
   
   PACKAGE.standardSetup()
   return

def loadPlugins( config = {} ):
   import Lib.Backends
   pass
   #import Lib.Applications
   #import Lib.RTHandlers
   #import Lib.Datasets
   #import Lib.Datafiles
   #import Lib.Splitters
