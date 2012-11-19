def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return

def loadPlugins( config = {} ):
   import Lib.SAGA
