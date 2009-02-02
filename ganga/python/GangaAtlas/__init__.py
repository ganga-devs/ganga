# File: GangaAtlas/__init__.py

def loadPlugins( config = {} ):

   import warnings
   warnings.filterwarnings('ignore','Python C API version mismatch for module pycurl')
   warnings.filterwarnings('ignore','Python C API version mismatch for module _lfc')

   import Lib.Athena
   import Lib.ATLASDataset
   import Lib.AthenaMC
   import Lib.AMAAthena
   import Lib.Tnt
   import Lib.AtlasLCGRequirements
   import Lib.Tasks

   
   return None

def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    
    #   set up X509_CERT_DIR for DQ2
    from Ganga.Utility.GridShell import getShell
    gshell = getShell()
    if gshell:
       try:
          return { 'X509_CERT_DIR' : gshell.env['X509_CERT_DIR'] }
       except KeyError:
          return { 'X509_CERT_DIR' : '/etc/grid-security/certificates' }

