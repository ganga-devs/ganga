# File: GangaND280/__init__.py

from GangaCore.Utility.Config import makeConfig
from GangaCore.Utility.Config.Config import _after_bootstrap

if not _after_bootstrap:
   configND280 = makeConfig('ND280', 'Configuration parameters for the ND280 Ganga plugin')
   configND280.addOption('ND280DCacheDatasetCommandStr',
                         {'TRIUMF' : 'curl --compressed -sk --header "Accept: text/plain" https://user:password@nd280web.nd280.org/full/path/to/https/nd280data/%s'},
                         'How to get the list of files on the DCache server')
   configND280.addOption('ND280DCacheDatasetFileBasePath',
                         {'TRIUMF' : 'dcap://t2ksrm.nd280.org/full/path/to/dcap/nd280data/'},
                         'How to access files on the DCache sever')

def loadPlugins( config = {} ):

   import GangaND280.ND280Dataset
   import GangaND280.ND280Control
   import GangaND280.ND280Executable
   import GangaND280.ND280RecoValidation
   import GangaND280.ND280Skimmer
   import GangaND280.Highland
   import GangaND280.Tasks
   import GangaND280.ND280TPCGasInteractions
   import GangaND280.ND280Checkers

   return None
