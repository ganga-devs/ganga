# File: GangaND280/__init__.py

from Ganga.Utility.Config import makeConfig
from Ganga.Utility.Config.Config import _after_bootstrap

if not _after_bootstrap:
   configND280 = makeConfig('ND280', 'Configuration parameters for the ND280 Ganga plugin')
   configND280.addOption('ND280DCacheDatasetCommandStr',
                         {'TRIUMF' : 'curl --compressed -sk --header "Accept: text/plain" https://user:password@nd280web.nd280.org/full/path/to/https/nd280data/%s'},
                         'How to get the list of files on the DCache server')
   configND280.addOption('ND280DCacheDatasetFileBasePath',
                         {'TRIUMF' : 'dcap://t2ksrm.nd280.org/full/path/to/dcap/nd280data/'},
                         'How to access files on the DCache sever')

def loadPlugins( config = {} ):

   from . import ND280Dataset
   from . import ND280Splitter
   from . import ND280Control
   from . import ND280Executable
   from . import ND280RecoValidation
   from . import ND280Skimmer
   from . import Highland
   from . import Tasks
   from . import ND280TPCGasInteractions
   from . import ND280Checkers

   return None
