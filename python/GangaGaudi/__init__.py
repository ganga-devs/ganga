import os
import Ganga.Utility.logging
import Ganga.Utility.Config

from Ganga.Utility.Config.Config import _after_bootstrap
from Ganga.Utility.logging import getLogger
logger = getLogger()

if not _after_bootstrap:

    configGaudi = Ganga.Utility.Config.makeConfig(
    'GAUDI', 'Generic GAUDI based parameters')

    dscrpt = 'The command used to make a CMT application.'
    configGaudi.addOption('make_cmd', 'make', dscrpt)

    dscrpt = 'Levels below InstallArea/[<platform>]/python to decend when looking for .py files to include'
    configGaudi.addOption('pyFileCollectionDepth', 2, dscrpt)


def standardSetup():

    import PACKAGE
    PACKAGE.standardSetup()


def loadPlugins(config=None):
    #import Lib.Backends
    #import Lib.Checkers
    pass
    #import Lib.Applications
    #import Lib.RTHandlers
    #import Lib.Datasets
    #import Lib.Datafiles
    #import Lib.Splitters
