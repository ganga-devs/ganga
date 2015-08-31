import os
import Ganga.Utility.logging
import Ganga.Utility.Config
configGaudi = Ganga.Utility.Config.makeConfig(
    'GAUDI', 'Generic GAUDI based parameters')

dscrpt = 'The command used to make a CMT application.'
configGaudi.addOption('make_cmd', 'make', dscrpt)

dscrpt = 'Levels below InstallArea/[<platform>]/python to decend when looking for .py files to include'
configGaudi.addOption('pyFileCollectionDepth', 2, dscrpt)

configGaudi.addOption('useCMakeApplications', False,
                      'Flag to implement CMake style applications within Ganga')


def getEnvironment(config={}):
    import sys
    import os.path
    import PACKAGE

    PACKAGE.standardSetup()
    return


def loadPlugins(config={}):
    #import Lib.Backends
    import Lib.Checkers
    pass
    #import Lib.Applications
    #import Lib.RTHandlers
    #import Lib.Datasets
    #import Lib.Datafiles
    #import Lib.Splitters
