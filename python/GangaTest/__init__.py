
def standardSetup():
    from . import PACKAGE
    PACKAGE.standardSetup()


def loadPlugins( config = None ):
    from . import Lib.GListApp
    from . import Lib.TFile
    from . import Lib.TestApplication
    from . import Lib.TestSubmitter # TestSubmitter, TestSplitter here
    from . import Lib.TestRobot
    from . import Lib.TestObjects
    from . import Framework.runner


