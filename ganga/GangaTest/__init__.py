
def standardSetup():
    import PACKAGE
    PACKAGE.standardSetup()


def loadPlugins( config = None ):
    import Lib.GListApp
    import Lib.TFile
    import Lib.TestApplication
    import Lib.TestSubmitter # TestSubmitter, TestSplitter here
    import Lib.TestRobot
    import Lib.TestObjects
    import Framework.runner


