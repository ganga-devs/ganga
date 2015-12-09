
def getEnvironment(c = None):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}

def loadPlugins( config = None ):
    import Lib.TestSplitter
    import Lib.TestApplication
    import Lib.TestSubmitter
    import Lib.TestRobot
    import Lib.TestObjects
    import Framework.runner


