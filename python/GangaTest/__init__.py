
def getEnvironment(c = None):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}

def loadPlugins( config = None ):
    import Lib.TestApplication
    import Lib.TestSubmitter # TestSubmitter, TestSplitter here
    import Lib.TestRobot
    import Lib.TestObjects
    import Framework.runner


