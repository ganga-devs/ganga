
def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}

def loadPlugins( config = {} ):
    import Lib.TestApplication
    import Lib.TestSubmitter
    import Lib.TestRobot
    import Lib.TestObjects
    import Framework.runner


