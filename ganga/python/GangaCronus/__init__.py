def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}

def loadPlugins( config = {} ):
    import Lib.Cronus
