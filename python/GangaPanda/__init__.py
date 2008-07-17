def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}
    
def loadPlugins(c):

    import sys
    from Ganga.Utility.logging import getLogger

    import Lib.Panda
    import Lib.Athena

    return None
