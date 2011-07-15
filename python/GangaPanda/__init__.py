def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}
    
def loadPlugins(c):

    import sys
    from Ganga.Utility.logging import getLogger

    try:
        import Lib.Panda
    except SystemExit:
        from Ganga.Core.exceptions import ApplicationConfigurationError
        import commands
        (s,o) = commands.getstatusoutput('curl --version')
        if (s):
            raise ApplicationConfigurationError(None,"Couldn't load Panda Client: ensure 'curl' is available")
        else:
            raise ApplicationConfigurationError(None,"Couldn't load Panda Client")
    import Lib.Athena
    import Lib.Executable
    import Lib.ProdTrans

    return None

