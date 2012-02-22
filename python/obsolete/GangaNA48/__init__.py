def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}

def loadPlugins(c):

    import sys
    from Ganga.Utility.logging import getLogger

    try:
        import Lib.Nasim
        import Lib.Compact
        import Lib.NA48Dataset
        import Lib.NA48LCGRequirements
        import Lib.Fluka
        
    except SystemExit:
        from Ganga.Core.exceptions import ApplicationConfigurationError
        raise ApplicationConfigurationError(None,"Couldn't load NA48 Extensions")

    return None
