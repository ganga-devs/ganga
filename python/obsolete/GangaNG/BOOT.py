def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}
    
def loadPlugins(c):

    import os,sys
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    logger.info('You are now using Python %s',sys.version.split()[0])

    import Lib.NG
    import Lib.NG.Root
        
    # Test for GangaAtlas before we load Lib.NG.Athena
    import Ganga.Utility.Config
    config = Ganga.Utility.Config.getConfig('Configuration')
    if config['RUNTIME_PATH'].find('GangaAtlas')>-1:
        import Lib.NG.Athena
    
    return None
