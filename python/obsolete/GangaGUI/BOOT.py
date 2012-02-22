def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}
    

def loadPlugins(c):
    import sys
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    logger.info('You are now using Python %s',sys.version.split()[0])
    import PACKAGE
    # FIXME: a temporary(?) hack to clean environment variables
    PACKAGE.removeSetup()
    logger.info('')
    
    
