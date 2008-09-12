def ChangeDiracLoggerLevel(opt,value):
    """Modify the Logging level used inside the DIRAC modules"""
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()
    import logging
    import DIRAC.Utility.Logger as diracLogger
    if opt.find('DiracLoggerLevel') == 0:
        logger.debug("Setting new DiracLoggerLevel to %s",value)
        if value =='DEBUG':
            diracLogger.gLog.setLevel(logging.DEBUG)
        if value =='INFO':
            diracLogger.gLog.setLevel(logging.INFO)
        if value =='WARNING':
            diracLogger.gLog.setLevel(logging.WARNING)
        if value =='ERROR':
            diracLogger.gLog.setLevel(logging.ERROR)
        if value =='CRITICAL':
            diracLogger.gLog.setLevel(logging.CRITICAL)

