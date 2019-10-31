
__all__ = ['test_logger']

# this function may be used to test logging problems in runtime packages
def test_logger():
    import GangaCore.Utility.logging
    logger = GangaCore.Utility.logging.getLogger()
    # this is to test manually (visually) the logging level mechanism
    print("logger test RAW ",logger,logger.name)
    logger.debug('logger test DEBUG')
    logger.info('logger test INFO')
    logger.warning('logger test WARNING')
    logger.error('logger test ERROR')
    logger.critical('logger test CRITICAL')


_this_junk_will_not_be_imported_to_GPI = None #because it is not listed in __all__ (and also it starts with underscore)

