
_global_queues = None

_added_additional = None

def startUpQueues(additional_interface=None):
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    global _global_queues
    global _added_additional
    _added_additional = additional_interface
    if _global_queues is None:
        logger.debug("Starting Queues")
        # start queues
        from Ganga.Runtime.GPIexport import exportToGPI
        from Ganga.Core.GangaThread.WorkerThreads.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
        _global_queues = ThreadPoolQueueMonitor()
        exportToGPI('queues', _global_queues, 'Objects', additional_interface=additional_interface)

        import atexit
        atexit.register((100, shutDownQueues))

    else:
        logger.error("Cannot Start queues if they've already started")

def shutDownQueues():
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    logger.debug("Shutting Down Queues system")
    global _global_queues
    try:
        _global_queues.lock()
        _global_queues._purge_all()
    except:
        logger.warning("Error in shutting down queues thread. Likely harmless")
    del _global_queues
    _global_queues = None
    import Ganga.GPI
    if hasattr(Ganga.GPI, 'queues'):
        delattr(Ganga.GPI, 'queues')
    if _added_additional:
        if hasattr(_added_additional, 'queues'):
            delattr(_added_additional, 'queues')

