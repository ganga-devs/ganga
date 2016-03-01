
_global_queues = None

def startUpQueues(extra_interface=None):
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    global _global_queues
    if _global_queues is None:
        logger.debug("Starting Queues")
        # start queues
        from Ganga.Runtime.GPIexport import exportToGPI
        from Ganga.Core.GangaThread.WorkerThreads.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
        _global_queues = ThreadPoolQueueMonitor()
        exportToGPI('queues', _global_queues, 'Objects', extra_interface)

        import atexit
        atexit.register((100, shutDownQueues))

    else:
        logger.error("Cannot Start queues if they've already started")

def shutDownQueues():
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    logger.debug("Shutting Down Queues system")
    global _global_queues
    _global_queues.lock()
    _global_queues._purge_all()
    del _global_queues
    _global_queues = None
    import Ganga.GPI
    delattr(Ganga.GPI, 'queues')


