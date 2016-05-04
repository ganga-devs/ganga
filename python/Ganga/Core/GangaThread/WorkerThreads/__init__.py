
_global_queues = None
_queues_interface = None

def getQueues():
    return _global_queues

def startUpQueues(my_interface=None):
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    global _global_queues
    global _queues_interface
    if not my_interface:
        import Ganga.GPI
        my_interface = Ganga.GPI
    _queues_additional = my_interface
    if _global_queues is None:
        logger.debug("Starting Queues")
        # start queues
        from Ganga.Runtime.GPIexport import exportToInterface
        from Ganga.Core.GangaThread.WorkerThreads.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
        _global_queues = ThreadPoolQueueMonitor()
        exportToInterface(my_interface, 'queues', _global_queues, 'Objects')

    else:
        logger.error("Cannot Start queues if they've already started")

def shutDownQueues():
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    logger.debug("Shutting Down Queues system")
    global _global_queues
    global _queues_interface
    try:
        if _global_queues:
            _global_queues.freeze()
            _global_queues._purge_all()
            _global_queues._stop_all_threads()
    except:
        logger.warning("Error in shutting down queues thread. Likely harmless")
    _global_queues = None
    if _queues_interface:
        if hasattr(_queues_interface, 'queues'):
            delattr(_queues_interface, 'queues')
    _queues_interface = None

