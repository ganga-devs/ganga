"""
Shutdown all Ganga services in the correct order

Attributes:
    logger (logger): Logger for the shutdown manager
"""

import atexit

# Ganga imports
from Ganga.Core.GangaThread import GangaThreadPool
from Ganga.Core.GangaThread.WorkerThreads import _global_queues, shutDownQueues
from Ganga.Core import monitoring_component
from Ganga.Core.InternalServices import Coordinator
from Ganga.Runtime import Repository_runtime, bootstrap
from Ganga.Utility import stacktracer
from Ganga.Utility.logging import getLogger, requires_shutdown, final_shutdown
from Ganga.Utility.Config import setConfigOption
from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import getStackTrace, _purge_actions_queue,\
    stop_and_free_thread_pool
from Ganga.GPIDev.Lib.Tasks import stopTasks
from Ganga.Core.GangaRepository.SessionLock import removeGlobalSessionFiles, removeGlobalSessionFileHandlers

# Globals
logger = getLogger()

def register_exitfunc():
    """
    This registers the exit functiona and actually tracks that it's done so,
    registereing this  300 times is just bad...
    """
    if not register_exitfunc._has_registered:
        atexit.register(_ganga_run_exitfuncs)
        register_exitfunc._has_registered = True
register_exitfunc._has_registered = False

def _ganga_run_exitfuncs():
    """Run all exit functions from plugins and internal services in the correct order

    Go over all plugins and internal services and call the appropriate shutdown functions in the correct order. Because
    we want each shutdown function to be run (e.g. to make sure flushing is done) we put each call into a try..except
    and report to the user before continuing.
    """

    # Set the disk timeout to 1 sec, sacrifice stability for quicker exit
    setConfigOption('Configuration', 'DiskIOTimeout', 1)

    # Stop the monitoring loop from iterating further
    if monitoring_component is not None:
        try:
            getStackTrace()
            if monitoring_component.alive:
                monitoring_component.disableMonitoring()
                monitoring_component.stop()
                monitoring_component.join()
        except Exception as err:
            logger.exception("Exception raised while stopping the monitoring: %s" % err)

    # Stop the tasks system from running
    try:
        stopTasks()
    except Exception as err:
        logger.exception("Exception raised while stopping Tasks: %s" % err)

    # purge the monitoring queues
    try:
        _purge_actions_queue()
        stop_and_free_thread_pool()
    except Exception as err:
        logger.exception("Exception raised while purging monitoring queues: %s" % err)

    # Freeze queues
    try:
        if _global_queues:
            _global_queues.freeze()
    except Exception as err:
        logger.exception("Exception raised during freeze of Global Queues: %s" % err)

    # shutdown the threads in the GangaThreadPool
    try:
        GangaThreadPool.getInstance().shutdown()
    except Exception as err:
        logger.exception("Exception raised during shutdown of GangaThreadPool: %s" % err)

    # Shutdown queues
    try:
        logger.info("Stopping Job processing before shutting down Repositories")
        shutDownQueues()
    except Exception as err:
        logger.exception("Exception raised while purging shutting down queues: %s" % err)

    # shutdown the repositories
    try:
        logger.info("Shutting Down Ganga Repositories")
        Repository_runtime.shutdown()
    except Exception as err:
        logger.exception("Exception raised while shutting down repositories: %s" % err)

    # label services as disabled
    Coordinator.servicesEnabled = False

    # shutdown SessionLock
    try:
        removeGlobalSessionFileHandlers()
        removeGlobalSessionFiles()
    except Exception as err:
        logger.exception("Exception raised while shutting down SessionLocks: %s" % err)

    # Shutdown stacktracer
    if stacktracer._tracer:
        try:
            stacktracer.trace_stop()
        except Exception as err:
            logger.exception("Exception raised while stopping stack tracer: %s" % err)

    # do final shutdown
    if requires_shutdown is True:
        try:
            final_shutdown()
        except Exception as err:
            logger.exception("Exception raised while doing final shutdown: %s" % err)

    # show any open files after everything's shutdown
    if bootstrap.DEBUGFILES or bootstrap.MONITOR_FILES:
        bootstrap.printOpenFiles()
