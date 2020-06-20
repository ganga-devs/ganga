"""
Shutdown all Ganga services in the correct order

Attributes:
    logger (logger): Logger for the shutdown manager
"""

import atexit

# Ganga imports
from GangaCore.Core.GangaThread import GangaThreadPool
from GangaCore.Core.GangaThread.WorkerThreads import _global_queues, shutDownQueues
from GangaCore.Core import monitoring_component
from GangaCore.Core.InternalServices import Coordinator
from GangaCore.Runtime import Repository_runtime, bootstrap
from GangaCore.Utility import stacktracer
from GangaCore.Utility.logging import getLogger, requires_shutdown, final_shutdown
from GangaCore.Utility.Config import setConfigOption
from GangaCore.Core.MonitoringComponent.Local_GangaMC_Service import getStackTrace, _purge_actions_queue,\
    stop_and_free_thread_pool
from GangaCore.GPIDev.Lib.Tasks import stopTasks
from GangaCore.GPIDev.Credentials import CredentialStore
from GangaCore.Core.GangaRepository.SessionLock import removeGlobalSessionFiles, removeGlobalSessionFileHandlers
from GangaDirac.BOOT import stopDiracProcess

# Globals
logger = getLogger()

def register_exitfunc():
    """
    This registers the exit functiona and actually tracks that it's done so,
    registereing this  300 times is just bad...
    """
    if not register_exitfunc._has_registered:
        atexit.register(_protected_ganga_exitfuncs)
        register_exitfunc._has_registered = True
register_exitfunc._has_registered = False


def _protected_ganga_exitfuncs():
    try:
        _unprotected_ganga_exitfuncs()
    except KeyboardInterrupt:
        logger.error("PLEASE DO NOT hit Ctrl+C on ganga exit before waiting a while!")
        logger.error("This will more than likely lead to repo/job corruption")
        pass


def _unprotected_ganga_exitfuncs():
    """Run all exit functions from plugins and internal services in the correct order

    Go over all plugins and internal services and call the appropriate shutdown functions in the correct order. Because
    we want each shutdown function to be run (e.g. to make sure flushing is done) we put each call into a try..except
    and report to the user before continuing.
    """

    # Set the disk timeout to 1 sec, sacrifice stability for quicker exit
    setConfigOption('Configuration', 'DiskIOTimeout', 1)

    # Stop GUIServerThread
    from GangaGUI.start import stop_gui, gui_server
    if gui_server is not None:
        try:
            stop_gui()
        except Exception as err:
            logger.exception("Exception raised while stopping GUI: {}".format(err))

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

    # Terminate the Dirac server thread if it is running
    try:
        stopDiracProcess()
    except Exception as err:
        logger.exception("Exception raised while stopping the Dirac process: %s" %err)

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

    # clear the credential store
    try:
        CredentialStore.shutdown()
    except Exception as err:
        logger.exception("Exception raised while clearing the credential store: %s" % err)

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

