"""
Core package defines the fundamental subsystems of Ganga Architecture.
Subsystems are autonomous components (such as a remote services) which may be independetly deployed.
Subsystems may also be created as local objects in the Ganga Client process.
"""
from __future__ import absolute_import

import time

from .exceptions import GangaException, ApplicationConfigurationError, \
    BackendError, RepositoryError, BulkOperationRepositoryError, \
    IncompleteJobSubmissionError, IncompleteKillError, JobManagerError, \
    GangaAttributeError, GangaValueError, ProtectedAttributeError, \
    ReadOnlyObjectError, TypeMismatchError, SchemaError, ApplicationPrepareError, \
    GangaIOError, SplitterError

monitoring_component = None


def set_autostart_policy(interactive_session):
    """
    Change the default value of autostart of the monitoring, depending if the session is interactive or batch.
    The autostart value may be overriden in the config file, so warn if it differs from the default.
    This function should be called
    """
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import config

# internal helper variable for interactive shutdown
t_last = None


def bootstrap(reg, interactive_session):
    """
    Create local subsystems. In the future this procedure should be enhanced to connect to remote subsystems.
    FIXME: this procedure should be moved to the Runtime package.

    This function will change the default value of autostart of the monitoring, depending if the session is interactive or batch.
    The autostart value may be overriden in the config file, so warn if it differs from the default.
    """
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import JobRegistry_Monitor, config

    config.addOption('forced_shutdown_policy', 'session_type',
                     'If there are remaining background activities at exit such as monitoring, output download Ganga will attempt to wait for the activities to complete. You may select if a user is prompted to answer if he wants to force shutdown ("interactive") or if the system waits on a timeout without questions ("timeout"). The default is "session_type" which will do interactive shutdown for CLI and timeout for scripts.')

    config.addOption('forced_shutdown_timeout', 60,
                     "Timeout in seconds for forced Ganga shutdown in batch mode.")
    config.addOption('forced_shutdown_prompt_time', 10,
                     "User will get the prompt every N seconds, as specified by this parameter.")
    config.addOption('forced_shutdown_first_prompt_time', 5,
                     "User will get the FIRST prompt after N seconds, as specified by this parameter. This parameter also defines the time that Ganga will wait before shutting down, if there are only non-critical threads alive, in both interactive and batch mode.")

    from Ganga.Utility.logging import getLogger

    logger = getLogger()

    from Ganga.Core.GangaThread import GangaThreadPool

    # start the internal services coordinator
    from Ganga.Core.InternalServices import Coordinator
    Coordinator.bootstrap()

    # backend-specific setup (e.g. Remote: setup any remote ssh pipes)
    # for j in reg:
    #    if hasattr(j,'status') and j.status in ['submitted','running']:
    #        if hasattr(j,'backend'): # protect: EmptyGangaObject does not have backend either
    #            if hasattr(j.backend,'setup'): # protect: EmptyGangaObject does not have setup() method
    #                j.backend.setup()

    # start the monitoring loop
    global monitoring_component
    monitoring_component = JobRegistry_Monitor(reg)
    monitoring_component.start()

    # register the MC shutdown hook

    change_atexitPolicy(interactive_session)

    # export to GPI
    from Ganga.Runtime.GPIexport import exportToGPI
    exportToGPI(
        'runMonitoring', monitoring_component.runMonitoring, 'Functions')

    autostart_default = interactive_session
    config.overrideDefaultValue('autostart', bool(autostart_default))

    if config['autostart'] is not autostart_default:
        msg = 'monitoring loop %s (the default setting for %s session is %s)'
        val = {True: ('enabled', 'batch', 'disabled'),
               False: ('disabled', 'interactive', 'enabled')}
        logger.warning(msg % val[config['autostart']])

    if config['autostart']:
        monitoring_component.enableMonitoring()


def should_wait_interactive_cb(t_total, critical_thread_ids, non_critical_thread_ids):
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import config
    global t_last
    if t_last is None:
        t_last = -time.time()
    # if there are critical threads then prompt user or wait depending on
    # configuration
    if critical_thread_ids:
        if ((t_last < 0 and time.time() + t_last > config['forced_shutdown_first_prompt_time']) or
                (t_last > 0 and time.time() - t_last > config['forced_shutdown_prompt_time'])):
            msg = """Job status update or output download still in progress (shutdown not completed after %d seconds).
%d background thread(s) still running: %s.
Do you want to force the exit (y/[n])? """ % (t_total, len(critical_thread_ids), critical_thread_ids)
            resp = raw_input(msg)
            t_last = time.time()
            return resp.lower() != 'y'
        else:
            return True
    # if there are non-critical threads then wait or shutdown depending on
    # configuration
    elif non_critical_thread_ids:
        if t_total < config['forced_shutdown_first_prompt_time']:
            return True
        else:
            return False
    # if there are no threads then shutdown
    else:
        return False


def should_wait_batch_cb(t_total, critical_thread_ids, non_critical_thread_ids):
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import config
    from Ganga.Utility.logging import getLogger
    # if there are critical threads then wait or shutdown depending on
    # configuration
    if critical_thread_ids:
        if t_total < config['forced_shutdown_timeout']:
            return True
        else:
            getLogger().warning('Shutdown was forced after waiting for %d seconds for background activities to finish\
(monitoring, output download, etc). This may result in some jobs being corrupted.', t_total)
            return False
    # if there are non-critical threads then wait or shutdown depending on
    # configuration
    elif non_critical_thread_ids:
        if t_total < config['forced_shutdown_first_prompt_time']:
            return True
        else:
            return False
    # if there are no threads then shutdown
    else:
        return False

at_exit_should_wait_cb = None

current_shutdown_policy = None

def change_atexitPolicy(interactive_session=True, new_policy=None):

    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import config

    if new_policy is None:
        # select the shutdown method based on configuration and/or session type
        forced_shutdown_policy = config['forced_shutdown_policy']
    else:
        forced_shutdown_policy = new_policy

    if forced_shutdown_policy == 'interactive':
        should_wait_cb = should_wait_interactive_cb
    else:
        if forced_shutdown_policy == 'batch':
            should_wait_cb = should_wait_batch_cb
        else:
            if interactive_session:
                should_wait_cb = should_wait_interactive_cb
            else:
                should_wait_cb = should_wait_batch_cb

    global at_exit_should_wait_cb
    if at_exit_should_wait_cb is None:
        at_exit_should_wait_cb = should_wait_cb
        import atexit
        from Ganga.Core.GangaThread import GangaThreadPool
        # create generic Ganga thread pool
        thread_pool = GangaThreadPool.getInstance()
        atexit.register(
            thread_pool.shutdown, should_wait_cb=at_exit_should_wait_cb)
    else:
        at_exit_should_wait_cb = should_wait_cb


    global current_shutdown_policy
    current_shutdown_policy = forced_shutdown_policy

def getCurrentShutdownPolicy():
    global current_shutdown_policy
    if current_shutdown_policy is None:
        current_shutdown_policy = config['forced_shutdown_policy']
        return config['forced_shutdown_policy']
    return current_shutdown_policy

