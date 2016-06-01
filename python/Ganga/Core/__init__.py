"""Core module containing repository, registry, thread and monitoring services, etc.

The Core package defines the fundamental subsystems of Ganga Architecture.
Subsystems are autonomous components (such as a remote services) which may be independently deployed.
Subsystems may also be created as local objects in the Ganga Client process.

Attributes:
    monitoring_component (JobRegistry_Monitor): Global variable that is set to the single global monitoring thread. Set
        in the bootstrap function.
"""

# System Imports
import time

# Ganga Imports
from Ganga.Utility.Decorators import static_vars
from Ganga.Core.exceptions import GangaException, ApplicationConfigurationError, \
    BackendError, RepositoryError, BulkOperationRepositoryError, \
    IncompleteJobSubmissionError, IncompleteKillError, JobManagerError, \
    GangaAttributeError, GangaValueError, ProtectedAttributeError, \
    ReadOnlyObjectError, TypeMismatchError, SchemaError, ApplicationPrepareError, \
    GangaIOError, SplitterError

# Globals
monitoring_component = None


def bootstrap(reg_slice, interactive_session, my_interface=None):
    """Create local subsystems.

    This function will change the default value of autostart of the monitoring, depending if the session is interactive
    or batch. The autostart value may be overridden in the config file, so warn if it differs from the default.

    Args:
        reg_slice (RegistrySlice): A registry slice encompassing the Registry to monitor,
            e.g. from getRegistrySlice('jobs') -> JobRegistry.getSlice()
        interactive_session (bool): Flag indicating an interactive session or not
        my_interface (Optional[module]): Public interface to add the runMonitoring function to, None (default) will set
            it to Ganga.GPI
    """
    # Must do some Ganga imports here to avoid circular importing
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import JobRegistry_Monitor
    from Ganga.Utility.Config import getConfig
    from Ganga.Runtime.GPIexport import exportToInterface
    from Ganga.Utility.logging import getLogger
    global monitoring_component

    # start the monitoring loop
    monitoring_component = JobRegistry_Monitor(reg_slice)
    monitoring_component.start()

    # register the MC shutdown hook
    change_atexitPolicy(interactive_session)

    # override the default monitoring autostart value with the setting from interactive session
    config = getConfig("PollThread")
    config.overrideDefaultValue('autostart', interactive_session)

    # has the user changed monitoring autostart from the default? if so, warn them
    if config['autostart'] != interactive_session:
        if config['autostart']:
            getLogger().warning('Monitoring loop enabled (the default setting for a batch session is disabled)')
        else:
            getLogger().warning('Monitoring loop disabled (the default setting for an interactive session is enabled)')

    # Enable job monitoring if requested
    if config['autostart']:
        monitoring_component.enableMonitoring()

    # export the runMonitoring function to the public interface
    if not my_interface:
        import Ganga.GPI
        my_interface = Ganga.GPI

    exportToInterface(my_interface, 'runMonitoring', monitoring_component.runMonitoring, 'Functions')
        

@static_vars(t_last=None)
def should_wait_interactive_cb(t_total, critical_thread_ids, non_critical_thread_ids):
    """Function to ask the user every so often whether to shutdown

    This callback is used in the GangaThreadPool.shutdown function. If there are critical threads, it will wait until
    [PollThread]forced_shutdown_first_prompt_time secs before prompting to force quit. Afterwards it will ask every
    additional [PollThread]forced_shutdown_prompt_time secs and prompt again. If there are only non-critical threads it
    will wait for [PollThread]forced_shutdown_first_prompt_time secs before forcing the exit.

    Args:
        t_total (float): Total amount of time passed while waiting for threads to stop
        critical_thread_ids (list): A list of the critical thread ids still running
        non_critical_thread_ids (list): A list of the non-critical thread ids still running
    """
    from Ganga.Utility.Config import getConfig
    config = getConfig("PollThread")

    # Set the last check time on first call
    if should_wait_interactive_cb.t_last is None:
        should_wait_interactive_cb.t_last = -time.time()

    if critical_thread_ids:
        # if there are critical threads then prompt user or wait depending on configuration
        if ((should_wait_interactive_cb.t_last < 0 and time.time() + should_wait_interactive_cb.t_last > config['forced_shutdown_first_prompt_time']) or
                (should_wait_interactive_cb.t_last > 0 and time.time() - should_wait_interactive_cb.t_last > config['forced_shutdown_prompt_time'])):
            msg = """Job status update or output download still in progress (shutdown not completed after %d seconds).
%d background thread(s) still running: %s.
Do you want to force the exit (y/[n])? """ % (t_total, len(critical_thread_ids), critical_thread_ids)
            resp = raw_input(msg)
            should_wait_interactive_cb.t_last = time.time()
            return resp.lower() != 'y'
        else:
            return True
    elif non_critical_thread_ids:
        # if there are non-critical threads then wait or shutdown depending on configuration
        if t_total < config['forced_shutdown_first_prompt_time']:
            return True
        else:
            return False

    # otherwise just shutdown
    return False


def should_wait_batch_cb(t_total, critical_thread_ids, non_critical_thread_ids):
    """Function to wait for appropriate time before forcing quit

    This callback is used in the GangaThreadPool.shutdown function. If there are critical threads, it will wait until
    [PollThread]forced_shutdown_first_prompt_time secs before prompting to force quit. After that, it will just quit.
    If there are only non-critical threads it will wait for [PollThread]forced_shutdown_first_prompt_time secs before
    forcing the exit.

    Args:
        t_total (float): Total amount of time passed while waiting for threads to stop
        critical_thread_ids (list): A list of the critical thread ids still running
        non_critical_thread_ids (list): A list of the non-critical thread ids still running
    """
    from Ganga.Utility.Config import getConfig
    from Ganga.Utility.logging import getLogger
    config = getConfig("PollThread")

    if critical_thread_ids:
        # if there are critical threads then wait or shutdown depending on configuration
        if t_total < config['forced_shutdown_timeout']:
            return True
        else:
            getLogger().warning('Shutdown was forced after waiting for %d seconds for background activities to finish '
                                '(monitoring, output download, etc). This may result in some jobs being corrupted.',
                                t_total)
            return False
    elif non_critical_thread_ids:
        # if there are non-critical threads then wait or shutdown depending on configuration
        if t_total < config['forced_shutdown_first_prompt_time']:
            return True
        else:
            return False

    # otherwise just shutdown
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
    at_exit_should_wait_cb = should_wait_cb

    global current_shutdown_policy
    current_shutdown_policy = forced_shutdown_policy

def getCurrentShutdownPolicy():
    global current_shutdown_policy
    if current_shutdown_policy is None:
        current_shutdown_policy = config['forced_shutdown_policy']
        return config['forced_shutdown_policy']
    return current_shutdown_policy

