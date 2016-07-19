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


def start_jobregistry_monitor(reg_slice):
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import JobRegistry_Monitor
    # start the monitoring loop
    global monitoring_component
    monitoring_component = JobRegistry_Monitor(reg_slice)
    monitoring_component.start()


def bootstrap(reg_slice, interactive_session, my_interface=None):
    """
    Create local subsystems. In the future this procedure should be enhanced to connect to remote subsystems.
    FIXME: this procedure should be moved to the Runtime package.

    This function will change the default value of autostart of the monitoring, depending if the session is interactive or batch.
    The autostart value may be overriden in the config file, so warn if it differs from the default.
    """
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import JobRegistry_Monitor, config
    from Ganga.Utility.logging import getLogger

    logger = getLogger()

    from Ganga.Core.GangaThread import GangaThreadPool

    # start the internal services coordinator
    from Ganga.Core.InternalServices import Coordinator
    Coordinator.bootstrap()

    # backend-specific setup (e.g. Remote: setup any remote ssh pipes)
    # for j in reg_slice:
    #    if hasattr(j,'status') and j.status in ['submitted','running']:
    #        if hasattr(j,'backend'): # protect: EmptyGangaObject does not have backend either
    #            if hasattr(j.backend,'setup'): # protect: EmptyGangaObject does not have setup() method
    #                j.backend.setup()

    start_jobregistry_monitor(reg_slice)

    # Set the shutdown policy depending on whether we're interactive or not
    if config['forced_shutdown_policy'] in ['interactive', 'batch']:
        GangaThreadPool.shutdown_policy = config['forced_shutdown_policy']
    else:
        if interactive_session:
            GangaThreadPool.shutdown_policy = 'interactive'
        else:
            GangaThreadPool.shutdown_policy = 'batch'

    # export to GPI moved to Runtime bootstrap

    autostart_default = interactive_session
    config.overrideDefaultValue('autostart', bool(autostart_default))

    if config['autostart'] is not autostart_default:
        msg = 'monitoring loop %s (the default setting for %s session is %s)'
        val = {True: ('enabled', 'batch', 'disabled'),
               False: ('disabled', 'interactive', 'enabled')}
        logger.warning(msg % val[config['autostart']])

    if config['autostart']:
        monitoring_component.enableMonitoring()

    if not my_interface:
        import Ganga.GPI
        my_interface = Ganga.GPI
    from Ganga.Runtime.GPIexport import exportToInterface
    exportToInterface(my_interface, 'runMonitoring', monitoring_component.runMonitoring, 'Functions')

