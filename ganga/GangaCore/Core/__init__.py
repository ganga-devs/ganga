"""Core module containing repository, registry, thread and monitoring services, etc.
The Core package defines the fundamental subsystems of Ganga Architecture.
Subsystems are autonomous components (such as a remote services) which may be independently deployed.
Subsystems may also be created as local objects in the Ganga Client process.

Attributes:
    monitoring_component (JobRegistry_Monitor): Global variable that is set to the single global monitoring thread. Set
        in the bootstrap function.
"""
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
            it to GangaCore.GPI
    """
    # Must do some Ganga imports here to avoid circular importing
    from GangaCore import GANGA_SWAN_INTEGRATION
    from GangaCore.Core.MonitoringComponent.Local_GangaMC_Service import JobRegistry_Monitor
    from GangaCore.Utility.Config import getConfig
    from GangaCore.Runtime.GPIexport import exportToInterface
    from GangaCore.Utility.logging import getLogger
    global monitoring_component

    # start the monitoring loop
    monitoring_component = JobRegistry_Monitor(reg_slice)
    monitoring_component.start()

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
        import GangaCore.GPI
        my_interface = GangaCore.GPI

    exportToInterface(my_interface, 'runMonitoring', monitoring_component.runMonitoring, 'Functions')
    if GANGA_SWAN_INTEGRATION:
        exportToInterface(my_interface, 'reloadJob', monitoring_component.reloadJob, 'Functions')
