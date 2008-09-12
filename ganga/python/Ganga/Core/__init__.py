"""
Core package defines the fundamental subsystems of Ganga Architecture.
Subsystems are autonomous components (such as a remote services) which may be independetly deployed.
Subsystems may also be created as local objects in the Ganga Client process.
"""

from exceptions import *

monitoring_component = None

def set_autostart_policy(interactive_session):
    """
    Change the default value of autostart of the monitoring, depending if the session is interactive or batch.
    The autostart value may be overriden in the config file, so warn if it differs from the default.
    This function should be called
    """
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import config
        

def bootstrap(reg, interactive_session):
    """
    Create local subsystems. In the future this procedure should be enhanced to connect to remote subsystems.
    FIXME: this procedure should be moved to the Runtime package.

    This function will change the default value of autostart of the monitoring, depending if the session is interactive or batch.
    The autostart value may be overriden in the config file, so warn if it differs from the default.
    """
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import JobRegistry_Monitor, config

    from Ganga.Utility.logging import getLogger

    logger = getLogger()
    #start the internal services coordinator    
    from Ganga.Core.InternalServices import Coordinator,ShutdownManager
    Coordinator.bootstrap()
    #load the shutdown manager    
    ShutdownManager.install()

    # backend-specific setup (e.g. Remote: setup any remote ssh pipes)
    for j in reg:
        if hasattr(j.backend,'setup'): # protect: EmptyGangaObject does not have setup() method
            j.backend.setup()
            
    #start the monitoring loop
    global monitoring_component
    monitoring_component = JobRegistry_Monitor( reg )
    monitoring_component.start()
    
    #register the MC shutdown hook
    import atexit
    #in interactive sessions we ask user whether a retry attempt should be made 
    #when the internal MC fails to stop gracefully
    if interactive_session:
        def mc_fail_cb():
            resp = raw_input("Job status update or output download still in progress (monitoring shutdown not completed yet). \n" 
                             "Do you want to force the exit (y/[n])")
            return resp.lower() != 'y'
    else: #in *scripts* mode always try a clean shutdown        
        def mc_fail_cb():
            return True    
    #register the exit function with the highest priority (==0)    
    atexit.register((0,monitoring_component.stop), fail_cb=mc_fail_cb,max_retries=config['max_shutdown_retries'])
    #export to GPI
    from Ganga.Runtime.GPIexport import exportToGPI
    exportToGPI('runMonitoring',monitoring_component.runMonitoring,'Functions')     

    autostart_default = interactive_session
    config.overrideDefaultValue('autostart',bool(autostart_default))

    if config['autostart'] is not autostart_default:
        msg = 'monitoring loop %s (the default setting for %s session is %s)'
        val = { True : ('enabled', 'batch', 'disabled'), 
                False: ('disabled', 'interactive', 'enabled')}
        logger.warning(msg%val[config['autostart']])

    if config['autostart']:        
        monitoring_component.enableMonitoring()

