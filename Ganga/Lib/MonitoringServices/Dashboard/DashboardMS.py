"""Dashboard Monitoring Service plugin.

N.B. This code is under development and should not generally be used or relied upon.

"""


import sys

from Ganga.Lib.MonitoringServices.MSGMS import MSGUtil
from Ganga.Lib.MonitoringServices.Dashboard import FormatUtil


def _initconfig():
    """Initialize DashboardMS configuration."""
    try:
        from Ganga.Utility import Config
        # create configuration
        config = Config.makeConfig('DashboardMS', 'Settings for Dashboard Messaging Service.')
        config.addOption('server', 'dashb-mb.cern.ch', 'The MSG server name.')
        config.addOption('port', 61113, 'The MSG server port.')
        config.addOption('user', 'ganga-atlas', '')
        config.addOption('password', 'analysis', '')
        config.addOption('destination_job_status', '/topic/dashboard.atlas.jobStatus', 'The MSG destination (topic or queue) for job status messages.')
        config.addOption('destination_job_processing_attributes', '/topic/dashboard.atlas.jobProcessingAttributes', 'The MSG destination (topic or queue) for job processing attributes messages.')
        config.addOption('destination_job_meta', '/topic/dashboard.atlas.jobMeta', 'The MSG destination (topic or queue) for job meta messages.')
        config.addOption('destination_task_meta', '/topic/dashboard.atlas.taskMeta', 'The MSG destination (topic or queue) for task meta messages.')
        config.addOption('task_type', 'analysis', 'The type of task. e.g. analysis, production, hammercloud,...')
        # prevent modification during the interactive ganga session
        def deny_modification(name, value):
            raise Config.ConfigError('Cannot modify [DashboardMS] settings (attempted %s=%s)' % (name, value))
        config.attachUserHandler(deny_modification, None)
    except ImportError:
        # on worker node so Config is not needed since it is copied to DashboardMS constructor
        pass
_initconfig()


# singleton publisher
_publisher = None
def _get_publisher(server, port, username, password):
    """Return the singleton publisher, lazily instantiating it.
    
    N.B. The configuration enforces that server/port/username/password cannot
    change so this method can cache a singleton publisher.
    """
    global _publisher
    if _publisher is None:
        _publisher = MSGUtil.createPublisher(server, port, username, password)
        _publisher.start()
    return _publisher


from Ganga.GPIDev.Adapters import IMonitoringService
class DashboardMS(IMonitoringService.IMonitoringService):
    """Dashboard Monitoring Service base class.
    
    Subclasses should override getSandboxModules(), getJobInfo() and the event
    methods: submit(), start(), progress(), stop(), etc.. Typically, the event
    methods will use job_info and _send() to send job meta-data via MSG using
    the WLCG format.
    """
    
    def __init__(self, job_info, config_info):
        """Construct the Dashboard Monitoring Service."""
        IMonitoringService.IMonitoringService.__init__(self, job_info, config_info)
        # try to initialize logger or default to None if on worker node. see log()
        try:
            import Ganga.Utility.logging
            self._logger = Ganga.Utility.logging.getLogger()
        except ImportError:
            self._logger = None

    def getConfig():
        """Return DashboardMS Config object."""
        from Ganga.Utility import Config
        return Config.getConfig('DashboardMS')
    getConfig = staticmethod(getConfig)

    def getSandboxModules(self):
        """Return list of DashboardMS module dependencies."""
        import Ganga.Lib.MonitoringServices.Dashboard
        return IMonitoringService.IMonitoringService.getSandboxModules(self) + [
            Ganga.Lib.MonitoringServices.Dashboard,
            Ganga.Lib.MonitoringServices.Dashboard.DashboardMS,
            Ganga.Lib.MonitoringServices.Dashboard.FormatUtil,
            ] + MSGUtil.getSandboxModules()

    def _send(self, destination, message):
        """Send the message to the configured destination."""
        # get publisher
        p = _get_publisher(
                self.config_info['server'],
                self.config_info['port'],
                self.config_info['user'], 
                self.config_info['password'],
            )
        # send message
        headers = {'persistent':'true'}
        wlcg_msg = FormatUtil.dictToWlcg(message, include_microseconds=False)
        p.send(destination, wlcg_msg, headers)

    def _log(self, level='info', message=''):
        """Log message to logger on client or stderr on worker node."""
        if self._logger and hasattr(self._logger, level):
            getattr(self._logger, level)(message)
        else:
            print >>sys.stderr, '[DashboardMS %s] %s' % (level, message)

