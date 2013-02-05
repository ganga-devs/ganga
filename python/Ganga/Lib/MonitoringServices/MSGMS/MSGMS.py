"""GangaMon Monitoring Service plugin."""

from Ganga.Lib.MonitoringServices.MSGMS import MSGUtil


def _initconfig():
    """Initialize MSGMS configuration."""
    try:
        from Ganga.Utility import Config
        # create configuration
        config = Config.makeConfig('MSGMS','Settings for the MSGMS monitoring plugin. Cannot be changed ruding the interactive Ganga session.')
        config.addOption('server', 'dashb-mb.cern.ch', 'The server to connect to')
        config.addOption('port', 6163, 'The port to connect to')
        config.addOption('username', 'ganga', '') 
        config.addOption('password', 'analysis', '') 
        config.addOption('message_destination', '/queue/ganga.status', '')
        config.addOption('usage_message_destination',"/queue/ganga.usage",'')
        config.addOption('job_submission_message_destination',"/queue/ganga.jobsubmission",'')
        
        # prevent modification during the interactive ganga session
        def deny_modification(name,x):
            raise Config.ConfigError('Cannot modify [MSGMS] settings (attempted %s=%s)' % (name, x))
        config.attachUserHandler(deny_modification, None)
    except ImportError:
        # on worker node so Config is not needed since it is copied to MSGMS constructor
        pass
_initconfig()


# singleton publisher
_publisher = None
def _get_publisher(server, port, username, password):
    #FIXME: this assumes server/port/username/password cannot change and caches a singleton publisher
    global _publisher
    if _publisher is None:
        _publisher = MSGUtil.createPublisher(server,port,username,password)
        _publisher.start()
    return _publisher


from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService
class MSGMS(IMonitoringService):
    """GangaMon Monitoring Service based on MSG.
    
    Publishes job meta-data to an MSG destination on submit, start and stop events
    for consumption by a GangaMon subscriber.
    
    See IMonitoringService for implementation details.
    """

    def __init__(self, job_info, config_info):
        """Construct the GangaMon monitoring service."""
        IMonitoringService.__init__(self, job_info, config_info)

    def getConfig():
        """Return MSGMS Config object."""
        from Ganga.Utility import Config
        return Config.getConfig('MSGMS')
    getConfig = staticmethod(getConfig)

    def getSandboxModules(self):
        """Return list of MSGMS module dependencies."""
        import Ganga.Lib.MonitoringServices.MSGMS
        return IMonitoringService.getSandboxModules(self) + [
            Ganga.Lib.MonitoringServices.MSGMS,
            Ganga.Lib.MonitoringServices.MSGMS.MSGMS,
            ] + MSGUtil.getSandboxModules()

    def getJobInfo(self): # called on client, so job_info is Job object
        """Create job_info from Job object."""
        if self.job_info.master is None: # no master job; this job is not splitjob
            ganga_job_id = str(self.job_info.id)
            ganga_job_uuid = self.job_info.info.uuid
            ganga_master_uuid = 0 
        else: # there is a master job; we are in a subjob
            ganga_job_id = str(self.job_info.master.id) + '.' + str(self.job_info.id)
            ganga_job_uuid = self.job_info.info.uuid
            ganga_master_uuid = self.job_info.master.info.uuid
        from Ganga.Utility import Config
        return { 'ganga_job_uuid' : ganga_job_uuid
               , 'ganga_master_uuid' : ganga_master_uuid 
               , 'ganga_user_repository' : Config.getConfig('Configuration')['user']
                                           + '@' + Config.getConfig('System')['GANGA_HOSTNAME']
                                           + ':' + Config.getConfig('Configuration')['gangadir']
               , 'ganga_job_id' : ganga_job_id
               , 'subjobs' : len(self.job_info.subjobs)
               , 'backend' : self.job_info.backend.__class__.__name__
               , 'application' : self.job_info.application.__class__.__name__
               , 'job_name' : self.job_info.name
               , 'hostname' : '' # place-holder updated in getMessage
               , 'event' : '' # place-holder updated in getMessage
               }

    def getMessage(self, event):
        """Create message from job_info adding hostname and event."""
        import types
        if isinstance(self.job_info, types.DictType):
            # on worker node so just copy job_info
            message = self.job_info.copy()
        else:
            # on client so just create job_info
            message = self.getJobInfo()
        message['hostname'] = hostname()
        message['event'] = event
        return message

    def send(self, message):
        """Send the message to the configured destination."""
        # get publisher
        p = _get_publisher(
                self.config_info['server'],
                self.config_info['port'],
                self.config_info['username'], 
                self.config_info['password'],
            )
        # send message
        headers = {'persistent':'true'}
        p.send(self.config_info['message_destination'], repr(message), headers)

    def submit(self, **opts): # called on client, so job_info is Job object
        """Log submit event on client."""
        # if this job has a master and it is the first subjob then sent submitted for master job
        if self.job_info.master is not None:
            if self.job_info.id == 0:
                masterjob_msg = self.getMessage('submitted')
                masterjob_msg['subjobs'] = len(self.job_info.master.subjobs)
                masterjob_msg['ganga_job_id'] = str(masterjob_msg['ganga_job_id']).split('.')[0]
                # override ganga_job_uuid as the message 'from the master' is really sent from the subjob
                masterjob_msg['ganga_job_uuid'] = masterjob_msg['ganga_master_uuid']
                masterjob_msg['ganga_master_uuid'] = 0
                self.send(masterjob_msg)

        from Ganga.Utility import Config
        gangausername = Config.getConfig('Configuration')['user']
        self.job_info.info.monitoring_links.append(('http://gangamon.cern.ch/ganga/#user=%s'%gangausername,'dashboard'))

        # send submitted for this job
        msg = self.getMessage('submitted')
        self.send(msg)

    def start(self, **opts): # called on worker node, so job_info is dictionary
        """Log start event on worker node."""
        message = self.getMessage('running')
        self.send(message)

    def progress(self, **opts): # called on worker node, so job_info is dictionary
        """Log progress event on worker node. NOP."""
        pass

    def stop(self, exitcode, **opts): # called on worker node, so job_info is dictionary
        """Log stop event on worker node."""
        if exitcode == 0:
            event = 'finished'
        else:
            event = 'failed'
        message = self.getMessage(event)
        self.send(message)


# utility method copied from Ganga.Utility.util
def hostname():
    """ Try to get the hostname in the most possible reliable way as described in the Python 
LibRef."""
    import socket
    try:
        return socket.gethostbyaddr(socket.gethostname())[0]
    # [bugfix #20333]: 
    # while working offline and with an improper /etc/hosts configuration       
    # the localhost cannot be resolved 
    except:
        return 'localhost'
