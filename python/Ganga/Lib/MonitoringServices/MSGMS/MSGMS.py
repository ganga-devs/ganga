from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService

from types import DictionaryType
from time import time, sleep
import stomputil 

# create the configuration options for MSGMS
from Ganga.Utility.Config import makeConfig, getConfig
config = makeConfig('MSGMS','Settings for the MSGMS monitoring plugin. Cannot be changed ruding the interactive Ganga session.')
config.addOption('server', 'gridmsg101.cern.ch', 'The server to connect to')
config.addOption('port', 6163, 'The port to connect to')
config.addOption('username', '', '') 
config.addOption('password', '', '') 
config.addOption('message_destination', '/topic/ganga.status', '')

# prevent the modification of the MSGMS configuration during the interactive ganga session
import Ganga.Utility.Config
def deny_modification(name,x):
    raise Ganga.Utility.Config.ConfigError('Cannot modify [System] settings (attempted %s=%s)'%(name,x))
config.attachUserHandler(deny_modification,None)
                           

try:
    from Ganga.Core.GangaThread import GangaThread as Thread
except ImportError:
    pass
from threading import Thread

from Ganga.Utility.logging import getLogger
publisher = None

def send(dst, msg): # enqueue the msg in msg_q for the connection thread to consume and send
    global publisher
    if publisher is None:
        publisher = stomputil.createPublisher(Thread, config['server'], config['port'], username=config['username'],
            password=config['password'], logger=getLogger('MSGMSErrorLog'))
        publisher.start()
    publisher.send((dst, msg)) 

def sendJobStatusChange(msg):
    send(config['message_destination'], msg)
        
def sendJobSubmitted(msg):
    send(config['message_destination'], msg)

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

class MSGMS(IMonitoringService):

    #KUBA: we should send the following information
    # ganga_job_uuid - unique job identifier
    # ganga_job_master_uuid - unique job identifier of the master (==job_uuid if no master)
    # ganga_user_repository - "lostman@host:/path/to/repository"
    # ganga_job_id - plain job id which may be used as argument to jobs, e.g. jobs("i.k") in case of split job
    # application._name
    # job name

    def __init__(self, job_info):
        IMonitoringService.__init__(self,job_info)
        global config
        if type(job_info) is DictionaryType: # on the workernode
            for o in ['username', 'password', 'server', 'port', 'message_destination']:
                config.setSessionValue(o, job_info['_config'][o])
        from Ganga.Lib.MonitoringServices.MSGMS.compatibility import uuid
        self.ganga_job_uuid = uuid()

    def getMessage(self): # returns a dictionary that contains data common to all messages
        msg = self.job_info.copy() # copy the dictionary!
        msg['hostname'] = hostname() # override the hostname value with the worker host name
        return msg

    def getJobInfo(self): # more detailed message
        if self.job_info.master is None: # no master job; this job is not splitjob
            ganga_job_id = str(self.job_info.id)
            ganga_job_uuid = self.job_info.info.uuid
            ganga_master_uuid = 0 
        else: # there is a master job; we are in a subjob
            ganga_job_id = str(self.job_info.master.id) + '.' + str(self.job_info.id)
            ganga_job_uuid = self.job_info.info.uuid
            ganga_master_uuid = self.job_info.master.info.uuid

        return { 'ganga_job_uuid' : ganga_job_uuid
               , 'ganga_master_uuid' : ganga_master_uuid 
               , 'ganga_user_repository' : getConfig('Configuration')['user']
                                           + '@' + getConfig('System')['GANGA_HOSTNAME']
                                           + ':' + getConfig('Configuration')['gangadir']
               , 'ganga_job_id' : ganga_job_id
               , 'subjobs' : len(self.job_info.subjobs)
               , 'backend' : self.job_info.backend.__class__.__name__
               , 'application' : self.job_info.application.__class__.__name__
               , 'job_name' : self.job_info.name
               , 'hostname' : hostname()
               , 'event' : 'dummy' # should be updated in appropriate methods
               , '_config' : config.getEffectiveOptions() # pass the MSGMS configuration to the worker nodes
               }
        return data

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.MSGMS
        import Ganga.Lib.MonitoringServices.MSGMS.MSGMS
        return [
            Ganga,
            Ganga.Lib,
            Ganga.Lib.MonitoringServices,
            Ganga.Lib.MonitoringServices.MSGMS,
            Ganga.Lib.MonitoringServices.MSGMS.MSGMS,
            Ganga.Lib.MonitoringServices.MSGMS.compatibility,
            Ganga.Utility,
            Ganga.Utility.util,
            Ganga.Utility.logic,
            Ganga.Utility.logging,
            Ganga.Utility.strings,
            Ganga.Utility.files,
            Ganga.Utility.ColourText,
            Ganga.Utility.Config,
            Ganga.Utility.Config.Config,
            Ganga.GPIDev,
            Ganga.GPIDev.Lib,
            Ganga.GPIDev.Lib.Config,
            Ganga.GPIDev.Lib.Config.Config,
            Ganga.GPIDev.TypeCheck,
            Ganga.Core,
            Ganga.Core.exceptions,
            Ganga.Core.exceptions.GangaException,
            stomputil,
            stomputil.stompwrapper,
            stomputil.stomp
            ] + IMonitoringService.getSandboxModules(self)

    def start(self, **opts): # same as with stop
        import atexit
        atexit.register(sleep, 5)
        message = self.getMessage()
        message['event'] = 'running'
        sendJobStatusChange( message )

    def progress(self, **opts):
        pass
        #return

    def stop(self, exitcode, **opts): #this one is on the woerker node; operate on dictonary or whatever getJobInfo returns
        status = None
        if exitcode == 0:
            exit_status = "finished"
        else:
            exit_status = "failed"

        message = self.getMessage()
        message['event'] = exit_status
        sendJobStatusChange( message )

    def submit(self, **opts): #this one is on the client side; so operate on Job object
        # send 'submitted' message only from the master job
        #if self.job_info.master is None:
        #    sendJobSubmitted( msg )
        #else:
        #    if self.job_info.id == 0: sendJobSubmitted( msg ) #len(self.job_info.master.subjobs) -> number of subjobs

        # send 'submitted' message from all jobs
        if self.job_info.master is not None:
            if self.job_info.id == 0:
                masterjob_msg = self.getJobInfo()
                masterjob_msg['event'] = 'submitted'
                masterjob_msg['subjobs'] = len(self.job_info.master.subjobs)
                masterjob_msg['ganga_job_id'] = str(masterjob_msg['ganga_job_id']).split('.')[0]
                # override ganga_job_uuid as the message 'from the master' is really sent from the subjob
                masterjob_msg['ganga_job_uuid'] = masterjob_msg['ganga_master_uuid']
                masterjob_msg['ganga_master_uuid'] = 0
                sendJobSubmitted( masterjob_msg )

        #1. send job submitted message with more detailed info
        msg = self.getJobInfo()
        msg['event'] = 'submitted'
        sendJobSubmitted( msg )
            
            
        #2. send status change message with new submitted status
        # message = self.getCommonMessage()
        # message['event'] = 'submitted'
        # sendJobStatusChange( message )
