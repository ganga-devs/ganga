from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService

from types import DictionaryType
from time import time, sleep
from Ganga.GPIDev.Lib.Config.Config import config

import MSGUtil


try:
    from Ganga.Core.GangaThread import GangaThread as Thread
except ImportError:
    pass
from threading import Thread

publisher = MSGUtil.createPublisher(Thread)
publisher.start()

def send(dst, msg): # enqueue the msg in msg_q for the connection thread to consume and send
    publisher.send((dst, msg)) 

def sendJobStatusChange(msg):
    send('/queue/lostman-test/status', msg)
        
def sendJobSubmitted(msg):
    send('/queue/lostman-test/submitted', msg)

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
        from Ganga.Lib.MonitoringServices.MSGMS.compatibility import uuid
        self.ganga_job_uuid = uuid()

    def getMessage(self): # returns a dictionary that contains data common to all messages
        msg = self.job_info.copy() # copy the dictionary!
        msg['hostname'] = hostname() # override the hostname value with the worker host name
        return msg

    def getJobInfo(self): # more detailed message
        if self.job_info.master is None:
            ganga_job_id = str(self.job_info.id)
        else:
            ganga_job_id = str(self.job_info.master.id) + '.' + str(self.job_info.id)

        return { 'ganga_job_uuid' : self.ganga_job_uuid
               , 'ganga_job_master_uuid' : 0
               , 'ganga_user_repository' : config.Configuration.user
                                           + '@' + config.System.GANGA_HOSTNAME
                                           + ':' + config.Configuration.gangadir
               , 'ganga_job_id' : ganga_job_id
               , 'subjobs' : len(self.job_info.subjobs)
               , 'backend' : self.job_info.backend.__class__.__name__
               , 'application' : self.job_info.application.__class__.__name__
               , 'job_name' : self.job_info.name
               , 'hostname' : hostname()
               , 'event' : 'dummy' # should be updated in appropriate methods
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
            Ganga.Lib.MonitoringServices.MSGMS.MSGUtil,
            Ganga.Lib.MonitoringServices.MSGMS.compatibility,
            Ganga.Lib.MonitoringServices.MSGMS.stomp,
            Ganga.Utility,
            Ganga.Utility.logging,
            Ganga.Utility.strings,
            Ganga.Utility.files,
            #Ganga.Utility.files.remove_prefix,
            Ganga.Utility.ColourText,
            Ganga.Utility.Config,
            Ganga.Utility.Config.Config,
            Ganga.GPIDev,
            Ganga.GPIDev.Lib,
            Ganga.GPIDev.Lib.Config,
            Ganga.GPIDev.Lib.Config.Config,
            Ganga.Core,
            Ganga.Core.exceptions,
            Ganga.Core.exceptions.GangaException
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
        if self.job_info.master is None:
            pass
        else:
            if self.job_info.id == 0:
                masterjob_msg = self.getJobInfo()
                masterjob_msg['subjobs'] = len(self.job_info.master.subjobs)
                masterjob_msg['ganga_job_id'] = str(masterjob_msg['ganga_job_id']).split('.')[0]
                sendJobSubmitted( masterjob_msg )

        #1. send job submitted message with more detailed info
        msg = self.getJobInfo()
        msg['event'] = 'submitted'
        sendJobSubmitted( msg )
            
            
        #2. send status change message with new submitted status
        # message = self.getCommonMessage()
        # message['event'] = 'submitted'
        # sendJobStatusChange( message )
