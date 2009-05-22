import MSGUtil

from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService

from types import DictionaryType
from time import time
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
    send('/topic/lostman-test/status', msg)
        
def sendJobSubmitted(msg):
    send('/topic/lostman-test/submitted', msg)

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
        return self.job_info
        

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
               , 'backend' : self.job_info.backend.__class__.__name__
               , 'application' : self.job_info.application.__class__.__name__
               , 'job_name' : self.job_info.name
               }
        return data

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.MSGMS
        return IMonitoringService.getSandboxModules(self) + [
            Ganga,
            Ganga.Lib,
            Ganga.Lib.MonitoringServices,
            Ganga.Lib.MonitoringServices.MSGMS,
            Ganga.Lib.MonitoringServices.MSGMS.MSGMS,
            ]

    def start(self, **opts): # same as with stop
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
        #1. send job submitted message with more detailed info
        msg = self.getJobInfo()
        msg['event'] = 'submitted'
        if self.job_info.master is None:
            sendJobSubmitted( msg )
        else:
            if self.job_info.id == 0: sendJobSubmitted( msg ) #len(self.job_info.master.subjobs) -> number of subjobs
            
        #2. send status change message with new submitted status
        # message = self.getCommonMessage()
        # message['event'] = 'submitted'
        # sendJobStatusChange( message )
