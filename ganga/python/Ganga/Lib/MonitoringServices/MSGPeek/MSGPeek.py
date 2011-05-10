import os.path
import string
import datetime
from time import sleep
from threading import Thread
from types import DictionaryType

from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService
from Ganga.GPIDev.Lib.Config.Config import config
try:
    from Ganga.Core.GangaThread import GangaThread as Thread
except ImportError:
    pass

import MSGUtil


"""Number of seconds to poll a queue where the order "streaming" the output of the job is received"""
NUM_SEC = 8
SessionNumber = 32

"""The thread is created to send the data and the status to the client"""
publisher = MSGUtil.createPublisher(Thread)
publisher.start()

def send(id, msg):
    """If all the job's status go to the same topic uncomment it and finish it""" 
    #dst = '/topic/job.status.%d' %SessionNumber
    dst = '/queue/data.%s' % id
    publisher.send((dst, msg))#, { "status": id })

def subscribe(id):
    dst = '/topic/control.session.%d' %SessionNumber #MSGPeekCollector.control
    sleep(0.4)
    publisher.connection.subscribe(destination=dst, ack='auto',
                                   headers={ 'selector' : "clientid = '%s'" % str(id)})

def unsubscribe(dst):
    dst = '/topic/control.session.%s' %MSGPeekCollector.SessionId
    publisher.connection.unsubscribe(destination=dst)

        
def is_streaming(id):
    if len(publisher.listener.streaming) <> 0 : 
        return publisher.listener.streaming.get(id, 'end') == 'begin'    
    return False


def hostname():
    import socket
    try:
        return socket.gethostbyaddr(socket.gethostname())[0]
    except:
        return 'localhost'

class MSGPeek(IMonitoringService):

    #KUBA: we should send the following information
    # ganga_job_uuid - unique job identifier
    # ganga_job_master_uuid - unique job identifier of the master (==job_uuid if no master)
    # ganga_user_repository - "lostman@host:/path/to/repository"
    # ganga_job_id - plain job id which may be used as argument to jobs, e.g. jobs("i.k") in case of split job
    # application._name
    # job name

    def __init__(self, job_info):
        IMonitoringService.__init__(self, job_info)
        from Ganga.Lib.MonitoringServices.MSGPeek.compatibility import uuid
        self.ganga_job_uuid = uuid()
        """stdoutFile actualPos are needed to know until where the client has read""" 
        self.stdoutFile = None
        self.actualPos = 0
        """The streaming is only check to regular intervals @NUM_SEC"""
        self.start_time = datetime.datetime.now()
        self.stream=False
        
   

    def getMessage(self): # returns a dictionary that contains data common to all messages
        msg = self.job_info.copy() 
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
               , 'backend' : self.job_info.backend.__class__.__name__
               , 'application' : self.job_info.application.__class__.__name__
               , 'job_name' : self.job_info.name
               , 'hostname' : hostname()
               , 'status' : self.job_info.status
               }
        return data

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.MSGPeek
        import Ganga.Lib.MonitoringServices.MSGPeek.MSGPeek
        return [
            Ganga,
            Ganga.Lib,
            Ganga.Lib.MonitoringServices,
            Ganga.Lib.MonitoringServices.MSGPeek,
            Ganga.Lib.MonitoringServices.MSGPeek.MSGPeek,
            Ganga.Lib.MonitoringServices.MSGPeek.MSGUtil,
            Ganga.Lib.MonitoringServices.MSGPeek.compatibility,
            Ganga.Lib.MonitoringServices.MSGPeek.stomp,
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
        send(str(self.job_info['ganga_job_id']), message)
        
        ## stdoutFile attribute is needed for reading the output
        if self.stdoutFile is None:
            self.stdoutFile = open('stdout', 'r+')

        subscribe(self.job_info['ganga_job_id'])      


    def progress(self, **opts):
        """ The message to the server is send when the output of the job if 
            obtained, the output of the job is written in 'stdout' file in 
            the folder belonging to the job, usually in: 
            ~/gangadir/workspace/USER/LocalAMGA/NUMBER-OF-JOB
            where USER is the user in the machine and NUMBER-OF-JOB is the number
            from Ganga when the job is submitted
        """
        """check only if @NUM_SEC has not been now and streaming signal haven't been received"""
        if self.stream or (datetime.datetime.now() - self.start_time) >= datetime.timedelta(seconds = NUM_SEC) :
            self.start_time  = datetime.datetime.now()
        
            if is_streaming(self.job_info['ganga_job_id']) :
                self.stream = True
                if self.stdoutFile is None:
                    self.stdoutFile = open('stdout', 'r+')
                                      
                msg = {}        
                LenStdout = self.stdoutFile.tell()
                self.stdoutFile.seek(self.actualPos)
                line = self.stdoutFile.read()                
                if not line or LenStdout < self.actualPos:
                    return
                else:
                    self.actualPos += len(line)
                    msg['stdout'] = line
                    msg['event'] = "Streaming"
                    send(str(self.job_info['ganga_job_id']) , msg)
            else :
                self.stream = False
    #            msg['event'] ="Job %s finished" %(self.job_info['ganga_job_id'])
        #pass
        #return


    def stop(self, exitcode, **opts):
        status = None
        if exitcode == 0:
            exit_status = "finished"
        else:
            exit_status = "failed"

        message = self.getMessage()
        message['event'] = exit_status
        send(str(self.job_info['ganga_job_id']), message)

        
        self.stdoutFile.close()
        #Remove the job from the dictionary
        publisher.listener.streaming.pop(self.job_info['ganga_job_id'], False)    
        unsubscribe(self.job_info['ganga_job_id'])
        
    def submit(self, **opts): #this one is on the client side; so operate on Job object
        #1. send job submitted message with more detailed info
        
        msg = self.getJobInfo()
        msg['event'] = 'submitted'
        dst = '/queue/data.%s' % msg['ganga_job_id']
        if self.job_info.master is None:
            send(id, msg)
        else:
            if self.job_info.id == 0: 
                send(id, msg)
            

