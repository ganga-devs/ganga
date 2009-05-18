import MSGWrap

from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService

from types import DictionaryType
from time import time

class MSGMS(IMonitoringService):

    gangaJobId = None
    gangaTaskId = None
    gridJobId = None

    def __init__(self, job_info):
        IMonitoringService.__init__(self,job_info)

        if type(job_info) is DictionaryType:
            pass
        else:
            self.gridJobId = job_info.backend.id
            self.gangaJobId = str(job_info.id)
            if self.job_info.master is not None:
                self.gangaTaskId = "ganga_" + str(self.job_info.master.id) + "_"
            else:
                self.gangaTaskId = "ganga_" + str(self.job_info.id) + "_"
 

    def getJobInfo(self):
        if self.job_info.master is None:
            id = self.job_info.id
        else:
            id = str(self.job_info.master.id) + '.' + str(self.job_info.id)
            
        return { 'gangaJobId' : self.gangaJobId
               , 'gangaTaskId' : self.gangaTaskId
               , 'gridJobId' : self.gridJobId
               , 'id' : id
               , 'backend' : self.job_info.backend.__class__.__name__
               }

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.MSGMS
        return IMonitoringService.getSandboxModules(self) + [
            Ganga,
            Ganga.Lib,
            Ganga.Lib.MonitoringServices,
            Ganga.Lib.MonitoringServices.MSGMS,
            Ganga.Lib.MonitoringServices.MSGMS.MSGMS
            ]

    import MSGWrap

    def start(self, **opts): # same as with stop
        MSGWrap.sendJobStatusChange(self.job_info['id'], self.job_info['backend'], "running")
        pass

    def progress(self, **opts):
        return

    def stop(self, exitcode, **opts): #this one is on the woerker node; operate on dictonary or whatever getJobInfo returns
        if exitcode == 0:
            status = "finished"
        else:
            status = "failed"
        MSGWrap.sendJobStatusChange(self.job_info['id'], self.job_info['backend'], status)

    def submit(self, **opts): #this one is on the client side; so operate on Job object
        if self.job_info.master is None:
            id = self.job_info.id
            MSGWrap.sendJobSubmitted(self.job_info.name, self.job_info.id, time(), self.job_info.application.exe, 1, 1)
        else:
            id = str(self.job_info.master.id) + '.' + str(self.job_info.id)
            if self.job_info.id == 0:
                MSGWrap.sendJobSubmitted(self.job_info.name, self.job_info.master.id, time(), self.job_info.application.exe, len(self.job_info.master.subjobs), len(self.job_info.master.subjobs))

        MSGWrap.sendJobStatusChange(id, self.job_info.backend.__class__.__name__, "submitted")
