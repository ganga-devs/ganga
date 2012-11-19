from Ganga.Lib.MonitoringServices.ARDADashboard.LCG.ARDADashboardLCG import ARDADashboardLCG
from types import DictionaryType, IntType

class ARDADashboardLCGExecutable(ARDADashboardLCG):
    
    application = 'unknown'
    applicationVersion = 'unknown'
    dataset = 'unknown'
    activity = 'unknown'
    
    def __init__(self, job_info):
        
        ARDADashboardLCG.__init__(self, job_info)
        if self._complete == False:
            return
        
        self._complete = False
        if type(job_info) is DictionaryType:
            # we are on the worker node
            try:
                self._complete = True
            except KeyError,msg:
                return
        
        else:
            # we are on the client. We get the info from the job_info
            # (which is a job)
            
            self._complete = True

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.ARDADashboard.LCG
        return ARDADashboardLCG.getSandboxModules(self) + [Ganga.Lib.MonitoringServices.ARDADashboard.LCG.ARDADashboardLCGExecutable]
        
    def submit(self, **opts):
        
        if self._complete:
            try:
                self.dashboard.sendValues(message = {
                   'Application':self.application,
                   'ApplicationVersion':self.applicationVersion,
                   'DatasetName':self.dataset,
                   'GridJobID':self.gridJobId,
                   'GridUser':self.gridCertificate,
                   'JSTool':'ganga',
                   'Scheduler':self.gridBackend,
                   'TaskType':self.activity,
                   'VO':self.VO
                   },
               jobId=self.gangaJobId + '_' + self.gridJobId,
               taskId=self.gangaTaskId
                )
            except Exception,msg:
                self._logger.debug('could not send monalisa message: %s' % msg)