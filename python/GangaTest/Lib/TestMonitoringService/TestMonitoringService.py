
class TestMonitoringService:
    def __init__(self, jobID, workdir='',application=''):
        self._jobID = jobID
        self.workdir = workdir
        print 'TestMonitoringService initialized: jobID=%s workdir=%s application=%s'%(jobID,workdir,application)
        
    def send(self):
        print 'TestMonitoringService.send'

    def event(self,what='',values={}):
        print 'TestMonitoringService.event: what=%s values=%s'%(str(what),str(values))

    def sandBoxModule(self):
        import GangaTest.Lib.TestMonitoringService
        return [
               GangaTest,
               GangaTest.Lib,
               GangaTest.Lib.TestMonitoringService,
               GangaTest.Lib.TestMonitoringService.TestMonitoringService
               ]
        


