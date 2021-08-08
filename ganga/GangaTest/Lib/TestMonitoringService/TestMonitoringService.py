from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)

class TestMonitoringService:
    def __init__(self, jobID, workdir='',application=''):
        self._jobID = jobID
        self.workdir = workdir
        logger.info('TestMonitoringService initialized: jobID=%s workdir=%s application=%s'%(jobID,workdir,application))
        
    def send(self):
        logger.info('TestMonitoringService.send')

    def event(self,what='',values={}):
        logger.info('TestMonitoringService.event: what=%s values=%s'%(str(what),str(values)))

    def sandBoxModule(self):
        import GangaTest.Lib.TestMonitoringService
        return [
               GangaTest,
               GangaTest.Lib,
               GangaTest.Lib.TestMonitoringService,
               GangaTest.Lib.TestMonitoringService.TestMonitoringService
               ]
        


