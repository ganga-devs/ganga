from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService

class DummyMS(IMonitoringService):
    def __init__(self, job_info):
        IMonitoringService.__init__(self,job_info)
        
    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.DummyMS
        return IMonitoringService.getSandboxModules(self) + [
            Ganga,
            Ganga.Lib,
            Ganga.Lib.MonitoringServices,
            Ganga.Lib.MonitoringServices.DummyMS,
            Ganga.Lib.MonitoringServices.DummyMS.DummyMS
            ]
        


