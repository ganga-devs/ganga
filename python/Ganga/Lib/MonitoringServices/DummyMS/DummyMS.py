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
        

    def submit(self, **opts): # called on client, so job_info is Job object
        """Log submit event on client."""
        #append monitoring link
        #self.job_info.info.monitoring_links.append('http://ganga.web.cern.ch/ganga/')
        #self.job_info.info.monitoring_links.append('http://ganga.web.cern.ch/ganga/user/index.php')
        self.job_info.info.monitoring_links = [('http://atlas.ch/','atlas'), ('http://cms.web.cern.ch/cms/index.html', 'cms')]

