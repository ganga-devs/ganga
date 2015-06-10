from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService

class OutputServerMS(IMonitoringService):
    """ A very simple debugging tool: in case of application failure, the stdout and stderr is sent back to the xmlrpm server on the client. This assumes that the stderr and stdout files are produced in CWD. Reliability of this mechanism depends on the network configuration (firewalls etc). Unless environment variable GANGA_OUTPUTSERVERMS_URL is defined, the server on local host on port 8182 is assumed. The variable GANGA_OUTPUTSERVERMS_URL should be of form: http://host.name:port.
    """
    def __init__(self, job_info):
        IMonitoringService.__init__(self,job_info)

    def stop(self,exitcode,*other):
        if exitcode:
            def send_file(name):
                try:
                    import xmlrpclib
                    s = xmlrpclib.ServerProxy(self.job_info['server_url'])
                    s.send_output(self.job_info['jobid'],name,file(name,'r').read())
                except Exception as x:
                    import sys
                    print >> sys.stderr, 'OutputServerMS exception raised while sending "%s": %s'%(name,str(x))
                    import traceback
                    traceback.print_exc()

            send_file('stderr')
            send_file('stdout')

    def getJobInfo(self):
        import os

        try:
            URL = os.environ['GANGA_OUTPUTSERVERMS_URL']
        except KeyError:
            from Ganga.Utility.util import hostname
            import ganga_output_server
            URL = 'http://%s:%d'%(hostname(),ganga_output_server.DEFAULT_PORT)
        
        return {'jobid':self.job_info.getFQID('.'),'server_url': URL}

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.OutputServerMS.OutputServerMS

        return [Ganga, Ganga.Lib, Ganga.Lib.MonitoringServices, Ganga.Lib.MonitoringServices.OutputServerMS, Ganga.Lib.MonitoringServices.OutputServerMS.OutputServerMS] + IMonitoringService.getSandboxModules(self)
    
