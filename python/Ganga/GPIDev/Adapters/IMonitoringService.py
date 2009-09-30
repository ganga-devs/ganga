
class IMonitoringService:
    """ Interface of the monitoring service.
    Each method correponds to particular events which occur at ganga client or job wrapper (worker node).
    The event() method is retained for backwards compatibility and other methods by default use it.
    However it will be removed in the future.
    """
    def __init__(self, job_info):
        """ Initialize the monitoring service. If the monitoring service is created in ganga client then the job_info is the original job object. If the monitoring service is created on the worker node then the job_info is the result of a call to the getJobInfo() method.
        """
        self.job_info = job_info
        
    def start(self, **opts):
        """Application is about to start on the worker node.
        Called by: job wrapper.
        """
        return self.event('start')
    
    def progress(self,**opts):
        """Application execution is in progress (called periodically, several times a second).
        Called by: job wrapper. """
        return self.event('progress')

    def stop(self,exitcode,**opts):
        """Application execution finished.
        Called by: job wrapper. """
        return self.event('end',{'exitcode':exitcode})

    def prepare(self,**opts):
        """Preparation of a job.
        Called by: ganga client. """
        return self.event('prepare',{'job':self.job_info})

    def submitting(self,**opts):
        """Just before the submission of a job.
        Called by: ganga client. """
        return self.event('submitting',{'job':self.job_info})
        
    def submit(self,**opts):
        """Submission of a job.
        Called by: ganga client. """
        return self.event('submit',{'job':self.job_info})

    def complete(self,**opts):
        """Completion of a job (successful or failed).
        Called by: ganga client. """
        return self.event('complete',{'job':self.job_info})

    def rollback(self,**opts):
        """Rollback of a job to new state (caused by error during submittage).
        Called by: ganga client. """
        return self.event('rollback',{'job':self.job_info})

    def getSandboxModules(self):
        """ Get the list of module dependencies of this monitoring module.
        Called by: ganga client.

        Returns a list of modules which are imported by this module and
        therefore must be shipped automatically to the worker node. The list
        should include the module where this class is defined plus all modules
        which represent the parent packages. The module containing the
        IMonitoringService class is added automatically by the call to the
        base class sandBoxModule() method. An example for a class defined in
        the module Ganga/Lib/MonitoringServices/DummyMS/DummyMS.py which does
        not have any further dependencies:
        
        import Ganga.Lib.MonitoringServices.DummyMS
        return IMonitoringService.getSandboxModules(self) + [
                 Ganga,
                 Ganga.Lib,
                 Ganga.Lib.MonitoringServices,
                 Ganga.Lib.MonitoringServices.DummyMS,
                 Ganga.Lib.MonitoringServices.DummyMS.DummyMS
                ]
        Note, that it should be possible to import all parent modules without side effects (so without importing automatically their other children).
        """
        import Ganga.GPIDev.Adapters.IMonitoringService

        return [ Ganga, Ganga.GPIDev, Ganga.GPIDev.Adapters, Ganga.GPIDev.Adapters.IMonitoringService ]

    def getJobInfo(self):
        """ Return a static info object which static information about the job
        at submission time. Called by: ganga client.
        
        The info object is passed to the contructor. Info
        object may only contain the standard python types (such as lists,
        dictionaries, int, strings).  """

        return None

    def getWrapperScriptConstructorText(self):
        """ Return a line of python source code which creates the instance of the monitoring service object to be used in the job wrapper script. This method should not be overriden.
        """
        text =  "def createMonitoringObject(): from %s import %s; return %s(%s)\n" % (self._mod_name,self.__class__.__name__,self.__class__.__name__,self.getJobInfo())

        return text


    ### COMPATIBILITY INTERFACE - INTERNAL AND OBSOLETE
        
    def event(self,what='',values={}):
        """Obsolete method. """
        pass
