
class IMonitoringService:

    """ Interface of the monitoring service.
    Each method correponds to particular events which occur at ganga client or job wrapper (worker node).
    The event() method is retained for backwards compatibility and other methods by default use it.
    However it will be removed in the future.
    """

    def __init__(self, job_info, config_info=None):
        """Initialize the monitoring service.

        If the monitoring service is created in the Ganga client then job_info
        is the original job object, and config_info is the original config\
        object returned by getConfig().

        If the monitoring service is created on the worker node then job_info is
        the return value of getJobInfo(), and config_info is the return value of
        getConfig().getEffectiveOptions(), i.e. a dictionary.

        In order to support existing monitoring classes, which do not use
        config_info, if getConfig() returns None, the constructor is called with
        only the job_info argument.
        """
        self.job_info = job_info
        self.config_info = config_info

    def start(self, **opts):
        """Application is about to start on the worker node.
        Called by: job wrapper.
        """
        return self.event('start')

    def progress(self, **opts):
        """Application execution is in progress (called periodically, several times a second).
        Called by: job wrapper. """
        return self.event('progress')

    def stop(self, exitcode, **opts):
        """Application execution finished.
        Called by: job wrapper. """
        return self.event('end', {'exitcode': exitcode})

    def prepare(self, **opts):
        """Preparation of a job.
        Called by: ganga client. """
        return self.event('prepare', {'job': self.job_info})

    def submitting(self, **opts):
        """Just before the submission of a job.
        Called by: ganga client. """
        return self.event('submitting', {'job': self.job_info})

    def submit(self, **opts):
        """Submission of a job.
        Called by: ganga client. """
        return self.event('submit', {'job': self.job_info})

    def complete(self, **opts):
        """Completion of a job.
        Called by: ganga client. """
        return self.event('complete', {'job': self.job_info})

    def fail(self, **opts):
        """Failure of a job.
        Called by: ganga client. """
        return self.event('fail', {'job': self.job_info})

    def kill(self, **opts):
        """Killing of a job.
        Called by: ganga client. """
        return self.event('kill', {'job': self.job_info})

    def rollback(self, **opts):
        """Rollback of a job to new state (caused by error during submission).
        Called by: ganga client. """
        return self.event('rollback', {'job': self.job_info})

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
        import Ganga.GPIDev.Adapters
        return [Ganga, Ganga.GPIDev, Ganga.GPIDev.Adapters, Ganga.GPIDev.Adapters.IMonitoringService]

    def getJobInfo(self):
        """ Return a static info object which static information about the job
        at submission time. Called by: ganga client.

        The info object is passed to the contructor. Info
        object may only contain the standard python types (such as lists,
        dictionaries, int, strings).  """

        return None

    def getConfig():
        """Return the config object for this class.

        By default this method returns None. If it is overridden to
        return a Config object then the configuration is made available
        consistently in the Ganga client and on the worker node via the instance
        variable config_info. See __init__().

        For example, in your IMonitoringService implementation you could add::
            def getConfig():
                from Ganga.Utility import Config
                return Config.getConfig("MyMS")
            getConfig = staticmethod(getConfig)

        N.B. this is a static method.
        """
        return None
    getConfig = staticmethod(getConfig)

    def getWrapperScriptConstructorText(self):
        """ Return a line of python source code which creates the instance of the monitoring service object to be used in the job wrapper script. This method should not be overriden.
        """
        config = self.__class__.getConfig()
        if config is None:
            text = "def createMonitoringObject(): from %s import %s; return %s(%s)\n" % (
                self._mod_name, self.__class__.__name__, self.__class__.__name__, self.getJobInfo())
        else:
            text = "def createMonitoringObject(): from %s import %s; return %s(%s, %s)\n" % (self._mod_name,
                                                                                             self.__class__.__name__, self.__class__.__name__, self.getJobInfo(), config.getEffectiveOptions())

        return text

    # COMPATIBILITY INTERFACE - INTERNAL AND OBSOLETE

    def event(self, what='', values={}):
        """Obsolete method. """
        pass
