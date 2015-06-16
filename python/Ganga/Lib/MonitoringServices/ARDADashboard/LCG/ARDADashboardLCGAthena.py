from Ganga.Lib.MonitoringServices.ARDADashboard.LCG.ARDADashboardLCG import ARDADashboardLCG
from types import DictionaryType, IntType


class ARDADashboardLCGAthena(ARDADashboardLCG):

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
                self.application = job_info['application']
                self.applicationVersion = job_info['applicationVersion']
                self.dataset = job_info['dataset'][0]
                self.activity = job_info['activity']
                self._complete = True
            except KeyError as msg:
                return

        else:
            # we are on the client. We get the info from the job_info
            # (which is a job)

            job = job_info

            self.application = job.application._name

            try:
                self.applicationVersion = job.application.atlas_release
            except AttributeError:
                self.applicationVersion = ''

            # activity
            if self.application in ['Athena', 'AthenaMC', 'AMAAthena']:
                self.activity = 'analysis'
            else:
                activity = 'unknown'

            # dataset name
            try:
                self.dataset = job.inputdata.dataset[0]
            except AttributeError:
                self.dataset = 'unknown'
            except IndexError:
                self.dataset = 'unknown'

            self._complete = True

    def getSandboxModules(self):
        import Ganga.Lib.MonitoringServices.ARDADashboard.LCG
        return ARDADashboardLCG.getSandboxModules(self) + [Ganga.Lib.MonitoringServices.ARDADashboard.LCG.ARDADashboardLCGAthena]

    def getJobInfo(self):
        if self._complete:
            job_info = ARDADashboardLCG.getJobInfo(self)
            job_info['application'] = self.application
            job_info['applicationVersion'] = self.applicationVersion
            job_info['dataset'] = self.dataset
            job_info['activity'] = self.activity
            self._logger.debug(job_info)
            return job_info
        else:
            self._logger.debug('incomplete job info')
            return {}

    def submit(self, **opts):

        if self._complete:
            try:
                self._logger.debug("sending to the monalisa server")
                self.dashboard.sendValues(message={
                    'Application': self.application,
                    'ApplicationVersion': self.applicationVersion,
                    'DatasetName': self.dataset,
                    'GridJobID': self.gridJobId,
                    'GridUser': self.gridCertificate,
                    'JSTool': 'ganga',
                    'Scheduler': self.gridBackend,
                    'TaskType': self.activity,
                    'VO': self.VO
                },
                    jobId=self.gangaJobId + '_' + self.gridJobId,
                    taskId=self.gangaTaskId
                )
                self._logger.debug("sent to the monalisa server")
            except Exception as msg:
                self._logger.debug('could not send monalisa message: %s' % msg)
        else:
            self._logger.debug(
                'did not send the monitoring message because the job is not complete')
