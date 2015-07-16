"""Dashboard LCG Monitoring Service plugin

N.B. This code is under development and should not generally be used or relied upon.

"""

# TODO: disable if backend is not LCG
# TODO: report LB state change times


from Ganga.Lib.MonitoringServices.Dashboard import CommonUtil
from Ganga.Lib.MonitoringServices.Dashboard import LCGUtil


from Ganga.Lib.MonitoringServices.Dashboard.DashboardMS import DashboardMS


class LCGMS(DashboardMS):

    """Dashboard LCG Monitoring Service based on MSG."""

    def __init__(self, job_info, config_info):
        """Construct the Dashboard LCG Monitoring Service."""
        DashboardMS.__init__(self, job_info, config_info)

    def getSandboxModules(self):
        """Return list of module dependencies."""
        import Ganga.Lib.MonitoringServices.Dashboard
        return DashboardMS.getSandboxModules(self) + [
            Ganga.Lib.MonitoringServices.Dashboard.CommonUtil,
            Ganga.Lib.MonitoringServices.Dashboard.LCGMS,
            Ganga.Lib.MonitoringServices.Dashboard.LCGUtil,
        ]

    def getJobInfo(self):
        """Create job_info from Job object."""
        j = self.job_info  # called on client, so job_info is Job object
        ji = {
            'fqid': j.fqid,
            'EXECUTION_BACKEND': LCGUtil.cl_execution_backend(j),
            'OWNERDN': LCGUtil.cl_ownerdn(),
            'JOB_ID_INSIDE_THE_TASK': LCGUtil.cl_job_id_inside_the_task(j),
            'TASKNAME': LCGUtil.cl_task_name(j),
            'UNIQUEJOBID': LCGUtil.cl_unique_job_id(j),
        }
        return ji

    #----- event call-backs -----

    def submitting(self, **opts):
        j = self.job_info  # called on client, so job_info is Job object
        self._log('debug', 'submitting %s' % j.fqid)

    def prepare(self, **opts):
        j = self.job_info  # called on client, so job_info is Job object
        self._log('debug', 'prepare %s' % j.fqid)

    def submit(self, **opts):
        """Log submit event on client."""
        j = self.job_info  # called on client, so job_info is Job object
        self._log('debug', 'submit %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log(
                'debug', 'Not sending unwanted message on submit for master wrapper job %s.' % j.fqid)
            return
        # send Ganga submitted job-status message
        message = self._cl_job_status_message(
            'submitted', 'Ganga', CommonUtil.utcnow())
        if message['GRIDJOBID'] is None:
            # This is to handle the temporary workaround in
            # LCG.master_bulk_updateMonitoringInformation() which results in two
            # submit messages being sent, one without a grid_job_id.
            self._log(
                'debug', 'Not sending redundant message on submit without grid_job_id for job %s.' % j.fqid)
        else:
            self._send(self.config_info['destination_job_status'], message)

    def start(self, **opts):
        """Log start event on worker node."""
        ji = self.job_info  # called on worker node, so job_info is dictionary
        self._log('debug', 'start %s' % ji['fqid'])
        # send Ganga running job-status message
        message = self._wn_job_status_message(
            'running', 'Ganga', CommonUtil.utcnow())
        self._send(self.config_info['destination_job_status'], message)

    def stop(self, exitcode, **opts):
        """Log stop event on worker node."""
        ji = self.job_info  # called on worker node, so job_info is dictionary
        self._log('debug', 'stop %s' % ji['fqid'])
        if exitcode == 0:
            status = 'completed'
        else:
            status = 'failed'
        # send Ganga completed or failed job-status message
        message = self._wn_job_status_message(
            status, 'Ganga', CommonUtil.utcnow())
        message['JOBEXITCODE'] = exitcode
        message['JOBEXITREASON'] = None  # TODO: how can we know this?
        self._send(self.config_info['destination_job_status'], message)

    def complete(self, **opts):
        """Log complete event on client."""
        j = self.job_info  # called on client, so job_info is Job object
        self._log('debug', 'complete %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log(
                'debug', 'Not sending unwanted message on complete for master wrapper job %s.' % j.fqid)
            return
        # send LB Done job-status message
        message = self._cl_job_status_message(
            LCGUtil.cl_grid_status(j), 'LB', None)
        message['GRIDEXITCODE'] = LCGUtil.cl_grid_exit_code(j)
        message['GRIDEXITREASON'] = LCGUtil.cl_grid_exit_reason(j)
        self._send(self.config_info['destination_job_status'], message)

    def fail(self, **opts):
        """Log fail event on client."""
        j = self.job_info  # called on client, so job_info is Job object
        self._log('debug', 'fail %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log(
                'debug', 'Not sending unwanted message on fail for master wrapper job %s.' % j.fqid)
            return
        # send LB Done or Aborted job-status message
        message = self._cl_job_status_message(
            LCGUtil.cl_grid_status(j), 'LB', None)
        message['GRIDEXITCODE'] = LCGUtil.cl_grid_exit_code(j)
        message['GRIDEXITREASON'] = LCGUtil.cl_grid_exit_reason(j)
        self._send(self.config_info['destination_job_status'], message)

    def kill(self, **opts):
        """Log kill event on client."""
        j = self.job_info  # called on client, so job_info is Job object
        self._log('debug', 'kill %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log(
                'debug', 'Not sending unwanted message on kill for master wrapper job %s.' % j.fqid)
            return
        # send LB Cancelled job-status message
        message = self._cl_job_status_message('Cancelled', 'LB', None)
        self._send(self.config_info['destination_job_status'], message)

    def rollback(self, **opts):
        j = self.job_info  # called on client, so job_info is Job object
        self._log('debug', 'rollback %s' % j.fqid)

    #----- message builders -----

    def _cl_job_status_message(self, status, status_source, status_start_time=None):
        # Not null: EXECUTION_BACKEND, GRIDJOBID, JOB_ID_INSIDE_THE_TASK,
        # TASKNAME, UNIQUEJOBID
        j = self.job_info  # called on client, so job_info is Job object
        msg = {
            # Actual CE. e.g. ce-3-fzk.gridka.de:2119/jobmanager-pbspro-atlasXS
            'DESTCE': LCGUtil.cl_dest_ce(j),
            'DESTSITE': None,  # Actual site. e.g. FZK-LCG2
            # Actual worker node hostname. e.g. c01-102-103.gridka.de
            'DESTWN': None,
            # Backend. e.g. LCG
            'EXECUTION_BACKEND': LCGUtil.cl_execution_backend(j),
            'GRIDEXITCODE': None,  # e.g. 0
            'GRIDEXITREASON': None,  # e.g. Job terminated successfully
            # e.g. https://grid-lb0.desy.de:9000/moqY5njFGurEuoDkkJmtBA
            'GRIDJOBID': LCGUtil.cl_grid_job_id(j),
            'JOBEXITCODE': None,  # e.g. 0
            'JOBEXITREASON': None,
            # subjob id e.g. 0
            'JOB_ID_INSIDE_THE_TASK': LCGUtil.cl_job_id_inside_the_task(j),
            # Grid certificate. e.g. /DC=ch/DC=cern/OU=Organic
            # Units/OU=Users/CN=dtuckett/CN=671431/CN=David Tuckett/CN=proxy
            'OWNERDN': LCGUtil.cl_ownerdn(),
            'REPORTER': 'ToolUI',  # e.g. ToolUI, JobWN
            # e.g. 2009-11-25T14:59:24.754249Z
            'REPORTTIME': CommonUtil.utcnow(),
            'STATENAME': status,  # e.g. submitted, Done (Success)
            'STATESOURCE': status_source,  # e.g. Ganga, LB
            # e.g. 2009-11-25T14:32:51.428988Z
            'STATESTARTTIME': status_start_time,
            # e.g. ganga:6702b50a-8a31-4476-8189-62ea5b8e00b3:TrigStudy
            'TASKNAME': LCGUtil.cl_task_name(j),
            # Ganga uuid e.g. 1c08ff3b-904f-4f77-a481-d6fa765813cb
            'UNIQUEJOBID': LCGUtil.cl_unique_job_id(j),
            '___fqid': j.fqid,
        }
        return msg

    def _wn_job_status_message(self, status, status_source, status_start_time):
        # Not null: EXECUTION_BACKEND, GRIDJOBID, JOB_ID_INSIDE_THE_TASK,
        # TASKNAME, UNIQUEJOBID
        ji = self.job_info  # called on worker node, so job_info is dictionary
        msg = {
            'DESTCE': LCGUtil.wn_dest_ce(ji),
            'DESTSITE': LCGUtil.wn_dest_site(ji),
            'DESTWN': LCGUtil.wn_dest_wn(),
            'EXECUTION_BACKEND': ji['EXECUTION_BACKEND'],
            'GRIDEXITCODE': None,
            'GRIDEXITREASON': None,
            'GRIDJOBID': LCGUtil.wn_grid_job_id(ji),
            'JOBEXITCODE': None,
            'JOBEXITREASON': None,
            'JOB_ID_INSIDE_THE_TASK': ji['JOB_ID_INSIDE_THE_TASK'],
            'OWNERDN': ji['OWNERDN'],
            'REPORTER': 'JobWN',
            'REPORTTIME': CommonUtil.utcnow(),
            'STATENAME': status,
            'STATESOURCE': status_source,
            'STATESTARTTIME': status_start_time,
            'TASKNAME': ji['TASKNAME'],
            'UNIQUEJOBID': ji['UNIQUEJOBID'],
            '___fqid': ji['fqid'],
        }
        return msg
