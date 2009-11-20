"""Dashboard LCG Monitoring Service plugin

N.B. This code is under development and should not generally be used or relied upon.

"""

#TODO: disable if backend is not LCG
#TODO: report LB state change times


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
        j = self.job_info # called on client, so job_info is Job object
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
        j = self.job_info # called on client, so job_info is Job object
        self._log('debug', 'submitting %s' % j.fqid)

    def prepare(self, **opts):
        j = self.job_info # called on client, so job_info is Job object
        self._log('debug', 'prepare %s' % j.fqid)

    def submit(self, **opts):
        """Log submit event on client."""
        j = self.job_info # called on client, so job_info is Job object
        self._log('debug', 'submit %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log('debug', 'Not sending unwanted message on submit for master wrapper job %s.' % j.fqid)
            return
        # create message (Ganga submitted)
        message = self._cl_job_status_message('submitted', 'Ganga', CommonUtil.utcnow())
        if message['GRIDJOBID'] is None:
            # This is to handle the temporary workaround in
            # LCG.master_bulk_updateMonitoringInformation() which results in two
            # submit messages being sent, one without a grid_job_id.
            self._log('debug', 'Not sending redundant message on submit without grid_job_id for job %s.' % j.fqid)
            return
        # send
        self._send(self.config_info['destination_job_status'], message)

    def start(self, **opts):
        """Log start event on worker node."""
        ji = self.job_info # called on worker node, so job_info is dictionary
        self._log('debug', 'start %s' % ji['fqid'])
        # create message (Ganga running)
        message = self._wn_job_status_message('running', 'Ganga', CommonUtil.utcnow())
        # send
        self._send(self.config_info['destination_job_status'], message)

    def progress(self, **opts):
        ji = self.job_info # called on worker node, so job_info is dictionary
        self._log('debug', 'progress %s' % ji['fqid'])

    def stop(self, exitcode, **opts):
        """Log stop event on worker node."""
        ji = self.job_info # called on worker node, so job_info is dictionary
        self._log('debug', 'stop %s' % ji['fqid'])
        if exitcode == 0:
            status = 'completed'
        else:
            status = 'failed'
        # create message (Ganga completed or failed)
        message = self._wn_job_status_message(status, 'Ganga', CommonUtil.utcnow())
        message['JOBEXITCODE'] = exitcode
        message['JOBEXITREASON'] = None #TODO: how can we know this?
        # send
        self._send(self.config_info['destination_job_status'], message)

    def complete(self, **opts):
        """Log complete event on client."""
        j = self.job_info # called on client, so job_info is Job object
        self._log('debug', 'complete %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log('debug', 'Not sending unwanted message on complete for master wrapper job %s.' % j.fqid)
            return
        # create message (LB Done)
        message = self._cl_job_status_message(LCGUtil.cl_grid_status(j), 'LB', None)
        message['GRIDEXITCODE'] = LCGUtil.cl_grid_exit_code(j)
        message['GRIDEXITREASON'] = LCGUtil.cl_grid_exit_reason(j)
        # send
        self._send(self.config_info['destination_job_status'], message)

    def fail(self, **opts):
        """Log fail event on client."""
        j = self.job_info # called on client, so job_info is Job object
        self._log('debug', 'fail %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log('debug', 'Not sending unwanted message on fail for master wrapper job %s.' % j.fqid)
            return
        # create message (LB Done or Aborted)
        message = self._cl_job_status_message(LCGUtil.cl_grid_status(j), 'LB', None)
        message['GRIDEXITCODE'] = LCGUtil.cl_grid_exit_code(j)
        message['GRIDEXITREASON'] = LCGUtil.cl_grid_exit_reason(j)
        # send
        self._send(self.config_info['destination_job_status'], message)

    def kill(self, **opts):
        """Log kill event on client."""
        j = self.job_info # called on client, so job_info is Job object
        self._log('debug', 'kill %s' % j.fqid)
        # ignore master wrapper jobs
        if j.subjobs:
            self._log('debug', 'Not sending unwanted message on kill for master wrapper job %s.' % j.fqid)
            return
        # create message (LB Cancelled)
        message = self._cl_job_status_message('Cancelled', 'LB', None)
        # send
        self._send(self.config_info['destination_job_status'], message)

    def rollback(self, **opts):
        j = self.job_info # called on client, so job_info is Job object
        self._log('debug', 'rollback %s' % j.fqid)

    #----- message builders -----

    def _cl_job_status_message(self, status, status_source, status_start_time=None):
        # Not null: EXECUTION_BACKEND, GRIDJOBID, JOB_ID_INSIDE_THE_TASK, TASKNAME, UNIQUEJOBID
        j = self.job_info # called on client, so job_info is Job object
        msg = {
            'DESTCE': LCGUtil.cl_dest_ce(j),
            'DESTSITE': None,
            'DESTWN': None,
            'EXECUTION_BACKEND': LCGUtil.cl_execution_backend(j),
            'GRIDEXITCODE': None,
            'GRIDEXITREASON': None,
            'GRIDJOBID': LCGUtil.cl_grid_job_id(j),
            'JOBEXITCODE': None,
            'JOBEXITREASON': None,
            'JOB_ID_INSIDE_THE_TASK': LCGUtil.cl_job_id_inside_the_task(j),
            'OWNERDN': LCGUtil.cl_ownerdn(),
            'REPORTER': 'ToolUI',
            'REPORTTIME': CommonUtil.utcnow(),
            'STATENAME': status,
            'STATESOURCE': status_source,
            'STATESTARTTIME': status_start_time,
            'TASKNAME': LCGUtil.cl_task_name(j),
            'UNIQUEJOBID': LCGUtil.cl_unique_job_id(j),
            '___fqid' : j.fqid,
            }
        return msg

    def _wn_job_status_message(self, status, status_source, status_start_time):
        # Not null: EXECUTION_BACKEND, GRIDJOBID, JOB_ID_INSIDE_THE_TASK, TASKNAME, UNIQUEJOBID
        ji = self.job_info # called on worker node, so job_info is dictionary
        msg = {
            'DESTCE': LCGUtil.wn_dest_ce(),
            'DESTSITE': LCGUtil.wn_dest_site(),
            'DESTWN': LCGUtil.wn_dest_wn(),
            'EXECUTION_BACKEND': ji['EXECUTION_BACKEND'],
            'GRIDEXITCODE': None,
            'GRIDEXITREASON': None,
            'GRIDJOBID': LCGUtil.wn_grid_job_id(),
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
            '___fqid' : ji['fqid'],
            }
        return msg
