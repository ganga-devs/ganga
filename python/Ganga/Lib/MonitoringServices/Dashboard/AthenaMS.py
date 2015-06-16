"""Dashboard Backend/Athena Monitoring Service plugin

N.B. This code is under development and should not generally be used or relied upon.

"""


from Ganga.Lib.MonitoringServices.Dashboard import CommonUtil
from Ganga.Lib.MonitoringServices.Dashboard import AthenaUtil


from Ganga.Lib.MonitoringServices.Dashboard.BackendMS import BackendMS


class AthenaMS(BackendMS):

    """Dashboard Backend/Athena Monitoring Service based on MSG."""

    def __init__(self, job_info, config_info):
        """Construct the Dashboard Backend/Athena Monitoring Service."""
        BackendMS.__init__(self, job_info, config_info)

    def getSandboxModules(self):
        """Return list of module dependencies."""
        import Ganga.Lib.MonitoringServices.Dashboard
        return BackendMS.getSandboxModules(self) + [
            Ganga.Lib.MonitoringServices.Dashboard.AthenaMS,
            Ganga.Lib.MonitoringServices.Dashboard.AthenaUtil,
        ]

    #----- event call-backs -----

    def submit(self, **opts):
        """Log submit event on client."""
        self._cl_send_meta_messages()
        BackendMS.submit(self, **opts)

    def stop(self, exitcode, **opts):
        """Log stop event on worker node."""
        BackendMS.stop(self, exitcode, **opts)
        # send job-processing-attributes message if job successful
        if exitcode == 0:
            message = self._wn_job_processing_attributes_message()
            self._send(
                self.config_info['destination_job_processing_attributes'], message)

    def complete(self, **opts):
        """Log complete event on client."""
        BackendMS.complete(self, **opts)
        self._cl_send_meta_messages()

    def fail(self, **opts):
        """Log fail event on client."""
        BackendMS.fail(self, **opts)
        self._cl_send_meta_messages()

    def kill(self, **opts):
        """Log kill event on client."""
        BackendMS.kill(self, **opts)
        self._cl_send_meta_messages()

    def _cl_send_meta_messages(self):
        """Send task_meta and job_meta messages on client."""
        j = self.job_info  # called on client, so job_info is Job object
        # send task-meta message if this is a master or single job
        if j.master is None:
            message = self._cl_task_meta_message()
            self._send(self.config_info['destination_task_meta'], message)
        # send job-meta message if this is a sub or single job
        if not j.subjobs:
            message = self._cl_job_meta_message()
            if message['GRIDJOBID'] is None:
                # This is to handle the temporary workaround in
                # Backend.master_bulk_updateMonitoringInformation() which results in two
                # submit messages being sent, one without a grid_job_id.
                self._log(
                    'debug', 'Not sending redundant message without grid_job_id for job %s.' % j.fqid)
            else:
                self._send(self.config_info['destination_job_meta'], message)

    #----- message builders -----

    def _cl_task_meta_message(self):
        j = self.job_info  # called on client, so job_info is Job object
        msg = {
            'APPLICATION': AthenaUtil.cl_application(j),  # e.g. ATHENA
            # e.g. 15.5.1
            'APPLICATIONVERSION': AthenaUtil.cl_application_version(j),
            # e.g. fdr08_run2.0052283.physics_Muon.merge.AOD.o3_f8_m10
            'INPUTDATASET': AthenaUtil.cl_input_dataset(j),
            'JSTOOL': 'Ganga',  # e.g. Ganga, Panda
            # hostname of client. e.g. lxplus246.cern.ch
            'JSTOOLUI': AthenaUtil.cl_jstoolui(),
            # Unknown at submission. e.g.
            # user09.DavidTuckett.ganga.420.20091125.FZK-LCG2_SCRATCHDISK
            'OUTPUTDATASET': AthenaUtil.cl_output_dataset(j),
            # Unknown at submission. e.g. FZK-LCG2_SCRATCHDISK
            'OUTPUTSE': AthenaUtil.cl_output_se(j),
            # Grid certificate. e.g. /DC=ch/DC=cern/OU=Organic
            # Units/OU=Users/CN=dtuckett/CN=671431/CN=David Tuckett/CN=proxy
            'OWNERDN': self.dynamic_util.cl_ownerdn(),
            'REPORTER': 'ToolUI',  # e.g. ToolUI, JobWN
            # e.g. 2009-11-25T14:59:24.754249Z
            'REPORTTIME': CommonUtil.utcnow(),
            'SUBMISSIONTYPE': 'direct',
            # e.g. CE_xxx,SITE_CSCS-LCG2_DATADISK,SITE_DESY-ZN_DATADISK
            'TARGET': AthenaUtil.cl_target(j),
            # e.g. ganga:6702b50a-8a31-4476-8189-62ea5b8e00b3:TrigStudy
            'TASKNAME': self.dynamic_util.cl_task_name(j),
            # e.g. analysis, production, hammercloud etc.
            'TASKTYPE': AthenaUtil.cl_task_type(self.config_info),
            '___fqid': j.fqid,
        }
        return msg

    def _cl_job_meta_message(self):
        j = self.job_info  # called on client, so job_info is Job object
        msg = {
            # e.g. https://grid-lb0.desy.de:9000/moqY5njFGurEuoDkkJmtBA
            'GRIDJOBID': self.dynamic_util.cl_grid_job_id(j),
            # e.g. fdr08_run2.0052283.physics_Muon.merge.AOD.o3_f8_m10
            'INPUTDATASET': AthenaUtil.cl_input_dataset(j),
            # subjob id e.g. 0
            'JOB_ID_INSIDE_THE_TASK': self.dynamic_util.cl_job_id_inside_the_task(j),
            # None or non-negative number e.g. 100
            'NEVENTSREQUESTED': AthenaUtil.cl_nevents_requested(j),
            # e.g. user09.DavidTuckett.ganga.420.20091125.FZK-LCG2_SCRATCHDISK
            'OUTPUTDATASET': AthenaUtil.cl_output_dataset(j),
            # Unknown at submission. e.g. FZK-LCG2_SCRATCHDISK
            'OUTPUTSE': AthenaUtil.cl_output_se(j),
            'PILOT': 0,  # 0 = not pilot, 1 = pilot
            'PILOTNAME': None,
            'REPORTER': 'ToolUI',  # e.g. ToolUI, JobWN
            # e.g. 2009-11-25T14:59:24.754249Z
            'REPORTTIME': CommonUtil.utcnow(),
            # e.g. CE_xxx,SITE_CSCS-LCG2_DATADISK,SITE_DESY-ZN_DATADISK
            'TARGET': AthenaUtil.cl_target(j),
            # e.g. ganga:6702b50a-8a31-4476-8189-62ea5b8e00b3:TrigStudy
            'TASKNAME': self.dynamic_util.cl_task_name(j),
            # Ganga uuid e.g. 1c08ff3b-904f-4f77-a481-d6fa765813cb
            'UNIQUEJOBID': self.dynamic_util.cl_unique_job_id(j),
            '___fqid': j.fqid,
        }
        return msg

    def _wn_job_processing_attributes_message(self):
        ji = self.job_info  # called on worker node, so job_info is dictionary
        athena_stats = AthenaUtil.wn_load_athena_stats()
        msg = {
            # e.g. https://grid-lb0.desy.de:9000/moqY5njFGurEuoDkkJmtBA
            'GRIDJOBID': self.dynamic_util.wn_grid_job_id(ji),
            # subjob id e.g. 0
            'JOB_ID_INSIDE_THE_TASK': ji['JOB_ID_INSIDE_THE_TASK'],
            # number of events processed. e.g. 100
            'NEVENTSPROCESSED': athena_stats.get('totalevents'),
            # number of files processed. e.g. 2
            'NFILESPROCESSED': athena_stats.get('numfiles'),
            'REPORTER': 'JobWN',  # e.g. ToolUI, JobWN
            # e.g. 2009-11-25T14:59:24.754249Z
            'REPORTTIME': CommonUtil.utcnow(),
            # system cpu time in seconds. e.g. 38.45
            'SYSTEMTIME': athena_stats.get('systemtime'),
            # e.g.
            # ganga_420_dtuckett@lxplus246.cern.ch:/afs/cern.ch/user/d/dtuckett/gangadir/repository/dtuckett/LocalAMGA
            'TASKNAME': ji['TASKNAME'],
            # Ganga uuid e.g. 1c08ff3b-904f-4f77-a481-d6fa765813cb
            'UNIQUEJOBID': ji['UNIQUEJOBID'],
            # user cpu time in seconds. e.g. 479.0
            'USERTIME': athena_stats.get('usertime'),
            # wallclock time in seconds. e.g. 1040
            'WALLCLOCK': athena_stats.get('wallclock'),
            '___fqid': ji['fqid'],
        }
        return msg
