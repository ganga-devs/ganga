"""Dashboard LCG/Athena Monitoring Service plugin

N.B. This code is under development and should not generally be used or relied upon.

"""

#TODO: disable if backend is not LCG/Athena
#TODO: send athena-specific messages:
#      job_meta_message, job_processing_attributes_message


from Ganga.Lib.MonitoringServices.Dashboard import CommonUtil
from Ganga.Lib.MonitoringServices.Dashboard import LCGUtil


from Ganga.Lib.MonitoringServices.Dashboard.LCGMS import LCGMS
class LCGAthenaMS(LCGMS):
    """Dashboard LCG/Athena Monitoring Service based on MSG."""
    
    def __init__(self, job_info, config_info):
        """Construct the Dashboard LCG/Athena Monitoring Service."""
        LCGMS.__init__(self, job_info, config_info)

    def getSandboxModules(self):
        """Return list of module dependencies."""
        import Ganga.Lib.MonitoringServices.Dashboard
        return LCGMS.getSandboxModules(self) + [
            Ganga.Lib.MonitoringServices.Dashboard.LCGAthenaMS,
            ]

    #----- event call-backs -----

    def submit(self, **opts):
        """Log submit event on client."""
        LCGMS.submit(self, **opts)
        j = self.job_info # called on client, so job_info is Job object
        if j.master is None:
            # create message (task meta)
            message = self._cl_task_meta_message()
            # send
            self._send(self.config_info['destination_task_meta'], message)
        if not j.subjobs:
            # create message (job meta attributes)
            message = self._cl_job_meta_message()
            if message['GRIDJOBID'] is None:
                # This is to handle the temporary workaround in
                # LCG.master_bulk_updateMonitoringInformation() which results in two
                # submit messages being sent, one without a grid_job_id.
                self._log('debug', 'Not sending redundant message on submit without grid_job_id for job %s.' % j.fqid)
            else:
                # send
                self._send(self.config_info['destination_job_meta'], message)
        

    def stop(self, exitcode, **opts):
        """Log stop event on client."""
        LCGMS.stop(self, exitcode, **opts)
        ji = self.job_info # called on worker node, so job_info is dictionary
        # create message (job processing attributes)
        message = self._wn_job_processing_attributes_message()
        # send
        self._send(self.config_info['destination_job_processing_attributes'], message)
        

    #----- message builders -----

    def _cl_task_meta_message(self):
        j = self.job_info # called on client, so job_info is Job object
        msg = {
            'APPLICATION': cl_application(j), # e.g. Athena
            'APPLICATIONVERSION': cl_application_version(j), # e.g. 15.5.1
            'INPUTDATASET': cl_input_dataset(j), # e.g. fdr08_run2.0052283.physics_Muon.merge.AOD.o3_f8_m10
            'JSTOOL': 'Ganga', # e.g. Ganga, Panda
            'JSTOOLUI': cl_jstoolui(), # hostname of client
            'OUTPUTDATASET': None,# Unknown at submission
            'OUTPUTSE': None, # Unknown at submission
            'OWNERDN': LCGUtil.cl_ownerdn(),
            'REPORTER': 'ToolUI',
            'REPORTTIME': CommonUtil.utcnow(),
            'SUBMISSIONTYPE': 'direct',
            'TARGET': cl_target(j), # e.g. CE_xxx,SITE_xxx,SITE_xxx
            'TASKNAME': LCGUtil.cl_task_name(j),
            'TASKTYPE': cl_task_type(self.config_info), # e.g. analysis, production, hammercloud etc.
            '___fqid' : j.fqid,
            }
        return msg

    def _cl_job_meta_message(self):
        j = self.job_info # called on client, so job_info is Job object
        msg = {
            'GRIDJOBID': LCGUtil.cl_grid_job_id(j),
            'JOB_ID_INSIDE_THE_TASK': LCGUtil.cl_job_id_inside_the_task(j),
            'NEVENTSREQUESTED': cl_nevents_requested(j),
            'PILOT': 0, # 0 = not pilot, 1 = pilot
            'PILOTNAME': None,
            'REPORTER': 'ToolUI',
            'REPORTTIME': CommonUtil.utcnow(),
            'TASKNAME': LCGUtil.cl_task_name(j),
            'UNIQUEJOBID': LCGUtil.cl_unique_job_id(j),
            '___fqid' : j.fqid,
            }
        return msg

    def _wn_job_processing_attributes_message(self):
        ji = self.job_info # called on worker node, so job_info is dictionary
        athena_stats = wn_load_athena_stats()
        msg = {
            'GRIDJOBID': LCGUtil.wn_grid_job_id(),
            'JOB_ID_INSIDE_THE_TASK': ji['JOB_ID_INSIDE_THE_TASK'],
            'NEVENTSPROCESSED': athena_stats.get('totalevents'),
            'NFILESPROCESSED': athena_stats.get('numfiles'),
            'REPORTER': 'JobWN',
            'REPORTTIME': CommonUtil.utcnow(),
            'SYSTEMTIME': athena_stats.get('systemtime'),
            'TASKNAME': ji['TASKNAME'],
            'UNIQUEJOBID': ji['UNIQUEJOBID'],
            'USERTIME': athena_stats.get('usertime'),
            'WALLCLOCK': athena_stats.get('wallclock'),
            '___fqid' : ji['fqid'],
            }
        return msg


#----- client meta-data builders ----- 
#TODO: add error handling code in following methods
#TODO: move to appropriate XxxUtil modules

def cl_application(job):
    """Build application. Only run on client."""
    return CommonUtil.strip_to_none(job.application.atlas_exetype)

def cl_application_version(job):
    """Build application_version. Only run on client."""
    if job.application.atlas_production:
        application_version = job.application.atlas_production
    else:
        application_version = job.application.atlas_release
    return CommonUtil.strip_to_none(application_version)

def cl_input_dataset(job):
    """Build input_dataset. Only run on client."""
    datasetcsv = ','.join(job.inputdata.dataset)
    return CommonUtil.strip_to_none(datasetcsv)

def cl_jstoolui():
    """Build jstoolui. Only run on client."""
    return CommonUtil.hostname()

def cl_nevents_requested(job):
    """Build nevents_requested. Only run on client."""
    max_events = None
    if job.application.max_events > -1:
        max_events = job.application.max_events
    return max_events

def cl_target(job):
    """Build target. Only run on client."""
    ces = []
    sites = []
    targets = []
    for j in [job] + job.subjobs:
        ce = j.backend.CE
        if ce and ce not in ces:
            ces.append(ce)
            targets.append('CE_%s' % ce)
        for site in j.backend.requirements.sites:
            if site and site not in sites:
                sites.append(site)
                targets.append('SITE_%s' % site)
    targetcsv = ','.join(targets)
    return CommonUtil.strip_to_none(targetcsv)

def cl_task_type(config):
    """Build task_type. Only run on client."""
    return config['task_type']

#----- worker node meta-data builders ----- 

def wn_load_athena_stats():
    """Load Athena stats. Only run on worker node.
    
    If the Athena stats.pickle file cannot be read then an empty dictionary is
    returned.
    """
    import cPickle as pickle
    try:
        f = open('stats.pickle','r')
        try:
            stats = pickle.load(f)
        finally:
            f.close()
    except:
        stats = {}
    return stats
    
    