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
            # create message (Ganga running)
            message = self._cl_task_meta_message()
            # send
            self._send(self.config_info['destination_task_meta'], message)
        

    def stop(self, exitcode, **opts):
        LCGMS.stop(self, exitcode, **opts)

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
            'TASKNAME': LCGUtil.cl_task_name(j),
            'JOB_ID_INSIDE_THE_TASK': LCGUtil.cl_job_id_inside_the_task(j),
            'UNIQUEJOBID': LCGUtil.cl_unique_job_id(j),
            'GRIDJOBID': LCGUtil.cl_grid_job_id(j),
            'PILOT': None,
            'PILOTNAME': None,
            'NEVENTSREQUESTED': None,
            'REPORTER': 'ToolUI',
            'REPORTTIME': CommonUtil.utcnow(),
            '___fqid' : j.fqid,
            }
        return msg

    def _wn_job_processing_attributes_message(self):
        ji = self.job_info # called on worker node, so job_info is dictionary
        msg = {
            'TASKNAME': ji['TASKNAME'],
            'JOB_ID_INSIDE_THE_TASK': ji['JOB_ID_INSIDE_THE_TASK'],
            'UNIQUEJOBID': ji['UNIQUEJOBID'],
            'GRIDJOBID': LCGUtil.wn_grid_job_id(),
            'NEVENTSPROCESSED': None,
            'CPU': None,
            'WALLCLOCK': None,
            'REPORTER': 'JobWN',
            'REPORTTIME': CommonUtil.utcnow(),
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
