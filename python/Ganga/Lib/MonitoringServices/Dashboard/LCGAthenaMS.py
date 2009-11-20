"""Dashboard LCG/Athena Monitoring Service plugin

N.B. This code is under development and should not generally be used or relied upon.

"""

#TODO: disable if backend is not LCG/Athena
#TODO: send athena-specific messages:
#      task_meta_message, job_meta_message, job_processing_attributes_message


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
        LCGMS.submit(self, **opts)

    def stop(self, exitcode, **opts):
        LCGMS.stop(self, exitcode, **opts)

    #----- message builders -----

    def _cl_task_meta_message(self):
        j = self.job_info # called on client, so job_info is Job object
        msg = {
            'TASKNAME': LCGUtil.cl_task_name(j),
            'TASKTYPE': 'analysis', #TODO: this needs to be configurable. other values are hammercloud etc.
            'OWNERDN': LCGUtil.cl_ownerdn(),
            'JSTOOL': 'Ganga', # e.g. Ganga, Panda
            'JSTOOLUI': cl_jstoolui(), # hostname of client
            'SUBMISSIONTYPE': 'direct',
            'APPLICATION': cl_application(j), # e.g. Athena
            'APPLICATIONVERSION': cl_application_version(j), # e.g. 15.5.1
            'INPUTDATASET': None, # e.g. fdr08_run2.0052283.physics_Muon.merge.AOD.o3_f8_m10
            'OUTPUTDATASET': None,# e.g. user09.DavidTuckett.ganga.387.20091119 (csv if list)
            'OUTPUTSE': None, # ?
            'TARGET': None, # e.g. CLOUD_xxx, SITE_xxx, SE_xxx, CE_xxx (csv if list)
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
            '___fqid' : ji['fqid'],
            }
        return msg


#----- client meta-data builders ----- 
#TODO: add error handling code in following methods
#TODO: move to appropriate XxxUtil modules

def cl_jstoolui(job):
    """Build jstoolui. Only run on client."""
    return CommonUtil.hostname()

def cl_application(job):
    """Build application. Only run on client."""
    return CommonUtil.strip_to_none(job.application.atlas_exetype)

def cl_application_version(job):
    """Build application_version. Only run on client."""
    return CommonUtil.strip_to_none(job.application.atlas_release)

