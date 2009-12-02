"""LCG meta-data utilities.

N.B. This code is under development and should not generally be used or relied upon.

"""

from Ganga.Lib.MonitoringServices.Dashboard import CommonUtil

#----- client meta-data builders ----- 
#TODO: add error handling code in following methods

def cl_dest_ce(job):
    """Build dest_ce. Only run on client."""
    return CommonUtil.strip_to_none(job.backend.actualCE)

def cl_execution_backend(job):
    """Build execution_backend. Only run on client."""
    return job.backend._name

def cl_grid_exit_code(job):
    """Build grid_exit_code. Only run on client."""
    return CommonUtil.strip_to_none(job.backend.exitcode_lcg)

def cl_grid_exit_reason(job):
    """Build grid_exit_reason. Only run on client."""
    return CommonUtil.strip_to_none(job.backend.reason)

def cl_grid_job_id(job):
    """Build grid_job_id. Only run on client."""
    return CommonUtil.strip_to_none(job.backend.id)

def cl_ownerdn():
    """Build ownerdn. Only run on client."""
    from Ganga.GPIDev import Credentials
    proxy = Credentials.getCredential('GridProxy')
    ownerdn = proxy.info('-subject')
    return CommonUtil.strip_to_none(ownerdn)

def cl_grid_status(job):
    """Build grid_status. Only run on client."""
    return CommonUtil.strip_to_none(job.backend.status)

def cl_job_id_inside_the_task(job):
    """Build job_id_inside_the_task. Only run on client."""
    if job.master is None:
        job_id_inside_the_task = 0
    else:
        job_id_inside_the_task = job.id
    return job_id_inside_the_task

def cl_job_exit_code(job):
    """Build job_exit_code. Only run on client."""
    return CommonUtil.strip_to_none(job.backend.exitcode)

def cl_task_name(job):
    """Build task_name. Only run on client."""
    from Ganga.Utility import Config
    user = Config.getConfig('Configuration')['user']
    rep_type = Config.getConfig('Configuration')['repositorytype']
    if 'Local' in rep_type:
        from Ganga.Runtime import Repository_runtime
        rep_dir = Repository_runtime.getLocalRoot()
        rep_hostname = Config.getConfig('System')['GANGA_HOSTNAME']
        repository = rep_hostname + ':' + rep_dir
    elif 'Remote' in rep_type:
        rep_host = Config.getConfig(rep_type +'_Repository')['host']
        rep_port = Config.getConfig(rep_type +'_Repository')['port']
        repository = rep_host + ':' + rep_port
    else:
        repository = ''

    if job.master is None:
        task_id = job.id
    else:
        task_id = job.master.id
    task_name = 'ganga_%s_%s@%s' %(task_id, user, repository)
    return task_name

def cl_unique_job_id(job):
    """Build unique_job_id. Only run on client."""
    return job.info.uuid

    
#----- worker node meta-data builders ----- 
#TODO: add error handling code in following methods

def wn_dest_ce():
    """Build dest_ce. Only run on worker node."""
    dest_ce = CommonUtil.env('GLOBUS_CE')
    if not dest_ce:
        dest_ce = CommonUtil.stdout('edg-brokerinfo getCE')
    if not dest_ce:
        dest_ce = CommonUtil.stdout('glite-brokerinfo getCE')
    return CommonUtil.strip_to_none(dest_ce)

def wn_dest_site():
    """Build dest_site. Only run on worker node."""
    return CommonUtil.env('SITE_NAME')
    
def wn_dest_wn():
    """Build dest_wn. Only run on worker node."""
    return CommonUtil.hostname()

def wn_grid_job_id():
    """Build grid_job_id. Only run on worker node."""
    grid_job_id = CommonUtil.env('EDG_WL_JOBID')
    if not grid_job_id:
        grid_job_id = CommonUtil.env('GLITE_WMS_JOBID')
    return grid_job_id
