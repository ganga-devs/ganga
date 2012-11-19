"""Batch meta-data utilities.

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
    return CommonUtil.strip_to_none(job.backend.exitcode)

def cl_grid_exit_reason(job):
    """Build grid_exit_reason. Only run on client."""
    return None

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

def cl_task_name(job):
    """Build task_name. Only run on client."""
    # format: ganga:<job.info.uuid>:<job.name>
    if job.master:
        job = job.master
    task_name = 'ganga:%s:%s' % (job.info.uuid, job.name,)
    return task_name

def cl_unique_job_id(job):
    """Build unique_job_id. Only run on client."""
    return job.info.uuid

#----- worker node meta-data builders ----- 
#TODO: add error handling code in following methods


#retrieve site name from ip adress
def wn_dest_site(ji):
    """Build dest_site. Only run on worker node."""
    return ji['GANGA_HOSTNAME'] 
#this is ok  
def wn_dest_wn():
    """Build dest_wn. Only run on worker node."""
    return CommonUtil.hostname()
#return something like task name
def wn_grid_job_id(ji):
    """Build grid_job_id. Only run on worker node."""
    task_name = 'ganga:%s:%s' % (ji['JOB_UUID'], ji['JOB_NAME'],)
    return task_name
