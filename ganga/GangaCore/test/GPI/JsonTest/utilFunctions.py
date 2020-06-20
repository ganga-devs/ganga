
from os import path

# jobs repo
def getJobsPath():
    """ Returns the path to the jobs repo on disk as used by Ganga """
    from GangaCore.Runtime.Repository_runtime import getLocalRoot
    jobs_path = path.join(getLocalRoot(), '6.0', 'jobs')
    return jobs_path

# JSON dir for job/job_id
def getJSONDir(this_job):
    """ Returns the path of the given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    if not isinstance(this_job, int):
        _id = this_job.id
    else:
        _id = this_job
    jobs_path = getJobsPath()
    jobs_master_path = path.join(jobs_path, "%sxxx" % str(int(_id/1000)))
    return path.join(jobs_master_path, str(_id))

# JSON of job
def getJSONFile(this_job):
    """ Returns the path of the JSON data file for a given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    return path.join(getJSONDir(this_job), 'data')

# JSON sub-j index of job
def getSJJSONIndex(this_j):
    """ Returns the path of the subjob index file for a given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    return path.join(getJSONDir(this_j), 'subjobs.idx')

# JSON of sub-j
def getSJJSONFile(this_sj):
    """ Returns the path of the JSON data file for a given job (subJob or id) within the jobs repo
    Args:
        this_sj (tuple, Job): This is the subjob of interest or a tuple of (job_id, sj_id)
    """
    if not isinstance(this_sj, tuple):
        from GangaCore.GPIDev.Base.Proxy import stripProxy
        return path.join(getJSONDir(stripProxy(this_sj).master), '%s' % str(this_sj.id), 'data')
    else:
        return path.join(getJSONDir(this_sj[0]), str(this_sj[1]), 'data')

# Index of job
def getIndexFile(this_job):
    """ Returns the path of the index file for a given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    return path.join(getJSONDir(this_job), '../%s.index' % str(this_job.id))

