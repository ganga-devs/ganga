import os

# jobs repo
def getJobsPath():
    """ Returns the path to the jobs repo on disk as used by Ganga as of 6.2.0 """
    from GangaCore.Runtime.Repository_runtime import getLocalRoot
    jobs_path = os.path.join(getLocalRoot(), '6.0', 'jobs')
    return jobs_path

# Json dir for job/job_id
def getJsonDir(this_job):
    """ Returns the path of the given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    if not isinstance(this_job, int):
        _id = this_job.id
    else:
        _id = this_job
    jobs_path = getJobsPath()
    jobs_master_path = os.path.join(jobs_path, "%sxxx" % str(int(_id/1000)))
    return os.path.join(jobs_master_path, str(_id))

# Json of job
def getXMLFile(this_job):
    """ Returns the path of the XML data file for a given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    return os.path.join(getJobsPath(this_job), 'data')

# Json sub-j index of job
def getSJXMLIndex(this_j):
    """ Returns the path of the subjob index file for a given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    return os.path.join(getJsonDir(this_j), 'subjobs.idx')

# Json of sub-j
def getSJXMLFile(this_sj):
    """ Returns the path of the XML data file for a given job (subJob or id) within the jobs repo
    Args:
        this_sj (tuple, Job): This is the subjob of interest or a tuple of (job_id, sj_id)
    """
    if not isinstance(this_sj, tuple):
        from GangaCore.GPIDev.Base.Proxy import stripProxy
        return os.path.join(getJsonDir(stripProxy(this_sj).master), '%s' % str(this_sj.id), 'data')
    else:
        return os.path.join(getJsonDir(this_sj[0]), str(this_sj[1]), 'data')

# Index of job
def getIndexFile(this_job):
    """ Returns the path of the index file for a given job (Job or id) within the jobs repo
    Args:
        this_job (Job, int): The Job or Job_ID of interest
    """
    return os.path.join(getJsonDir(this_job), '../%s.index' % str(this_job.id))

