
from os import path

# jobs repo
def getJobsPath():
    from Ganga.Runtime.Repository_runtime import getLocalRoot
    jobs_path = path.join(getLocalRoot(), '6.0', 'jobs')
    return jobs_path

# XML dir for job/job_id
def getXMLDir(this_job):
    if not isinstance(this_job, int):
        _id = this_job.id
    else:
        _id = this_job
    jobs_path = getJobsPath()
    jobs_master_path = path.join(jobs_path, "%sxxx" % str(int(_id/1000)))
    return path.join(jobs_master_path, str(_id))

# XML of job
def getXMLFile(this_job):
    return path.join(getXMLDir(this_job), 'data')

# XML sub-j index of job
def getSJXMLIndex(this_j):
    return path.join(getXMLDir(this_j), 'subjobs.idx')

# XML of sub-j
def getSJXMLFile(this_sj):
    from Ganga.GPIDev.Base.Proxy import stripProxy
    return path.join(getXMLDir(stripProxy(this_sj).master), '%s' % str(this_sj.id), 'data')

# Index of job
def getIndexFile(this_job):
    return path.join(getXMLDir(this_job), '../%s.index' % str(this_job.id))

