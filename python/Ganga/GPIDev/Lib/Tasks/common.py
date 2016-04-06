def getTaskByID(this_id):
    """returns proxied task accoding to this_id"""
    return getObjectByID(this_id, 'tasks')

def getJobByID(this_id):
    """returns proxied job according to this_id"""
    return getObjectByID(this_id, 'jobs')

def getObjectByID(this_id, reg_name):
    """returns proxies object from a repo based upon it's ID"""
    from Ganga.Core.GangaRepository import getRegistryProxy
    jobs_reg = getRegistryProxy(reg_name)
    this_job = jobs_reg( this_id )
    return this_job

def makeRegisteredJob():
    """Makes a new Job and registers it with the Registry"""
    from Ganga.GPIDev.Lib.Job.Job import Job
    j = Job()
    j._auto__init__()
    return j

