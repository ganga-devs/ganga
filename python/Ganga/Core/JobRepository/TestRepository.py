from Ganga.Utility.logging import getLogger

logger = getLogger(modulename=1)

from Base import JobRepository


class JobInfo:

    def __init__(self, job, **kwds):
        self.j = job
        self.commit = 0
        self.checkout = 0
        self.register = 0

        for k in kwds:
            setattr(self, k, kwds[k])


class TestRepository(JobRepository):

    def __init__(self, schema, role, streamer, root_dir, **kwds):
        logger.debug('__init__ %s', str((schema, role, streamer, root_dir)))
        JobRepository.__init__(self, schema, role, streamer)
        self.root = root_dir
        self.id_counter = 0
        self.jobs = {}
        self.trash = {}

    def registerJobs(self, jobs):
        # file(self.root)
        for j in jobs:
            self.id_counter += 1
            j.id = self.id_counter
            self.jobs[j.id] = JobInfo(j, register=1)
        logger.debug('registerJobs %s', str([j.id for j in jobs]))

    def commitJobs(self, jobs):
        """commitJobs(self, jobs) --> None
        throws RepositoryError
        jobs -- list of jobs to commit
        jobs must already be registered 
        """
        logger.debug('commitJobs %s', str([j.id for j in jobs]))

        for j in jobs:
            if j.id not in self.jobs:
                raise ValueError(
                    'attempt to commit job is not registered', j.id)

            self.jobs[j.id].commit += 1
            logger.debug("commited job %d (%d)", j.id, self.jobs[j.id].commit)

    def checkoutJobs(self, ids_or_attributes):
        """checkoutJobs(self, ids_or_attributes) --> list of jobs
        throws RepositoryError
        ids_or_attributes -- list of job ids
        or dictionary of job attributes saved
        in the DB as metadata. Example of attributes:
        attributes = {'status':'submitted', 'application':'DaVinci'}
        """
        logger.debug('checkoutJobs %s', str(ids_or_attributes))
        return []

    def deleteJobs(self, ids):
        """deleteJob(self, ids) --> None
        throws RepositoryError
        ids -- list of job ids
        """
        logger.debug('deleteJobs %s', str(ids))
        for id in ids:
            if id not in self.jobs:
                raise ValueError(
                    'attempt to delet job which does not exist %d', id)
            self.trash[id] = self.jobs[id]
            del self.jobs[id]
            logger.debug('deleted job %d', id)
        return

    def getJobIds(self, ids_or_attributes):
        """getJobIds(self, ids_or_attributes) --> list of ids for the jobs having
        specified attributes.
        throws RepositoryError
        ids_or_attributes -- list of job ids
        or dictionary of job attributes saved
        in the DB as metadata. Example of attributes:
        attributes = {'status':'submitted', 'application':'DaVinci'}
        """
        logger.debug('getJobIds %s', str(ids_or_attributes))
        return []

    def getJobAttributes(self, ids_or_attributes):
        """getJobAttributes(self, ids_or_attributes) --> list of dictionaries with 
        job metadata stored in the registry.
        throws RepositoryError
        ids_or_attributes -- list of job ids
        or dictionary of job attributes saved
        in the DB as metadata. Example of attributes:
        attributes = {'status':'submitted', 'application':'DaVinci'}
        """
        logger.debug('getJobAttributes %s', str(ids_or_attributes))
        return []

    def setJobsStatus(self, statusList):
        """setJobsStatus(self, statusList) --> None
        throws RepositoryError
        statusList -- list of tuples (<jobId>, <status>)
        Supposed to be used by JobManager
        """
        logger.debug('setJobsStatus %s', str(statusList))
        return

    def getJobsStatus(self, ids_or_attributes):
        """getJobsStatus(self, ids_or_attributes) --> list of tuples, indicating 
        jobid and job status: (id, status).
        throws RepositoryError
        ids_or_attributes -- list of job ids
        or dictionary of job attributes saved
        in the DB as metadata. Example of attributes:
        attributes = {'application':'DaVinci'}        
        """
        logger.debug('getJobStatus %s', str(ids_or_attributes))
        return []
