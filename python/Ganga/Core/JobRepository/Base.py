##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Base.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
##########################################################################


class JobRepository(object):

    """JobRepository class is an interface for developers, that need to access
    jobs/job attributes stored in the database.
    Implementation details are given in the derived class.
    """

    def __init__(self, schema, role, streamer=None, tree_streamer=None):
        """schema is a subset of job schema. It should be list of tuples
        (attr, dbtype), where attr is a name of attribute, and dbtype is
        database specific type.
        role can be 'Client' or 'JobManager'. 'Client' can only modify job
        status of 'new' jobs
        streamer is an object which converts job dictionary into a string and
        vice versa.
        tree_streamer is an object which converts jobtree into a string and
        vice versa.
        """
        self.schema = schema
        assert(role in ['Client', 'JobManager'])
        self._role = role
        self._streamer = streamer
        self._tree_streamer = tree_streamer
        from Ganga.Utility.guid import newGuid
        self.guid = newGuid(self)

    def _getStreamFromJob(self, job):
        if self._streamer:
            return self._streamer.getStreamFromJob(job)
        return ''

    def _getJobFromStream(self, stream):
        if self._streamer:
            return self._streamer.getJobFromStream(stream)
        return

    def registerJobs(self, jobs):
        """registerJobs(self, jobs) --> None
        throws RepositoryError
        jobs -- list of jobs to register
        """
        return

    def commitJobs(self, jobs):
        """commitJobs(self, jobs) --> None
        throws RepositoryError
        jobs -- list of jobs to commit
        jobs must already be registered 
        """
        return

    def checkoutJobs(self, ids_or_attributes):
        """checkoutJobs(self, ids_or_attributes) --> list of jobs
        throws RepositoryError
        ids_or_attributes -- list of job ids
        or dictionary of job attributes saved
        in the DB as metadata. Example of attributes:
        attributes = {'status':'submitted', 'application':'DaVinci'}
        """
        return []

    def deleteJobs(self, ids):
        """deleteJob(self, ids) --> None
        throws RepositoryError
        ids -- list of job ids
        """
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
        return []

    def setJobsStatus(self, statusList):
        """setJobsStatus(self, statusList) --> None
        throws RepositoryError
        statusList -- list of tuples (<jobId>, <status>)
        Supposed to be used by JobManager
        """
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
        return []

    def getJobTree(self, tree_id=0):
        """getJobTree(self, tree_id = 0) --> jobtree object.
        throws RepositoryError.
        tree_id - id of jobtree in registry. Can be used to support back up"""
        return

    def setJobTree(self, jobtree, tree_id=0):
        """setJobTree(self, jobtree, tree_id = 0) --> None.
        throws RepositoryError.
        Registers and/or modifies jobtree object in the repository.
        jobtree - jobtree object
        tree_id - id of jobtree in registry. Can be used to support back up"""
        return
