################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GangaRepository.py,v 1.1.2.1 2009-06-04 12:00:37 moscicki Exp $
################################################################################

class GangaRepository(object):
    """GangaRepository class is an interface for developers, that need to access
    Ganga object attributes stored in the database.
    Implementation details are given in the derived class.
    """

    def __init__(self, name, location):
        """ name is: box, jobs, templates, tasks
        location is: gangadir/.../...
        """
        self.name = name
        self.location = location

        #from Ganga.Utility.guid import newGuid
        #self.guid = newGuid(self)

        # MANAGE the locks: session lock, and counter lock
        # this may spawn a thread
        # this may install atexit handlers to release the locks

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

    def getJobTree(self, tree_id = 0):
        """getJobTree(self, tree_id = 0) --> jobtree object.
        throws RepositoryError.
        tree_id - id of jobtree in registry. Can be used to support back up"""
        return

    def setJobTree(self, jobtree, tree_id = 0):
        """setJobTree(self, jobtree, tree_id = 0) --> None.
        throws RepositoryError.
        Registers and/or modifies jobtree object in the repository.
        jobtree - jobtree object
        tree_id - id of jobtree in registry. Can be used to support back up"""
        return
    

    def getWriteLock(self,obj):
        """ Obtain lock. It is OK to acquire lock multiple times.
        Set obj.write_lock to the timestamp.
        This function should be thread safe.
        Raise CannotAcquireWriteLock.
        Raise ObjectNotInRepository.
        """
        pass

    def releaseWriteLock(self,obj):
        """ This method is only used atexit. The write lock is taken for the whole duration of a session 
        (for the moment).
        Raise ObjectNotInRepository.
        """
