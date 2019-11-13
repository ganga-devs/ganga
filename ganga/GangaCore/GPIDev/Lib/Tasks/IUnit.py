

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem, FileItem, GangaFileItem
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.ColourText import status_colours, overview_colours, ANSIMarkup
markup = ANSIMarkup()
from GangaCore.GPIDev.Lib.Tasks.common import getJobByID
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Base.Proxy import stripProxy
import time
from GangaCore.GPIDev.Lib.Tasks.ITask import addInfoString
import sys
import traceback
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
from GangaCore.GPIDev.Lib.File.File import File

logger = getLogger()

def formatTraceback():
   "Helper function to printout a traceback as a string"
   return "\n %s\n%s\n%s\n" % (''.join( traceback.format_tb(sys.exc_info()[2])), sys.exc_info()[0], sys.exc_info()[1])

class IUnit(GangaObject):
    _schema = Schema(Version(1, 0), {
        'status': SimpleItem(defvalue='new', protected=1, doc='Status - running, pause or completed', typelist=[str]),
        'name': SimpleItem(defvalue='Simple Unit', doc='Name of the unit (cosmetic)', typelist=[str]),
        'application': ComponentItem('applications', defvalue=None, optional=1, load_default=False, doc='Application of the Transform.'),
        'inputdata': ComponentItem('datasets', defvalue=None, optional=1, load_default=False, doc='Input dataset'),
        'outputdata': ComponentItem('datasets', defvalue=None, optional=1, load_default=False, doc='Output dataset'),
        'active': SimpleItem(defvalue=False, hidden=1, doc='Is this unit active'),
        'active_job_ids': SimpleItem(defvalue=[], typelist=[int], sequence=1, hidden=1, doc='Active job ids associated with this unit'),
        'prev_job_ids': SimpleItem(defvalue=[], typelist=[int], sequence=1,  hidden=1, doc='Previous job ids associated with this unit'),
        'minor_resub_count': SimpleItem(defvalue=0, hidden=1, doc='Number of minor resubmits'),
        'major_resub_count': SimpleItem(defvalue=0, hidden=1, doc='Number of major resubmits'),
        'req_units': SimpleItem(defvalue=[], typelist=[str], sequence=1, hidden=1, doc='List of units that must complete for this to start (format TRF_ID:UNIT_ID)'),
        'start_time': SimpleItem(defvalue=0, hidden=1, doc='Start time for this unit. Allows a delay to be put in'),
        'copy_output': ComponentItem('datasets', defvalue=None, load_default=0, optional=1, doc='The dataset to copy the output of this unit to, e.g. Grid dataset -> Local Dataset'),
        'merger': ComponentItem('mergers', defvalue=None, load_default=0, optional=1, doc='Merger to be run after this unit completes.'),
        'splitter': ComponentItem('splitters', defvalue=None, optional=1, load_default=False, doc='Splitter used on each unit of the Transform.'),
        'postprocessors': ComponentItem('postprocessor', defvalue=None, doc='list of postprocessors to run after job has finished'),
        'inputsandbox': FileItem(defvalue=[], sequence=1, doc="list of File objects shipped to the worker node "),
        'inputfiles': GangaFileItem(defvalue=[], sequence=1, doc="list of file objects that will act as input files for a job"),
        'outputfiles': GangaFileItem(defvalue=[], sequence=1, doc="list of OutputFile objects to be copied to all jobs"),
        'info' : SimpleItem(defvalue=[],typelist=[str],protected=1,sequence=1,doc="Info showing status transitions and unit info"),
        'id': SimpleItem(defvalue=-1, protected=1, doc='ID of the Unit', typelist=[int]),
    })

    _category = 'units'
    _name = 'IUnit'
    _exportmethods = []
    _hidden = 0

# Special methods:
    def __init__(self):
        super(IUnit, self).__init__()
        
    def _auto__init__(self):
        self.updateStatus("new")

    def _readonly(self):
        """A unit is read-only if the status is not new."""
        if self.status == "new":
            return 0
        return 1

    def validate(self):
        """Validate that this unit is OK and set it to active"""
        self.active = True
        return True

    def getID(self):
        """Get the ID of this unit within the transform"""

        # if the id isn't already set, use the index from the parent Task
        if self.id < 0:
           trf = self._getParent()
           if not trf:
              raise ApplicationConfigurationError(
                 "This unit has not been associated with a transform and so there is no ID available")
           self.id = trf.units.index(self)
           
        return self.id

    def updateStatus(self, status):
        """Update status hook"""
        addInfoString(self, "Status change from '%s' to '%s'" % (self.status, status))
        self.status = status

    def createNewJob(self):
        """Create any jobs required for this unit"""
        pass

    def checkCompleted(self, job):
        """Check if this unit is complete"""
        if job.status == "completed":
            return True
        else:
            return False

    def checkForSubmission(self):
        """Check if this unit should submit a job"""

        # check the delay
        if time.time() < self.start_time:
            return False

        # check if we already have a job
        if len(self.active_job_ids) != 0:
            return False

        # if we're using threads, check the max number
        from GangaCore.Core.GangaThread.WorkerThreads import getQueues
        if self._getParent().submit_with_threads and getQueues().totalNumUserThreads() > self._getParent().max_active_threads:
            return False

        return True

    def checkForResubmission(self):
        """check if this unit should be resubmitted"""

        # check if we already have a job
        if len(self.active_job_ids) == 0:
            return False
        else:
            job = getJobByID(self.active_job_ids[0])
            if job.status in ["failed", "killed"]:
                return True

            return False

    def checkParentUnitsAreComplete(self):
        """Check to see if the parent units are complete"""
        req_ok = True
        task = self._getParent()._getParent()
        for req in self.req_units:
            req_trf_id = int(req.split(":")[0])

            if req.find("ALL") == -1:
                req_unit_id = int(req.split(":")[1])
                if task.transforms[req_trf_id].units[req_unit_id].status != "completed":
                    req_ok = False

            else:
                # need all units from this trf
                for u in task.transforms[req_trf_id].units:
                    if u.status != "completed":
                        req_ok = False

        return req_ok

    def checkMajorResubmit(self, job):
        """check if this job needs to be fully rebrokered or not"""
        pass

    def majorResubmit(self, job):
        """perform a mjor resubmit/rebroker"""
        self.prev_job_ids.append(job.id)
        self.active_job_ids.remove(job.id)

    def minorResubmit(self, job):
        """perform just a minor resubmit"""
        try:
            trf = self._getParent()
        except Exception as err:
            logger.debug("GetParent exception!\n%s" % str(err))
            trf = None
        if trf is not None and trf.submit_with_threads:
            addInfoString( self, "Attempting job re-submission with queues..." )
            from GangaCore.Core.GangaThread.WorkerThreads import getQueues
            getQueues().add(job.resubmit)
        else:
            addInfoString( self, "Attempting job re-submission..." )
            job.resubmit()

    def update(self):
        """Update the unit and (re)submit jobs as required"""

        # if we're complete, then just return
        if self.status in ["completed", "recreating"] or not self.active:
            return 0

        # check if submission is needed
        task = self._getParent()._getParent()
        trf = self._getParent()
        maxsub = task.n_tosub()

        # check parent unit(s)
        req_ok = self.checkParentUnitsAreComplete()

        # set the start time if not already set
        if len(self.req_units) > 0 and req_ok and self.start_time == 0:
            self.start_time = time.time() + trf.chain_delay * 60 - 1

        if req_ok and self.checkForSubmission() and maxsub > 0:

            # create job and submit
            addInfoString( self, "Creating Job..." )
            j = self.createNewJob()
            if j.name == '':
                j.name = "T%i:%i U%i" % (task.id, trf.getID(), self.getID())

            try:
                if trf.submit_with_threads:
                    addInfoString( self, "Attempting job submission with queues..." )
                    from GangaCore.Core.GangaThread.WorkerThreads import getQueues
                    getQueues().add(j.submit)
                else:
                    addInfoString( self, "Attempting job submission..." )
                    j.submit()

            except Exception as err:
                logger.debug("update Err: %s" % str(err))
                addInfoString( self, "Failed Job Submission")
                addInfoString( self, "Reason: %s" % (formatTraceback()))
                logger.error("Couldn't submit the job. Deactivating unit.")
                self.prev_job_ids.append(j.id)
                self.active = False
                trf._setDirty()  # ensure everything's saved
                return 1

            self.active_job_ids.append(j.id)
            self.updateStatus("running")
            trf._setDirty()  # ensure everything's saved

            if trf.submit_with_threads:
                return 0

            return 1

        # update any active jobs
        for jid in self.active_job_ids:

            # we have an active job so see if this job is OK and resubmit if
            # not
            try:
                job = getJobByID(jid)
            except Exception as err:
                logger.debug("Update2 Err: %s" % str(err))
                logger.warning("Cannot find job with id %d. Maybe reset this unit with: tasks(%d).transforms[%d].resetUnit(%d)" %
                               (jid, task.id, trf.getID(), self.getID()))
                continue

            if job.status == "completed":

                # check if actually completed
                if not self.checkCompleted(job):
                    return 0

                # check for DS copy
                if trf.unit_copy_output:
                    if not self.copy_output:
                        trf.createUnitCopyOutputDS(self.getID())

                    if not self.copyOutput():
                        return 0

                # check for merger
                if trf.unit_merger:
                    if not self.merger:
                        self.merger = trf.createUnitMerger(self.getID())

                    if not self.merge():
                        return 0

                # all good so mark unit as completed
                self.updateStatus("completed")

            elif job.status == "failed" or job.status == "killed":

                # check for too many resubs
                if self.minor_resub_count + self.major_resub_count > trf.run_limit - 1:
                    logger.error("Too many resubmits (%i). Deactivating unit." % (
                        self.minor_resub_count + self.major_resub_count))
                    addInfoString( self, "Deactivating unit. Too many resubmits (%i)" % ( self.minor_resub_count + self.major_resub_count))
                    self.active = False
                    return 0

                rebroker = False

                if self.minor_resub_count > trf.minor_run_limit - 1:
                    if self._getParent().rebroker_on_job_fail:
                        rebroker = True
                    else:
                        logger.error(
                            "Too many minor resubmits (%i). Deactivating unit." % self.minor_resub_count)
                        addInfoString( self, "Deactivating unit. Too many resubmits (%i)" % (self.minor_resub_count + self.minor_resub_count))
                        self.active = False
                        return 0

                if self.major_resub_count > trf.major_run_limit - 1:
                    logger.error(
                        "Too many major resubmits (%i). Deactivating unit." % self.major_resub_count)
                    addInfoString( self, "Deactivating unit. Too many resubmits (%i)" % (self.minor_resub_count + self.major_resub_count))
                    self.active = False
                    return 0

                # check the type of resubmit
                if rebroker or self.checkMajorResubmit(job):

                    self.major_resub_count += 1
                    self.minor_resub_count = 0

                    try:
                        addInfoString( self, "Attempting major resubmit...")
                        self.majorResubmit(job)
                    except Exception as err:
                        logger.debug("Update Err3: %s" % str(err))
                        logger.error("Couldn't resubmit the job. Deactivating unit.")
                        addInfoString( self, "Failed Job resubmission")
                        addInfoString( self, "Reason: %s" % (formatTraceback()))
                        self.active = False

                    # break the loop now because we've probably changed the
                    # active jobs list
                    return 1
                else:
                    self.minor_resub_count += 1
                    try:
                        addInfoString( self, "Attempting minor resubmit...")
                        self.minorResubmit(job)
                    except Exception as err:
                        logger.debug("Update Err4: %s" % str(err))
                        logger.error("Couldn't resubmit the job. Deactivating unit.")
                        addInfoString( self, "Failed Job resubmission")
                        addInfoString( self, "Reason: %s" % (formatTraceback()))
                        self.active = False
                        return 1

    def reset(self):
        """Reset the unit completely"""
        addInfoString( self, "Reseting Unit...")
        self.minor_resub_count = 0
        self.major_resub_count = 0
        if len(self.active_job_ids) > 0:
            self.prev_job_ids += self.active_job_ids
        self.active_job_ids = []

        self.active = True

        # if has parents, set to recreate
        if len(self.req_units) > 0:
            self.updateStatus("recreating")
        else:
            self.updateStatus("running")

    # Info routines
    def n_active(self):

        if self.status == 'completed':
            return 0

        tot_active = 0
        active_states = ['submitted', 'running']

        for jid in self.active_job_ids:

            try:
                job = getJobByID(jid)
            except Exception as err:
                logger.debug("n_active Err: %s" % str(err))
                task = self._getParent()._getParent()
                trf = self._getParent()
                logger.warning("Cannot find job with id %d. Maybe reset this unit with: tasks(%d).transforms[%d].resetUnit(%d)" %
                               (jid, task.id, trf.getID(), self.getID()))
                continue

            j = stripProxy(job)

            # try to preserve lazy loading
            if hasattr(j, '_index_cache') and j._index_cache and 'subjobs:status' in j._index_cache:
                if len(j._index_cache['subjobs:status']) > 0:
                    for sj_stat in j._index_cache['subjobs:status']:
                        if sj_stat in active_states:
                            tot_active += 1
                else:
                    if j._index_cache['status'] in active_states:
                        tot_active += 1
            else:
                #logger.warning("WARNING: (active check) No index cache for job object %d" % jid)
                if j.status in active_states:
                    if j.subjobs:
                        for sj in j.subjobs:
                            if sj.status in active_states:
                                tot_active += 1
                    else:
                        tot_active += 1

        return tot_active

    def n_status(self, status):
        tot_active = 0
        for jid in self.active_job_ids:

            try:
                job = getJobByID(jid)
            except Exception as err:
                logger.debug("n_status Err: %s" % str(err))
                task = self._getParent()._getParent()
                trf = self._getParent()
                logger.warning("Cannot find job with id %d. Maybe reset this unit with: tasks(%d).transforms[%d].resetUnit(%d)" %
                               (jid, task.id, trf.getID(), self.getID()))
                continue

            j = stripProxy(job)

            # try to preserve lazy loading
            if hasattr(j, '_index_cache') and j._index_cache and 'subjobs:status' in j._index_cache:
                if len(j._index_cache['subjobs:status']) > 0:
                    for sj_stat in j._index_cache['subjobs:status']:
                        if sj_stat == status:
                            tot_active += 1
                else:
                    if j._index_cache['status'] == status:
                        tot_active += 1

            else:
                #logger.warning("WARNING: (status check) No index cache for job object %d" % jid)
                if j.subjobs:
                    for sj in j.subjobs:
                        if sj.status == status:
                            tot_active += 1
                else:
                    if j.status == status:
                        tot_active += 1

        return tot_active

    def n_all(self):
        total = 0
        for jid in self.active_job_ids:

            try:
                job = getJobByID(jid)
            except Exception as err:
                logger.debug("n_all Err: %s" % str(err))
                task = self._getParent()._getParent()
                trf = self._getParent()
                logger.warning("Cannot find job with id %d. Maybe reset this unit with: tasks(%d).transforms[%d].resetUnit(%d)" %
                               (jid, task.id, trf.getID(), self.getID()))
                continue

            j = stripProxy(job)

            # try to preserve lazy loading
            if hasattr(j, '_index_cache') and j._index_cache and 'subjobs:status' in j._index_cache:
                if len(j._index_cache['subjobs:status']) != 0:
                    total += len(j._index_cache['subjobs:status'])
                else:
                    total += 1
            else:
                #logger.warning("WARNING: (status check) No index cache for job object %d" % jid)
                if j.subjobs:
                    total = len(j.subjobs)
                else:
                    total = 1

        return total

    def overview(self):
        """Print an overview of this unit"""
        o = "    Unit %d: %s        " % (self.getID(), self.name)

        for s in ["submitted", "running", "completed", "failed", "unknown"]:
            o += markup("%i   " % self.n_status(s), overview_colours[s])

        print(o)

    def copyOutput(self):
        """Copy any output to the given dataset"""
        logger.error(
            "No default implementation for Copy Output - contact plugin developers")
        return False
