

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaCore.GPIDev.Lib.Job import MetadataDict
from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.GPIDev.Lib.Tasks.common import getJobByID
import time

logger = getLogger()

########################################################################
def addInfoString( task_obj, info_str ):
    """Helper function to add an info string with a timestamp"""
    if len(task_obj.info) > 0 and task_obj.info[-1].find(info_str) > -1:
        # repeated string. Add one to instances
        num_rpts = 1
        if task_obj.info[-1].find("(rpt ") > -1:
            num_rpts = int( task_obj.info[-1][ task_obj.info[-1].find("(rpt ") + 5:-1 ] )
            
        task_obj.info[-1] = "%s: %s (rpt %i)" % (time.ctime(), info_str, num_rpts + 1)
        return
    
    task_obj.info.append("%s: %s" % (time.ctime(), info_str) )

class ITask(GangaObject):

    """This is the framework of a task without special properties"""
    _schema = Schema(Version(1, 0), {
        'transforms': ComponentItem('transforms', defvalue=[], sequence=1, copyable=0, doc='list of transforms'),
        'id': SimpleItem(defvalue=-1, protected=1, doc='ID of the Task', typelist=[int]),
        'name': SimpleItem(defvalue='NewTask', copyable=1, doc='Name of the Task', typelist=[str]),
        'comment': SimpleItem('', protected=0, doc='comment of the task', typelist=[str]),
        'status': SimpleItem(defvalue='new', protected=1, doc='Status - new, running, pause or completed', typelist=[str]),
        'float': SimpleItem(defvalue=0, copyable=1, doc='Number of Jobs run concurrently', typelist=[int]),
        'metadata': ComponentItem('metadata', defvalue=MetadataDict(), doc='the metadata', protected=1),
        'creation_date': SimpleItem(defvalue="19700101", copyable=0, protected=1, doc='Creation date of the task', typelist=[str]),
        'check_all_trfs': SimpleItem(defvalue=True, doc='Check all Transforms during each monitoring loop cycle'),
    })

    _category = 'tasks'
    _name = 'ITask'
    _exportmethods = ['run', 'appendTransform', 'overview', 'getJobs', 'remove', 'clone', 'pause', 'check', 'setBackend', 'setParameter',
                      'insertTransform', 'removeTransform', 'table', 'resetUnitsByStatus', 'removeUnusedJobs', 'n_all', 'n_status', 'n_all']

    _tasktype = "ITask"

    default_registry = "tasks"

# Special methods:
    def _auto__init__(self, registry=None):
        if registry is None:
            from GangaCore.Core.GangaRepository import getRegistry
            registry = getRegistry(self.default_registry)
        # register the job (it will also commit it)
        # job gets its id now
        registry._add(self)
        self.creation_date = time.strftime('%Y%m%d%H%M%S')
        self.startup()
        self.status = 'new'

    def startup(self):
        """Startup function on Ganga startup"""
        for t in self.transforms:
            t.startup()

    def getTransform(self, trf):
        """Get transform using either index or name"""
        if isinstance(trf, str):
            for trfid in range(0, len(self.transforms)):
                if trf == self.transforms[trfid].name:
                    return self.transforms[trfid]
            logger.warning("Couldn't find transform with name '%s'." % trf)
        elif isinstance(trf, int):
            if trf < 0 and trf > len(self.transforms):
                logger.warning("Transform number '%d' out of range" % trf)
            else:
                return self.transforms[trf]
        else:
            logger.warning('Incorrect type for transform referral. Allowed types are int or string.')

        return None

    def update(self):
        """Called by the monitoring thread. Base class just calls update on each Transform"""

        # if we're new, then do nothing
        if self.status == "new":
            return

        # loop over all transforms and call update
        for trf in self.transforms:
            if trf.status != "running":
                continue

            if trf.update() and not self.check_all_trfs:
                break

        # update status and check
        self.updateStatus()

# Public methods:
#
# - remove() a task
# - clone() a task
# - check() a task (if updated)
# - run() a task to start processing
# - pause() to interrupt processing
# - setBackend(be) for all transforms
# - setParameter(myParam=True) for all transforms
# - insertTransform(id, tf) insert a new processing step
# - removeTransform(id) remove a processing step

    def remove(self, remove_jobs="do_nothing"):
        """Delete the task"""

        # make sure the task isn't running
        if self.status.find("running") != -1:
            logger.error(
                "Task is still running. Please pause before removing!")
            return

        if not remove_jobs in [True, False]:
            logger.info("You want to remove the task %i named '%s'." %
                        (self.id, self.name))
            logger.info(
                "Since this operation cannot be easily undone, please call this command again:")
            logger.info(
                " * as tasks(%i).remove(remove_jobs=True) if you want to remove all associated jobs," % (self.id))
            logger.info(
                " * as tasks(%i).remove(remove_jobs=False) if you want to keep the jobs." % (self.id))
            return
        if remove_jobs:

            for trf in self.transforms:
                for unit in trf.units:
                    for jid in unit.active_job_ids:
                        try:
                            j = getJobByID(jid)
                            j.remove()
                        except Exception as err:
                            logger.debug("Remove Err: %s" % str(err))
                            pass

                    for jid in unit.prev_job_ids:
                        try:
                            j = getJobByID(jid)
                            j.remove()
                        except Exception as err2:
                            logger.debug("Remove Err2: %s" % str(err2))
                            pass

        self._getRegistry()._remove(self, auto_removed=1)
        logger.info("Task #%s deleted" % self.id)

    def clone(self):
        c = super(ITask, self).clone()
        for tf in c.transforms:
            tf.status = "new"
        c.check()
        return c

    def check(self):
        """This function is called by run() or manually by the user"""
        if self.status != "new":
            logger.error(
                "The check() function may modify a task and can therefore only be called on new tasks!")
            return
        try:
            for t in self.transforms:
                t.check()
        finally:
            self.updateStatus()
        return True

    def run(self):
        """Confirms that this task is fully configured and ready to be run."""
        if self.status == "new":
            self.check()

        if self.status != "completed":
            if self.float == 0:
                logger.warning(
                    "The 'float', the number of jobs this task may run, is still zero. Type 'tasks(%i).float = 5' to allow this task to submit 5 jobs at a time" % self.id)
            try:
                for tf in self.transforms:
                    if tf.status != "completed":
                        tf.run(check=False)

            finally:
                self.updateStatus()
        else:
            logger.info("Task is already completed!")

    def pause(self):
        """Pause the task - the background thread will not submit new jobs from this task"""
        float_cache = self.float
        self.float = 0
        if self.status != "completed":
            for tf in self.transforms:
                tf.pause()
            self.status = "pause"
        else:
            logger.info("Transform is already completed!")
        self.float = float_cache

    def insertTransform(self, id, tf):
        """Insert transfrm tf before index id (counting from 0)"""
        if self.status != "new" and id < len(self.transforms):
            logger.error(
                "You can only insert transforms at the end of the list. Only if a task is new it can be freely modified!")
            return
        # self.transforms.insert(id,tf.copy()) # this would be safer, but
        # breaks user exspectations
        # this means that t.insertTransform(0,t2.transforms[0]) will cause
        # Great Breakage
        self.transforms.insert(id, tf)
        stripProxy(tf).id = id

    def appendTransform(self, tf):
        """Append transform"""
        return self.insertTransform(len(self.transforms), tf)

    def removeTransform(self, id):
        """Remove the transform with the index id (counting from 0)"""
        if self.status != "new":
            logger.error("You can only remove transforms if the task is new!")
            return
        del self.transforms[id]

    def getJobs(self):
        """ Get the job slice of all jobs that process this task """
        jobslice = JobRegistrySlice("tasks(%i).getJobs()" % (self.id))
        for trf in self.transforms:
            for jid in trf.getJobs():
                jobslice.objects[getJobByID(jid).fqid] = stripProxy(getJobByID(jid))

        return JobRegistrySliceProxy(jobslice)

# Internal methods
    def updateStatus(self):
        """Updates status based on transform status.
           Called from check() or if status of a transform changes"""
        # Calculate status from transform status:
        states = [tf.status for tf in self.transforms]
        if "running" in states and "pause" in states:
            new_status = "running/pause"
        elif "running" in states:
            new_status = "running"
        elif "pause" in states:
            new_status = "pause"
        elif "new" in states:
            new_status = "new"
        elif "completed" in states:
            new_status = "completed"
        else:
            new_status = "new"  # no tranforms
        # Handle status changes here:
        if self.status != new_status:
            if new_status == "running/pause":
                logger.info("Some Transforms of Task %i '%s' have been paused. Check tasks.table() for details!" % (
                    self.id, self.name))
            elif new_status == "completed":
                logger.info("Task %i '%s' has completed!" %
                            (self.id, self.name))
            elif self.status == "completed":
                logger.warning(
                    "Task %i '%s' has been reopened!" % (self.id, self.name))
        self.status = new_status
        return self.status

    # Information methods
    def n_tosub(self):
        return self.float - sum([t.n_active() for t in self.transforms])

    def n_all(self):
        return sum([t.n_all() for t in self.transforms])

    def n_status(self, status):
        return sum([t.n_status(status) for t in self.transforms])

    def table(self):
        from GangaCore.Core.GangaRepository import getRegistryProxy
        t = getRegistryProxy('tasks').table(id=self.id)

    def overview(self, status=''):
        """ Show an overview of the Task """
        if status and not status in ['bad', 'hold', 'running', 'completed', 'new']:
            logger.error(
                "Not a valid status for unitOverview. Possible options are: 'bad', 'hold', 'running', 'completed', 'new'.")
            return

        print("Lists the units in each transform and give the state of the subjobs")
        print('')
        print(" " * 41 + "Active\tSub\tRun\tComp\tFail\tMinor\tMajor")
        for trfid in range(0, len(self.transforms)):
            print("----------------------------------------------------------------------------------------------------------------------")
            print("----   Transform %d:  %s" % (trfid, self.transforms[trfid].name))
            print('')
            self.transforms[trfid].overview(status)
            print('')

    def info(self):
        for t in self.transforms:
            t.info()

    def help(self):
        print("This is a Task without special properties")

    def resetUnitsByStatus(self, status='bad'):
        """Reset all units of the given status"""
        for trf in self.transforms:
            trf.resetUnitsByStatus(status)

    def removeUnusedJobs(self):
        """Remove any unused jobs"""
        for trf in self.transforms:
            trf.removeUnusedJobs()

