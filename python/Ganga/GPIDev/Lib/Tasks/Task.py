from __future__ import absolute_import
from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Base.Proxy import addProxy
from Ganga.GPIDev.Base.Proxy import stripProxy
from .common import logger, overview_colours, markup
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
import time

########################################################################


class Task(GangaObject):

    """This is a Task without special properties"""
    _schema = Schema(Version(1, 0), {
        'transforms': ComponentItem('transforms', defvalue=[], sequence=1, copyable=1, doc='list of transforms'),
        'id': SimpleItem(defvalue=-1, protected=1, doc='ID of the Task', typelist=["int"]),
        'name': SimpleItem(defvalue='NewTask', copyable=1, doc='Name of the Task', typelist=["str"]),
        'comment': SimpleItem('', protected=0, doc='comment of the task', typelist=["str"]),
        'status': SimpleItem(defvalue='new', protected=1, doc='Status - new, running, pause or completed', typelist=["str"]),
        'float': SimpleItem(defvalue=0, copyable=1, doc='Number of Jobs run concurrently', typelist=["int"]),
        'resub_limit': SimpleItem(defvalue=0.9, copyable=1, doc='Resubmit only if the number of running jobs is less than "resub_limit" times the float. This makes the job table clearer, since more jobs can be submitted as subjobs.', typelist=["float"]),
        'creation_date': SimpleItem(defvalue="19700101", copyable=0, hidden=1, doc='Creation date of the task (used in dq2 datasets)', typelist=["str"]),
    })

    _category = 'tasks'
    _name = 'Task'
    _exportmethods = [
        # Settings
        'setBackend', 'setParameter', 'insertTransform', 'appendTransform', 'removeTransform',
        'check', 'run', 'pause', 'remove',  # Operations
        # Info
        'overview', 'info', 'n_all', 'n_status', 'help', 'getJobs', 'table',
        'float_all', 'run_all'  # Helper
    ]

    default_registry = "tasks"

# Special methods:
    def _auto__init__(self, registry=None):
        if registry is None:
            from Ganga.Core.GangaRepository import getRegistry
            registry = getRegistry(self.default_registry)
        # register the job (it will also commit it)
        # job gets its id now
        registry._add(self)
        self.creation_date = time.strftime('%Y%m%d%H%M%S')
        self.initialize()
        self.startup()
        self._setDirty()

    def initialize(self):
        pass

    def startup(self):
        """Startup function on Ganga startup"""
        for t in self.transforms:
            t.startup()

#   def _readonly(self):
#      """A task is read-only if the status is not new."""
#      if self.status == "new":
#         return 0
#      return 1

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
        if not remove_jobs in [True, False]:
            logger.info("You want to remove the task %i named '%s'." % (self.id, self.name))
            logger.info("Since this operation cannot be easily undone, please call this command again:")
            logger.info(" * as tasks(%i).remove(remove_jobs=True) if you want to remove all associated jobs," % (self.id))
            logger.info(" * as tasks(%i).remove(remove_jobs=False) if you want to keep the jobs." % (self.id))
            return
        if remove_jobs:
            for j in GPI.jobs:
                try:
                    stid = j.application.tasks_id.split(":")
                    if int(stid[-2]) == self.id:
                        j.remove()
                except Exception as err:
                    logger.debug("Task remove_jobs task split Error!")
                    logger.debug("Error:\n%s" % str(err))
                    pass
        self._getRegistry()._remove(self)
        logger.info("Task #%s deleted" % self.id)

    def clone(self):
        c = super(Task, self).clone()
        for tf in c.transforms:
            tf.status = "new"
            # This is cleared separately since it is not in the schema
            tf._partition_apps = {}
        # self._getParent().register(c)
        c.check()
        return c

    def check(self):
        """This function is called by run() or manually by the user"""
        if self.status != "new":
            logger.error("The check() function may modify a task and can therefore only be called on new tasks!")
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
                logger.warning("The 'float', the number of jobs this task may run, is still zero. Type 'tasks(%i).float = 5' to allow this task to submit 5 jobs at a time" % self.id)
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

    def setBackend(self, backend):
        """Sets the backend on all transforms"""
        for tf in self.transforms:
            if backend is None:
                tf.backend = None
            else:
                tf.backend = stripProxy(backend).clone()

    def setParameter(self, **args):
        """Use: setParameter(processName="HWW") to set the processName in all applications to "HWW"
           Warns if applications are not affected because they lack the parameter"""
        for name, parm in args.iteritems():
            for tf in [t for t in self.transforms if t.application]:
                if name in tf.application.getNodeData():
                    addProxy(tf.application).__setattr__(name, parm)
                else:
                    logger.warning("Transform %i was not affected!", tf.name)

    def insertTransform(self, id, tf):
        """Insert transfrm tf before index id (counting from 0)"""
        if self.status != "new" and id < len(self.transforms):
            logger.error("You can only insert transforms at the end of the list. Only if a task is new it can be freely modified!")
            return
        # self.transforms.insert(id,tf.copy()) # this would be safer, but
        # breaks user exspectations
        # this means that t.insertTransform(0,t2.transforms[0]) will cause
        # Great Breakage
        self.transforms.insert(id, tf)

    def appendTransform(self, tf):
        """Append transform"""
        return self.insertTransform(len(self.transforms), tf)

    def removeTransform(self, id):
        """Remove the transform with the index id (counting from 0)"""
        if self.status != "new":
            logger.error("You can only remove transforms if the task is new!")
            return
        del self.transforms[id]

    def getJobs(self, transform=None, partition=None, only_master_jobs=True):
        """ Get the job slice of all jobs that process this task """
        if not partition is None:
            only_master_jobs = False
        jobslice = JobRegistrySlice("tasks(%i).getJobs(transform=%s, partition=%s, only_master_jobs=%s)" % (
            self.id, transform, partition, only_master_jobs))

        def addjob(j):
            if transform is None or partition is None or self.transforms[int(transform)]._app_partition[j.application.id] == partition:
                jobslice.objects[j.fqid] = stripProxy(j)

        for j in GPI.jobs:
            try:
                stid = j.application.tasks_id.split(":")
                if int(stid[-2]) == self.id and (transform is None or stid[-1] == str(transform)):
                    if j.subjobs and not only_master_jobs:
                        for sj in j.subjobs:
                            addjob(sj)
                    else:
                        addjob(j)
            except Exception as err:
                logger.debug("getJobs Error!!")
                logger.debug("Error:\n%s" % str(err))
                # print x
                pass
        return JobRegistrySliceProxy(jobslice)

# Internal methods
    def finaliseTransforms(self):
        """Check for any things needing doing after a transform has completed"""
        for t in self.transforms:
            t.finalise()

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
                logger.warning("Task %i '%s' has completed!" %
                               (self.id, self.name))
            elif self.status == "completed":
                logger.warning(
                    "Task %i '%s' has been reopened!" % (self.id, self.name))
        self.status = new_status
        return self.status

    def submitJobs(self):
        """Submits as many jobs as necessary to maintain the float. Internal"""
        numjobs = 0
        if not self.status in ["running", "running/pause"]:
            return 0

        for i in range(len(self.transforms) - 1, -1, -1):
            tf = self.transforms[i]
            to_run = self.float - self.n_status("running")
            run = (self.resub_limit * self.float >= self.n_status("running"))
            if tf.status == "running" and to_run > 0 and run:
                numjobs += tf.submitJobs(to_run)
        return numjobs

    # Information methods
    def n_all(self):
        return sum([t.n_all() for t in self.transforms])

    def n_status(self, status):
        return sum([t.n_status(status) for t in self.transforms])

    def table(self):
        from Ganga.GPI import tasks
        tasks[self.id:self.id].table()

    def overview(self):
        """ Get an ascii art overview over task status. Can be overridden """
        logger.info("Colours: " + ", ".join([markup(key, overview_colours[key])
                                             for key in ["hold", "ready", "running", "completed", "attempted", "failed", "bad", "unknown"]]))
        logger.info(
            "Lists the partitions of events that are processed in one job, and the number of failures to process it.")
        logger.info("Format: (partition number)[:(number of failed attempts)]")
        logger.info('')
        for t in self.transforms:
            t.overview()

    def info(self):
        for t in self.transforms:
            t.info()

    def help(self):
        logger.info("This is a Task without special properties")

    # Helper methods
    def float_all(self):
        self.float = self.n_all()

    def run_all(self):
        self.float_all()
        self.run()
