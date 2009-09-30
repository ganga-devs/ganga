from common import *
from Ganga.GPIDev.Lib.JobRegistry.JobRegistryDev import JobRegistryInstanceInterface
from Ganga.GPIDev.Lib.JobRegistry.JobRegistry import JobRegistryInterface

########################################################################

class Task(GangaObject):
    """This is a Task without special properties"""
    _schema = Schema(Version(1,0), {
        'transforms'  : ComponentItem('transforms',defvalue=[],sequence=1,copyable=1,doc='list of transforms'),
        'id'          : SimpleItem(defvalue=-1, protected=1, doc='ID of the Task', typelist=["int"]),
        'name'        : SimpleItem(defvalue='NewTask', copyable=1, doc='Name of the Task', typelist=["str"]),
        'status'      : SimpleItem(defvalue='new', protected=1, doc='Status - new, running, pause or completed', typelist=["str"]),
        'float'       : SimpleItem(defvalue=0, copyable=1, doc='Number of Jobs run concurrently', typelist=["int"]),
        'resub_limit' : SimpleItem(defvalue=0.9, copyable=1, doc='Resubmit only if the number of running jobs is less than "resub_limit" times the float. This makes the job table clearer, since more jobs can be submitted as subjobs.', typelist=["float"]),
        })

    _category = 'tasks'
    _name = 'Task'
    _exportmethods = [
                'setBackend', 'setParameter', 'insertTransform', 'appendTransform', 'removeTransform', # Settings
                'check', 'run', 'pause', 'remove', # Operations
                'overview', 'info', 'n_all', 'n_status', 'help', 'getJobs' # Info
                ]

## Special methods:  
    def __init__(self):
        super(Task,self).__init__()
        GPI.tasks._impl.register(self)
        self.initialize()
        self.startup()

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

## Public methods:
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

    def remove(self,remove_jobs="do_nothing"):
        """Delete the task"""
        if not remove_jobs in [True,False]:
            print "You want to remove the task %i named '%s'." % (self.id,self.name)
            print "Since this operation cannot be easily undone, please call this command again:"
            print " * as tasks(%i).remove(remove_jobs=True) if you want to remove all associated jobs," % (self.id)
            print " * as tasks(%i).remove(remove_jobs=False) if you want to keep the jobs." % (self.id)
            return
        if remove_jobs:
            for j in GPI.jobs:
                try:
                    stid = j.application.tasks_id.split(":")
                    if int(stid[-2]) == self.id:
                        j.remove()
                except Exception, x:
                    pass
        self._getParent().tasks.remove(self)
        logger.info("Task #%s deleted" % self.id)

    def clone(self):
        c = super(Task,self).clone()
        for tf in c.transforms:
            tf.status = "new"
            tf._partition_apps = {} # This is cleared separately since it is not in the schema
        self._getParent().register(c)
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
        if self.status != "completed":
            for tf in self.transforms:
                tf.pause()
            self.status = "pause"
        else:
            logger.info("Transform is already completed!")

    def setBackend(self,backend):
        """Sets the backend on all transforms, except if the backend is None"""
        for tf in self.transforms:
            tf.backend = stripProxy(backend)

    def setParameter(self,**args):
        """Use: setParameter(processName="HWW") to set the processName in all applications to "HWW"
           Warns if applications are not affected because they lack the parameter"""
        for name, parm in args.iteritems():
            for tf in [t for t in self.transforms if t.application]:
                if name in tf.application._data:
                    addProxy(tf.application).__setattr__(name, parm)
                else:
                    logger.warning("Transform %i was not affected!", tf.name)

    def insertTransform(self, id, tf):
        """Insert transfrm tf before index id (counting from 0)"""
        if self.status != "new" and id < len(self.transforms):
            logger.error("You can only insert transforms at the end of the list. Only if a task is new it can be freely modified!")
            return
        self.transforms.insert(id,tf.copy())

    def appendTransform(self, tf):
        """Append transform"""
        return self.insertTransform(len(self.transforms), tf)

    def removeTransform(self, id):
        """Remove the transform with the index id (counting from 0)"""
        if self.status != "new":
            logger.error("You can only remove transforms if the task is new!")
            return
        del self.transforms[id]

    def getJobs(self, only_master_jobs=True):
        """ Get the job slice of all jobs that process this task """
        jobslice = JobRegistryInstanceInterface("tasks(%i).getJobs(only_master_jobs=%s)"%(self.id, only_master_jobs))
        for j in GPI.jobs:
            try:
                stid = j.application.tasks_id.split(":")
                if int(stid[-2]) == self.id:
                    if j.subjobs and not only_master_jobs:
                        for sj in j.subjobs:
                            jobslice.jobs[sj.fqid] = stripProxy(sj)
                    else:
                        jobslice.jobs[j.fqid] = stripProxy(j)
            except Exception, x:
                print x
                pass
        return JobRegistryInterface(jobslice)

## Internal methods
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
        # Handle status changes here:
        if self.status != new_status:
            if new_status == "running/pause":
                logger.info("Some Transforms of Task %i '%s' have been paused. Check tasks.table() for details!" % (self.id, self.name))
            elif new_status == "completed":
                logger.warning("Task %i '%s' has completed!" % (self.id, self.name))
            elif self.status == "completed":
                logger.warning("Task %i '%s' has been reopened!" % (self.id, self.name))
        self.status = new_status
        return self.status

    def submitJobs(self):
        """Submits as many jobs as necessary to maintain the float. Internal"""
        for i in range(len(self.transforms)-1,-1,-1):
            tf = self.transforms[i]
            to_run = self.float - self.n_status("running")
            run = (self.resub_limit * self.float >= self.n_status("running"))
            if tf.status == "running" and to_run > 0 and run:
                tf.submitJobs(to_run)

    ## Information methods
    def n_all(self):
        return sum([t.n_all() for t in self.transforms])

    def n_status(self,status):
        return sum([t.n_status(status) for t in self.transforms])

    def overview(self):
        """ Get an ascii art overview over task status. Can be overridden """
        print "Colours: " + ", ".join([markup(key, overview_colours[key])
            for key in ["hold", "ready", "running", "completed", "attempted", "failed", "bad", "unknown"]])
        print "Lists the partitions of events that are processed in one job, and the number of failures to process it."
        print "Format: (partition number)[:(number of failed attempts)]"
        print
        for t in self.transforms:
            t.overview()

    def info(self):
        for t in self.transforms:
            t.info()

    def help(self):
        print "This is a Task without special properties"
