from common import *

########################################################################

class Task(GangaObject):
   """This is a Task without special properties"""
   _schema = Schema(Version(1,0), {
        'transforms'  : ComponentItem('transforms',defvalue=[],sequence=1,copyable=1,checkset='dirty',doc='list of transforms'),
        'id'          : SimpleItem(defvalue=-1, protected=1, doc='ID of the Task', typelist=["int"]),
        'name'        : SimpleItem(defvalue='NewTask', doc='Name of the Task', typelist=["str"]),
        'status'      : SimpleItem(defvalue='new', protected=1, doc='Status - new, running, pause or completed', typelist=["str"]),
        'float'       : SimpleItem(defvalue=0, doc='Number of Jobs run concurrently', typelist=["int"]),
        'resub_limit' : SimpleItem(defvalue=0.9, doc='Resubmit only if the number of running jobs is less than "resub_limit" times the float. This makes the job table clearer, since more jobs can be submitted as subjobs.', typelist=["float"]),
    })

   _category = 'tasks'
   _name = 'Task'
   _exportmethods = [
                'run', 'pause', # Operations
                'setBackend', 'setParameter', 'insertTransform', 'removeTransform', # Settings
                'overview', 'info', 'n_all', 'n_status', 'help' # Info
                ]
   _do_update = True
   
   def dirty(self,var=0):
      self._do_update = True
      return var

   def __init__(self):
      # some black magic to allow derived classes to specify inherited methods in
      # the _exportmethods variable without redefining them
      from Ganga.GPIDev.Base.Proxy import ProxyMethodDescriptor
      for t in Task.__dict__:
         if (not t in self._proxyClass.__dict__) and (t in self._exportmethods):
            f = ProxyMethodDescriptor(t,t)
            f.__doc__ = Task.__dict__[t].__doc__
            setattr(self._proxyClass, t, f)
#      f = ProxyMethodDescriptor("copy", "copy")
#      f.__doc__ = "Copy this task, register it in the task list and return a new task"
#      setattr(self._proxyClass, t, f)
      super(Task,self).__init__()
      # The following will only be executed if the user directly created this instance
      # (if it was not loaded from a file)
      if "tasks" in GPI.__dict__.keys():
         GPI.tasks._impl.register(self)
         self.initialize()

   def clone(self):
      c = super(Task,self).clone()
      for tf in c.transforms:
         tf.task_id = c.id
         tf.status = "pause"
         tf._partition_status = {}
         tf._app_partition = {}
         tf._next_app_id = 0
      GPI.tasks._impl.register(c)
      return c

   def initialize(self):
      """This function is executed only if the task is created new """
      pass

   def setup(self):
      """This function is executed only on ganga startup"""
      for t in self.transforms:
         t.setup()
      self.update()

   def update(self):
      """This function is called initially and on major updates"""
      if not self._do_update:
         return False
      else:
         self._do_update = False
      for i in range(0,len(self.transforms)):
         self.transforms[i].id = i
         self.transforms[i].task_id = self.id
      for t in self.transforms:
         t.update()
      self.updateStatus()
      return True

### Run control
   def run(self):
      """Confirms that this task is fully configured and ready to be run."""
      self.update()
      if self.status != "completed":
         self.status = "running"
         if self.float == 0:
            logger.warning("The 'float', the number of jobs this task may run, is still zero. Type 'tasks.get(%i).float = 5' to allow this task to submit 5 jobs at a time")
         for tf in self.transforms:
            if tf.status != "completed":
               tf.run()
      else:
         logger.info("Task is already completed!")

   def pause(self):
      """Pause the task - the background thread will not submit new jobs from this task"""
      self.update()
      if self.status != "completed":
         self.status = "pause"
      else:
         logger.info("Transform is already completed!")

   def updateStatus(self): 
      self.update()
      if self.status == "running":
         for tf in self.transforms:
            if tf.status != "completed":
               return
         self.status = "completed"
         print "Task %i '%s' has completed!" % (self.id, self.name)
      if self.status == "completed":
         for tf in self.transforms:
            if tf.status != "completed":
               self.status = "running"
               print "Task %i '%s' has been reopened!" % (self.id, self.name)
               return

### Submission
   def submitJobs(self):
      """Submits as many jobs as necessary to maintain the float."""
      self.update()
      if self.status != "running":
         logger.error("Cannot run jobs for task #%i because the task is %s." % (self.id, self.status))
         return
      for i in range(len(self.transforms)-1,-1,-1):
         tf = self.transforms[i]
         to_run = self.float - self.n_status("running")
         run = (self.resub_limit * self.float >= self.n_status("running"))
         if tf.status != "pause" and to_run > 0 and run:
            tf.submitJobs(to_run)

   def insertTransform(self, id, tf):
      """Insert transfrm tf before index id (counting from 0)"""
      self.transforms.insert(id,tf)
      self.dirty()
      self.update()

   def removeTransform(self, id):
      """Remove the transform with the index id (counting from 0)"""
      del self.transforms[id]
      self.dirty()
      self.update()

   def setBackend(self,backend):
      """Sets the backend on all transforms, except if the backend is None"""
      for tf in self.transforms:
         if tf.backend:
            tf.backend = stripProxy(backend)

   def setParameter(self,**args):
      """Use: setParameter(processName="HWW") to set the processName in all applications to "HWW"
         Warns if applications are not affected because they lack the parameter"""
      for name, parm in args.iteritems():
         for tf in [t for t in self.transforms if t.application]:
            if name in tf.application._data:
               addProxy(tf.application).__setattr__(name, parm)
            else:
               logger.warning("Transform %i was not affected!", tf.id)

################# INFO STUFF
   def n_all(self):
      self.update()
      return sum([t.n_all() for t in self.transforms])

   def n_status(self,status):
      self.update()
      return sum([t.n_status(status) for t in self.transforms])

   def overview(self):
      """ Get an ascii art overview over task status. Can be overridden """
      self.update()
      print "Colours: " + ", ".join([markup(key, overview_colours[key])
          for key in ["hold", "ready", "running", "completed", "attempted", "failed", "bad", "unknown"]])
      print "Lists the partitions of events that are processed in one job, and the number of failures to process it."
      print "Format: (partition number)[:(number of failed attempts)]"
      print

      for t in self.transforms:
         t.overview()

   def info(self):
      self.update()
      for t in self.transforms:
         t.info()

   def help(self):
      print "This is a Task without special properties"
