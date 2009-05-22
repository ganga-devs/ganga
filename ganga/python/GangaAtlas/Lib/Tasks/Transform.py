
from common import *
from sets import Set
from TaskApplication import ExecutableTask
from Ganga.GPIDev.Lib.Job.Job import JobError

class Transform(GangaObject):
   _schema = Schema(Version(1,0), {
        'status'         : SimpleItem(defvalue='new', protected=1, copyable=0, doc='Status - running, pause or completed', typelist=["str"]),
        'name'           : SimpleItem(defvalue='Simple Transform', doc='Name of the transform (cosmetic)', typelist=["str"]),
        'application'    : ComponentItem('applications', defvalue=None, optional=1, load_default=False, checkset="checkTaskApplication",doc='Application of the Transform. Must be a Task-Supporting application.'),
        'inputsandbox'   : FileItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File'],sequence=1,doc="list of File objects shipped to the worker node "),
        'outputsandbox'  : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of filenames or patterns shipped from the worker node"),
        'inputdata'      : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Input dataset'),
        'outputdata'     : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Output dataset'),
        'backend'        : ComponentItem('backends', defvalue=None, optional=1,load_default=False, doc='Backend of the Transform.'),
        'run_limit'      : SimpleItem(defvalue=4, doc='Number of times a partition is tried to be processed.', protected=1, typelist=["int"]),
        '_partition_status': SimpleItem(defvalue={}, hidden=1, copyable=0, doc='Map (only necessary) partitions to their status'),
        '_app_partition' : SimpleItem(defvalue={}, hidden=1, copyable=0,doc='Map of applications to partitions'),
        '_app_status'    : SimpleItem(defvalue={}, hidden=1, copyable=0, doc='Map of applications to status'),
        '_next_app_id'   : SimpleItem(defvalue=0, hidden=1, copyable=0, doc='Next ID used for the application', typelist=["int"]),
    })

   _category = 'transforms'
   _name = 'Transform'
   _exportmethods = [ 
                      'run', 'pause', # Operations
                      'setPartitionStatus', 'setRunlimit', 'setFailed', # Control Partitions
                      'overview', 'info', 'n_all', 'n_status' # Info
                    ]

#   _app_status = {}
   _partition_apps = {}

   # possible partition status values:
   # ignored, hold, ready, running, completed, attempted, failed, bad

## Special methods:
   def __init__(self):
      super(Transform,self).__init__()
      self.initialize()
      self.startup()

   def _readonly(self):
      """A transform is read-only if the status is not new."""
      if self.status == "new":
         return 0
      return 1

   def initialize(self):
      self.backend = stripProxy(GPI.Local()) 

   def check(self):
      pass

   def startup(self):
      """This function is used to set the status after restarting Ganga"""
      ## Create the reverse map _partition_apps from _app_partition 
      self._partition_apps = {}
      for (app, partition) in self._app_partition.iteritems():
         if partition in self._partition_apps:
            self._partition_apps[partition].append(app)
         else:
            self._partition_apps[partition] = [app]

   def fix(self):
      """This function fixes inconsistencies in application status"""
      ## Create the reverse map _partition_apps from _app_partition 
      self._app_status = {}
      # Make sure that no partitions are kept "running" from previous sessions
      clist = self._partition_status.keys()
      for c in clist:
         self.updatePartitionStatus(c)
      # At this point the applications still need to notify the Transformation of their status
      # Search jobs for task-supporting applications
      for j in GPI.jobs:
         if "tasks_id" in j.application._impl._data:
            try:
               if j.subjobs:
                  for sj in j.subjobs:
                     app = sj.application._impl
                     app.getTransform()._impl.setAppStatus(app,app._getParent().status)
               else:
                  app = j.application._impl
                  app.getTransform()._impl.setAppStatus(app,app._getParent().status)
            except AttributeError, e:
               logger.error("%s",e)

## Public methods
   def run(self, check=True):
      """Sets this transform to running status"""
      if check:
         self.check()
      if self.status != "completed":
         self.status = "running"
      else:
         logger.warning("Transform is already completed!")

   def pause(self):
      """Pause the task - the background thread will not submit new jobs from this task"""
      if self.status != "completed":
         self.status = "pause"
      else:
         logger.warning("Transform is already completed!")

   def setRunlimit(self,newRL):
      """Set the number of times a job should be resubmitted before the transform is paused"""
      self.run_limit = newRL
      cs = self._partition_status.items()
      for (c,s) in cs:
         if s in ["attempted","failed"]:
            failures = len([1 for app in self._partition_apps[c] if app in self._app_status and self._app_status[app] in ["new","failed"]])
            if failures >= newRL:
               self._partition_status[c] = "failed"
            else:
               self._partition_status[c] = "attempted"
      logger.debug("Runlimit set to %i", newRL)

   def setPartitionStatus(self, partition, status):
      """ Set the Status of the given partition to "ready", "hold", "bad" or "completed".
          The status is then updated to the status indicated by the applications"""
      self.setPartitionsStatus([partition],status) 

   def setFailed(self, partition):
      """ Tells Tasks that all Applications that have executed this partition have actually failed."""
      for aid in self._app_partition:
         if aid in self._app_status and self._app_status[aid] == "removed":
            continue
         # Save the status
         self._app_status[aid] = "failed"
         # Update the corresponding partition status
      self.setPartitionStatus(partition, "ready")

## Internal methods
   def submitJobs(self, n):
      """Create Ganga Jobs for the next N partitions that are ready and submit them."""
      next = self.getNextPartitions(n)
      if len(next) == 0:
         return
      for j in self.getJobsForPartitions(next):
         j.application._impl.transition_update("submitting")
         try:
            j.submit()
         except JobError:
            logger.error("Error on job submission! The current transform will be paused until this problem is fixed.")
            logger.error("type tasks(%i).run() to continue after the problem has been fixed.", self._getParent().id)
            self.pause()

   def checkTaskApplication(self,app):
      """warns the user if the application is not compatible """
      if app == None:
         return
      try:
         app.tasks_id = "-1:-1"
      except AttributeError:
         logger.error("The application %s can not be used with the Tasks package.", app)
         logger.error("Please contact the Tasks developers if you want to use this Application with tasks.")
         logger.error("(This is a very simple operation, so do not hesitate!)")
         raise AttributeError("This application can not yet be used with the Tasks package, tell us if this should change!")
      return app
  
   def setAppStatus(self, app, new_status):
      """Reports status changes in application jobs
         possible status values: 
         normal   : (new, submitting,) submitted, running, completing, completed
         failures : killed, failed
         transient: incomplete (->new), unknown, removed"""

      # Check if we know the occurring application...
      if not app.id in self._app_partition:
         logger.warning("%s was contacted by an unknown application %i.", self.fqn(), self._getParent().id, app.id)
         return
      # Silently ignore message if the application is already removed
      if app.id in self._app_status and self._app_status[app.id] == "removed":
         return
      # Check the status
      if new_status == "completed" and not self.checkCompletedApp(app):
         # TODO: make this message only appear once, and not in every Ganga session...
         logger.error("%s app %i failed despite listed as completed!",self.fqn(), app.id)
         new_status = "failed"
      # Save the status
      self._app_status[app.id] = new_status
      # Update the corresponding partition status
      self.updatePartitionStatus(self._app_partition[app.id])

   def updatePartitionStatus(self, partition):
      """ Calculate the correct status of the given partition. 
          "completed" and "bad" is never changed here
          "hold" is only changed to "completed" here. """
      #print "updatePartitionStatus ", partition, " transform ", self.id
      ## If the partition has status, and is not in a fixed state, check it!
      if partition in self._partition_status and (not self._partition_status[partition] in ["bad","completed"]):
         ## if we have no applications, we are in "ready" state
         if not partition in self._partition_apps:
            if self._partition_status[partition] != "hold":
               self._partition_status[partition] = "ready"
         else:
            status = [self._app_status[app] for app in self._partition_apps[partition] 
               if app in self._app_status and not self._app_status[app] in ["removed","killed"]]
            ## Check if we have completed this partition
            if "completed" in status:
               self._partition_status[partition] = "completed"
            ## Check if we are not on hold
            elif self._partition_status[partition] != "hold":
               ## Check if we are running
               running = False
               for stat in ["completing", "running", "submitted", "submitting"]:
                  if stat in status:
                     self._partition_status[partition] = "running"
                     running = True
                     break
               if not running:
                  ## Check if we failed
                  failures = len([stat for stat in status if stat in ["failed","new"]])
                  if failures >= self.run_limit:
                     self._partition_status[partition] = "failed"
                  elif failures > 0:
                     self._partition_status[partition] = "attempted"
                  else:
                     ## Here we only have some "unknown" applications
                     ## This could prove difficult when launching new applications. Care has to be taken
                     ## to get the applications out of "unknown" stats as quickly as possible, to avoid double submissions.
                     #logger.warning("Partition with only unknown applications encountered. This is probably not a problem.")
                     self._partition_status[partition] = "ready"
      ## Notify the next transform (if any) of the change in input status
      task = self._getParent()
      if task and (task.transforms.index(self) + 1 < len(task.transforms)):
         task.transforms[task.transforms.index(self) + 1].updateInputStatus(self, partition)
      ## Update the Tasks status if necessary
      if partition in self._partition_status and self._partition_status[partition] in ["completed","bad"] and self.status == "running":
         for s in self._partition_status.values():
            if s != "completed" and s != "bad":
               return
         self.status = "completed"
         if task:
            task.updateStatus()
      elif self.status == "completed":
         for s in self._partition_status.values():
            if s != "completed" and s != "bad":
               self.status = "running"
               if task:
                  task.updateStatus()
               return

   def setPartitionsStatus(self, partitions, status):
      """ Set the Status of the partitions to "ready", "hold", "bad" or "completed".
          The status is then updated to the status indicated by the applications
          "bad" and "completed" is never changed except to "ignored", "hold" is only changed to "completed". """
      if status == "ignored":
         [self._partition_status.pop(c) for c in partitions if c in self._partition_status]
      elif status in ["ready","hold", "bad", "completed"]:
         for c in partitions:
            self._partition_status[c] = status
      else:
         logger.error("setPartitionsStatus called with invalid status string %s", status)
      for c in partitions:
         self.updatePartitionStatus(c)

   def setPartitionsLimit(self, limitpartition):
      """ Set all partitions from and including limitpartition to ignored """
      partitions = [c for c in self._partition_status if c >= limitpartition]
      self.setPartitionsStatus(partitions,"ignored")

   def getPartitionStatus(self, partition):
      if partition in self._partition_status:
         return self._partition_status[partition]
      else:
         return "ignored"

   def getNextPartitions(self, n):
      """Returns the N next partitions to process"""
      partitionlist = [c for c in self._partition_status if self._partition_status[c] in ["ready","attempted"]]
      partitionlist.sort()
      return partitionlist[:n]

   def getNewAppID(self, partition):
      """ Returns a new application ID and associates this ID with the partition given. """
      id = self._next_app_id
      self._app_partition[id] = partition
      if partition in self._partition_apps:
         self._partition_apps[partition].append(id)
      else:
         self._partition_apps[partition] = [id]
      self._next_app_id += 1
      return id

   def createNewJob(self, partition):
      """ Returns a new job initialized with the transforms application, backend and name """
      task = self._getParent() # this works because createNewJob is only called by a task
      id = task.transforms.index(self)
      j = GPI.Job()
      j.backend = self.backend
      j.application = self.application
      j.application._impl.tasks_id = "%i:%i" % (task.id, id)
      j.application._impl.id = self.getNewAppID(partition)
      j.inputdata = self.inputdata
      j.outputdata = self.outputdata
      j.inputsandbox = self.inputsandbox
      j.outputsandbox = self.outputsandbox
      j.name = "T%i:%i C%i" % (task.id, id, partition)
      return j

## Methods that can/should be overridden by derived classes
   def checkCompletedApp(self, app):
      """Can be overriden to improve application completeness checking"""
      return True
 
   def updateInputStatus(self, ltf, partition):
      """Is called my the last transform (ltf) if the partition 'partition' changes status"""
      # per default no dependencies exist
      pass

   def getJobsForPartitions(self, partitions):
      """This is only an example, this class should be overridden by derived classes"""
      return [self.createNewJob(p) for p in partitions]

## Information methods
   def fqn(self):
      task = self._getParent()
      if task:
         return "Task %i Transform %i" % (task.id, task.transforms.index(self))
      else:
         return "Unassigned Transform '%s'" % (self.name)

   def n_all(self):
      return len(self._partition_status)

   def n_status(self,status):
      return len([cs for cs in self._partition_status.values() if cs == status])

   def overview(self):
      """ Get an ascii art overview over task status. Can be overridden """
      o = markup("%s '%s'\n" % (self.__class__.__name__, self.name), status_colours[self.status])
      i = 0
      for (c,s) in self._partition_status.iteritems():
         if c in self._partition_apps:
            failures = len([1 for app in self._partition_apps[c] if app in self._app_status and self._app_status[app] in ["new","failed"]])
            o += markup("%i:%i " % (c, failures), overview_colours[s])
         else:
            o += markup("%i " % c, overview_colours[s])
         i+=1
         if i % 20 == 0: o+="\n"
      print o

   def info(self):
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      print "* backend: %s" % self.backend.__class__.__name__
      print "Application:"
      self.application.printTree() 


