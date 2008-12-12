
from common import *
from sets import Set
from TaskApplication import ExecutableTask
from Ganga.GPIDev.Lib.Job.Job import JobError

class Transform(GangaObject):
   _schema = Schema(Version(1,0), {
        'id'             : SimpleItem(defvalue=-1, protected=1, doc='ID of the Transform', typelist=["int"]),
        'task_id'        : SimpleItem(defvalue=-1, protected=1, doc='ID of the Task of this Transform', typelist=["int"]),
        'status'         : SimpleItem(defvalue='pause', protected=1, doc='Status - running, pause or completed', typelist=["str"]),
        'name'           : SimpleItem(defvalue='Simple Transform', doc='Name of the transform (cosmetic)', typelist=["str"]),
        'application'    : ComponentItem('applications', defvalue=None, optional=1, load_default=False, checkset="checkTaskApplication",doc='Application of the Transform. Must be a Task-Supporting application.'),
        'inputdata'      : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Input dataset'),
        'outputdata'     : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Output dataset'),
        'backend'        : ComponentItem('backends', defvalue=None, optional=1,load_default=False, doc='Backend of the Transform.'),
        'run_limit'      : SimpleItem(defvalue=4, doc='Number of times a partition is tried to be processed.', checkset="newRunlimit",typelist=["int"]),
        '_partition_status': SimpleItem(defvalue={}, hidden=1, doc='Map (only necessary) partitions to their status'),
        '_app_partition' : SimpleItem(defvalue={}, hidden=1, doc='Map of applications to partitions'),
        '_next_app_id'   : SimpleItem(defvalue=0, hidden=1, doc='Next ID used for the application', typelist=["int"]),
    })

   _category = 'transforms'
   _name = 'Transform'
   _exportmethods = [ 
                      'run', 'pause', # Operations
                      'setPartitionStatus', # Control Partitions
                      'overview', 'info', 'n_all', 'n_status' # Info
                    ]

   _app_status = {}
   _partition_apps = {}

   def dirty(self,var = 0):
      if not "task_id" in self._data:
         return var
      if self.task_id == -1:
         return var
      task = self.getTask()
      if task:
         task._impl._do_update = True
      return var

   def task_update(self):
      if self.task_id == -1:
         return
      task = self.getTask()
      if task:
         task._impl.update()

   def newRunlimit(self,newRL):
      cs = self._partition_status.items()
      for (c,s) in cs:
         if s in ["attempted","failed"]:
            failures = len([1 for app in self._partition_apps[c] if app in self._app_status and self._app_status[app] in ["new","failed"]])
            if failures >= newRL:
               self._partition_status[c] = "failed"
            else:
               self._partition_status[c] = "attempted"

   # possible partition status values:
   # ignored, hold, ready, running, completed, attempted, failed, bad
  
   def __init__(self):
      # some black magic to allow derived classes to specify inherited methods in
      # the _exportmethods variable without redefining them
      from Ganga.GPIDev.Base.Proxy import ProxyMethodDescriptor
      for t in Transform.__dict__:
         if (not t in self._proxyClass.__dict__) and (t in self._exportmethods):
            f = ProxyMethodDescriptor(t,t)
            f.__doc__ = Transform.__dict__[t].__doc__
            setattr(self._proxyClass, t, f)
      # Call GangaObject (= superclass of Transform) constructor
      super(Transform,self).__init__()
      # Set backend to Local
      self.backend = GPI.Local()._impl
      # Initialize only if this transform is created new
      if "tasks" in GPI.__dict__.keys():
         self.initialize()

   def initialize(self):
      pass

   def setup(self):
      """This function is used to set the status after restarting Ganga"""
      ## Create the reverse map _partition_apps from _app_partition 
      self._partition_apps = {}
      self._app_status = {}
      for (app, partition) in self._app_partition.iteritems():
         if partition in self._partition_apps:
            self._partition_apps[partition].append(app)
         else:
            self._partition_apps[partition] = [app]
      # Make sure that no partitions are kept "running" from previous sessions
      clist = self._partition_status.keys()
      for c in clist:
         self.updatePartitionStatus(c)
      # At this point the applications still need to notify the Transformation of their status
      # this is done in the "setup" routine of the tasklist.

   def update(self):
      """This function is used to update the task status after major updates"""
      pass

### Run Control
   def run(self):
      """Confirms that this task is fully configured and ready to be run."""
      self.task_update()
      if self.status != "completed":
         self.status = "running"
      else:
         logger.warning("Transform is already completed!")

   def pause(self):
      """Pause the task - the background thread will not submit new jobs from this task"""
      self.task_update()
      if self.status != "completed":
         self.status = "pause"
      else:
         logger.warning("Transform is already completed!")

   def submitJobs(self, n):
      """Create Ganga Jobs for the next N partitions that are ready and submit them."""
      self.dirty()
      self.task_update()
      next = self.getNextPartitions(n)
      if len(next) == 0:
         return
      for j in self.getJobsForPartitions(next):
         j.application._impl.transition_update("submitting")
         try:
            j.submit()
         except JobError:
            logger.error("Error on job submission! The current transform will be paused until this problem is fixed.")
            logger.error("type tasks.get(%i).run() to continue after the problem has been fixed.", self.task_id)
            self.pause()

### Application check
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
  
### Partition and Application Handling
   def setAppStatus(self, app, new_status):
      """Reports status changes in application jobs
         possible status values: 
         normal   : (new, submitting,) submitted, running, completing, completed
         failures : killed, failed
         transient: incomplete (->new), unknown, removed"""
      self.task_update()
      #print "setAppStatus: ", app.id, new_status
      # Check if we know the occurring application...

      if not app.id in self._app_partition:
         logger.warning("Transform %i of task %i was contacted by an unknown application %i.", self.id, self.task_id, app.id)
         return
      if app.id in self._app_status and self._app_status[app.id] == "removed":
         return
      # Check the status
      if new_status == "completed" and not self.checkCompletedApp(app):
         logger.error("Transform %i of task %i app %i failed despite listed as completed!",self.id, self.task_id, app.id)
         new_status = "failed"
      # Save the status
      self._app_status[app.id] = new_status
      # Update the corresponding partition status
      self.updatePartitionStatus(self._app_partition[app.id])

   def checkCompletedApp(self, app):
      return True
 
   ## status calculation for partitions:
   def updatePartitionStatus(self, partition):
      """ Calculate the correct status of the given partition. 
          "completed" and "bad" is never changed here
          "hold" is only changed to "completed" here. """
      #print "updatePartitionStatus ", partition, " transform ", self.id
      self.task_update()
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
      ## Notify the next transform of the change in input status
      if self.id + 1 < len(self.getTask().transforms):
         self.getTask().transforms[self.id + 1]._impl.updateInputStatus(self, partition)
      ## Update the Tasks status if necessary
      if partition in self._partition_status and self._partition_status[partition] in ["completed","bad"] and self.status == "running":
         for s in self._partition_status.values():
            if s != "completed" and s != "bad":
               return
         self.status = "completed"
         self.getTask()._impl.updateStatus()
      if self.status == "completed":
         for s in self._partition_status.values():
            if s != "completed" and s != "bad":
               self.status = "running"
               self.getTask()._impl.updateStatus()
               return
            

   def updateInputStatus(self, ltf, partition):
      # per default no dependencies exist
      pass

   def setPartitionStatus(self, partition, status):
      """ Set the Status of the partitions to "ready", "hold", "bad" or "completed".
          The status is then updated to the status indicated by the applications
          "bad" and "completed" is never changed except to "ignored", "hold" is only changed to "completed". """
      self.setPartitionsStatus([partition],status) 

   def setPartitionsStatus(self, partitions, status):
      """ Set the Status of the partitions to "ready", "hold", "bad" or "completed".
          The status is then updated to the status indicated by the applications
          "bad" and "completed" is never changed except to "ignored", "hold" is only changed to "completed". """
      #print "setPartitionsStatus transform", self.id, ": Status ", status, partitions
      self.task_update()
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
      self.task_update()
      partitions = [c for c in self._partition_status if c >= limitpartition]
      self.setPartitionsStatus(partitions,"ignored")

### Several Getters
   def getPartitionStatus(self, partition):
      if partition in self._partition_status:
         return self._partition_status[partition]
      else:
         return "ignored"

   def getTask(self):
      return GPI.tasks.get(self.task_id)

   def getNextPartitions(self, n):
      """Returns the N next partitions to process"""
      self.task_update()
      partitionlist = [c for c in self._partition_status if self._partition_status[c] in ["ready","attempted"]]
      partitionlist.sort()
      return partitionlist[:n]

   def getNewAppID(self, partition):
      """ Returns a new application ID and associates this ID with the partition given. """
      self.task_update()
      id = self._next_app_id
      self._app_partition[id] = partition
      if partition in self._partition_apps:
         self._partition_apps[partition].append(id)
      else:
         self._partition_apps[partition] = [id]
      self._next_app_id += 1
      return id

## Job creation
   def createNewJob(self, partition):
      """ Returns a new job initialized with the transforms application, backend and name """
      j = GPI.Job()
      j.backend = self.backend
      j.application = self.application
      j.application._impl.tasks_id = "%i:%i" % (self.task_id, self.id)
      j.application._impl.id = self.getNewAppID(partition)
      j.inputdata = self.inputdata
      j.outputdata = self.outputdata
      j.name = "T%i:%i C%i" % (self.task_id, self.id, partition)
      return j

## Job creation to override
   def getJobsForPartitions(self, partitions):
      """This is only an example, this class should be overridden by derived classes"""
      return [self.createNewJob(p) for p in partitions]

################# INFO STUFF
   def logname(self):
      return "Task %i Transform %i" % (self.task_id, self.id)

   def n_all(self):
      self.task_update()
      return len(self._partition_status)

   def n_status(self,status):
      self.task_update()
      return len([cs for cs in self._partition_status.values() if cs == status])

   def overview(self):
      """ Get an ascii art overview over task status. Can be overridden """
      self.task_update()
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
      self.task_update()
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      print "* backend: %s" % self.backend.__class__.__name__
      print "Application:"
      self.application.printTree() 


