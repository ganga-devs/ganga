from common import *
import os, time, thread

class TaskList(GangaObject):
   """This command is the main interface to Ganga Tasks. For a short introduction and 'cheat sheet', try tasks.help()"""
   _schema = Schema(Version(2,0), {
        'print_help'   : SimpleItem(defvalue=True,copyable=1,doc='If True print the Help every time tasks is first typed in a session.',typelist=["bool"]),
        'tasks'        : ComponentItem('tasks',defvalue=[],sequence=1,hidden=1,copyable=1,doc='list of tasks'),
        'next_task_id' : SimpleItem(defvalue=0,protected=1,hidden=1,copyable=1,doc='the number of the next created task.',typelist=["int"]),
        })    
   _category = 'tasklists'
   _name = 'TaskList'
   _exportmethods = ['get', 'remove', 'help', 'table', '__str__']

   _help_printed = False
   _run_thread = False
   _thread = None

   def __init__(self):
      super(TaskList,self).__init__()

   def save(self):
      """Writes all tasks to a file 'tasks.xml' in the job repository. This function is usually called automatically. """
      # First write to tasks.dat.tmp to avoid losing the content in case of crash
      fna = os.path.join(GPI.config.Configuration["gangadir"], "tasks.xml.tmp")
      fnb = os.path.join(GPI.config.Configuration["gangadir"], "tasks.xml")
      from Ganga.Core.JobRepositoryXML.VStreamer import to_file 
      to_file(self, file(fna,"w"))
      try:
         os.rename(fna,fnb)
      except OSError:
         logger.debug("Saving resulted in no file being created. This is normal during startup.")

   def setup(self):
      """ Setup the task structure, scan the applications and correct ids.
          MUST not reset user settings. """
      # Call tasks setup first
      for t in self.tasks:
         t.setup()
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
      self.save()

   def register(self,chi):
      """ Adds a task 'chi' to the task list. Should only be called by the Task constructor """
      self.tasks.append(chi)
      chi.id = self.next_task_id
      self.next_task_id += 1
      self.save()
      chi.setup()
      chi.update()

   def table(self):
      """Prints a more detailed table of tasks and their transforms"""
      return self.__str__(False)

   def __str__(self, short=True):
      """Prints an overview over the currently running tasks"""
      if self.print_help and not self._help_printed:
         self.help(short = True)
         self._help_printed = True
         print "If you don't want to see this help message again each session, type 'tasks.print_help = False'."
         print "To show this help message again, type 'tasks.help()'."
         print

      fstring = " %3s | %17s | %12s | %9s | %33s | %5s\n"
      ds = "\n" + fstring % ("#", "Type", "Name", "State", "%4s: %4s/ %4s/ %4s/ %4s/ %4s" % (
           "Jobs",markup("done",overview_colours["completed"])," "+markup("run",overview_colours["running"]),markup("fail",overview_colours["failed"]),markup("hold",overview_colours["hold"])," "+markup("bad",overview_colours["bad"])), "Float")
      ds += "-"*96 + "\n"
      for p in self.tasks:
         stat = "%4i: %4i/ %4i/ %4i/ %4i/ %4i" % (
                p.n_all(), p.n_status("completed"),p.n_status("running"),p.n_status("failed"),p.n_status("hold"),p.n_status("bad"))
         ds += markup(fstring % (p.id, p.__class__.__name__, p.name, p.status, stat, p.float), status_colours[p.status])
         if short:
            continue
         for t in p.transforms:
            if t.__class__.__name__ == "DQ2Input":
               ds += markup(fstring[:19] % (".%i"%t.id, t.__class__.__name__, t.name), status_colours[t.status]) + "\n"
            else:
               stat = "%4i: %4i/ %4i/ %4i/ %4i/ %4s" % (
                      t.n_all(), t.n_status("completed"),t.n_status("running"),t.n_status("failed"),t.n_status("hold"),t.n_status("bad"))
               ds += markup(fstring % (".%i"%t.id, t.__class__.__name__, t.name, t.status, stat, ""), status_colours[t.status])
         ds += "-"*96 + "\n"
      return ds + "\n"

   def get(self,id):
      """Returns the task"""
      ps = [p for p in self.tasks if p.id == id]
      if len(ps) > 0:
         return addProxy(ps[0])
      logger.error("No Task with ID #%i" % id)

   def remove(self,id,really=False):
      """Delete the task"""
      for i in range(0,len(self.tasks)):
         if self.tasks[i].id == id:
            if not really == True:
               print "You want to remove the task %i named '%s'." % (id,self.tasks[i].name)
               print "Since this operation cannot be easily undone, please call this command again as tasks.remove(%i,really=True)" % (id)
               return
            del self.tasks[i]
            logger.info("Task #%s deleted" % id)
            self.save()
            return
      logger.error("No Task with ID #%i" % id)

   def _thread_main(self):
      """ This is an internal function; the main loop of the background thread """
      ## Wait until Ganga is fully initialized      
      while not ("jobs" in reload(GPI).__dict__):
         time.sleep(0.4)
      while not ("config" in reload(GPI).__dict__):
         time.sleep(0.4)

      time.sleep(0.5)

      ## Setup all relations
      self.setup()

      ## Main loop
      while self._run_thread: 
         logger.debug("Background task thread waking up...")
         ## Try to save the task to file
         try:
            self.save()
         except Exception,x:
            logger.error("Could not save tasks to file: %s" % x)
            logger.error("Disk full? Quota exceeded?")

         ## For each task try to run it
         for p in [p for p in self.tasks if p.status in ["running"]]:
            try:
               # TODO: Make this user-configurable and add better error message 
               if (p.n_status("failed")*100.0/(20+p.n_status("completed")) > 20):
                  p.pause()
                  logger.error("Task %s paused - %i jobs have failed while only %i jobs have completed successfully." % (p.name,f, c))
                  logger.error("Please investigate the cause of the failing jobs and then remove the previously failed jobs using job.remove()")
                  logger.error("You can then continue to run this task with tasks.get(%i).run()" % p.id)
                  continue
               p.submitJobs()
            except Exception, x:
                  logger.error("Exception occurred in task monitoring loop: %s\nThe offending task was paused." % x)
                  p.pause()
                  self.save()
         time.sleep(10)
      logger.error("Backgroud task thread stopped manually.")

   def start(self):
      """ Start a background thread that periodically run()s"""
      if self._run_thread or self._thread:
         logger.warning("It seems that the task thread is already running.")
         return
      self._run_thread = True
      self._thread = thread.start_new(self._thread_main, ())

   def help(self, short=False):
      """Print a short introduction and 'cheat sheet' for the Ganga Tasks package"""
      print
      print markup(" *** Ganga Tasks: Short Introduction and 'Cheat Sheet' ***", fgcol("blue"))
      print 
      print markup("Definitions: ", fgcol("red")) + "'Partition'     - A unit of processing, for example processing a file or processing some events from a file"
      print "             'Transform' - A group of partitions that have a common Ganga Application and Backend."
      print "             'Task'      - A group of one or more 'Transforms' that can have dependencies on each other"
      print 
      print markup("Possible status values for partitions:", fgcol("red"))
      print ' * "' + markup("ready", overview_colours["ready"]) + '"    - ready to be executed '
      print ' * "' + markup("hold", overview_colours["hold"]) + '"     - dependencies not completed'
      print ' * "' + markup("running", overview_colours["running"]) + '"  - at least one job tries to process this partition'
      print ' * "' + markup("attempted", overview_colours["attempted"]) + '"- tasks tried to process this partition, but has not yet succeeded'
      print ' * "' + markup("failed", overview_colours["failed"]) + '"   - tasks failed to process this partition several times'
      print ' * "' + markup("bad", overview_colours["bad"]) + '"      - this partition is excluded from further processing and will not be used as input to subsequent transforms'
      print ' * "' + markup("completed", overview_colours["completed"]) + '" '
      print
      def c(s):
         return markup(s,fgcol("blue"))
      print markup("Important commands:", fgcol("red"))
      print " Get a quick overview     : "+c("tasks")+"                  Get a detailed view    : "+c("tasks.table()") 
      print " Access an existing task  : "+c("t = tasks.get(id)")+"      Remove a Task          : "+c("tasks.remove(id)")
      print " Create a new (MC) Task   : "+c("t = MCTask()")+"           Copy a Task            : "+c("nt = t.copy()")
      print " Show task configuration  : "+c("t.info()")+"               Show processing status : "+c("t.overview()")
      print " Set the float of a Task  : "+c("t.float = 100")+"          Set the name of a task : "+c("t.name = 'My Own Task v1'")
      print " Start processing         : "+c("t.run()")+"                Pause processing       : "+c("t.pause()")
      print " Access Transform id N    : "+c("tf = t.transforms[N]")+"   Pause processing of tf : "+c("tf.pause()")+"  # This command is reverted by using t.run()"
      print " Transform Application    : "+c("tf.application")+"         Transform Backend      : "+c("tf.backend")
      print 
      print " Set parameter in all applications       : "+c("t.setParameter(my_software_version='1.42.0')")
      print " Set backend for all transforms          : "+c("t.setBackend(backend) , p.e. t.setBackend(LCG())")
      print " Limit on how often jobs are resubmitted : "+c("tf.run_limit = 4")
      print " Manually change the status of partitions: "+c("tf.setPartitionStatus(partition, 'status')")
      print 
      print " For a Monte Carlo Production Example and specific help type: "+c("MCTask?")
      print 
      if not True:
#      if not short:
         print "ADVANCED COMMANDS:"
         print "Add Transform  at position N      : t.insertTransform(N, transform)"
         print "Remove Transform  at position N   : t.removeTransform(N)"
         print "Set Transform Application         : tf.application = TaskApp() #This Application must be a 'Task Version' of the usual application" 
         print "   Adding Task Versions of Applications is easy, contact the developers to request an inclusion"
      

