from common import *
import os, time, threading

from Ganga.Core.JobRepositoryXML.VStreamer import to_file
from StringIO import StringIO

class TaskList(GangaObject):
   """This command is the main interface to Ganga Tasks. For a short introduction and 'cheat sheet', try tasks.help()"""
   _schema = Schema(Version(2,0), {
        'tasks'        : ComponentItem('tasks',defvalue=[],sequence=1,hidden=1,copyable=1,doc='list of tasks'),
        'next_task_id' : SimpleItem(defvalue=0,protected=1,hidden=1,copyable=1,doc='the number of the next created task.',typelist=["int"]),
        'print_help'   : SimpleItem(defvalue=True,copyable=1,doc='If True print the Help every time tasks is first typed in a session.',typelist=["bool"]),
        })    
   _category = 'tasklists'
   _name = 'TaskList'
   _exportmethods = ['help', 'table', '__str__', '__call__']
   _help_printed = False
   _save_thread = None
   _main_thread = None

   def save(self):
      """Writes all tasks to a file 'tasks.xml' in the job repository. This function is usually called automatically. """
      # TODO: Add check if this object is correctly initialized, and the tasks are actually tasks
      # First write to tasks.dat.tmp to avoid losing the content in case of crash (tasksfile is set in BOOT.py)
      fna = self.tasksfile + ".new"
      fnb = self.tasksfile
      fnc = self.tasksfile + "~"
      try:
         tmpfile = open(fna, "w")
         to_file(self, tmpfile)
         # Important: Flush, then sync file before renaming!
         tmpfile.flush()
         os.fsync(tmpfile.fileno())
         tmpfile.close()
      except IOError, e:
         logger.error("Could not write tasks to file %s (%s). Disk full or over quota?" % (e, fna))
         return False
      # Try to make backup copy...
      try:
         os.rename(fnb,fnc)
      except OSError, e:
         logger.debug("Error making backup copy: %s " % e)
      try:
         os.rename(fna,fnb)
      except OSError, e:
         logger.error("Error renaming temporary file: ", e)
         return False
      return True

   def startup(self):
      """Called on ganga startup after the complete tasks tree has been loaded """
      for t in self.tasks:
         t.startup()

   def register(self,chi):
      """ Adds a task 'chi' to the task list. Should only be called by the Task constructor """
      self.tasks.append(chi)
      chi.id = self.next_task_id
      self.next_task_id += 1
      chi._setParent(self) # Should not be necessary after fixing ganga bug
      self.save()

   def __call__(self,id):
      """Returns the task with the given id"""
      ps = [p for p in self.tasks if p.id == id]
      if len(ps) > 0:
         return addProxy(ps[0])
      logger.error("No Task with ID #%i" % id)

## Thread methods
   def refresh_lock(self):
      try:
         os.utime(self.tasksfile, None) # keep lock
      except OSError:
         return False
      return True

   def _thread_save(self):
      """ This is an internal function; the loop that is responsible for saving """
      ## Wait until Ganga is fully initialized 
      while not ("jobs" in reload(GPI).__dict__):
         time.sleep(0.5)
         self.refresh_lock()
      while not ("config" in reload(GPI).__dict__):
         time.sleep(0.5)
         self.refresh_lock()
      time.sleep(0.5)
      ## .. hopefully ganga is now initialized. TODO: better way to find this out.
      self.logger = getLogger()
      while not self._save_thread.should_stop():
         # Sleep and refresh lock interruptible for 30 seconds
         for i in range(0,60):
            self.refresh_lock()
            time.sleep(0.5)
            if self._save_thread.should_stop():
               break
         # Save
         self.save()

   def _thread_main(self):
      """ This is an internal function; the main loop of the background thread """
      ## Wait until Ganga is fully initialized      
      while not ("jobs" in reload(GPI).__dict__):
         time.sleep(0.4)
      while not ("config" in reload(GPI).__dict__):
         time.sleep(0.4)
      time.sleep(0.5)
      self.logger = getLogger()
      ## .. hopefully ganga is now initialized. TODO: better way to find this out.

      ## Add runtime handlers for all the taskified applications, since now all the backends are loaded
      from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
      from TaskApplication import handler_map
      for basename, name in handler_map:
         for backend in allHandlers.getAllBackends(basename):
            allHandlers.add(name, backend, allHandlers.get(basename,backend))

      ## Main loop
      while not self._main_thread.should_stop():
         ## For each task try to run it
         for p in [p for p in self.tasks if p.status in ["running","running/pause"]]:
            if self._main_thread.should_stop():
               break
            try:
               # TODO: Make this user-configurable and add better error message 
               if (p.n_status("failed")*100.0/(20+p.n_status("completed")) > 20):
                  p.pause()
                  print ("Task %s paused - %i jobs have failed while only %i jobs have completed successfully." % (p.name,p.n_status("failed"), p.n_status("completed")))
                  print ("Please investigate the cause of the failing jobs and then remove the previously failed jobs using job.remove()")
                  print ("You can then continue to run this task with tasks(%i).run()" % p.id)
                  continue
               p.submitJobs()
            except Exception, x:
                  print "Exception occurred in task monitoring loop: %s\nThe offending task was paused." % x
                  logger.error("Exception occurred in task monitoring loop: %s\nThe offending task was paused." % x)
                  p.pause()
                  self.save()
         # Sleep interruptible for 10 seconds
         for i in range(0,100):
            time.sleep(0.1)
            if self._main_thread.should_stop():
               break


   def start(self):
      """ Start a background thread that periodically run()s"""
      from Ganga.Core.GangaThread import GangaThread
      self._save_thread = GangaThread(name="GangaTasksRepository", target=self._thread_save)
      self._main_thread = GangaThread(name="GangaTasks", target=self._thread_main)
      self._save_thread.start()
      self._main_thread.start()


## Information methods
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
         print " The following is the output of "+markup("tasks.table()",fgcol("blue"))
         short = False

      fstring = " %5s | %17s | %30s | %9s | %33s | %5s\n"
      lenfstring = 98
      ds = "\n" + fstring % ("#", "Type", "Name", "State", "%4s: %4s/ %4s/ %4s/ %4s/ %4s" % (
           "Jobs",markup("done",overview_colours["completed"])," "+markup("run",overview_colours["running"]),markup("fail",overview_colours["failed"]),markup("hold",overview_colours["hold"])," "+markup("bad",overview_colours["bad"])), "Float")
      ds += "-"*lenfstring + "\n"
      for p in self.tasks:
         stat = "%4i: %4i/ %4i/ %4i/ %4i/ %4i" % (
                p.n_all(), p.n_status("completed"),p.n_status("running"),p.n_status("failed"),p.n_status("hold"),p.n_status("bad"))
         ds += markup(fstring % (p.id, p.__class__.__name__, p.name, p.status, stat, p.float), status_colours[p.status])
         if short:
            continue
         for ti in range(0, len(p.transforms)):
            t = p.transforms[ti]
            stat = "%4i: %4i/ %4i/ %4i/ %4i/ %4s" % (
                   t.n_all(), t.n_status("completed"),t.n_status("running"),t.n_status("failed"),t.n_status("hold"),t.n_status("bad"))
            ds += markup(fstring % ("%i.%i"%(p.id, ti), t.__class__.__name__, t.name, t.status, stat, ""), status_colours[t.status])
         ds += "-"*lenfstring + "\n"
      return ds + "\n"

   def help(self, short=False):
      """Print a short introduction and 'cheat sheet' for the Ganga Tasks package"""
      print
      print markup(" *** Ganga Tasks: Short Introduction and 'Cheat Sheet' ***", fgcol("blue"))
      print 
      print markup("Definitions: ", fgcol("red")) + "'Partition' - A unit of processing, for example processing a file or processing some events from a file"
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
      print " Access an existing task  : "+c("t = tasks(id)")+"          Remove a Task          : "+c("tasks(id).remove()")
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
      print " For an Analysis Example and help type: "+c("AnaTask?")
      print 

      if not True:
#      if not short:
         print "ADVANCED COMMANDS:"
         print "Add Transform  at position N      : t.insertTransform(N, transform)"
         print "Remove Transform  at position N   : t.removeTransform(N)"
         print "Set Transform Application         : tf.application = TaskApp() #This Application must be a 'Task Version' of the usual application" 
         print "   Adding Task Versions of Applications is easy, contact the developers to request an inclusion"
      

