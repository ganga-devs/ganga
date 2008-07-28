import os.path, time, thread

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Utility.logging import getLogger
logger = getLogger()

sleeptime = 30

class TaskList(GangaObject):
   """This is a list of all tasks.
   
   If called(), it prints a short overview of all its tasks.
   
   The individual tasks can be accessed by their number:
   p = tasks.get(2)
   or by specifying the name of the task:
   p = tasks.get("MyTask")
   Tasks can be deleted:
   p.remove("MyTask")

   Public Methods:
     run()        - Cause all running tasks to submit jobs so the float is maintained
     start()      - Start a background thread that periodically run()s
     stop()       - Tell the background thread to stop (actual termination may be later)
     get(name)    - Returns the Task named 'name'
     remove(name) - Removes the Task 'name'
     save()       - Saves the current state of all tasks"""

   _schema = Schema(Version(1,0), {
        'data'       : ComponentItem('Tasks',defvalue=[],sequence=1,copyable=1,doc='list of tasks'),
        'user_called_tasks' : SimpleItem(defvalue=False,doc="true if user already used tasks"),
        })
    
   _category = 'TaskLists'
   _name = 'TaskList'
   _exportmethods = ['run', 'get','__str__',"_register", "remove", "start", "save"]

   _run_thread = False
   _thread = None

   def __init__(self):
      super(self.__class__,self).__init__()
   
   def save(self):
      """Writes all tasks to a file 'tasks.dat' in the job repository."""
      from Ganga.GPI import config #from Ganga.Utility.Config import getConfig
      from Ganga.GPIDev.Persistency import export
      import os.path

      fna = os.path.join(config.Configuration["gangadir"], "tasks.dat.tmp")
      fnb = os.path.join(config.Configuration["gangadir"], "tasks.dat")
      #fna = os.path.join(getConfig("DefaultJobRepository").getEffectiveOption("local_root"), "tasks.dat.tmp")
      #fnb = os.path.join(getCtasonfig("DefaultJobRepository").getEffectiveOption("local_root"), "tasks.dat")

      export(self._proxyObject, fna)
      import os
      try:
         os.rename(fna,fnb)
      except OSError:
         logger.debug("Saving resulted in no file being created. This is normal during startup.")

   def _register(self,chi):
      """ Adds a task 'chi' to the task list.
          This function takes care not to add a proxy object, but to dereference it first."""
      if "_impl" in chi.__dict__.keys():
         self.data.append(chi._impl)
      else:
         self.data.append(chi)  
      self.save()

   def __str__(self):
      """Prints an overview over the currently running tasks"""
      self.user_called_tasks=True
      from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects
      markup = ANSIMarkup()
      cnt = len(self.data)
      fg = Foreground()
      fx = Effects()
      bg = Background()
      status_colours = { 'new'        : fx.normal,
                         'running'    : fg.green,
                         'completed'  : fg.blue,
                         'pause'      : fg.cyan }


      fstring = "%2s | %10s | %20s | %10s | %6s %27s | %15s\n"
      ds = "\n" 
      ds += fstring % ("#", "Type", "Name", "State", "Jobs:", "%3s / %3s / %3s / %3s / %3s" % ("t","d","w","i","fi"), "Total Float")
      ds += "-"*100 + "\n"

      for i in range(0,len(self.data)):
         p = self.data[i]
         try:
            colour = status_colours[p.status]
         except KeyError:
            colour = fx.normal

	 t = p.get_total_jobs()
         c = p.get_done_jobs()
         r = p.get_working_jobs()
	 ig = p.get_ignored_jobs()
	 fig = p.get_forced_ignored_jobs()
         ds += markup(fstring % (str(i), p.__class__.__name__, p.name, p.status, "", "%3i / %3i / %3i / %3i / %3i" % (t,c,r,ig,fig), p.float), colour)
      ds +=  "\n"
      ds += "t = total; d = done; w = working; i = ignored; fi = forced ignorance"
      return ds

   def get(self,name, warn=True):
      """Returns the task 'name'"""
      if len(self.data)==0:
         logger.warning("Tasks list is empty!")
         return
      
      from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
      if isinstance(name, int):
         if len(self.data)<=name:
            if len(self.data)==1: logger.warning("Tasks list contains only one task ...!")
            else: logger.warning("Tasks list contains only %d tasks ...!"%len(self.data))
            return         
         return GPIProxyObjectFactory(self.data[name])
         #return self.data[name]
      for p in self.data:
         if p.name == name:
            return GPIProxyObjectFactory(p)
            #return p
      if warn:
         logger.warning("Task %s not found!" % name)	    

   def remove(self,name,warn = True):
      """Delete the task 'name'"""
      # removeing all tasks (should warn the user)
      if name=='all':
         if len(self.data)==0:
            logger.warning("Tasks list is empty!")
            return
         for i in range(0,len(self.data)):
            logger.info("Removing jobs of task %s"%self.data[0].name)
            self.data[0].remove_jobs('all',True)
            del self.data[0]
            logger.info("Task number %d deleted" % i)
         self.save()
         return
      #removing tasks by number
      if isinstance(name, int):
         if len(self.data)<=name:
            if len(self.data)==0: logger.warning("Tasks list is empty ...!")
            elif len(self.data)==1: logger.warning("Tasks list contains only one task ...!")
            else: logger.warning("Tasks list contains only %d tasks ...!"%len(self.data))
            return
         self.remove(self.data[name].name)
         return
      #removing task by name
      for i in range(0,len(self.data)):
         if self.data[i].name == name:
            logger.info("Removing jobs of task %s"%name)
            self.data[i].remove_jobs('all',True)
            del self.data[i]
            logger.info("Task %s deleted" % name)
            self.save()
	    return
      if warn:
         logger.warning("Task %s not found!" % name)

   def run(self):
      """ Starts as many jobs as are necessary to have 'float' jobs running
          for all 'running' tasks"""
      for p in self.data:
         if p.status == "running":
            logger.debug("Calling run() on task %s..." % p.name)
            p.run()

   def _thread_main(self):
      """ This is an internal function; the main loop of the background thread """
      import Ganga.GPI
      while True:
         if "jobs" in reload(Ganga.GPI).__dict__:
            break
         time.sleep(3)
      while self._run_thread:
         logger.debug("Background task thread waking up...")
         try:
            self.save()
         except Exception,x:
            logger.error("Could not save tasks to file: %s" % x)

         for p in [p for p in self.data if p.status == "running"]:
            try:
               #print "getting total"
               t = p.get_total_jobs()
               #print "total =%d"%t
               c = p.get_done_jobs()
               r = p.get_working_jobs()
               i = p.get_ignored_jobs()
               fig=p.get_forced_ignored_jobs()
               
               if r == 0 and t == c and t > 0:
                  p.status = "completed"
                  logger.info("Task %s has been completed!" % p.name)
                  p.on_complete()
                  continue

               if t==0:print "please waite ... ",;continue #this happens sometimes at the beginning when a task is submitted
               if p.float>t:
                  logger.info(""" Task '%s': float is set to %d (larger than total jobs of %d), setting it to float=%d"""%(p.name,p.float,t,t))
                  p.float=t
                  
               if t==(i+fig): #t>0
                  logger.info("All of task's jobs are ignored. Pausing the task.")
                  p.pause()
                  continue

               threshold= p.get_ignored_jobs()*100.0/p.get_total_jobs()
               if threshold > 5:
                  p.pause()
                  logger.error("Task %s paused - %2i%% of its jobs have been resubmitted more than 'run_limit' times." % (p.name,threshold)) 
                  logger.error("Please investigate the cause of the failing jobs and then either:") 
                  logger.error(" * remove the previously failed jobs or") 
                  logger.error(" * increase the run_limit for the concerned jobs")
                  logger.error("You can continue the task with tasks.get('%s').unpause()" % p.name)
                  continue
               #print "going to run ........... "
               p.run()
               #print "after p. run ........... "
            except Exception, x:
               logger.error("Exception occurred in task monitoring loop: %s\nWaiting for 30 seconds to try again" % x)
               self.save()
               time.sleep(30)
         time.sleep(sleeptime)
      logger.warning("Backgroud task thread stopped.")

   def start(self):
      """ Start a background thread that periodically run()s"""
      if self._run_thread or self._thread:
         logger.warning("It seems that the task thread is already running.")
         return
      self._run_thread = True
      self._thread = thread.start_new(self._thread_main, ())

   def stop(self):
      """Tell the background thread to stop (actual termination may be later)"""
      logger.info("Commanding the task thread to stop. This can take a while.")
      self._run_thread = False
      self._thread = None

