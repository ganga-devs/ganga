
from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from time import time

class AbstractJob(GangaObject):
   """ A general job in a task """
   
   _schema = Schema(Version(1,0), {
       'name'        : SimpleItem(defvalue='analysis:0', comparable=1, doc='Name of this job'),
       'task'        : SimpleItem(defvalue='', comparable=1, doc='Name of the task this job is part of'),
       'run_limit'   : SimpleItem(defvalue=4,  comparable=0, doc='Number of attempts that should be made before an error is triggered'),
       'done'        : SimpleItem(defvalue=False, comparable=0, doc='This is set to true if this task is done. Unset if this was set to done by mistake'),
       'ignore_this' : SimpleItem(defvalue=False, doc='if this specific job makes problems ignore it without pausing the task'),
       'status_duration' :SimpleItem(defvalue={}  ,doc="duration of the GANGA-job status"),
       'excluded_CEs'  : SimpleItem(defvalue=[]  ,doc="exclude CEs for this job"),
       'sites'      : SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='Sites where the job could run'),
       #'sites'    : SimpleItem(defvalue = [], typelist=['str'], sequence=1,strict_sequence=0, doc="Sites where the job could run" ),
       })
   
   _category = 'AbstractJobs'
   _name = 'AbstractJob'
   _exportmethods = ["__repr__", "__cmp__", "get_jobs", "get_run_count", "status", "ready", "necessary", "prerequisites", "prepare"]
   #_GUIPrefs = [ { 'attribute' : 'sites', 'widget' : 'String_List' }]
   _GUIPrefs = [ { 'attribute' : 'sites', 'widget' : 'List',  'choices' : ['String'] } ]
                 
   jobs = None    # A list of jobs that run/ran this spjob
   _status = "new" # The status cache

   def __repr__(self):
      return self.name

   def __cmp__(self, t):
      return 2*self.name.__ge__(t.name)-1

   def get_task(self):
      if "_impl" in self.__dict__:
         return GPI.tasks.get(self.task)#return self._impl._getParent()
      else:
         return GPI.tasks.get(self.task)#return self._getParent()

   def check_job(self, j):
      """ Checks if the job j has sucessfully executed the job
          Returns "working" if j is still working, "done" if done
          or something else for failed"""
      return j.status
   
   def get_jobs(self):
      self.get_task()._update_jobs() 
      return self.jobs
   
   def get_run_count(self):
      """ Returns how often this spjob has been run."""
      return len(self.get_jobs())

   def status(self):
      self.get_task()._update_jobs() 
      return self._status

   def ready(self):
      self.get_task()._update_jobs() 
      if not self.necessary(): return False
      for t in self.prerequisites():
         if not t.done and t.necessary():
            return False
      return True

## Begin methods to be overridden
   def __init__(self, taskname='', name="New Job", task = None):
      """ Initialize self from the name 'name'. This could be overridden in derived classes. 
          An exception should be thrown if invalid names are specified """
      from Ganga.GPIDev.Base.Proxy import ProxyMethodDescriptor
      for t in AbstractJob.__dict__:
         if (not t in self._proxyClass.__dict__) and (t in self._exportmethods):
            f = ProxyMethodDescriptor(t)
            f.__doc__ = AbstractJob.__dict__[t].__doc__
            setattr(self._proxyClass, t, f)

      super(AbstractJob,self).__init__()
      self.task = taskname
      self.name = name
      self.jobs = []

   def done_callback(self):
      """This function is called when this spjob is done, once.
         Can for example add more Tasks via get_job_by_name()"""
      pass

   def prerequisites(self):
      """Returns a list of spjobs that need to be done before this job can run. 
         This should be overridden in derived classes"""
      return []
   
   def prepare(self, number = 1, backend = "Local"):
      j = GPI.Job()
      j.name = "%s:%s:%s" % (backend, self.task, self.name)
      return j

   def necessary(self):
      """ Defines if this job has to be executed to meet task specifications.
          The system assumes, that if a job is necessary and has prerequisites,
          at least one of its prerequisites are also necessary."""
      return True
