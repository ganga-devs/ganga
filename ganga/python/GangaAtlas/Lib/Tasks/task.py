
from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects
import Ganga.Utility.logging
import abstractjob
import time
from GangaAtlas.Lib.AtlasLCGRequirements.AtlasLCGRequirements import getCEsForSites

import MyList
mylist=MyList.MyList()

running_states =  ["submitting", "submitted", "running", "completing"]
done_states =  ["completed"]
markup = ANSIMarkup()
fg = Foreground()
fx = Effects()
bg = Background()
status_colours = { 'new'        : fx.normal,
                   'running'    : fg.green,
                   'completed'  : fg.boldblue,
                   'done'       : fg.boldblue,
                   'attempted'  : fg.orange,
                   'ignored'    : fg.red,
                   'pause'      : fg.cyan,
                   #for jobs
                   'completing'  : fx.normal,
                   'submitting'  : fx.normal,
                   'submitted'  : fg.orange
                   
                   }

overview_colours = {
                   'running'    : bg.green,
                   'done'       : bg.blue,
                   'attempted'  : bg.orange,
                   'ignored'    : bg.red,
                   'ready'      : bg.white,
                   #'unready'    : bg.black}
                   'unready'    : bg.white}

class Task(GangaObject):
   """Generic Task class
       Public variables:
        name      - Name of the task
        status    - either "new", "running", "paused" or "completed"
       
       Public Methods:
         submit()        - Mark this task as completely configured and ready to run
         info()          - Information on the settings of the task.
         stats()         - Statistics on Jobs
         overview()      - A nice ASCII overview of Job states
         
         pause()         - Pause the task - the background thread will not submit new jobs from this task
         unpause()       - Unpause the task
       """
##################################
   def check_new(self, val,intern_change=False):
      if "tasks" in GPI.__dict__ and ("status" in self._data and self.status != "new") and ("allow_change" in self._data and not self.allow_change):
         raise Exception("""
         %s
         Cannot change this value if the task is not new!
         If you want to change it, first copy the task. Do the following:
         %s 
         then set the attributes anew and
         %s. (type tasks to get a list of the tasks and their numbers.)
         %s
         %s
         (This exception has no influence on your running task)
         """%(markup("""##################################""",fg.red), markup("t=tasks.get('%s').copytask()"%self.name,fg.magenta),markup("t=tasks.get(Number or name of YourCopy)",fg.magenta), markup("t.submit() ",fg.magenta),markup("""##################################""",fg.red)))
      return val
#########################
   def check_name(self, val):
      if "tasks" in GPI.__dict__:
         if val=='all':
            raise Exception("The word 'all' is reserved. do not give it as a name to your task. Try another name.")
         if val.find("+")>-1:
            logger.info("The task's name should not contain the '+'-sign. Changing it to '-' on submitting.")
         if self.status != "new":
            raise Exception("Cannot change name if the task is not new! If you want to change it, first copy the task.")
         if GPI.tasks.get(val, False):
            raise Exception("Name is already given to an other task.")
      return val
##################################
   def check_CE(self, val):
      if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
         raise Exception("""
         %s
         Can not change CE.Use the methode  _change_CE('ce'), ['ce' is a string.]
         Do the following:
         %s
         %s
         (This exception has no influence on your running task)
         """%(markup("""##################################""",fg.red),
              markup("tasks.get('%s')._change_CE('%s')"%(self.name,val),fg.magenta),
              markup("""##################################""",fg.red))
                         )
      else: return val
##################################### check including sites
   def check_req_sites(self, val):
      if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
         raise Exception("""
         %s
         Cannot change requirements_sites. Try
         %s,
         sites is either a string or a list of strings.
         %s
         (This exception has no influence on your running task)
         """%(markup("""##################################""",fg.red),
              markup("tasks.get('%s').ext_req_sites([site]s)"%self.name,fg.magenta),
              markup("""##################################""",fg.red))
                         )
      return val
##################################### check including sites
   def check_exclud_sites(self, val):
      if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new" and not self.allow_change:
         raise Exception("""
         %s
         Cannot change excluded_sites. Try
         %s,
         sites is either a string or a list of strings.
         %s
         (This exception has no influence on your running task)
         """%(markup("""##################################""",fg.red),
              markup("tasks.get('%s').ext_excl_sites([site]s)"%self.name,fg.magenta),
              markup("""##################################""",fg.red))
                         )
      return val
#######################################n
##################################### check including sites

   _schema = Schema(Version(1,0), {
        'name'        : SimpleItem(defvalue='New Analysis Task', checkset="check_name", doc='Name of the Task'),
        'type'        : SimpleItem(defvalue='', doc='Type of the Task'),
        'status'      : SimpleItem(defvalue='new', doc='Status - new, running, paused or completed'),
        'spjobs'      : ComponentItem('AbstractJobs',defvalue=[],sequence=1,copyable=1,doc='List of spjobs'),
        'float'       : SimpleItem(defvalue=0,doc='How many jobs should be run at the same time'),
        'working_float'  : SimpleItem(defvalue=0,    doc='set the float to this value if the first job runs successfully'),        
        'jupdatetime' : SimpleItem(defvalue=0, hidden=1, doc=''),
        'AbstractJob'       : SimpleItem(defvalue=abstractjob.AbstractJob, hidden=1, doc=''),

        'requirements_sites'  : SimpleItem(defvalue=[], checkset="check_req_sites",doc="sites where the job should run"),
        'CE'               : SimpleItem(defvalue='',  checkset="check_CE",doc="Name of the CE queue where the computation should take place"),
        'excluded_CEs'  : SimpleItem(defvalue=[]  ,doc="exclude CEs"),
        'excluded_sites'  : SimpleItem(defvalue=[],  checkset="check_exclud_sites",doc="sites which you want to exclude for this task"),
        'allow_change' :SimpleItem(defvalue=False, doc=''),
        'report_output' : SimpleItem(defvalue=False,  doc="create a file with name 'Task_'+ taskname+'_report' and write all incidents to it"),
        'report_file' : SimpleItem(defvalue="", checkset="check_new",doc="create a file with name 'Task_'+ taskname+'_report' and write all incidents to it"),
        'stress_test' : SimpleItem(defvalue=False, doc=""),
        'stress_test_status' : SimpleItem(defvalue={}, doc=""),
        
        })
    #stress_test_status={'j0':{'new':{time:'time_new',others:{run:run_#,submitted:sub_#,completing:comp_#}},'submit':'time_submit',}}
   _category = 'Tasks'
   _name = 'Task'
   _exportmethods = ['submit', 'run', 'info', 'stats', '__str__', 'overview', 'get_next_jobs', 'get_job_by_name','get_total_jobs','get_done_jobs','get_working_jobs','get_ignored_jobs','get_forced_ignored_jobs', 'pause', 'unpause', '_next_name', 'failed_job', 'get_ignored_job', 'remove_jobs','_update_jobs', 'run_jobs','copytask', "get_name", "set_name","test_func","_change_CE",'ext_req_sites','ext_excl_sites','ext_excl_CEs','clean_req_sites','clean_excl_sites','clean_CE','clean_excl_CEs','get_duration_status','reset_absj']
   
   # Maps abstract job names to abstract jobs
   _spjobmap = {}
   _total_jobs = 0
   _done_jobs = 0
   _working_jobs = 0
   _ignored_jobs = 0
   _forced_ignored_jobs = 0
   _jobs_where_how={}# {job1:{CE1:[stat1,stat2], CE2:[stat1,stat2]}, job2:{CE1:[stat1,stat2], CE2:[stat1,stat2]},}
   _CEs_stats={} #{CE1:{failed:n,complete:m,total:k},CE2:{failed:n,complete:m,total:k}}

   def __init__(self):
      """Task(name)
         This class is not meant to be instantiated.
         For running an actual task, derive another class
         In the constructor (__init__) call:
         super(Task, self).__init__() and set basic parameters."""
      # some black magic to allow derived classes to specify inherited methods in
      # the _exportmethods variable without redefining them
      from Ganga.GPIDev.Base.Proxy import ProxyMethodDescriptor
      for t in Task.__dict__:
         if (not t in self._proxyClass.__dict__) and (t in self._exportmethods):
            f = ProxyMethodDescriptor(t,t)
            f.__doc__ = Task.__dict__[t].__doc__
            setattr(self._proxyClass, t, f)
           
      super(Task,self).__init__()
      # The following will only be executed if the user directly created this instance
      # (if it was not loaded from a file)
      if "tasks" in GPI.__dict__.keys():
         i = 1
         while GPI.tasks.get("NewTask%i" % i, False):
            i += 1
         self.name = "NewTask%i" % i
         # Put this task into the global task list
         GPI.tasks._register(self)
         self.type=GPI.__dict__["tasks"].get(self.name).__class__.__name__

   def copytask(self): 
      if "_impl" in self.__dict__:
         self = self._impl
      print "COPYING %s" % self.name
      m = self.__class__()
      i = 1
      while GPI.tasks.get(self.name + "-copy-%i" % i, False):
         i += 1
      mname = self.name + "-copy-%i" % i
      for i in self._schema.allItems():
         if i[0] == "name":
            m.__dict__["_data"]["name"] = mname
         elif i[0] == "status":
            m.__dict__["_data"]["status"] = "new"
         elif i[0] != "spjobs":
            m.__dict__["_data"][i[0]] = self.__dict__["_data"][i[0]]

   def _update_jobs(self, cache_override = False):
      """Update the status of all jobs (only executed every second, otherwise cached)"""
      if ((time.time() - self.jupdatetime < 200) ) and (not cache_override):
         return
      self._spjobmap = {}

      jobs_where_how={}# {job1:{CE1:[stat1,stat2], CE2:[stat1,stat2]}, job2:{CE1:[stat1,stat2], CE2:[stat1,stat2]},}
      CEs_stats={} #{CE1:{failed:n,complete:m,total:k},CE2:{failed:n,complete:m,total:k}}
      ########################################################
      def fill_states_dicts(job_name,job_ce,job_stat,js_state_locat,ces_stats):
         if job_ce in ces_stats:
            ces_stats[job_ce]["total"] +=1
            if job_stat=="failed": ces_stats[job_ce]["failed"] +=1
            elif job_stat=="completed": ces_stats[job_ce]["completed"] +=1
         else:
            ces_stats[job_ce]={"total":1,"failed":0,"completed":0}
            if job_stat=="failed":
               ces_stats[job_ce]["failed"]=1
               ces_stats[job_ce]["completed"]=0
            elif job_stat=="completed":
               ces_stats[job_ce]["failed"]=0
               ces_stats[job_ce]["completed"]=1

         if job_name in js_state_locat:
            if job_ce in js_state_locat[job_name]: js_state_locat[job_name][job_ce].append(job_stat)
            else: js_state_locat[job_name][job_ce]=[job_stat]
         else:
            js_state_locat[job_name]={job_ce:[job_stat]}
      ########################################################

      _all_tasks=GPI.__dict__["tasks"]
      _this_task=_all_tasks.get(self.name)

      for t in self.spjobs:
         t.jobs = []
         self._spjobmap[t.name] = t

      job_count=0
      for j in GPI.jobs:
         job_count +=1
         
         sname = j.name.split(":")
         if len(sname) < 3: continue
         if not sname[1] == self.name: continue
         tname = ":".join(sname[2:])
         if j.subjobs:
            for sj in j.subjobs:
               t = self.get_job_by_name(tname)
               if t:
                  stuck=self.check_job_duration(j)#remove_stuck(t,jstatus)
                  if stuck: GPI.jobs(j.id).remove()
                  jstatus = t.check_job(sj)
                  if jstatus in done_states:
                     t.done = True
                  t.jobs.append(jstatus)
                  ### fill the states dicts
                  temp_name=j.name+"+%d"%sj.id
                  fill_states_dicts(temp_name,sj.backend.actualCE,jstatus,jobs_where_how,CEs_stats)

               tname = self._next_name(tname)
         else:
            t = self.get_job_by_name(tname)
            if t:
               stuck=self.check_job_duration(j)#remove_stucking(t,jstatus)
               if stuck: GPI.jobs(j.id).remove()
               
               jstatus = t.check_job(j)                     
               if jstatus in done_states:
                  t.done = True
               t.jobs.append(jstatus)
               ### fill the states dicts
               fill_states_dicts(j.name,j.backend.actualCE,jstatus,jobs_where_how,CEs_stats)
               ##################################
               #################################
      self.check_CEs(jobs_where_how,CEs_stats,_this_task)

      self_jobs_where_how=jobs_where_how
      self._CEs_stats=CEs_stats
      
      self._update_jobs_more()

      self._total_jobs = 0
      self._done_jobs = 0
      self._working_jobs = 0
      self._ignored_jobs = 0
      self._forced_ignored_jobs = 0
      for t in self.spjobs:
         if not t.necessary():
            t._status = "unnecessary"
            
            continue
         
         if t.ignore_this:
            self._forced_ignored_jobs+=1
            t._status = "ignored"
            self._total_jobs += 1
            continue

         if t.done:
            t._status = "done"
            self._done_jobs += 1
         else:
            run_count = 0
            fail_count = 0
            for status in t.jobs:
               if status in running_states:
                  run_count += 1
               else:
                  fail_count += 1
            if run_count > 0:
               t._status = "working"
               self._working_jobs += 1
            elif fail_count >= t.run_limit:
               t._status = "ignored"
               self._ignored_jobs += 1
            else:
               t._status = "new"
         self._total_jobs += 1
      ###################################
      logger.debug("Done updating job status...")
      self.jupdatetime = time.time()

   def _update_jobs_more(self):
      pass

   def submit(self):
      """Confirms that this task is fully configured and ready to be run.
         (This could be overwritten for creating jobs in derived classes)"""
      self.status = "running"
      self.spjobs = []
      self._spjobmap = {}
################# check and set attributes
   def set_attributes(self):
      import os
      if self.name.count("+")>0:
         self.name=self.name.replace("+","-")
         
      if self.CE:
         if self.excluded_CEs or self.excluded_sites or self.requirements_sites:
            print "%s"%markup("""***********************************************************************""",fg.blue)
            logger.warning("""
                           If you specify a CE task.CE='ce_name' where your jobs should run ...
                           - you do not need to exclude any site or CEs.
                           - you do not need to specify sites where the jobs should run.""")
            add_txt=""
            if self.excluded_CEs:add_txt+="excluded_CEs, "
            if self.excluded_sites:add_txt+="excluded_sites, "
            if self.requirements_sites:add_txt+="requirements_sites"
            
            info_txt="Setting ' %s ' to default (empty)"%add_txt
            logger.info(info_txt)
            print "%s"%markup("""***********************************************************************""",fg.blue)

      if self.requirements_sites:
         if self.excluded_sites:
            print "%s"%markup("""***********************************************************************""",fg.blue)
            logger.warning("""
            If you specify sites in requirements_sites, other sites are automatically excluded.
            You do not need to specify excluded_sites.""")
            print "%s"%markup("""***********************************************************************""",fg.blue)
            self.clean_excl_sites()
      if self.report_output:
         from Ganga.GPI import config #from Ganga.Utility.Config import getConfig
         if not self.report_file:
            self.report_file=os.path.join(config.Configuration["gangadir"],'Task_'+self.name+'.report' )
         else:
            self.report_file=os.path.join(config.Configuration["gangadir"],self.report_file )
         if os.path.exists(self.report_file):
            logger.warning("A report file carrying the name of this task exists ! Removing it.")
            os.remove(self.report_file)
            
         print "%s"%markup("""##################################""",fg.red)
         logger.info("A report file will be created for task '%s':\n%s"%(self.name,self.report_file))
         logger.warning("Its your duty to remove this file manually if you remove the task!!!!")
         print "%s"%markup("""##################################""",fg.red)
         self._report("""
         #################################################
         #### This file is created by task %s
         #################################################
         """%self.name)
       
#######################
             
###########################################      
   def pause(self):
      """Pause the task - the background thread will not submit new jobs from this task"""
      if self.status == "running":
         self.status = "pause"

   def unpause(self):
      """Unpause the task"""
      if self.status == "pause":
         self.status = "running"
      else:
         logger.info("Task is not paused. Status is %s"%self.status)
   
   def run(self):
      """Submits as many jobs as necessary to maintain the float."""
      my_debugg=False
      from GangaAtlas.Lib.Tasks import logger
      if self.status != "running":
         logger.error("Cannot run jobs for Task %s because the task is not ready. Call tasks.get('%s').submit() to run jobs from this Task." % (self.name, self.name))
         return
      running_jobs = 0
      new_jobs = []
      for j in GPI.jobs:
         sname = j.name.split(":")
         if len(sname) < 3: continue
         if not sname[1] == self.name: continue
         if j.subjobs:
            for sj in j.subjobs:
               if sj.status in running_states:
                  running_jobs += 1
         else:
            
            if j.status in running_states: running_jobs += 1
            if j.status == "new":new_jobs.append(j)
      
      to_run = max(0, self.float - running_jobs)
      logger.debug("Task %s: %i/%i running, launching %i additional jobs..." % (self.name, running_jobs, self.float, to_run))
      new_jobs.sort(lambda a,b: a.id.__cmp__(b.id))
      for j in new_jobs[:to_run]:
         j.submit()
      to_run = max(0, to_run - len(new_jobs))
      if my_debugg: print "Calling task._update_jobs(True). if this goes well you will see a message 'jobs updated' "
      self._update_jobs(True)
      if my_debugg: print "jobs updated ... "
      jl = self.prepare_jobs(self.get_next_jobs(to_run))
      jl.sort(lambda a,b: a.id.__cmp__(b.id))
      for j in jl:
         j.submit()
########################################      
   def prepare_jobs(self,spjobs):
      """ prepare_spjobs(spjobs)
          Takes an arbitrary list of spjobs and prepares jobs to run them """
      from GangaAtlas.Lib.Tasks import logger
      spjobs.sort()
      jlist = []
      jl = []
      for j in spjobs:
         if len(jlist) == 0 or self._next_name(jlist[-1].name) == j.name:
            jlist.append(j)
         else:
            jl.append(jlist[0].prepare(len(jlist)))
            jlist = [j]
      if jlist:
         jl.append(jlist[0].prepare(len(jlist)))
      return jl
      
   def get_next_jobs(self, num):
      """get_next_jobs(n)
         return a list of n jobs that are next in line to be executed"""
      # build a list with jobs that are "free", not running or done and not ignored,
      # and ready: all prerequisites are fulfilled
      tlist = [t for t in self.spjobs if t.status() == "new" and t.ready()]
      tlist.sort()
      return tlist[:num]
   
   def get_job_by_name(self,name):
      if self.status == "new":
         return False   
      if name == "":
         return False
      if name in self._spjobmap:
         return self._spjobmap[name]
      else:
         for sj in self.spjobs:
            if sj.name == name:
               return sj
         t = self.AbstractJob(self.name, name, self)
         t._setParent(self)
         self.spjobs.append(t)
         self._spjobmap[name] = t
         return t

   def _next_name(self,name):
      """Returns the job name following 'name' in the subjob order"""
      return ""

   def on_complete(self):
      pass
#############################################
   def remove_jobs(self,to_be_removed="failed",intern=False):
      
      answer=1
      if not intern:
         stars_txt="%s"%markup("***********************************************************************************",fg.blue)
         stars_txt_short="%s"%markup("***  ",fg.blue)
         
         to_be_removed_txt="%s"%markup("all %s" % to_be_removed, fg.red)
         if to_be_removed=="all": to_be_removed_txt="%s"%markup("%s" % to_be_removed, fg.red)
         
         task_name_txt= "%s"%markup("%s"%self.name,fg.red)
         answer_txt="%s"%markup("(1=yes/0=no)", bg.red)

         warn_txt="%s \n%s Are you sure that you want to remove %s jobs of the task ' %s '? %s\n%s"%(stars_txt,stars_txt_short,to_be_removed_txt,task_name_txt,answer_txt,stars_txt)
         print warn_txt

         answer=input()
         if answer not in [1,0]:
            print "answer with '%s' or '%s'"%(markup("1 for yes",fg.blue),markup("0 for no",fg.blue))
            answer=input()
         if answer not in [0,1]: answer=0
      
      if answer==1:
         from Ganga.GPI import jobs
         for j in jobs:
            sname = j.name.split(":")
            if len(sname)>2 and sname[1]== self.name:
               tname = ":".join(sname[2:])
               t = self.get_job_by_name(tname)
               if to_be_removed=="all":
                  j.remove()
                  t.status_duration={}
                  t.done=False; t._status = "new"
                  if tname in self.stress_test_status:self.stress_test_status.pop(tname)

               elif to_be_removed==j.status:
                  t.status_duration={}
                  t.done=False; t._status = "new"
                  if tname in self.stress_test_status:
                     self.stress_test_status.pop(tname)
                  j.remove()
         self._update_jobs()

################# INFO STUFF
   def get_total_jobs(self):
      self._update_jobs()
      return self._total_jobs

   def get_done_jobs(self):
      self._update_jobs()
      return self._done_jobs
      
   def get_working_jobs(self):
      self._update_jobs()
      return self._working_jobs
      
   def get_forced_ignored_jobs(self):
      self._update_jobs()
      return self._forced_ignored_jobs

   def get_ignored_jobs(self):
      self._update_jobs()
      return self._ignored_jobs
   
   def failed_job(self):
      tname = ""
      for t in self.spjobs:
         if t.status() == "ignored":
            tname = t.name
            break
      if tname == "":
         return
      from Ganga.GPI import jobs
      fj = None
      for j in jobs:
         sname = j.name.split(":")
         if len(sname)>2 and sname[1] == self.name and sname[2] == tname:
            if j.status == "failed":
               fj = j
      return fj

   def get_ignored_job(self):
      ign_jobs=[]
      for t in self.spjobs:
         if t.status() == "ignored":
            ign_jobs.append(t)
      return ign_jobs
      
   def info(self):
      print "Generic Task %s" % self.name

   def stats(self):
      """ Prints some information on this task. Can be overridden. """
      done = 0
      working = 0
      ignoring = 0
      total = 0

      successes = 0
      attempts = 0
      rem_attempted = 0

      for t in [t for t in self.spjobs if t.necessary()]:
         total += 1
         numjobs = len(t.get_jobs())
         status = t.status()
         attempts += numjobs
         if status == "done":
            done += 1
            if numjobs > 0:
               successes += 1
         elif status == "working":
            working += 1
         elif status == "ignored":
            rem_attempted += 1
            ignoring += 1
         else:
            if numjobs > 0:
               rem_attempted += 1
      
      print "Task %s with %i jobs, float %i" % (self.name, total, self.float)
      if total == 0:
         print " * no jobs run\n"
         return
      print markup(" - %4i jobs ( %2i%% ) " % (done, done*100.0/total) + "done", status_colours["done"]) 
      print markup(" - %4i jobs ( %2i%% ) " % (working, working*100.0/total) + "in progress", status_colours["running"])
      print markup(" - %4i jobs ( %2i%% ) " % (rem_attempted, rem_attempted*100.0/total) + "attempted", status_colours["attempted"])
      if ignoring:
         print markup(" - ignoring: %i jobs" % (ignoring), status_colours["ignored"])
      if attempts == 0:
         logger.warning(" * no jobs run\n")
      else:
         print " * jobs run: %i; efficiency: %i%% to %i%%\n" % \
             (attempts, successes*100.0/attempts, (successes + working)*100.0/attempts)

   def overview(self):
      """ Get an ascii art overview over task status. Can be overridden """
      if self.status == "new":
         logger.warning("No jobs defined yet.")
         return
      print "Done: '%s' ; Running the nth time: '%s'-'%s' and '%s' ; Attempted: '%s' ; Not ready: '%s' ; Ready '%s'" % (markup("-", overview_colours["done"]), markup("1", overview_colours["running"]), markup("9", overview_colours["running"]), markup("+", overview_colours["running"]), markup(":", overview_colours["attempted"]), markup(",", overview_colours["unready"]), markup(".", overview_colours["ready"]))
      tlist = [t for t in self.spjobs if t.necessary()]
      tlist.sort()
      str = ""
      for t in tlist:
         status = t.status()
         if status == "done":
            str += markup("-", overview_colours["done"])
         elif status == "working":
            if t.get_run_count() < 10:
               str += markup("%i" % t.get_run_count(), overview_colours["running"])
            else:
               str += markup("+", overview_colours["running"])
         elif status == "ignored":
            str += markup("i", overview_colours["ignored"])
         elif t.get_run_count() > 0: ## job already run but not successfully 
            str += markup(":", overview_colours["attempted"])
         else:
            if t.ready():
               str += markup(".", overview_colours["ready"])
            else:
               str += markup(",", overview_colours["unready"])
      print str


#######################
   def clean_req_sites(self):
      if not self.requirements_sites:
         logger.info("No sites are specified for this task.")
         return False
      
      self.allow_change=True
      self.requirements_sites=[]
      self.allow_change=False
      GPI.tasks.save()
      return True
#######################
   def clean_excl_sites(self):
      if not self.excluded_sites:
         logger.info("No sites are excluded in this task.")
         return False
      
      self.allow_change=True
      self.excluded_sites=[]
      self.allow_change=False
      GPI.tasks.save()
      return True
#######################
   def clean_CE(self):
      if not self.CE:
         logger.info("No CE is specified for this task.")
         return False
      
      self.allow_change=True
      self.CE=''
      self.allow_change=False
      GPI.tasks.save()
      return True
#######################
   def clean_excl_CEs(self):
      if not self.excluded_CEs:
         logger.info("No CEs are excluded in this task.")
         return False
      
      self.allow_change=True
      self.excluded_CEs=[]
      self.allow_change=False
      GPI.tasks.save()
      return True
#######################
         
      
###############################################
   def ext_req_sites(self,sites,jobs=None):
      if not sites:
         info_txt="No sites are specified to extend."
         if self.requirements_sites:
            info_txt+="\nIf you want to clean 'requirements_sites' use the function clean_req_sites(). Do the following:\n"
            info_txt+="%s\n%s\n%s"%(markup("""##################################""",fg.red), markup("tasks.get('%s').clean_req_sites()"%self.name,fg.magenta),markup("""##################################""",fg.red))
         logger.info(info_txt)
         return []
      if self.CE:
         logger.warning("This task's jobs run on the specified CE '%s'."%self.CE)
         logger.info("Neglecting the specification and removing the CE. Extending requirements_sites.")
         self.clean_CE()
      ####################################
      if isinstance(sites, str): self.ext_req_sites([sites],jobs); return []
      
      if jobs and not isinstance(jobs, str):
         jbs=None
         if isinstance(jobs, list):
            if not isinstance(jobs[0], str): jbs=str(jobs[0]); type_of=type(jobs[0])
         else: jbs=str(jobs); type_of=type(jobs)
         if jbs:
            logger.warning("Jobs' names must be of type string or list of strings. Got '%s' of type '%s'. No extension."%(jbs,type_of))
            return []
        
      if jobs and isinstance(jobs, str): self.ext_req_sites(sites,[jobs]); return []
      ###############################
      ### testing sites
      for st in self.requirements_sites:
         if st in sites:
            logger.info("""Site %s is already in  requirements_sites"""%st)
            sites.remove(st)      
        #are there still sites to extend?
      if not sites:
         logger.warning("""All specified sites are in 'requirements_sites'. No sites to extend""")
         return []
        #sites must be put as a list before this point
      from GangaAtlas.Lib.Tasks.anatask import RespondingSites
      _resp_sites=RespondingSites([])
      sts=_resp_sites.get_responding_sites(sites)['resp']
      bad_sts=_resp_sites.get_responding_sites(sites)['non-resp']
        
      if not sts:logger.info("""None of the given sites responds. No extension."""); return []
      if self.excluded_sites:
         self.allow_change=True
         self.excluded_sites=mylist.difference(self.excluded_sites,sts)
         self.allow_change=False
      if bad_sts: self.ext_excl_sites(bad_sts)
      
      ####################################        
      ######################################
      all_jobs=[jb.name for jb in self.spjobs]
      non_valid_jobs=[]
      jobs_lst=[] # jobs left after all checks. to work with
      
      if jobs:
         for jb in jobs:
            if jb in all_jobs: jobs_lst.append(jb);continue
            else: non_valid_jobs.append(jb); continue
         warn_txt="Specified string(s) in '%s' do not represent jobs of this task.\nEither they do not follow"%jobs
         add_txt=""
         if self.type=='AnaTask': add_txt=""" the naming scheme
                                               'analysis:n', where n is an integer,
                                               OR the job numbers are out of range [1-%d] """%len(all_jobs) 
         elif self.type=='MCTask': add_txt=""" the naming scheme
                                                'evgen', 'simul' or 'recon' +'i-j' OR the job
                                                numbers are out of range."""
         else: add_txt=" the naming scheme OR the job number is out of range"
         
         if len(jobs_lst)==0:
            this_warn_txt=warn_txt+add_txt
            logger.warning(this_warn_txt)
            logger.info("No site extension.")
            return []
         elif len(jobs_lst)<len(jobs):
            warn_txt="The following jobs names do not fulfill"
            this_warn_txt=warn_txt+add_txt+"%s"%self.write_iterable( mylist.difference(jobs,jobs_lst))
            logger.warning(this_warn_txt)
      else:
         jobs_lst=all_jobs
      jobs_sites_dict={}
      jobs_lst_txt="+".join(all_jobs)
      jobs_sites_dict[jobs_lst_txt]=sts
      return jobs_sites_dict

####################
   def _change_CE(self,ce,jobs=None):
      if not ce:
         info_txt="No CE is specified to extend."
         if self.CE:
            info_txt+="\nA CE is specified. If you want to remove it use the function clean_CE(). Do the following:\n"
            info_txt+="%s\n%s\n%s"%(markup("""##################################""",fg.red), markup("tasks.get('%s').clean_CE()"%self.name,fg.magenta),markup("""##################################""",fg.red))
         logger.info(info_txt)
         return []
      
      if not isinstance(ce, str):
         logger.warning(""" 'ce' in function _change_CE(ce,jobs=None) must be of type string.""")
         return []

      if not self.CE:
         if self.requirements_sites: logger.info("""No CE to replace. The jobs of this task run on the following sites %s."""%self.requirements_sites)
         else:
            info_txt="Setting CE to '%s' (for all jobs)"%ce
            self.allow_change=True; self.CE=ce; self.allow_change=False
            logger.info(info_txt)
            return []
      if ce==self.CE:
         logger.info("""Given CE is similar to that of non-running jobs. No changes.""")
         return []
      if self.type=='AnaTask' and  not self.get_CE_site(ce.lower(),self.allowed_sites):
         logger.warning("Given CE does not belong to any of the accessable sites for your dataset\n%s"%self.allowed_sites)
         return []
        #check the jobs
       
      all_jobs=[jb.name for jb in self.spjobs]
      jobs_lst=[]
      if jobs:
         #make sure sites has the right type
         if not isinstance(jobs, str) and not isinstance(jobs, list):
            logger.warning("""Second parameter of _change_CE should be a job name (string) or a list of job names (list) !""")
            return []
            #if str create a list
         warn_txt="Specified string '%s' doese not represent a job in this task.\n"%jobs
         if self.type=='AnaTask': warn_txt+="Either it does not follow the naming scheme 'analysis:'+jobnumber OR the job number is out of range [1-%d] "%len(all_jobs)
         elif self.type=='MCTask': warn_txt+="Either it does not follow the naming scheme 'evgen', 'simul' or 'recon' +'i-j' OR the job number is out of range"
         else: warn_txt+="Either it does not follow the naming scheme OR the job number is out of range"
            
         if isinstance(jobs, str):
            #make sure the string represents a job
            if jobs not in all_jobs:
               logger.warning(warn_txt)
               return []
            jobs_lst=[jobs]
         else:
            for j in jobs:
               #make sure the list contain right jobs
               if j in all_jobs:
                  jobs_lst.append(j)
            if not jobs_lst:
               logger.warning(warn_txt)
               return []
      else:
         jobs_lst=all_jobs

      if not jobs_lst:
         logger.warning(""" All (%d) jobs are in running modus. No changes."""%len(all_jobs))
         return []
        #end checking the jobs
      if ce in self.excluded_CEs: self.excluded_CEs.remove(ce)
      return jobs_lst

#################################
################################# exclude CE
   def ext_excl_CEs(self,item):
      if self.CE:
         logger.warning("This task's jobs run on the specified CE '%s'."%self.CE)
         logger.info("Neglecting the specification and removing the CE. Extending excluded CEs.")
         self.clean_CE()
         
      if not item: logger.warning("No CEs are specified to exclude !!"); return
      if not isinstance(item, str) and not isinstance(item, list):
         logger.warning("Given CE name must be of type string or a list of strings.");return

      if isinstance(item, str): self.ext_excl_CEs([item]); return
        
      for f in item:
         if not isinstance(f, str): logger.warning("Given CE '%s' is of type %s. Must be a string"%( str(f),type(f) ) ); continue
         if f in self.excluded_CEs: logger.info("CE %s already excluded"%f); continue
         self.excluded_CEs.append(f)
################################# exclude sites
   def ext_excl_sites(self,item):
      if self.CE:
         logger.warning("This task's jobs run on the specified CE '%s'."%self.CE)
         logger.info("Neglecting the specification and removing the CE. Extending excluded sites.")
         self.clean_CE()
      
      if not item: logger.warning("No sites are specified to exclude !!"); return False
      if not isinstance(item, str) and not isinstance(item, list):
         logger.warning("Given site name must be of type string or a list of strings.");return False

      if isinstance(item, str): self.ext_excl_sites([item]); return
      empty_jobs=[]
      for f in item:
         if not isinstance(f, str): logger.warning("Given site '%s' is of type %s. Must be a string"%( str(f),type(f) ) ); continue
         if f in self.excluded_sites: logger.info("Site %s already excluded"%f); continue
         if f in self.requirements_sites: self.requirements_sites.remove(f)
         self.excluded_sites.append(f)
      ########################################## Only anatask
         if self.type=="MCTask":continue #IM MC CONTINUE
      ########################################## Only anatask
         site_in_jobs=False
         for j in self.abstract_jobs:
            if f in self.abstract_jobs[j]['sites_to_run']:
               self.abstract_jobs[j]['sites_to_run'].remove(f)
               if len(self.abstract_jobs[j]['sites_to_run'])==0: empty_jobs.append(j)
               site_in_jobs=True
                    
         if not site_in_jobs:
            logger.warning("Site %s is not considered for jobs of task %s"%(f,self.name))
      ########################################## Only anatask
      if self.type=="MCTask": return True #IM MC RETURN
      ########################################## Only anatask
      warn_txt=None
      info_txt=None
        
      all_jobs_sites=[]
      files_at_bad_sites=[]
      for j in self.abstract_jobs:
         if j not in empty_jobs: continue
         if self.get_job_by_name(j).status()=="done" or self.get_job_by_name(j).status()=="working":continue
         alter_sites=self.find_alternative_site(self.abstract_jobs[j]['all_sites'])
         alter_sites=mylist.difference(alter_sites,item)
         if alter_sites:
            self.abstract_jobs[j]['sites_to_run']=alter_sites
            empty_jobs.remove(j)
         else:
            files_at_bad_sites=mylist.extend_lsts(files_at_bad_sites,self.abstract_jobs[j]['files'])
            all_jobs_sites=mylist.extend_lsts(all_jobs_sites,self.abstract_jobs[j]['all_sites'])
            self.get_job_by_name(j).ignore_this=True
            
      if empty_jobs:
         abst_jobs_kys=self.abstract_jobs.keys()
         empty_jobs.sort()
         abst_jobs_kys.sort()
         if empty_jobs==abst_jobs_kys:
            warn_txt="""Because of excluding sites, non of your non-completed jobs can be run. No alternative sites could be found."""
            info_txt= "Setting task's status to paused\n"
            self.pause()
                
         else:
            warn_txt="Because of excluding sites, the following jobs can not be run\n%s. No alternative sites could be found."%self.write_iterable(empty_jobs,len(empty_jobs))
            info_txt= "Setting their status to force-ignored\n"
         info_txt+= "%s"% markup("You have two options:\n First: ",fg.blue)
         
         if empty_jobs==abst_jobs_kys:
            info_txt+="Unpause the task (less preferred), then its jobs will be analyzed at any of the sites where the dataset is located. Do the following\n"
            info_txt+="%s\n"%markup("tasks.get('%s').unpause()"%(self.name),fg.magenta)
            info_txt+=" ************* If the jobs are ignored you must release them before 'unpausing' the task: Do the following\n"
            info_txt+=" ************* %s\n"%markup("tasks.get('%s').release_ignored_jobs()"%(self.name),fg.magenta)
         else:
            info_txt+= "release these jobs, then they will be analyzed at any of the sites where the dataset is located (less preferred)\nDo the following:\n"
            info_txt+="%s\n"%markup("tasks.get('%s').release_ignored_jobs()"%(self.name),fg.magenta)
            
         info_txt+="%s\n"%markup("Second: see the following info-block.",fg.blue)
 
         logger.warning(warn_txt); logger.info(info_txt)
         if self.report_output: self._report(info_txt)
         
         print "%s"%markup("*************************************************\n************** Tasks info block\n",fg.blue)
         self.problem_no_resp_sites(all_jobs_sites,files_at_bad_sites,"files")
         print "%s"%markup("************** Info block ends\n",fg.blue)
         return True   
##############################################
#######################
   def what_is(self,param):
      print "I have no idea what '%s' could be !!"%param
#####################################
   def remove_spjob(self,jobs): #not used
      if isinstance(jobs, str):
         lst_j=[jobs]
         self.remove_spjob(lst_j)
         return
      empty_jobs=[]
      for spj in self.spjobs:
         if spj.name in jobs:
            empty_jobs.append(spj)
            
      for j in empty_jobs:
         print "removing %s"%j
         self.spjobs.remove(j)

##################################
   def get_CE_site(self,ce,sites):
      try:
         for ste in sites:
            ces=getCEsForSites([ste])
            if ce in ces:
               return ste
         
      except Exception,x:
         logger.error("given CE is not located at any of the sites holding the given dataset\n%s"%ce)
         return []
###########################################   
   def check_job_duration(self,ganga_job,spj=-1):
      #duration_status={'submitting':10,'completing':15,'completed':1,'submitted':60,'running':1500}
      duration_status={'submitting':10,'completing':15,'submitted':20,'running':1500}
      abst_job_name=":".join(ganga_job.name.split(":")[2:])
      abst_job=self.get_job_by_name(abst_job_name)
      jstatus= abst_job.check_job(ganga_job)
      
      if spj>-1:
         jstatus=ganga_job.subjobs[spj].status
         abst_job_name+="_spj_%d"%spj

      month_days={1:31,2:28,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}
      year=int(time.strftime("%Y"))
      if not year%4:month_days[2]=29
      now_str=time.strftime("%m:%d:%H:%M")
      now=now_str.split(":")
      #####################################################################  
      if self.stress_test:
         def get_CE(jobsname):
            for j in GPI.jobs:
               if j.name==jobsname:return j.backend.actualCE
               
         def get_env():
            at_same_CE={'running': 0, 'completing': 0, 'submitted': 0}
            at_same_site={'running': 0, 'completing': 0, 'submitted': 0}
            this_backend_actualCE=get_CE(ganga_job.name) #same CE
            if this_backend_actualCE:
               this_CEs_of_site=getCEsForSites(self.requirements_sites)#CEs of same site
            
               for j in GPI.jobs:
                  if ganga_job.name==j.name:continue
                  #lokk for jobs on the same site
                  if j.status in at_same_site: at_same_site[j.status]+=1
                  #look for jobs on the same CE
                  if j.backend.actualCE != this_backend_actualCE:continue
                  if j.status in at_same_CE: at_same_CE[j.status]+=1
               
            environment={'time':now_str,'same_CE':at_same_CE,'same_site':at_same_site}
            return environment
         if not abst_job_name in self.stress_test_status:
            environment=get_env()
            self.stress_test_status[abst_job_name]={jstatus:environment}
         else:
            
            if not jstatus in self.stress_test_status[abst_job_name] :
               environment=get_env()
               self.stress_test_status[abst_job_name][jstatus]=environment
      #####################################################################      
      if jstatus=="failed":
         if self.stress_test:self.stress_test_status.pop(abst_job_name);
         return False
      warn_me=False
      warn_txt="Job %s (id=%d)"%(ganga_job.name,ganga_job.id)
      warn_txt2=" is in status '%s' since more than %d minutes."#%markup("%s:",fg.magenta)           
      info_txt="If you think it is stuck, you could remove it. Do The following: (you get this message only once)\n"
      rem_txt=""#jobs(%d).remove()\n"%( ganga_job.id)
      _checked_status_txt=""
      if spj>-1:
         warn_txt="Subjob '%d' of job %s (id=%d)"%(spj,ganga_job.name,ganga_job.id)
      
      if not abst_job_name in abst_job.status_duration:
         _checked_status_txt=jstatus+":False"
         abst_job.status_duration[abst_job_name]={'time':"%s"%now_str,'stat':_checked_status_txt}
         if self.report_output:
            rept_txt="registering job %s in duration at %s: status=%s\n"%(abst_job_name,now_str,jstatus)
            self._report(rept_txt)
         return False
      _checked_status_txt=abst_job.status_duration[abst_job_name]['stat']
      if jstatus != _checked_status_txt.split(":")[0]:
         _checked_status_txt=jstatus+":False"
         abst_job.status_duration[abst_job_name]={'time':"%s"%now_str,'stat':_checked_status_txt}
         if self.report_output:
            rept_txt="Job status of job '%s' changes at %s to %s."%(abst_job_name,now_str,jstatus)
            self._report(rept_txt)
         return False
      else:
         duration=0
         reg_time_str=abst_job.status_duration[abst_job_name]['time']
         reg_time=reg_time_str.split(":")
         month=int(now[0])-int(reg_time[0])
         day  =int(now[1])-int(reg_time[1])
         hour =int(now[2])-int(reg_time[2])
         minut=int(now[3])-int(reg_time[3])
         duration = ( ( (month*month_days[int(reg_time[0])]) +day)*24 + hour)*60+minut
         for i in duration_status:
            if jstatus == i and duration >=duration_status[i]:
               if _checked_status_txt.split(":")[1]=="True": return False
                  
               #if i=='submitted':abst_job.status_duration[abst_job_name]={'time':"%s"%now_str,'stat':"new:False"}; return True
                  
               if i=='submitting' :
                  warn_txt_sub="""
                  ********************************************************************
                  ****  %s seem to be stuck in '%s' status. Removing it.
                  ********************************************************************
                  """%(warn_txt,markup("%s"%jstatus,fg.red))
                  
                  logger.warning(warn_txt_sub)
                  if self.report_output:
                     self._report(warn_txt_sub)
                     
                  abst_job.status_duration[abst_job_name]={'time':"%s"%now_str,'stat':"new:False"}
                  return True
               rem_txt="t=tasks.get('%s')\nabsj=t.get_job_by_name('%s')\nabsj.done=False\nabsj.status_duration={}\nabsj._status = 'new'\n"%(self.name, abst_job_name)
               rem_txt2="jobs(%d).remove()\n"%(ganga_job.id)
               if spj>-1:rem_txt2="jobs(%d).subjobs[%d].remove()\n"%(ganga_job.id,spj)
               rem_txt+=rem_txt2
               warn_txt+=warn_txt2 %(markup("%s"%i,fg.red),duration_status[i])
               _checked_status_txt=jstatus+":True"
               warn_me=True
               
            info_txt+="%s"%markup(rem_txt ,fg.magenta)
            if warn_me: break
      

      if warn_me:
         logger.warning(warn_txt);
         logger.info(info_txt)
         if self.report_output:
            self._report(warn_txt,info_txt)
         
      abst_job.status_duration[abst_job_name]['stat']=_checked_status_txt
      return False

################################
#####################################
   def check_CEs(self,jobs_state_locat,CEs_stats,_this_task):
      if self.stress_test: return
      
      def excl_bad_site(my_task,my_site,my_ces,my_spj):
         my_task.ext_excl_sites([my_site])
         my_task.excluded_CEs=mylist.difference(my_task.excluded_CEs,my_ces)
         my_spj.excluded_CEs=mylist.difference(my_spj.excluded_CEs,my_ces)

      for i in jobs_state_locat:
         spjn=(":").join(i.split("+")[0].split(":")[-2:])
         spj=self.get_job_by_name(spjn)
         if spj._status=="ignored": continue
         #the following only for anatask
         if self.type=="AnaTask":
            all_sites=[]
            #are we running with specified sites
            if _this_task.abstract_jobs[spjn]['sites_to_run'] and isinstance(_this_task.abstract_jobs[spjn]['sites_to_run'], list):all_sites=_this_task.abstract_jobs[spjn]['sites_to_run']
            elif _this_task.abstract_jobs[spjn]['all_sites']:all_sites=_this_task.abstract_jobs[spjn]['all_sites']
            else: all_sites=_this_task.allowed_sites
            if not all_sites: continue
         ## ende for anatask
         for ce in jobs_state_locat[i]:
            if ce==None:
               continue
            if ce in _this_task.excluded_CEs: continue
            ces_of_working_sites=[] #filled only with anatask
            if self.type=="AnaTask":
               working_site=self.get_CE_site(ce.lower(),all_sites)
               if not working_site: working_site=self.get_CE_site(ce.lower(),_this_task.allowed_sites)
               if not working_site: continue
               if working_site in _this_task.excluded_sites: continue
               ces_of_working_sites=getCEsForSites([working_site])
            ## ende for anatask

            bad_working_site=False
            bad_CE=False
            bad_job_CE_comp=False
                        
            if jobs_state_locat[i][ce].count("failed")==2 and jobs_state_locat[i][ce].count("completed")==0:
               bad_job_CE_comp=True
            failed_rel=0.0
            if CEs_stats[ce]["failed"]>0:failed_rel=float(CEs_stats[ce]["failed"])/float(CEs_stats[ce]["total"])

            if CEs_stats[ce]["failed"]>3 and failed_rel>0.7:
               bad_CE=True
               if len(ces_of_working_sites)==1: bad_working_site=True
               elif len(ces_of_working_sites)>1:#se:
                  for ce_work in ces_of_working_sites:
                     
                     if ce_work==ce:continue
                     if ce_work in CEs_stats and  CEs_stats[ce_work]["failed"]>3 and  CEs_stats[ce_work]["completed"]==0:
                        bad_working_site=True
                     else:
                        bad_working_site=False
                        break
            if bad_working_site:
               print "%s"%markup("""********************************** bad site""",fg.blue)
               excl_bad_site(_this_task, working_site, ces_of_working_sites,spj)
            elif bad_CE:
               print "%s"%markup("""**********************************""",fg.blue)
               info_txt="Excluding CE '%s' .\n"%ce
               info_txt+="Many jobs running on it are failing"
               logger.info(info_txt)
               print "%s"%markup("""**********************************""",fg.blue)
               _this_task.excluded_CEs.append(ce)

               if len(ces_of_working_sites)>0 and not mylist.difference(ces_of_working_sites,_this_task.excluded_CEs):
                  excl_bad_site(_this_task, working_site, ces_of_working_sites,spj)

            elif self.type=="AnaTask" and bad_job_CE_comp:
               print "%s"%markup("""********************************** bad_job_CE_comp""",fg.blue)
               spj.excluded_CEs.append(ce)
#####################################
#########################################
   def get_duration_status(self,stat=None,mod=None):
      print "get_duration_statu"
      if self.type=="AnaTask":mod=None
      print_lst=[]
      counter=0
      for i in self.spjobs:
         dur=i.status_duration
         
         for j in dur:
            if not dur[j]['stat'] or not dur[j]['time']:continue
            status=dur[j]['stat'].split(":")[0]
            txt=markup("Job %s is in status '%s' since %s (checked=%s)"%(j,status,dur[j]['time'],dur[j]['stat'].split(":")[1]),status_colours[status])
            if stat:
               if stat==status:
                  print_lst.append(txt)#print txt
               continue
            print_lst.append(txt)
            
      if stat:print "%s"%markup("The following %d jobs are in %s status"%(len(print_lst),stat),status_colours[stat])
      if not print_lst:
         logger.info("Task %s has no jobs !!"%self.name)

      for i in print_lst:
         print i
   
#########################################
   
   def get_CE_status(self,jobs_state_locat,CEs_stats):

      return []
#####################################
   def write_iterable(self,iterable,in_a_line=3):
      txt=""
      count=1
      for i in iterable:
         if not count%in_a_line or count==len(iterable):txt+="%s\n"%i
         else:txt+="%s, "%i
         count+=1
      return txt
#############################
   def _report(self,*txt):
      f=open(self.report_file,'a+')
      for i in txt:
         f.write(i)

      f.close()
#############################
   def reset_absj(self,absj_name):
      absj=self.get_job_by_name(absj_name)
      absj.done=False
      absj.ignore_this=False
      absj.status_duration={}
      absj._status = "new"

