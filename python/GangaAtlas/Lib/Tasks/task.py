
from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects
import Ganga.Utility.logging
import abstractjob
import time

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
                   'pause'      : fg.cyan }

overview_colours = {
                   'running'    : bg.green,
                   'done'       : bg.blue,
                   'attempted'  : bg.orange,
                   'ignored'    : bg.red,
                   'ready'      : bg.white,
                   'unready'    : fx.normal}

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

   def check_name(self, val):
      if "tasks" in GPI.__dict__:
         if val=='all':
            raise Exception("The word 'all' is reserved. do not give it as a name to your task. Try another name.")
         if self.status != "new":
            raise Exception("Cannot change name if the task is not new! If you want to change it, first copy the task.")
         if GPI.tasks.get(val, False):
            raise Exception("Cannot change name from %s to %s: A task with that name already exists!" % (self.name, val))
      return val

   _schema = Schema(Version(1,0), {
        'name'        : SimpleItem(defvalue='New Analysis Task', checkset="check_name", doc='Name of the Task'),
        'status'      : SimpleItem(defvalue='new', doc='Status - new, running, paused or completed'),
        'spjobs'      : ComponentItem('AbstractJobs',defvalue=[],sequence=1,copyable=1,doc='List of spjobs'),
        'float'       : SimpleItem(defvalue=0,doc='How many jobs should be run at the same time'),
        'jupdatetime' : SimpleItem(defvalue=0, hidden=1, doc=''),
        'AbstractJob'       : SimpleItem(defvalue=abstractjob.AbstractJob, hidden=1, doc='')
        })
    
   _category = 'Tasks'
   _name = 'Task'
   _exportmethods = ['submit', 'run', 'info', 'stats', '__str__', 'overview', 'get_next_jobs', 'get_job_by_name','get_total_jobs','get_done_jobs','get_working_jobs','get_ignored_jobs','get_forced_ignored_jobs', 'pause', 'unpause', '_next_name', 'failed_job', 'get_ignored_job', 'remove_jobs','_update_jobs', 'run_jobs','copytask', "get_name", "set_name"]
   
   # Maps abstract job names to abstract jobs
   _spjobmap = {}
   _total_jobs = 0
   _done_jobs = 0
   _working_jobs = 0
   _ignored_jobs = 0
   _forced_ignored_jobs = 0

   def __init__(self):
      """Task(name)
         This class is not meant to be instantiated.
	 For running an actual task, derive another class
	 In the constructor (__init__) call:
	 super(Task, self).__init__() and set basic parameters."""
      # some black magic to allow derived classes to specify inherited methods in
      # the _exportmethods variable without redefining them
##       from Ganga.GPIDev.Base.Proxy import ProxyMethodDescriptor
##       for t in Task.__dict__:
##          if (not t in self._proxyClass.__dict__) and (t in self._exportmethods):
##             f = ProxyMethodDescriptor(t)
##             f.__doc__ = Task.__dict__[t].__doc__
##             setattr(self._proxyClass, t, f)
            
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
      # If a second has not elapsed between two calls, return
      if ((time.time() - self.jupdatetime < 200) or (self.status=="completed")) and (not cache_override):
         return
      #logger.info("Task %s updating job status..." % self.name)
      self._spjobmap = {}

      jobs_state_locat={}
      CEs_stats={}
      _all_tasks=GPI.__dict__["tasks"]
      _this_task=_all_tasks.get(self.name)
      analysis_task=False
      if _this_task.__class__.__name__=="AnaTask": analysis_task=True
      #print "analysis_task=",; print analysis_task
      # 2 variables to allow accessing AnaTasks functions
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
                  jstatus = t.check_job(sj)
                  if jstatus in done_states:
                     t.done = True
                  t.jobs.append(jstatus)
               tname = self._next_name(tname)
         else:
            t = self.get_job_by_name(tname)
            if t:

               stucking=self.check_job_duration(j)#remove_stucking(t,jstatus)
               if stucking: GPI.jobs(j.id).remove()

               jstatus = t.check_job(j)                     
               if jstatus in done_states:
                  t.done = True
               t.jobs.append(jstatus)               

               ##################################
               #################################
               if analysis_task:
                  if not j.backend.actualCE:continue
                  if j.backend.actualCE in CEs_stats:
                     CEs_stats[j.backend.actualCE]["total"] +=1
                     if jstatus=="failed": CEs_stats[j.backend.actualCE]["failed"] +=1
                     elif jstatus=="completed": CEs_stats[j.backend.actualCE]["completed"] +=1
                  else:
                     CEs_stats[j.backend.actualCE]={"total":1,"failed":0,"completed":0}
                     if jstatus=="failed":
                        CEs_stats[j.backend.actualCE]["failed"]=1
                        CEs_stats[j.backend.actualCE]["completed"]=0
                     elif jstatus=="completed":
                        CEs_stats[j.backend.actualCE]["failed"]=0
                        CEs_stats[j.backend.actualCE]["completed"]=1

                  if j.name in jobs_state_locat:
                     if j.backend.actualCE in jobs_state_locat[j.name]: jobs_state_locat[j.name][j.backend.actualCE].append(jstatus)
                     else: jobs_state_locat[j.name][j.backend.actualCE]=[jstatus]
                  else:
                     jobs_state_locat[j.name]={j.backend.actualCE:[jstatus]}
               ##################################
               #################################
         
               ##################################
               #################################
      if analysis_task:
         #print "checking CE"
         self.check_CEs(jobs_state_locat,CEs_stats,_this_task)
         #print "checking CE. END"

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
      #print "to_run",;print to_run
      self._update_jobs(True)
      #print "preparing jobs"
      jl = self.prepare_jobs(self.get_next_jobs(to_run))
      jl.sort(lambda a,b: a.id.__cmp__(b.id))
      for j in jl:
         j.submit()
      
   def prepare_jobs(self,spjobs):
      """ prepare_spjobs(spjobs)
          Takes an arbitrary list of spjobs and prepares jobs to run them """
      from GangaAtlas.Lib.Tasks import logger
      spjobs.sort()
      #print spjobs
      jlist = []
      jl = []
      for j in spjobs:
         #print j
         if len(jlist) == 0 or self._next_name(jlist[-1].name) == j.name:
            jlist.append(j)
         else:
            jl.append(jlist[0].prepare(len(jlist)))
            jlist = [j]
      if jlist:
         #print "jlist"
         jl.append(jlist[0].prepare(len(jlist)))
      #print "returning jl"
      return jl
      
   def get_next_jobs(self, num):
      """get_next_jobs(n)
         return a list of n jobs that are next in line to be executed"""
      # build a list with jobs that are "free", not running or done and not ignored,
      # and ready: all prerequisites are fulfilled
      #print "---------- get_next_jobs:"
      #print self.spjobs
##       count=0
##       for t in self.spjobs:
##          print count
##          print t
##          print "++++++++++++++++"
##          print t.status()
##          print "calling t.ready()"
##          t.ready()
##          print "calling t.ready() done"
##          count+=1
##      print "getting tlist"
      tlist = [t for t in self.spjobs if t.status() == "new" and t.ready()]
      tlist.sort()
##      print "get_next_jobs: returning :",;print tlist
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
      #print "No data has been produced"
   
   def remove_jobs(self,to_be_removed="failed",intern=False):
      
      answer=1
      if not intern:
         print "%s"%markup("***********************************************************************************",fg.blue)
         print "%s"%markup("***  ",fg.blue),
         if to_be_removed=="all":
            print "Are you sure that you want to remove %s jobs of the task ' %s '? %s (1=yes/0=no)" % (markup("%s" % to_be_removed, fg.red), markup("%s"%self.name,fg.red), markup("(1=yes/0=no)", bg.red))
         else:
            print "Are you sure that you want to remove %s jobs of the task ' %s '? %s (1=yes/0=no)" % (markup("all %s" % to_be_removed, fg.red), markup("%s"%self.name,fg.red), markup("(1=yes/0=no)", bg.red))
            print "%s"%markup("***********************************************************************************",fg.blue)

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
               if to_be_removed=="all":j.remove()
               elif to_be_removed==j.status: j.remove()
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
      for t in self.spjobs:
         if t.status() == "ignored":
	    return t
      
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
      #print sites
      from GangaAtlas.Lib.AtlasLCGRequirements.AtlasLCGRequirements import getCEsForSites
      try:
         for ste in sites:
            ces=getCEsForSites([ste])
            if ce in ces: return ste
      except Exception,x:
         logger.error("given CE is not located at any of the sites holding the given dataset\n%s"%ce)
         return []
      
#################################
   def check_job_duration(self,ganga_job):
      abst_job_name=":".join(ganga_job.name.split(":")[2:])
      abst_job=self.get_job_by_name(abst_job_name)
      jstatus= abst_job.check_job(ganga_job)

      if jstatus=="failed":return False
      month_days={1:31,2:28,3:31,4:30,5:31,6:30,7:31,8:31,9:30,10:31,11:30,12:31}
      year=int(time.strftime("%Y"))
      if not year%4:month_days[2]=29
      now_str=time.strftime("%m:%d:%H:%M")
      now=now_str.split(":")

      war_txt=None
      info_txt=None
      _checked_status_txt=abst_job.status_duration[1]
      if jstatus != _checked_status_txt.split(":")[0]:  #abst_job.status_duration[1]:
         _checked_status_txt=jstatus+":False"
         abst_job.status_duration=["%s"%now_str,_checked_status_txt]
      else:
         duration=0
         reg_time_str=abst_job.status_duration[0]
         now=now_str.split(":")
         reg_time=reg_time_str.split(":")
         month=int(now[0])-int(reg_time[0])
         day  =int(now[1])-int(reg_time[1])
         hour =int(now[2])-int(reg_time[2])
         minut=int(now[3])-int(reg_time[3])
         duration = ( ( (month*month_days[int(reg_time[0])]) +day)*24 + hour)*60+minut
         if duration>=10 and jstatus == 'submitting':
            abst_job.status_duration=['0',''];
            logger.warning("""
            ****************************
            **** Job %s (%d) seem to be stucking in '%s' status. Removing it.
            ****************************
            """%(ganga_job.name,ganga_job.id,jstatus))               
            return True
         elif duration>=15 and jstatus == 'completing':
            if _checked_status_txt.split(":")[1]=="True": return False
            
            war_txt="Job %s is in status 'completing' since more than 15 minutes."%ganga_job.name
            info_txt="Either the job is stucking or it can not access dq2. Please check the dq2 avalability."
            info_txt+="If you think it is stucking, you could remove it. Do The following:\n"
            info_txt+="%s"%markup("jobs(%d).remove()\n"%( ganga_job.id) ,fg.magenta)
            _checked_status_txt=jstatus+":True"
            
         elif duration>=30 and jstatus == 'submitted':
            if _checked_status_txt.split(":")[1]=="True": return False
            
            war_txt="Job %s is in status 'submitted' since more than 30 minutes."%ganga_job.name
            info_txt="If you think it is stucking, you could remove it. Do The following:\n"
            info_txt+="%s"%markup("jobs(%d).remove()\n"%( ganga_job.id) ,fg.magenta)

            _checked_status_txt=jstatus+":True"            
         elif duration>=1440 and jstatus == 'running':
            if _checked_status_txt.split(":")[1]=="True": return False
            war_txt="Job %s is in status 'running' since more than one day."%ganga_job.name
            info_txt="If you think it is stucking, you could remove it. Do The following:\n"
            info_txt+="%s"%markup("jobs(%d).remove()\n"%( ganga_job.id) ,fg.magenta)

            _checked_status_txt=jstatus+":True"
         else: return False

         if war_txt: logger.warning(war_txt); logger.info(info_txt)
         abst_job.status_duration[1]=_checked_status_txt
      return False
################################
#####################################
   def check_CEs(self,jobs_state_locat,CEs_stats,_this_task):
      def excl_bad_site(my_task,my_site,my_ces,my_spj):
         my_task.ext_excl_sites([my_site])
         my_task.excluded_CEs=mylist.difference(my_task.excluded_CEs,my_ces)
         my_spj.excluded_CEs=mylist.difference(my_spj.excluded_CEs,my_ces)

      for i in jobs_state_locat:
         spjn=(":").join( i.split(":")[-2:])
         spj=self.get_job_by_name(spjn)
         if spj._status=="ignored": continue

         all_sites=[]
         #are we running with specified sites
         if _this_task.abstract_jobs[spjn]['sites_to_run']:all_sites=_this_task.abstract_jobs[spjn]['all_sites']
         else: all_sites=_this_task.allowed_sites
         if not all_sites: continue

         for ce in jobs_state_locat[i]:
            if ce in _this_task.excluded_CEs: continue
            from GangaAtlas.Lib.AtlasLCGRequirements.AtlasLCGRequirements import getCEsForSites
            working_site=self.get_CE_site(ce.lower(),all_sites)
            if not working_site: working_site=self.get_CE_site(ce.lower(),_this_task.allowed_sites)
            if not working_site: continue
            if working_site in _this_task.excluded_sites: continue
            #print "working_site:(%s)"%working_site
            ces_of_working_sites=getCEsForSites([working_site])
            bad_working_site=False
            bad_CE=False
            bad_job_CE_comp=False
            
            
            if jobs_state_locat[i][ce].count("failed")==2 and jobs_state_locat[i][ce].count("completed")==0:
               bad_job_CE_comp=True
            if CEs_stats[ce]["failed"]>3 and CEs_stats[ce]["completed"]==0:
               bad_CE=True
               if len(ces_of_working_sites)==1: bad_working_site=True
               else:
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
               print "%s"%markup("""********************************** bad CE""",fg.blue)
               _this_task.excluded_CEs.append(ce)
               if not mylist.difference(ces_of_working_sites,_this_task.excluded_CEs):
                  excl_bad_site(_this_task, working_site, ces_of_working_sites,spj)

            elif bad_job_CE_comp:
               print "%s"%markup("""********************************** bad_job_CE_comp""",fg.blue)
               spj.excluded_CEs.append(ce)
               
