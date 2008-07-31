
from math import ceil
from commands import getstatusoutput

from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

import task
import abstractjob
import mcjob
import random
import MyList
mylist=MyList.MyList()

from task import status_colours, overview_colours, fg, fx, bg, markup
from Ganga.GPIDev.Credentials import GridProxy
username = GridProxy().identity()
from dq2.clientapi.DQ2 import DQ2, DQException
dq2 = DQ2()

class MCTask(task.Task):
   """ This class describes a Monte-Carlo 'production' on the grid. """

   def check_new(self, val):
      if "tasks" in GPI.__dict__ and "status" in self._data and self.status != "new":#if self.status != "new" and "tasks" in GPI.__dict__:
         raise Exception("Cannot change this value if the task is not new! If you want to change it, first copy the task.")
      return val

   _schema = Schema(Version(1,1), dict(task.Task._schema.datadict.items() + {
        'run_number'  : SimpleItem(defvalue=0, checkset="check_new", doc="Official run number of the monte carlo process"),
        'total_events': SimpleItem(defvalue=0, checkset="check_new", doc="Total number of events to generate"),
        'skip_evgen_partitions': SimpleItem(defvalue=0, checkset="check_new", doc="Number of evgen partitions to skip"),
        'process_name': SimpleItem(defvalue="CUSTOM", checkset="check_new", doc="Name of the process"),
        'evgen_job_option': SimpleItem(defvalue='', checkset="check_new", doc="Name of the official evgen file, or path to local evgen file"),
        'generator': SimpleItem(defvalue='', checkset="check_new", doc="Name of the generator if input files are needed, p.e. McAtNlo or alpgen"),
        'events_per_job' : SimpleItem(defvalue={"evgen": 10000, "simul": 50, "recon": 1000}, checkset="check_new", doc="Events per job for evgen, simul and recon"),
        'athena_version' : SimpleItem(defvalue={"evgen": '12.0.7.2', "simul": '12.0.7.2', "recon": '12.0.7.2'}, doc="Athena versions to use"),
        'geometry_tag' : SimpleItem(defvalue={"simul": "ATLAS-CSC-01-00-00","recon": "ATLAS-CSC-01-00-00"}, checkset="check_new", doc="Geometry Tag to use, defaults to ideal Geometry"),
        'trigger_config':SimpleItem(defvalue='DEFAULT', checkset="check_new", doc="TriggerConfig string for later Athena versions"),
        'random_seed'  : SimpleItem(defvalue=1102362401, checkset="check_new", doc="Random seed for Monte Carlo"),
        'datasets_data'  : SimpleItem(defvalue={"evgen":"", "simul":"", "recon":""}, checkset="check_new", doc=""), 
        'filenames_data' : SimpleItem(defvalue={"evgen":"", "simul":"", "recon":""}, checkset="check_new", doc=""),
        'se_name' : SimpleItem(defvalue="FZKDISK",doc="Name of the SE where the data should be stored"),
        'modelist'  : SimpleItem(defvalue=["evgen", "simul","recon"], protected=1, hidden=0, doc="Modelist"),
       }.items()))
    
   _category = 'Tasks'
   _name = 'MCTask'
   _exportmethods = task.Task._exportmethods + ["from_evgen"]

   def __init__(self):
      """MCTask(task_name)"""
      super(MCTask, self).__init__() ## call constructor of Task
      #super(self.__class__, self).__init__() ## call constructor of Task
      self.AbstractJob = mcjob.MCJob
      self.random_seed = random.randint(1000000,999999999)
#############################
   def from_evgen(self, dataset):
      try:
         files = dq2.listFilesInDataset(dataset)
      except DQException:
         logger.error("Dataset %s not known in DQ2" % dataset)
         return

      self.evgen_job_option = ""
      self.generator = ""
      self.datasets_data["evgen"] = dataset
      
      # Get file prefix and list of numbers
      evgenlist = []
      for key in files[0]:
         s=files[0][key]["lfn"]
         npos = s.find("._")
         evgenlist.append(int(s[npos+2:npos+7]))
         prefix = s[:npos]
      print "Found file prefix %s " % prefix
      self.filenames_data["evgen"] = prefix
       
      # Try to extract parameters from dataset name
      # example: mc12.006107.AlpgenJimmyWmunuNp0LooseCut.evgen.EVNT.v12000501_tid014160    
      sds = dataset.split(".")
      if (len(sds) >= 6) and (sds[3]=="evgen") and (sds[4]=="EVNT"):
         self.run_number = int(sds[1])
         self.process_name = sds[2]
         print "Dataset conforms to naming convention, setting run_number to %i and Process Name to %s" % (self.run_number, self.process_name)
         print "You have now only to set the name, total_events, events_per_job, athena_version, geometry_tag, trigger_config and se_name to start"
      else:
         print "Dataset does not conform to naming convention"
######################################
   def submit(self):
      ignorelist = []
      if self.datasets_data["evgen"]:
         dataset = self.datasets_data["evgen"]
         try:
            files = dq2.listFilesInDataset(dataset)
         except DQException:
            logger.error("Dataset %s not known in DQ2" % dataset)
            return
         # Get file prefix and list of numbers
         evgenlist = []
         for key in files[0]:
            s=files[0][key]["lfn"]
            npos = s.find("._")
            evgenlist.append(int(s[npos+2:npos+7]))
         el = dict(zip(evgenlist,evgenlist)).keys()
         el.sort()
         nevents = len(el) * self.events_per_job["evgen"]
         if self.total_events > nevents:
            print "In evgen there are only %i events available. Not submitting." % nevents
            return
         ninputs = self.total_events/self.events_per_job["evgen"]
         ngoodinputs = 0

         for i in range(1, el[-1]+1):
            if i in el and i > self.skip_evgen_partitions:
               ngoodinputs += 1
               if ngoodinputs >= ninputs:
                  break
            else:
               self.total_events += self.events_per_job["evgen"]
               if self.skip_evgen_partitions >= i:
                  print "Partition number %i skipped..." % i
               else:
                  print "Partition number %i not in input dataset: setting to ignored" % i
               ignorelist.append(i)

      if not self.set_attributes():
         logger.info("Task '%s' not submitted."%self.name)
         return
      
      super(MCTask, self).submit()
      # Load evgen events into initial tasklist 
      simuls_per_evgen = self.events_per_job['evgen']/self.events_per_job["simul"]
      if "recon" in self.modelist:
         simuls_per_recon = self.events_per_job["recon"]/self.events_per_job["simul"]
      max_evgens = 2*ceil(self.total_events*1.0/self.events_per_job["evgen"])
      self.info()
      for i in range(self.skip_evgen_partitions, max_evgens):
         self.get_job_by_name("evgen:%i-0" % (i+1)) 
         for j in range(0, simuls_per_evgen):
            self.get_job_by_name("simul:%i-%i" % (i+1, j+1))
            if "recon" in self.modelist:
               jobnum = (j / simuls_per_recon) * simuls_per_recon + 1
               self.get_job_by_name("recon:%i-%i" % (i+1, jobnum))

      for i in ignorelist:
         self.get_job_by_name("evgen:%i-0"%i).ignore_this = True
      self._update_jobs(True);
      if len(ignorelist) > 0:
         print "Increased total_events to %i because some input files were not available and will be skipped." % self.total_events

      # Update jobs to have accurate numbers
      self._update_jobs(True) 

   def __repr__(self):
      """ Returns a prettyprint representation of this production task"""
      return "Small Monte-Carlo Production Task '%s' of %i %s-events (run number %i)" % \
                    (self.name, self.total_events, self.process_name, self.run_number)
###################################
   def set_attributes(self):   
      """ checks if the attributs are set properly."""
      super(self.__class__, self).set_attributes()
      for mod in self.modelist:
         if len(self.athena_version[mod].split("."))!= 4:
            logger.error("""
            Given version of Atlas Release for mode '%s' (%s) is not proper
            The Atlas Release must be given to 4 version digits: for example 14.1.0.1
            Look at http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/Production/kits/
            to list possible Versions."""%(mod,self.athena_version[mod]))
            return False
      return True

    
   def _next_name(self,name):
      """ returns the name following 'name' in subjobs"""
      mode = name.split(":")[0]
      enumber = int(name.split(":")[1].split("-")[0])
      snumber = int(name.split(":")[1].split("-")[1])
      if mode == "simul":
         return "simul:%i-%i" % (enumber, snumber + 1)
      elif mode == "recon":
         simuls_per_recon = self.events_per_job["recon"] / self.events_per_job["simul"]
         return "recon:%i-%i" % (enumber, snumber + simuls_per_recon)
      return ""
   
   def on_complete(self):
      from Ganga.GPIDev.Credentials import GridProxy
      username = GridProxy().identity()
      print "The data has been saved in the DQ2 dataset users.%s.ganga.datafiles.%s.%06i.%s.recon.AOD" % (username, self.name, self.run_number, self.process_name)
   
   def _update_jobs_more(self):

      logger.debug("Updating job status via DQ2...")
      files = {"evgen":[],"simul":[],"recon":[]}
      try:
         ds = {}
         for mode in self.modelist:
            ds[mode] = self.datasets_data[mode]
            if not ds[mode]:
               if mode == "evgen":
                  ds[mode] = "users.%s.ganga.datafiles.%s.%06i.%s.evgen.EVNT" % (username, self.name, self.run_number, self.process_name)
               if mode == "simul":
                  ds[mode] = "users.%s.ganga.datafiles.%s.%06i.%s.simul.RDO" % (username, self.name, self.run_number, self.process_name)
               if mode == "recon":
                  ds[mode] = "users.%s.ganga.datafiles.%s.%06i.%s.recon.AOD" % (username, self.name, self.run_number, self.process_name)
         for mode in self.modelist:         
            files[mode] = dq2.listFilesInDataset(ds[mode])
      except DQException:
         logger.debug("DQ2 error (ignored)")

      for mode in self.modelist:
        if files[mode]:
           donelist = []
           for key in files[mode][0]:
              s=files[mode][0][key]["lfn"]
              donelist.append(int(s[s.find("._")+2:s.find("._")+7]))
           for sj in self.spjobs:
              if sj.mode == mode and sj.partition_number() in donelist:
                 sj.done = True

########### FOLLOW INFORMATION FUNCTIONS
   def info(self):
      print markup("Production Task '%s' - %s " % (self.name, self.status), status_colours[self.status])
      print "Generating %s events of type %s, run_number %s" % (self.total_events, self.process_name, self.run_number)
      print
      print "Evgen Joboptions : %s" % self.evgen_job_option
      print "Events per job   : ", self.events_per_job
      print "Athena versions  : ", self.athena_version
      print "Geometry Tag     : %s" % self.geometry_tag
      print "Trigger Config   : %s" % self.trigger_config
      print "Random seed      : %s" % self.random_seed
      print "Storing Data on  : %s" % self.se_name
      if self.CE:
         print "Use only CE      : %s" % self.CE

   def stats(self):
      """ Prints some information on this task. Can be overridden. """
      done = {"evgen":0,"simul":0,"recon":0}
      working = {"evgen":0,"simul":0,"recon":0}
      ignoring = {"evgen":0,"simul":0,"recon":0}
      total = {"evgen":0,"simul":0,"recon":0}

      successes = {"evgen":0,"simul":0,"recon":0}
      attempts = {"evgen":0,"simul":0,"recon":0}
      rem_attempted = {"evgen":0,"simul":0,"recon":0}

      for t in [t for t in self.spjobs if t.necessary()]:
         total[t.mode] += 1
         numjobs = len(t.get_jobs())
         status = t.status()
         attempts[t.mode] += numjobs
         if status == "done":
            done[t.mode] += 1
            if numjobs > 0:
               successes[t.mode] += 1
         elif status == "working":
            working[t.mode] += 1
         elif status == "ignored":
            rem_attempted[t.mode] += 1
            ignoring[t.mode] += 1
         else:
            if numjobs > 0:
               rem_attempted[t.mode] += 1
      
      print "Task %s" % self.name
      print "Float: %i " % self.float
      print "Total %i jobs" % (total["evgen"]+total["simul"]+total["recon"])
      if total["evgen"] == 0: return

      for mode in self.modelist:
         print markup(mode.upper(), fx.bold)  + "  (%i jobs)" % (total[mode])
         if total[mode] > 0: 
            print markup(" - %4i jobs ( %2i%% ) " % (done[mode], done[mode]*100.0/total[mode]) + "done", status_colours["done"]) 
            print markup(" - %4i jobs ( %2i%% ) " % (working[mode], working[mode]*100.0/total[mode]) + "in progress", status_colours["running"])
            print markup(" - %4i jobs ( %2i%% ) " % (rem_attempted[mode], rem_attempted[mode]*100.0/total[mode]) + "attempted", status_colours["attempted"])
            if ignoring[mode]:
               print markup(" - ignoring: %i jobs" % (ignoring[mode]), status_colours["ignored"])
            if attempts[mode] == 0:
               print " * no jobs run\n"
            else:
               print " * jobs run: %i; efficiency: %i%% to %i%%\n" % \
                   (attempts[mode], successes[mode]*100.0/attempts[mode], (successes[mode] + working[mode])*100.0/attempts[mode])
      print ""
      d = reduce(int.__add__, successes.values(), 0)
      a = reduce(int.__add__, attempts.values(), 0)
      i = reduce(int.__add__, working.values(), 0)
      if a > 0:
         print "Total Efficiency: %i/%i = %i%% to %i/%i = %i%%" % (d, a, d*100.0/a, d+i, a, (d+i)*100.0/a)
###################################
   def overview(self,mod=None):
      """ Gives an ascii-art overview over task status. Overridden for MC mode splitting"""
      if self.status == "new":
         print "No jobs defined yet."
         return
      print "Done: '%s' ; Running the nth time: '%s'-'%s' and '%s' ; Attempted: '%s' ; Not ready: '%s' ; Ready '%s'" % (markup("-", overview_colours["done"]), markup("1", overview_colours["running"]), markup("9", overview_colours["running"]), markup("+", overview_colours["running"]), markup(":", overview_colours["attempted"]), markup(",", overview_colours["unready"]), markup(".", overview_colours["ready"]))
      for mode in self.modelist:
         if mod and mod !=mode:continue
         mod_txt="%s"%markup("------------\n%s:" % mode.upper(),fg.red)
         print mod_txt
         s = self.get_ascii_str(self.total_events/self.events_per_job[mode], mode)
         for l in s.split("\n"):
            print l
#######################
   def get_ascii_str(self, chunks, mode):
      """ Get an ascii art overview over task status. Can be overridden """
      str = ''
      tlist = [t for t in self.spjobs if t.mode == mode and t.necessary()]
      tlist.sort()
      l = len(tlist)
      n = 0 
      i = 0
      while i < chunks or n < l:
         i += 1
         if not l > n or not i == tlist[n].partition_number():
            str += "u"
            if l > n and tlist[n].partition_number() < i:
               raise Exception("Logic Error in get_ascii_string_expected - numbering of jobs not continuous! (%i,%i)"% (i,tlist[n].partition_number()))
         else:
            t = tlist[n]
            if t.simul_number == 1:
               str += "\n\n"
            n += 1
            status = t.status()
            job_number=t.name.split(":")[1]
            if status == "done":
               str += markup("%s : -"% job_number, overview_colours["done"])
            elif status == "working":
               if t.get_run_count() < 10:
                  str += markup("%s : %i" % (job_number,t.get_run_count()), overview_colours["running"])
               else:
                  str += markup("%s: +"% job_number, overview_colours["running"])
            elif status == "ignored":
               str += markup("%s : i"% job_number, overview_colours["ignored"])
            elif t.get_run_count() > 0: ## task already run but not successfully 
               str += markup("%s : :"% job_number, overview_colours["attempted"])
            else:
               if t.ready():
                  str += markup("%s : ."% job_number, overview_colours["ready"])
               else:
                  str += markup("%s : ,"% job_number, overview_colours["unready"])
         str+=" "
      return str

      __str__ = info 
      __repr__ = info 
#####################################
   def test_func(self,m=5):
      super(self.__class__, self).test_func()
      if m>5: print "m is larger 5"
      print "ich bin test funktion in mctask. Hallo, m=%d"%m
#######################
   def _change_CE(self,ce):
      jobs_lst=super(self.__class__, self)._change_CE(ce,None)
      if not jobs_lst: return
      self.allow_change=True
      self.CE=ce
      self.allow_change=False
            
#######################
   def ext_req_sites(self,sites,jobs=None):
      jobs_sites_dict=super(self.__class__, self).ext_req_sites(sites,jobs)
      if not jobs_sites_dict: return
        
      jobs_lst=jobs_sites_dict.keys()[0].split("+")
      sts=jobs_sites_dict.values()[0]
        ####################################
      for j in jobs_lst:         
         for ste in sts:
            if ste in self.requirements_sites: continue 
            self.requirements_sites.append(ste)
            
