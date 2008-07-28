
from math import ceil
from commands import getstatusoutput

from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

import task
import abstractjob
import mcjob
import random

from task import status_colours, overview_colours, fg, fx, bg, markup

class MCTask(task.Task):
   """ This class describes a Monte-Carlo 'production' on the grid. """

   def check_new(self, val):
      if self.status != "new" and "tasks" in GPI.__dict__:
         raise Exception("Cannot change this value if the task is not new! If you want to change it, first copy the task.")
      return val

   _schema = Schema(Version(1,0), dict(task.Task._schema.datadict.items() + {
        'run_number'  : SimpleItem(defvalue=0, checkset="check_new", doc="Official run number of the monte carlo process"),
        'total_events': SimpleItem(defvalue=0, checkset="check_new", doc="Total number of events to generate"),
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
        'CE' : SimpleItem(defvalue="",doc="Name of the CE queue where the computation should be"),
        'modelist'  : SimpleItem(defvalue=["evgen", "simul","recon"], protected=1, hidden=1, doc="Modelist"),
       }.items()))
    
   _category = 'Tasks'
   _name = 'MCTask'
   _exportmethods = task.Task._exportmethods

   def __init__(self):
      """MCTask(task_name)"""
      super(self.__class__, self).__init__() ## call constructor of Task
      self.AbstractJob = mcjob.MCJob
      self.random_seed = random.randint(1000000,999999999)

   def submit(self):
      # Set jobOptions and process names for known run numbers
      if self.run_number == 6315:
          self.process_name = 'H170Wplus_WWem'
          self.evgen_job_option = 'DC3.%06i.McAtNlo_Jimmy_H170Wp_WWem.py' % self.run_number
      elif self.run_number == 6316:
          self.process_name = 'H170Wminus_WWem'
          self.evgen_job_option = 'DC3.%06i.McAtNlo_Jimmy_H170Wm_WWem.py' % self.run_number
      elif self.run_number == 6317:
          self.process_name = 'H170Wplus_WWmu'
          self.evgen_job_option = 'DC3.%06i.McAtNlo_Jimmy_H170Wp_WWmu.py' % self.run_number
      elif self.run_number == 6318:
          self.process_name = 'H170Wminus_WWmu'
          self.evgen_job_option = 'DC3.%06i.McAtNlo_Jimmy_H170Wm_WWmu.py' % self.run_number
      # Check if stuff has the right format: 
      if not isinstance(self.run_number, int):
          raise Exception("The variable task.run_number has to be an integer! Submit aborted.")
      if not isinstance(self.total_events, int):
          raise Exception("The variable task.total_events has to be an integer! Submit aborted.")
      if not isinstance(self.process_name, str):
          raise Exception("The variable task.process_name has to be a string! Submit aborted.")
      if not isinstance(self.evgen_job_option, str):
          raise Exception("The variable task.evgen_job_option has to be a string! Submit aborted.")

      if not (isinstance(self.events_per_job, dict) and "evgen" in self.events_per_job and "simul" in self.events_per_job and "recon" in self.events_per_job):
          raise Exception("The variable task.events_per_job has to be a dictionary with the keys 'evgen','simul' and 'recon'! Submit aborted.")
      if not isinstance(self.events_per_job['evgen'], int):
          raise Exception("The variable task.events_per_job['evgen'] has to be an integer! Submit aborted.")
      if not isinstance(self.events_per_job['simul'], int):
          raise Exception("The variable task.events_per_job['simul'] has to be an integer! Submit aborted.")
      if not isinstance(self.events_per_job['recon'], int):
          raise Exception("The variable task.events_per_job['recon'] has to be an integer! Submit aborted.")

      if not (isinstance(self.athena_version, dict) and "evgen" in self.athena_version and "simul" in self.athena_version and "recon" in self.athena_version):
          raise Exception("The variable task.athena_version has to be a dictionary with the keys 'evgen','simul' and 'recon'! Submit aborted.")
      if not isinstance(self.athena_version['evgen'], str):
          raise Exception("The variable task.athena_version['evgen'] has to be a string! Submit aborted.")
      if not isinstance(self.athena_version['simul'], str):
          raise Exception("The variable task.athena_version['simul'] has to be a string! Submit aborted.")
      if not isinstance(self.athena_version['recon'], str):
          raise Exception("The variable task.athena_version['recon'] has to be a string! Submit aborted.")

      if not (isinstance(self.geometry_tag, dict) and "simul" in self.geometry_tag and "recon" in self.geometry_tag):
          raise Exception("The variable task.geometry_tag has to be a dictionary with the keys 'simul' and 'recon'! Submit aborted.")
      if not isinstance(self.geometry_tag['simul'], str):
          raise Exception("The variable task.geometry_tag['simul'] has to be a string! Submit aborted.")
      if not isinstance(self.geometry_tag['recon'], str):
          raise Exception("The variable task.geometry_tag['recon'] has to be a string! Submit aborted.")

      if not isinstance(self.trigger_config, str):
          raise Exception("The variable task.trigger_config has to be a string! Submit aborted.")
      if not isinstance(self.random_seed, int):
          raise Exception("The variable task.random_seed has to be an integer! Submit aborted.")

      if not (isinstance(self.datasets_data, dict) and "evgen" in self.datasets_data and "simul" in self.datasets_data and "recon" in self.datasets_data):
          raise Exception("The variable task.datasets_data has to be a dictionary with the keys 'evgen','simul' and 'recon'! Submit aborted.")
      if not isinstance(self.datasets_data['evgen'], str):
          raise Exception("The variable task.datasets_data['evgen'] has to be a string! Submit aborted.")
      if not isinstance(self.datasets_data['simul'], str):
          raise Exception("The variable task.datasets_data['simul'] has to be a string! Submit aborted.")
      if not isinstance(self.datasets_data['recon'], str):
          raise Exception("The variable task.datasets_data['recon'] has to be a string! Submit aborted.")

      if not (isinstance(self.filenames_data, dict) and "evgen" in self.filenames_data and "simul" in self.filenames_data and "recon" in self.filenames_data):
          raise Exception("The variable task.filenames_data has to be a dictionary with the keys 'evgen','simul' and 'recon'! Submit aborted.")
      if not isinstance(self.filenames_data['evgen'], str):
          raise Exception("The variable task.filenames_data['evgen'] has to be a string! Submit aborted.")
      if not isinstance(self.filenames_data['simul'], str):
          raise Exception("The variable task.filenames_data['simul'] has to be a string! Submit aborted.")
      if not isinstance(self.filenames_data['recon'], str):
          raise Exception("The variable task.filenames_data['recon'] has to be a string! Submit aborted.")

      if not isinstance(self.se_name, str):
          raise Exception("The variable task.se_name has to be a string! Submit aborted.")
      if not isinstance(self.CE, str):
          raise Exception("The variable task.CE has to be a string! Submit aborted.")

      super(MCTask, self).submit()
      # Load evgen events into initial tasklist 
      simuls_per_evgen = self.events_per_job['evgen']/self.events_per_job["simul"]
      simuls_per_recon = self.events_per_job["recon"]/self.events_per_job["simul"]
      max_evgens = 2*ceil(self.total_events*1.0/self.events_per_job["evgen"])
      for i in range(0, max_evgens):
         self.get_job_by_name("evgen:%i-0" % (i+1)) 
         for j in range(0, simuls_per_evgen):
            self.get_job_by_name("simul:%i-%i" % (i+1, j+1))
	    jobnum = (j / simuls_per_recon) * simuls_per_recon + 1
            self.get_job_by_name("recon:%i-%i" % (i+1, jobnum))

   def __repr__(self):
      """ Returns a prettyprint representation of this production task"""
      return "Small Monte-Carlo Production Task '%s' of %i %s-events (run number %i)" % \
                    (self.name, self.total_events, self.process_name, self.run_number)

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
      from Ganga.GPIDev.Credentials import GridProxy
      username = GridProxy().identity()
      from dq2.clientapi.DQ2 import DQ2, DQException
      dq2 = DQ2()
      logger.debug("Updating job status via DQ2...")
      files = {"evgen":[],"simul":[],"recon":[]}
      try:
         files["evgen"] = dq2.listFilesInDataset("users.%s.ganga.datafiles.%s.%06i.%s.evgen.EVNT" % (username, self.name, self.run_number, self.process_name))
         files["simul"] = dq2.listFilesInDataset("users.%s.ganga.datafiles.%s.%06i.%s.simul.RDO" % (username, self.name, self.run_number, self.process_name))
         files["recon"] = dq2.listFilesInDataset("users.%s.ganga.datafiles.%s.%06i.%s.recon.AOD" % (username, self.name, self.run_number, self.process_name))
      except DQException:
         logger.debug("DQ2 error (ignored)")

      for mode in ["evgen","simul","recon"]:
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
   
   def overview(self):
      """ Gives an ascii-art overview over task status. Overridden for MC mode splitting"""
      if self.status == "new":
         print "No jobs defined yet."
         return
      print "Done: '%s' ; Running the nth time: '%s'-'%s' and '%s' ; Attempted: '%s' ; Not ready: '%s' ; Ready '%s'" % (markup("-", overview_colours["done"]), markup("1", overview_colours["running"]), markup("9", overview_colours["running"]), markup("+", overview_colours["running"]), markup(":", overview_colours["attempted"]), markup(",", overview_colours["unready"]), markup(".", overview_colours["ready"]))
      for mode in self.modelist:
         print "%s:" % mode.upper()
         s = self.get_ascii_str(self.total_events/self.events_per_job[mode], mode)
         for l in s.split("\n"):
            print l

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
            if t.simul_number == 1: str += "\n"
            n += 1
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
            elif t.get_run_count() > 0: ## task already run but not successfully 
	       str += markup(":", overview_colours["attempted"])
            else:
               if t.ready():
                  str += markup(".", overview_colours["ready"])
               else:
                  str += markup(",", overview_colours["unready"])
      return str

      __str__ = info 
      __repr__ = info 
