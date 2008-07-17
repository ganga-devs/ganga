
from math import ceil

from Ganga import GPI
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

import task
import abstractjob

from Ganga.Utility.logging import getLogger
logger = getLogger()

class MCJob(abstractjob.AbstractJob):
   """ A job in a Monte-Carlo production task"""
   
   _schema = Schema(Version(1,0), dict(abstractjob.AbstractJob._schema.datadict.items() + {
       'mode'        : SimpleItem(defvalue="evgen", comparable=1, doc='Transformation mode of this job: evgen, simul, recon'),
       'evgen_number': SimpleItem(defvalue=0, comparable=1, doc='number of evgen partition this chunk of events is descended from'),
       'simul_number': SimpleItem(defvalue=0, comparable=1, doc='number of (first) simul partition this chunk of events is from'),
       }.items()))
 
   _category = 'AbstractJobs'
   _name = 'MCJob'
   _exportmethods = abstractjob.AbstractJob._exportmethods

   def __init__(self, taskname='', name='', task = None):
      """ Initialize a mcjob by a name"""
      super(self.__class__, self).__init__(taskname, name, task)
      if name == "": ## This happens iff the task is loaded from file
         return

      ## Extract evgen and simul numbers
      sname = name.split(":")
      if not len(sname) == 2: raise Exception("Unknown naming scheme")
      if task and not sname[0] in task.modelist: raise Exception
      self.mode = sname[0]
      numbers = sname[1].split("-")
      if len(numbers) == 2:
         self.evgen_number = int(numbers[0])
         self.simul_number = int(numbers[1])
      else: raise Exception("Unknown naming scheme")

      if self.mode == "evgen":
         if self.simul_number != 0:
            raise Exception("Tried to initialize invalid Evgen job (%s)" % name)
      ## Check if the recon number is valid
      if task and self.mode == "recon":
         simuls_per_recon = int(ceil(task.events_per_job["recon"] * 1.0 / task.events_per_job["simul"]))
         if not (self.simul_number - 1) % simuls_per_recon == 0:
            raise Exception("Tried to initialize invalid Recon job (%s)" % name)

   def check_job(self, j):
      if j.status == "completed":
         if len(j.outputdata.expected_output)>len(j.outputdata.actual_output):
            self.done = False
            logger.error("Job %s has more expected output than actual ouput (%s)!" % (j.id, j.outputdata.actual_output))
            j._impl.updateStatus("failed")
            return "failed"
         else:
            return "completed"
      else:
         return j.status

   def partition_number(self):
      """This defines the numbering scheme of output files"""
      epj = self.get_task().events_per_job
      enum = (self.evgen_number - 1) * epj["evgen"] 
      if self.simul_number > 0: 
         enum += (self.simul_number - 1) * epj["simul"]
      return enum / epj[self.mode] + 1
   
   def __cmp__(self,t): 
      """ Sort by index in modelist (evgen > simul > recon), then by evgen_numbers and then by simul_numbers"""
      if self.mode == t.mode:
         if self.evgen_number == t.evgen_number:
            return self.simul_number.__cmp__(t.simul_number)
         else:
            return self.evgen_number.__cmp__(t.evgen_number)
      else:
         return self.get_task().modelist.index(t.mode).__cmp__(self.get_task().modelist.index(self.mode))

   def prerequisites(self):
      """ Returns a list of spjobs that have to be run for this job to be ready"""
      if self.mode == "evgen": 
         return []
      elif self.mode == "simul":
         return [self.get_task().get_job_by_name("evgen:%i-0"%i) for i in range(1, self.evgen_number+1)]
      elif self.mode == "recon":
         simuls_per_recon = self.get_task().events_per_job["recon"] / self.get_task().events_per_job["simul"]
         return [self.get_task().get_job_by_name("simul:%i-%i" % (self.evgen_number, i)) for i in range(self.simul_number, self.simul_number + simuls_per_recon)]
    
   def necessary(self):
      """ Defines if this job has to be executed to meet task specifications"""
      # check for invalid job number combinations:
      if self.mode == "recon":
         simuls_per_recon = int(ceil(self.get_task().events_per_job["recon"] * 1.0 / self.get_task().events_per_job["simul"]))
         if not (self.simul_number - 1) % simuls_per_recon == 0:
            raise Exception("Tried to initialize invalid Recon job (%s)" % name)
      if self.mode == "evgen":
         if self.simul_number != 0:
            return False
      # check if number of events is inside total  
      number = (self.evgen_number - 1) * self.get_task().events_per_job["evgen"]
      if self.mode in ["simul", "recon"]:
         snumber = (self.simul_number - 1) * self.get_task().events_per_job["simul"] + self.get_task().events_per_job[self.mode]
         if snumber > self.get_task().events_per_job["evgen"]: ## Can only use this many events from one evgen 
            return False
         number += snumber
      else: # In evgen we generate _at_least_ as many events!
         number += 1
      if number > self.get_task().total_events:
         return False
      return True

   def _helper_prepare_job(self, atlas_release):
      """_helper_prepare_job(atlas_release)
      returns a job with the general, mode-independent parameters pre-set. 
      INTERNAL FUNCTION"""
      p = self.get_task()
      j=GPI.Job()
      j.application=GPI.AthenaMC()

      j.application.production_name = p.name
      j.application.process_name = p.process_name

      j.application.run_number = '%06i' % p.run_number
      j.application.se_name = p.se_name

      j.outputdata = GPI.AthenaMCOutputDatasets()
      j.outputdata.output_firstfile = self.partition_number()

      kiturl = "http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/Production/kits/"
      av = atlas_release.split(".")
      j.application.atlas_release=".".join(av[:3])
      if len(av) == 3:
         if av[2] == "31":
            av.append("8")
         elif av[2] == "4":
            av.append("2")
         elif av[2] == "5":
            av.append("3")
         elif av[2] == "6":
            av.append("5")
         elif av[2] == "7":
            av.append("2")
         else:
            raise Exception('ERROR: Unknown Athena Version specified.')
            return
      j.application.transform_archive=kiturl + "AtlasProduction_%s_noarch.tar.gz" % "_".join(av)
      return j

   def prepare(self, count = 1, backend = "Grid"):
      """ This method returns one (1) Ganga job object that is prepared to run this task, with no specific backend.
          If count is greater than 1, prepare a split job with 'count' subjobs."""
      p = self.get_task()

      number = self.partition_number()

      j = self._helper_prepare_job(p.athena_version[self.mode])
      j.application.mode = self.mode
      j.application.number_events_job = "%i" % p.events_per_job[self.mode]
      j.application.triggerConfig = p.trigger_config
      j.application.random_seed = "%s" % p.random_seed         

      j.backend=Ganga.GPI.LCG()
      #j.backend.middleware='EDG'
      j.backend.middleware='GLITE'
      j.backend.requirements.other = ['other.GlueCEStateStatus=="Production"']
      j.backend.requirements.cputime = "1400"
      if p.CE:
         j.backend.CE=p.CE

      # Set name
      j.name = "%s:%s:%s" % (backend, p.name, self.name)
      if count > 1:
         j.splitter=GPI.AthenaMCSplitterJob()
         j.splitter.numsubjobs = count
         if self.mode == "simul":
            j.splitter.nsubjobs_inputfile = count # one input file for all subjobs
         elif self.mode == "recon":   
            j.splitter.nsubjobs_inputfile = 1 # one (or more) input file for every subjob

      if self.mode == "evgen":
         j.backend.requirements.memory = "512"
         if p.generator:
            j.inputdata = GPI.AthenaMCInputDatasets()
            j.inputdata.datasetType = "DQ2"
            j.inputdata.DQ2dataset = '%s.%06i.%s'% (p.generator, p.run_number, j.application.process_name)
            j.inputdata.inputfiles = ['%s.%06i.%s._%05i.tar.gz'% (p.generator, p.run_number, j.application.process_name, number)]
            j.application.extraArgs=' inputGeneratorFile=`pwd`/atlas.tmp$$/%s.%06i.%s._%05i.tar.gz' % (p.generator, p.run_number, j.application.process_name, number)
            j.application.evgen_job_option = p.evgen_job_option
         else:
            j.application.firstevent = (self.evgen_number - 1) * p.events_per_job["evgen"] + 1  
            j.application.evgen_job_option = p.evgen_job_option

      elif self.mode == "simul":
         j.application.geometryTag = p.geometry_tag["simul"]
         j.backend.requirements.memory = "800"
         j.application.firstevent =  (self.simul_number - 1) * p.events_per_job["simul"] + 1
         j.inputdata = GPI.AthenaMCInputDatasets()
         if p.athena_version[self.mode][:2] == "13":
            j.application.extraArgs=' digiSeedOffset1=11 digiSeedOffset2=22 '
         if p.datasets_data["evgen"]:
            j.inputdata.datasetType = "DQ2"
            j.inputdata.DQ2dataset = p.datasets_data["evgen"]
            if p.filenames_data["evgen"]:
               j.inputdata.inputfiles = [p.filenames_data["evgen"] + "._%5.5d" % self.evgen_number]
            else:
               j.inputdata.inputpartitions = '%i' % self.evgen_number
         else:
            j.inputdata.inputpartitions = '%i' % self.evgen_number

      elif self.mode == "recon":
         j.application.geometryTag = p.geometry_tag["recon"]
         j.backend.requirements.memory = "1300"
         simuls_per_recon = int(ceil(self.get_task().events_per_job["recon"] * 1.0 / self.get_task().events_per_job["simul"]))
         pr = self.prerequisites()
         pr.sort()
         start = pr[0].partition_number()
         j.inputdata = GPI.AthenaMCInputDatasets()

         j.backend.requirements.cputime = 70*simuls_per_recon

         if p.datasets_data["simul"]:
            j.inputdata.datasetType = "DQ2"
            j.inputdata.DQ2dataset = p.datasets_data["evgen"]
            if p.filenames_data["simul"]:
               j.inputdata.inputfiles = [p.filenames_data["evgen"] + "._%5.5d" % i for i in range(start, start+simuls_per_recon)]
            else:
               j.inputdata.inputpartitions = '%i-%i' % (start, start + count*simuls_per_recon - 1)
         else:
            j.inputdata.inputpartitions = '%i-%i' % (start, start + count*simuls_per_recon - 1)

         j.inputdata.number_inputfiles = count*simuls_per_recon
         j.inputdata.n_infiles_job = simuls_per_recon
         
      
      return j

