from Ganga.GPIDev.Lib.Tasks.common import *
import random

from Ganga.GPIDev.Lib.Tasks import Transform
from TaskApplication import AthenaMCTask
from GangaAtlas.Lib.AthenaMC.AthenaMCDatasets import AthenaMCOutputDatasets, AthenaMCInputDatasets, _usertag
from Ganga.GPIDev.Schema import *

# Extract username from certificate
from Ganga.GPIDev.Credentials import GridProxy
proxy = GridProxy()
username = proxy.identity()
      
# AthenaMC public settings
#athenamcsettings = ["mode", "run_number", "production_name", "process_name", "atlas_release", "se_name", "transform_archive", "verbosity", "siteroot", "version", "cmtsite", "transform_script", "extraArgs", "extraIncArgs"]
athenamcsettings = ["mode", "run_number", "production_name", "process_name", "atlas_release", "se_name", "transform_archive"]
settings = {}
settings["EvgenTransform"] = athenamcsettings + ["evgen_job_option"]
settings["SimulTransform"] = athenamcsettings + ["triggerConfig", "geometryTag"]
settings["ReconTransform"] = athenamcsettings + ["triggerConfig", "geometryTag"]

class MCTransform(Transform):
   _schema = Schema(Version(1,0), dict(Transform._schema.datadict.items() + {
       'file_type': SimpleItem(defvalue=1, hidden=1, doc='string in the output file, pe. evgen.EVNT..',modelist=["str"]),
       'random_seeds': SimpleItem(defvalue={}, doc='random seeds to be used in the partition',modelist=["dict","int"]),

}.items()))
   _category = 'transforms'
   _name = 'MCTransform'
   _hidden = 1
   _exportmethods = Transform._exportmethods

## Special methods
   def initialize(self):
      super(MCTransform, self).initialize()
      self.application = AthenaMCTask()
      self.outputdata = AthenaMCOutputDatasets()

## Private methods (overridden from Transforms)
   def checkCompletedApp(self, app):
      if "dryrun" in stripProxy(app)._data and app.dryrun:
         return True
      j = app._getParent()
      for f in j.outputdata.actual_output:
         if self.file_type in f:
            return True
      logger.error("Job %s has not produced %s file, only: %s" % (j.fqid, self.file_type, j.outputdata.actual_output))
      return False

   def getJobsForPartitions(self, partitions):
      """Create Ganga Jobs for the next N partitions that are ready and submit them."""
      j = self.createNewJob(partitions[0])
      # Set random seed - random.seed() initializes with nanosecond time

      for part in partitions:
         random.seed()
         if not part in self.random_seeds:
            self.random_seeds[part] = random.randint(2**16,2**23)
      # Set splitter if number > 1
      if len(partitions) > 1:
         j.splitter=GPI.AthenaMCTaskSplitterJob()
         j.splitter.output_partitions = partitions
         j.splitter.task_partitions = partitions
         j.splitter.random_seeds = [self.random_seeds[part] for part in partitions]
      else:
         j.application.partition_number = partitions[0]
         j.application.random_seed = "%s" % self.random_seeds[partitions[0]]
      return [j]

   def updateInputStatus(self, ltf, partition):
      # Check all partitions that use this input
      partitions = self.application.getPartitionsForInputs([partition], self.inputdata)
      for c in partitions:
         if self.getPartitionStatus(c) in ["completed", "bad", "ignored"]:
            continue
         dep_status = [ltf.getPartitionStatus(i) for i in self.application.getInputsForPartitions([c], self.inputdata)]
         ready = [s in ["completed","bad","ignored"] for s in dep_status]
         if False in ready:
            self.setPartitionsStatus([c],"hold")
         elif "completed" in dep_status:
            self.setPartitionsStatus([c],"ready")
         else: # if all dependencies are on "bad", mark this as bad as well
            logger.warning("All input files of partition %i in %s are marked as 'bad' or 'ignored'. This partition is marked as 'bad' as well.", partition, self.fqn())
            self.setPartitionsStatus([c],"bad")

## Information methods
   def info(self):
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      skipstring = ""
      if self.inputdata and self.inputdata.skip_files > 0:
         skipstring += "skipping " + say(self.inputdata.skip_files,"file")
         if self.inputdata.skip_events > 0:
            skipstring += " and " + say(self.inputdata.skip_events,"event")
      elif self.inputdata and self.inputdata.skip_events > 0:
         skipstring += "skipping " + say(self.inputdata.skip_events,"event")
      elif not self.inputdata:
         skipstring += "skipping " + say(self.application.firstevent-1,"event")

      print "* %s per job" % say(self.application.number_events_job,"event")
      if skipstring != "":
         print "* " + skipstring
      print "* backend: %s" % self.backend.__class__.__name__
      print "* application settings:"
      for setting in settings[self.__class__.__name__]:
         print "  - %20s: %s" % (setting, self.application._data[setting])

   def overview(self):
      if self.inputdata and self._getParent() and self._getParent().transforms.index(self) == 0:
         if not "turls" in self.inputdata.__dict__:
            self.inputdata.get_dataset(self.application, self.backend._name)
         # compare MCTask.py check() function
         partitions = self.inputdata.filesToNumbers(self.inputdata.turls.keys())
         partitions.sort()
         o = markup("Inputdataset '%s' of %s '%s':\n" % (self.inputdata.DQ2dataset, self.__class__.__name__, self.name), status_colours["running"])
         for p in range(partitions[0], partitions[-1]+1):
            if p in partitions:
               o += markup("%i " % p, overview_colours["completed"])
            else:
               o += markup("%i " % p, overview_colours["hold"])
            if p % 20 == 0: o+="\n"
         print o
      super(MCTransform, self).overview()

class EvgenTransform(MCTransform):
   _schema = Schema(Version(1,0), dict(MCTransform._schema.datadict.items() + {}.items()))
   _category = 'transforms'
   _name = 'EvgenTransform'
   _exportmethods = MCTransform._exportmethods

   def initialize(self):
      super(EvgenTransform, self).initialize()
      self.name = "Evgen"
      self.application.mode = "evgen"
      self.application.number_events_job = 10000
      self.file_type = "evgen.EVNT"

class SimulTransform(MCTransform):
   _schema = Schema(Version(1,0), dict(MCTransform._schema.datadict.items() + {}.items()))
   _category = 'transforms'
   _name = 'SimulTransform'
   _exportmethods = MCTransform._exportmethods

   def initialize(self):
      super(SimulTransform, self).initialize()
      self.name = "Simul"
      self.application.number_events_job = 50
      self.application.mode = "simul"
      self.file_type = "simul.RDO"
      self.inputdata = AthenaMCInputDatasets()
      self.inputdata.use_partition_numbers = True

   def getJobsForPartitions(self, partitions):
      jl = super(SimulTransform, self).getJobsForPartitions(partitions)
      return jl

class ReconTransform(MCTransform):
   _schema = Schema(Version(1,0), dict(MCTransform._schema.datadict.items() + {}.items()))
   _category = 'transforms'
   _name = 'ReconTransform'
   _exportmethods = MCTransform._exportmethods

   def initialize(self):
      super(ReconTransform, self).initialize()
      self.name = "Recon"
      self.application.number_events_job = 1000
      self.application.mode = "recon"
      self.file_type = "recon.AOD"
      self.inputdata = AthenaMCInputDatasets()
      self.inputdata.use_partition_numbers = True

