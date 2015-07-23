from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Task
from MCTransforms import EvgenTransform, SimulTransform, ReconTransform
from GangaAtlas.Lib.AthenaMC.AthenaMCDatasets import AthenaMCInputDatasets
from Ganga.GPIDev.Schema import *

from Ganga.GPIDev.Base.Objects import Node

o = ""
o+="\n"+ markup(" *** Task for Athena Monte Carlo production ***", fgcol("blue"))
def c(s):
   return markup(s,fgcol("blue"))
o+="\n"+ " Set the total number of events to be processed: "+c("t.total_events = 10000")
o+="\n"+ " Applications for evgen/simul/recon            : "+c("t.evgen.application, t.simul.application, t.recon.application")
o+="\n"+ " Events processed per evgen/simul/recon        : "+c("t.evgen.application.number_events_job = 10000")
o+="\n"+ " Skip events / files from the previous dataset : "+c("t.simul.inputdata.skip_events = 1000")
o+="\n"+ " Use an input dataset for the event generator  : "+c("t.initializeFromGenerator(dataset, events_per_file_in_dataset)")
o+="\n"+ " Use an existing evgen EVNT dataset            : "+c("t.initializeFromEvgen(dataset, events_per_file_in_dataset)")
o+="\n"+ " Use an existing simul RDO dataset             : "+c("t.initializeFromSimul(dataset, events_per_file_in_dataset)")
o+="\n"
o+="\n"+ markup("Procedure to do a usual production:", fgcol("red"))
o+="\n"+ "t = MCTask()"
o+="\n"+ 't.name = "MyFirstTask"'
o+="\n"+ 't.evgen.application.evgen_job_option = "CSC.005145.PythiaZmumu.py" # or "path/to/my_evgen_job_option.py"'
o+="\n"+ 't.evgen.application.transform_script = "csc_evgen08_trf.py"'
o+="\n"+ "t.total_events = 1000"
o+="\n"+ 't.setParameter(run_number="5145", production_name="MyProd-01", process_name="PythiaZmumu")'
o+="\n"+ 't.setParameter(atlas_release="14.2.24.1", se_name="FZK-LCG2_SCRATCHDISK")'
o+="\n"+ 't.setParameter(triggerConfig="DEFAULT", geometryTag="ATLAS-GEO-05-00-00")'
o+="\n"+ "t.float = 10"
o+="\n"+ "t.info() # Check here if settings are correct"
o+="\n"+ "t.run()"
o+="\n"+ "t.overview() # Watch the processing"
mctask_help = o
mctask_help_nocolor = mctask_help.replace(fgcol("blue"),"").replace(fx.normal, "").replace(fgcol("red"),"")

class MCTask(Task):
   __doc__ = mctask_help_nocolor
   _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + {
        'total_events': SimpleItem(defvalue=1000, doc="Total number of events to generate", typelist=["int"]),
        'evgen': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Evgen Transform'),
        'simul': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Simul Transform'),
        'recon': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Recon Transform'),
       }.items()))
   _category = 'tasks'
   _name = 'MCTask'
   _exportmethods = Task._exportmethods + [
                    'initializeFromGenerator', 'initializeFromEvgen', 'initializeFromSimul',
                    ]

## Special methods
   def initialize(self):
      super(MCTask,self).initialize()
      evgen = EvgenTransform()
      simul = SimulTransform()
      recon = ReconTransform()
      self.transforms = [evgen,simul,recon]
      self.setBackend(GPI.LCG())

   def startup(self):
      super(MCTask,self).startup()
      self.initAliases()

   def check(self):
      self.initAliases()
      # Set the status in reverse order so the propagation works correctly
      # the minus signs are used to get a correct rounding behaviour
      for i in range(1,len(self.transforms)):
         if self.transforms[i].inputdata:
            self.transforms[i].inputdata.use_partition_numbers = True
            self.transforms[i].inputdata.number_events_file = self.transforms[i-1].application.number_events_job
      for i in range(len(self.transforms)-1,-1,-1):
         tf = self.transforms[i]
         lastpartition = -((-self.total_events)/tf.application.number_events_job)
         tf.setPartitionsLimit(lastpartition+1)
         tf.setPartitionsStatus([c for c in range(1,lastpartition+1) if tf.getPartitionStatus(c) == "ignored"], "ready")
      # if the first transformation has an input dataset, check which partitions are ready
      if len(self.transforms) > 0 and self.transforms[0].inputdata:
         tf = self.transforms[0]
         tf.inputdata.get_dataset(tf.application, tf.backend._name)
         inputnumbers = tf.inputdata.filesToNumbers(tf.inputdata.turls.keys())
         partitions = tf.application.getPartitionsForInputs(inputnumbers, tf.inputdata)
         tf.setPartitionsStatus([c for c in range(1,lastpartition+1) if c in partitions], "ready")
         tf.setPartitionsStatus([c for c in range(1,lastpartition+1) if not c in partitions], "hold")
      super(MCTask,self).check()

   def initAliases(self):
      self.evgen = None
      self.simul = None
      self.recon = None
      for tf in self.transforms:
         if "Recon" in tf.__class__.__name__:
            self.recon = tf
         elif "Simul" in tf.__class__.__name__:
            self.simul = tf
         elif "Evgen" in tf.__class__.__name__:
            self.evgen = tf

## Public methods
   def initializeFromGenerator(self,dataset,events_per_file):
      self.initialize()
      self.transforms[0].inputdata = AthenaMCInputDatasets()
      self.transforms[0].inputdata.use_partition_numbers = True
      self.transforms[0].inputdata.DQ2dataset = dataset
      self.transforms[0].inputdata.number_events_file = events_per_file
      self.check()

   def initializeFromEvgen(self,dataset,events_per_file):
      self.initialize()
      self.removeTransform(0) # remove evgen
      self.transforms[0].inputdata.use_partition_numbers = True
      self.transforms[0].inputdata.DQ2dataset = dataset
      self.transforms[0].inputdata.number_events_file = events_per_file
      self.check()

   def initializeFromSimul(self,dataset,events_per_file):
      self.initialize()
      self.removeTransform(0) # remove evgen
      self.removeTransform(0) # remove simul
      self.transforms[0].inputdata.use_partition_numbers = True
      self.transforms[0].inputdata.DQ2dataset = dataset
      self.transforms[0].inputdata.number_events_file = events_per_file
      self.check()

## Information methods
   def info(self):
      print "* total events: %i\n" % (self.total_events) 
      super(MCTask,self).info()

   def help(self):
      print mctask_help

