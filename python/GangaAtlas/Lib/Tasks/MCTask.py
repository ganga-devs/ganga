from common import *
from Task import Task
from MCTransforms import EvgenTransform, SimulTransform, ReconTransform

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
o+="\n"+ "t.total_events = 1000"
o+="\n"+ 't.setParameter(run_number="5145", production_name="MyProd-01", process_name="PythiaZmumu")'
o+="\n"+ 't.setParameter(atlas_release="14.2.24.1", se_name="FZK-LCG2_USERDISK")'
o+="\n"+ 't.setParameter(triggerConfig="DEFAULT", geometryTag="ATLAS-GEO-05-00-00")'
o+="\n"+ "t.float = 10"
o+="\n"+ "t.info() # Check here if settings are correct"
o+="\n"+ "t.run()"
o+="\n"+ "t.overview() # Watch the processing"
mctask_help = o

class MCTask(Task):
   __doc__ = mctask_help
   _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + {
        'total_events': SimpleItem(defvalue=1000, doc="Total number of events to generate", checkset="dirty", typelist=["int"]),
        'evgen': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Evgen Transform'),
        'simul': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Simul Transform'),
        'recon': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Recon Transform'),
       }.items()))
   _category = 'tasks'
   _name = 'MCTask'
   _exportmethods = Task._exportmethods + [
                    'initializeFromGenerator', 'initializeFromEvgen', 'initializeFromSimul',
                    ]

   def initialize(self, fake=False):
      evgen = EvgenTransform()
      simul = SimulTransform()
      recon = ReconTransform()
      self.transforms = [evgen,simul,recon]
      self.fillESR()
      self.setBackend(GPI.LCG())
      self.dirty()

   def fillESR(self):
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

   def setup(self):
      self.fillESR()
      super(MCTask, self).setup()

   def update(self):
      # Set the status in reverse order so the propagation works correctly
      # the minus signs are used to get a correct rounding behaviour
      #for tf in self.transforms.__reversed__(): (Does not work!!)
      if not super(MCTask, self).update():
         return False
      self.fillESR()
      for i in range(1,len(self.transforms)):
         self.transforms[i].inputdata.number_events_file = self.transforms[i-1].application.number_events_job
      for i in range(len(self.transforms)-1,-1,-1):
         tf = self.transforms[i]
         lastpartition = -((-self.total_events)/tf.application.number_events_job)
         tf.setPartitionsLimit(lastpartition+1)
         tf.setPartitionsStatus([c for c in range(1,lastpartition+1) if tf.getPartitionStatus(c) == "ignored"], "ready")
      return True

   def initializeFromGenerator(self,dataset,events_per_file):
      self.initialize()

   def initializeFromEvgen(self,dataset,events_per_file):
      self.initialize()
      self.removeTransform(0) # remove evgen
      self.evgen = None

   def initializeFromSimul(self,dataset,events_per_file):
      self.initialize()
      self.removeTransform(0) # remove evgen
      self.removeTransform(0) # remove simul
      self.evgen = None
      self.simul = None

   def addDQ2Download(self,path):
      pass
 
   def info(self):
      print "* total events: %i\n" % (self.total_events) 
      super(MCTask,self).info()

   def help(self):
      print mctask_help

