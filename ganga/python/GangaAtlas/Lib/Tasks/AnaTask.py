from common import *
from Task import Task
from AnaTransform import AnaTransform

o = ""
o+="\n"+ markup(" *** Task for Athena Analysis ***", fgcol("blue"))
def c(s):
   return markup(s,fgcol("blue"))
o+="\n"+ " Analysis Application     : "+c("t.analysis.application")
o+="\n"+ " Set Dataset              : "+c("t.setDataset('my.dataset')")
o+="\n"+ " Input Dataset Object     : "+c("t.analysis.inputdata")
o+="\n"+ " Output Dataset Object    : "+c("t.analysis.outputdata")
o+="\n"+ " Files processed per job  : "+c("t.analysis.files_per_job = 10")
o+="\n"
o+="\n"+ markup("Procedure to do a usual analysis:", fgcol("red"))
o+="\n"+ "t = AnaTask()"
o+="\n"+ 't.name = "FirstAnalysis"'
o+="\n"+ 't.setDataset("user08.MyName.ganga.dataset.recon.AOD.root")'
o+="\n"+ "t.analysis.outputdata.outputdata  = ['nTuple.root' ]"
o+="\n"+ 't.analysis.application.exclude_from_user_area=["*.o","*.root*","*.exe", "*.txt"]'
o+="\n"+ "t.analysis.application.prepare(athena_compile=False)"
o+="\n"+ "t.analysis.application.option_file = ['./myTopOptions.py' ]"
o+="\n"+ "t.float = 10"
o+="\n"+ "t.info() # Check here if settings are correct"
o+="\n"+ "t.run()"
o+="\n"+ "t.overview() # Watch the processing"
task_help = o

class AnaTask(Task):
   __doc__ = task_help
   _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + {
        'analysis': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Analysis Transform'),
       }.items()))
   _category = 'tasks'
   _name = 'AnaTask'
   _exportmethods = Task._exportmethods + ["setDataset"]

   def initialize(self):
      super(AnaTask, self).initialize()
      analysis = AnaTransform()
      analysis.exclude_from_user_area=["*.root*"]
      analysis.name = "Analysis"
      self.transforms = [analysis]
      self.setBackend(GPI.LCG())

   def startup(self):
      super(AnaTask,self).startup()
      self.initAliases()

   def check(self):
      self.initAliases()
      super(AnaTask,self).check()

   def initAliases(self):
      self.analysis = None
      for tf in self.transforms:
         if "Ana" in tf.__class__.__name__:
            self.analysis = tf

   def help(self):
      print task_help

   def setDataset(self,ds):
      self.analysis.inputdata.dataset = ds
      self.check() 

