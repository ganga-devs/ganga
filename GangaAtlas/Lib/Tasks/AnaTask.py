from common import *
from Task import Task
from AnaTransform import AnaTransform

from Ganga.Core.exceptions import ApplicationConfigurationError


o = [""]
def c(s):
   return markup(s,fgcol("blue"))
o.append(markup(" *** Task for Athena Analysis ***", fgcol("blue")))
o.append(" Analysis Application     : "+c("t.analysis.application"))
o.append(" Set Dataset              : "+c("t.setDataset('my.dataset')"))
o.append(" Input Dataset Object     : "+c("t.analysis.inputdata"))
o.append(" Output Dataset Object    : "+c("t.analysis.outputdata"))
o.append(" Files processed per job  : "+c("t.analysis.files_per_job = 10"))
o.append("")
o.append(markup("Procedure to do a usual analysis:", fgcol("red")))
#o.append("config.Tasks.merged_files_per_job = 1 # default files per job for merged datasets")
#o.append("config.Tasks.recon_files_per_job = 10 # default files per job for recon (non-merged) datasets")
o.append("t = AnaTask()")
o.append('t.name = "MyAnalysisR1"')
o.append("t.analysis.outputdata.outputdata  = ['nTuple.root' ]")
o.append("t.analysis.application.option_file = ['./myTopOptions.py' ]")
o.append("t.analysis.application.prepare()")
o.append("t.float = 10")
o.append('t.initializeFromDatasets(["user08.MyName.ganga.dataset.recon.AOD"])')
o.append("#t.info() # Check here if settings are correct")
o.append("t.run()")
o.append("t.overview() # Watch the processing")
o.append("")
o.append("A container dataset with all outputs will be created as")
o.append(c("user.YourNickname.<YYYYMMDD>.task_<id>.<task name>/"))
o.append("Subtask (Transform) outputs will be put into these datasets:")
o.append(c("user.YourNickname.<YYYYMMDD>.task_<id>.subtask_<nr>.<inputdataset name>"))
task_help = "\n".join(o)

from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException
dq2=DQ2()

config.addOption('merged_files_per_job',1,'OBSOLETE', type=int)
config.addOption('recon_files_per_job',10,'OBSOLETE', type=int)

class AnaTask(Task):
   __doc__ = task_help
   _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + {
        'analysis': SimpleItem(defvalue=None, transient=1, typelist=["object"], doc='Analysis Transform'),
       }.items()))
   _category = 'tasks'
   _name = 'AnaTask'
   _exportmethods = Task._exportmethods + ["initializeFromDatasets"]

   def initialize(self):
      super(AnaTask, self).initialize()
      analysis = AnaTransform()
      analysis.exclude_from_user_area=["*.root*"]
      analysis.name = "Analysis"
      self.transforms = [analysis]
      self.setBackend(None)

   def initializeFromDatasets(self,dataset_list):
      """ For each dataset in the dataset_list a transform is created. 
          The output dataset names are set using the run numbers and tags of the input datasets appended to the current t.analysis.outputdata.datasetname field."""
      if not type(dataset_list) is list:
         logger.error("dataset_list must be a python list: ['ds1','ds2',...]")
         return
      if self.analysis:
         if self.analysis.application.user_area.name:
            trf = stripProxy(self.analysis)
         else:
            logger.error("Cannot find user_area in Athena application of first transform! Have you run application.prepare()?")
            return
      else:
         logger.error("Could not find 'template' analysis transform! Create a new AnaTask and configure t.analysis, then try again.")
         return
      tid_list = []
      for dset in dataset_list:
         dset = dset.strip()
         try:
            if "*" in dset:
               logger.error("WARNING: Wildcards may include unexpected datasets in your processing! Please list your datasets before specifying them here!")
            try:
               if dset[-1] == "/":
                  tid_datasets = dq2.listDatasetsInContainer(dset)
               else:
                  tid_datasets = dq2.listDatasetsInContainer(dset+"/")
            except DQUnknownDatasetException:
               dslist = dq2.listDatasets(dset).keys()
               if len(dslist) == 0:
                  logger.error("Dataset %s not found!" % dset)
                  return
               tid_datasets = [ds for ds in dslist if "_tid" in ds and not "_sub" in ds]
               if len(tid_datasets) == 0:
                  if len(dslist) > 1:
                     logger.error("Found no tid dataset but multiple datasets match %s*!" % dset)
                     return
                  tid_datasets = [dslist[0]]
         except Exception, e:
            logger.error('DQ2 Error while listing dataset %s*! %s' % (dset, e))
            return
         logger.info("Found %i datasets matching %s..." % (len(tid_datasets), dset))
         tid_list.extend(tid_datasets)

      new_tfs = []

      for tid in tid_list:
         tf = trf.clone()
         tf.name = ".".join(tid.split(".")[1:3])

         #if "merge" in tid:
         #   tf.files_per_job = config["merged_files_per_job"]
         #   logger.warning("Files per job for %s set to %i - use 'config.Tasks.merged_files_per_job = 42' to change this value!" % (tid,tf.files_per_job))
         #else:
         #   tf.files_per_job = config["recon_files_per_job"]
         #   logger.warning("Files per job for %s set to %i - use 'config.Tasks.recon_files_per_job = 42' to change this value!" % (tid,tf.files_per_job))
         tf.inputdata.dataset=tid
         new_tfs.append(tf)
      self.transforms = new_tfs
      self.initAliases()

   def startup(self):
      super(AnaTask,self).startup()
      self.initAliases()

   def check(self):
      self.initAliases()
      if not all(ss.isalnum() for ss in self.name.split(".")):
         logger.error("Invalid character in task name! Task names are now used for DQ2 datasets; so no spaces, slashes or other special characters are allowed.")
         raise ApplicationConfigurationError("Invalid Task name!")
      super(AnaTask,self).check()

   def initAliases(self):
      self.analysis = None
      if len(self.transforms) == 1:
         self.analysis = self.transforms[0]

   def help(self):
      print task_help

