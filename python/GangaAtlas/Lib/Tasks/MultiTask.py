from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Task
from MultiTransform import MultiTransform

from Ganga.Core.exceptions import ApplicationConfigurationError

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 


o = [""]
def c(s):
   return markup(s,fgcol("blue"))
o.append(markup(" *** Task for Multiple Athena Analyses ***", fgcol("blue")))
#o.append(" Analysis Application     : "+c("t.analysis.application"))
#o.append(" Set Dataset              : "+c("t.setDataset('my.dataset')"))
#o.append(" Input Dataset Object     : "+c("t.analysis.inputdata"))
#o.append(" Output Dataset Object    : "+c("t.analysis.outputdata"))
#o.append(" Files processed per job  : "+c("t.analysis.files_per_job = 10"))
#o.append("")
#o.append(markup("Procedure to do a usual analysis:", fgcol("red")))
##o.append("config.Tasks.merged_files_per_job = 1 # default files per job for merged datasets")
##o.append("config.Tasks.recon_files_per_job = 10 # default files per job for recon (non-merged) datasets")
#o.append("t = AnaTask()")
#o.append('t.name = "MyAnalysisR1"')
#o.append("t.analysis.outputdata.outputdata  = ['nTuple.root' ]")
#o.append("t.analysis.application.option_file = ['./myTopOptions.py' ]")
#o.append("t.analysis.application.prepare()")
#o.append("t.float = 10")
#o.append('t.initializeFromDatasets(["user08.MyName.ganga.dataset.recon.AOD"])')
#o.append("#t.info() # Check here if settings are correct")
#o.append("t.run()")
#o.append("t.overview() # Watch the processing")
#o.append("")
#o.append("A container dataset with all outputs will be created as")
#o.append(c("user.YourNickname.<YYYYMMDDHHMMSS>.task_<id>.<task name>/"))
#o.append("Subtask (Transform) outputs will be put into these datasets:")
#o.append(c("user.YourNickname.<YYYYMMDDHHMMSS>.task_<id>.subtask_<nr>.<inputdataset name>"))
task_help = "\n".join(o)
task_help_nocolor = task_help.replace(fgcol("blue"),"").replace(fx.normal, "").replace(fgcol("red"),"")

from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException
dq2=DQ2()

#config.addOption('merged_files_per_job',1,'OBSOLETE', type=int)
#config.addOption('recon_files_per_job',10,'OBSOLETE', type=int)

class MultiTask(Task):
   __doc__ = task_help_nocolor
   _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + {
       }.items()))
   _category = 'tasks'
   _name = 'MultiTask'
   _exportmethods = Task._exportmethods + ["initializeFromDatasets"]
   
   def initialize(self):
      super(MultiTask, self).initialize()
      self.transforms = []

   def get_container_name(self):
      name_base = ["user",getNickname(),self.creation_date,"task_%s" % self.id]
      return ".".join(name_base + [self.name]) + "/"

   def initializeFromDatasets(self,dataset_list):
      """ For each dataset in the dataset_list a unit is created. 
          The output dataset names are set using the run numbers and tags of the input datasets appended to the current t.analysis.outputdata.datasetname field."""
      if not type(dataset_list) is list:
         logger.error("dataset_list must be a python list: ['ds1','ds2',...]")
         return

      # check for primary transforms
      primary_tfs = []
      for tf in self.transforms:
         if tf.required_trf == -1:
            primary_tfs.append( tf )

      if len(primary_tfs) == 0:
         logger.error("No primary transforms specified. Yout need at least one before the Task can be initialised.")
         return
         
      _tid_list = []
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

         for tf in primary_tfs:
            tf.tid_list.extend(tid_datasets)

         # set the units for each tf (1 unit per ds)
         for tf in self.transforms:
            for a in tid_datasets:
               tf.unit_partition_list.append( [] )
               
   def startup(self):
      super(MultiTask,self).startup()

   def check(self):
      super(MultiTask,self).check()

   def help(self):
      print task_help

   def overview(self):
      super(MultiTask, self).overview()
