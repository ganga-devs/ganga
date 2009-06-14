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
o+="\n"+ 't.initializeFromDataset("user08.MyName.ganga.dataset.recon.AOD")'
o+="\n"+ "t.analysis.outputdata.outputdata  = ['nTuple.root' ]"
o+="\n"+ 't.analysis.application.exclude_from_user_area=["*.o","*.root*","*.exe", "*.txt"]'
o+="\n"+ "t.analysis.application.option_file = ['./myTopOptions.py' ]"
o+="\n"+ "t.analysis.application.prepare()"
o+="\n"+ "t.float = 10"
o+="\n"+ "t.info() # Check here if settings are correct"
o+="\n"+ "t.run()"
o+="\n"+ "t.overview() # Watch the processing"
task_help = o

from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException
dq2=DQ2()

config.addOption('merged_files_per_job',1,'default number of files per job in AnaTask if using merged datasets', type=int)
config.addOption('recon_files_per_job',10,'default number of files per job in AnaTask if using recon datasets', type=int)

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
            if not trf.outputdata.datasetname or "." in trf.outputdata.datasetname:
               logger.error("You have to set t.analysis.outputdata.datasetname to the identifying name of your analysis, for example 'MyDPDProject01'. There should be no dots in the name.")
               return
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
               logger.error("No wildcards are allowed in dataset specification! Please list your datasets before specifying them here!")
            try:
               if dset[-1] == "/":
                  tid_datasets = dq2.listDatasetsInContainer(dset)
               else:
                  tid_datasets = dq2.listDatasetsInContainer(dset+"/")
            except DQUnknownDatasetException:
               dslist = dq2.listDatasets(dset+"*").keys()
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
         if "merge" in tid:
            tf.files_per_job = config["merged_files_per_job"]
         else:
            tf.files_per_job = config["recon_files_per_job"]
         tf.inputdata.dataset=tid
         stid = tid.split(".")
         if len(stid) > 1:
            tf.outputdata.datasetname = ".".join([tf.outputdata.datasetname,stid[1],"NTUP",stid[-1]])
         else:
            tf.outputdata.datasetname = ".".join([tf.outputdata.datasetname,tid,"NTUP"])

         logger.warning("Saving data from %s into %s" % (tid, tf.outputdata.datasetname))
         new_tfs.append(tf)
      self.transforms = new_tfs
      self.initAliases()

   def startup(self):
      super(AnaTask,self).startup()
      self.initAliases()

   def check(self):
      self.initAliases()
      super(AnaTask,self).check()

   def initAliases(self):
      self.analysis = None
      if len(self.transforms) == 1:
         self.analysis = self.transforms[0]

   def help(self):
      print task_help

