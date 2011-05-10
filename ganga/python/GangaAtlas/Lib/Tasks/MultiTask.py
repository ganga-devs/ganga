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
   _exportmethods = Task._exportmethods + ["initializeFromDatasets", "unitOverview", 'getTransform']
   
   def initialize(self):
      super(MultiTask, self).initialize()
      self.transforms = []

   def getTransform(self, trf):
      """Get transform using either index or name"""
      if isinstance(trf, str):
         for trfid in range(0, len(self.transforms)):
            if trf == self.transforms[trfid].name:
               return self.transforms[trfid]
         logger.warning("Couldn't find transform with name '%s'." % trf)
      elif isinstance(trf, int):
         if trf < 0 and trf > len(self.transforms):
            logger.warning("Transform number '%d' out of range" % trf)
         else:
            return self.transforms[trf]
      else:
         logger.warning('Incorrect type for transform referral. Allowed types are int or string.')

      return None
      
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
         if len(tf.required_trfs) == 0:
            primary_tfs.append( tf )

      if len(primary_tfs) == 0:
         logger.error("No primary transforms specified. Yout need at least one before the Task can be initialised.")
         return
      
      unit_num = 0
         
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

         prev_num = unit_num
         for tf in primary_tfs:
            unit_num = prev_num
            for ds in tid_datasets:
               tf.addUnit("Unit_%d" % unit_num, ds)
               unit_num += 1
               
   def startup(self):
      super(MultiTask,self).startup()

   def check(self):
      super(MultiTask,self).check()

   def help(self):
      print task_help

   def overview(self):
      super(MultiTask, self).overview()

   def unitOverview(self):
      """Show a overview of the units"""
      #print "Colours: " + ", ".join([markup(key, overview_colours[key])
      #                             for key in ["hold", "ready", "running", "completed", "attempted", "failed", "bad", "unknown"]])
      print "Lists the units for each partition and their current status"
      #print "Format: (partition number)[:(number of failed attempts)]"
      print
      print " "* 47 + "Active\tConfigured\tSubmitted\tDownload\tMerged\tReason"
      for trfid in range(0, len(self.transforms)):
         print "----------------------------------------------------------------------------------------------------------------------"
         print "----   Transform %d:  %s" % (trfid, self.transforms[trfid].name)
         print
         self.transforms[trfid].unit_overview()
         print
      
   def run(self):
      self.checkTRFConsistency()
      super(MultiTask, self).run()

   def checkTRFConsistency(self):
      """Check the consistency of the attached transforms"""
      for trf in self.transforms:

         # check primary trfs
         if len(trf.required_trfs) == 0:

            # ensure the number unit input data, output data and partition lists are setup properly
            if len(trf.unit_partition_list) == 0:
               raise ApplicationConfigurationError(None, "Transform %d (%s) is primary but hasn't been initialised with input data")

         else:

            if not trf.isLocalTRF():

               if trf.single_unit:
                  raise ApplicationConfigurationError(None, "Transform %d (%s) set as a single unit but has a grid backend")
               
               # this will just take the DQ2 output of the previous TRFs and process each unit itself
               trf.unit_inputdata_list = []
               trf.unit_outputdata_list = []
               trf.unit_partition_list = []
               
               for req_trf in trf.required_trfs:
                  trf.unit_inputdata_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ] 
                  trf.unit_outputdata_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                  trf.unit_partition_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]

            else:
               
               if trf.single_unit:
                  
                  # we only have one unit
                  trf.unit_inputdata_list = [[]]
                  trf.unit_outputdata_list = [[]]
                  trf.unit_partition_list = [[]]
               else:

                  # we have units dependant on mergers, etc.
                  trf.unit_inputdata_list = []
                  trf.unit_outputdata_list = []
                  trf.unit_partition_list = []
               
                  for req_trf in trf.required_trfs:
                     if not self.transforms[req_trf].merger:
                        trf.unit_inputdata_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ] 
                        trf.unit_outputdata_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                        trf.unit_partition_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                     else:
                        trf.unit_inputdata_list += [[]]
                        trf.unit_outputdata_list += [[]]
                        trf.unit_partition_list += [[]]
                     
         
