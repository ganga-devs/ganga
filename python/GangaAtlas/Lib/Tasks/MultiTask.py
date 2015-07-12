from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset
from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Task
from MultiTransform import MultiTransform

from Ganga.Core.exceptions import ApplicationConfigurationError
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 
from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset
from dq2.common.DQException import DQException

import copy

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
dq2=DQ2(force_backend='rucio')

#config.addOption('merged_files_per_job',1,'OBSOLETE', type=int)
#config.addOption('recon_files_per_job',10,'OBSOLETE', type=int)

class MultiTask(Task):
   __doc__ = task_help_nocolor
   _schema = Schema(Version(1,1), dict(Task._schema.datadict.items() + {
       }.items()))
   _category = 'tasks'
   _name = 'MultiTask'
   _exportmethods = Task._exportmethods + ["initializeFromTagInfo", "initializeFromDatasets", "unitOverview", 'getTransform', 'getContainerName', 'listAllDatasets', 'restartTask', 'clearExcludedSites', 'activateAllUnits', 'setAllRunlimits', 'checkContainerContents']
   
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

   def restartTask(self):
      # loop over transforms and units and change status as required
      for trf in self.transforms:
         trf_complete = True
         if trf.status == "pause":
            continue
         
         for uind in range(0, len(trf.unit_partition_list)):
            if not trf.isUnitComplete(uind):
               trf_complete = False
               break
            
         if not trf_complete:
            trf.status = "running"
            self.status = "running"
            
   def getContainerName(self):
      if self.name == "":
         name = "task"
      else:
         name = self.name
         
      name_base = ["user",getNickname(),self.creation_date, name, "id_%i" % self.id ]
      
      return (".".join(name_base) + "/").replace(" ", "_")

   def listAllDatasets(self):
      "List all datasets in container of this transform"
      ds_list = []
      try:
         try:
            dq2_lock.acquire()
            ds_list = dq2.listDatasetsInContainer(self.getContainerName())
         except DQContainerDoesNotHaveDataset:
            pass
         except Exception as x:
            logger.error('Problem finding datasets associated with TRF container %s: %s %s' %( self.getContainerName(), x.__class__, x))
         except DQException as x:
            logger.error('DQ2 Problem finding datasets associated with TRF container %s: %s %s' %( self.getContainerName(), x.__class__, x))
      finally:
          dq2_lock.release()
          
      return ds_list

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
         except Exception as e:
            logger.error('DQ2 Error while listing dataset %s*! %s' % (dset, e))
            return
         logger.info("Found %i datasets matching %s..." % (len(tid_datasets), dset))

         if len(tid_datasets) == 0:
            logger.error("No tid datasets found from dataset list. Maybe the container '%s' is empty?" % dset)
            return
         
         prev_num = unit_num
         for tf in primary_tfs:
            unit_num = prev_num
            for ds in tid_datasets:
               tf.addUnit("Unit_%d" % unit_num, ds)
               unit_num += 1

   def initializeFromTagInfo(self,tag_info):
      """ For each dataset in the dataset_list a unit is created. 
          The output dataset names are set using the run numbers and tags of the input datasets appended to the current t.analysis.outputdata.datasetname field."""
      if not type(tag_info) is dict:
         logger.error("tag_info is not a dictionary")
         return

      # check for primary transforms
      primary_tfs = []
      for tf in self.transforms:
         if len(tf.required_trfs) == 0:
            primary_tfs.append( tf )

      if len(primary_tfs) == 0:
         logger.error("No primary transforms specified. Your need at least one before the Task can be initialised.")
         return

      for tf in primary_tfs:
         unit_num = 0      

         # find the number of units
         num_refs = 50
         for dsf in tag_info.keys():

            num_units = round( 0.5 + len(tag_info[dsf]['refs']) / num_refs)

            ref_num = 0
            for i in range(0, num_units):
               tf.addUnit("Unit_%d" % unit_num, ":".join([dsf, str(ref_num), str(ref_num + num_refs)]))
               unit_num += 1
               ref_num += num_refs

         # finally copy the tag info data
         tf.inputdata = DQ2Dataset()
         tf.inputdata.tag_info = copy.deepcopy(tag_info)
               
   def startup(self):
      super(MultiTask,self).startup()

   def check(self):
      super(MultiTask,self).check()

   def help(self):
      print task_help

   def overview(self):
      super(MultiTask, self).overview()

   def unitOverview(self, status = ''):
      """Show a overview of the units. Use the 'status' argument to only view units of a particular status. Options are: 'bad', 'hold', 'running', 'completed', 'ready'"""
      if status and not status in ['bad', 'hold', 'running', 'completed', 'ready']:
         logger.error("Not a valid status for unitOverview. Possible options are: 'bad', 'hold', 'running', 'completed', 'ready'.")
         return
      
      print "Colours: " + ", ".join([markup(key, overview_colours[key])
                                     for key in ["hold", "ready", "running", "completed", "bad"]])
      print "Lists the units for each partition and their current status"
      #print "Format: (partition number)[:(number of failed attempts)]"
      print
      print " "* 41 + "Active\tConf'd\tSub'd\tDwnl'd\tMerged\tComp.\tExcep.\tReason"
      for trfid in range(0, len(self.transforms)):
         print "----------------------------------------------------------------------------------------------------------------------"
         print "----   Transform %d:  %s" % (trfid, self.transforms[trfid].name)
         print
         self.transforms[trfid].unit_overview(status)
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
               raise ApplicationConfigurationError(None, "Transform %d (%s) is primary but hasn't been initialised with input data" % (trf.getID(), trf.name))

         else:

            if not trf.isLocalTRF():

               if trf.single_unit:
                  raise ApplicationConfigurationError(None, "Transform %d (%s) set as a single unit but has a grid backend" % (trf.getID(), trf.name))
               
               # this will just take the DQ2 output of the previous TRFs and process each unit itself
               trf.unit_inputdata_list = []
               trf.unit_outputdata_list = []
               trf.unit_state_list = []
               trf.unit_partition_list = []
               
               for req_trf in trf.required_trfs:
                  trf.unit_inputdata_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ] 
                  trf.unit_outputdata_list += [ self.transforms[req_trf].unit_outputdata_list[uind] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                  trf.unit_partition_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                  trf.unit_state_list += [ {'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force' : False} for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
            else:
               
               if trf.single_unit:
                  
                  # we only have one unit
                  trf.unit_inputdata_list = [[]]
                  trf.unit_outputdata_list = [[]]
                  trf.unit_partition_list = [[]]
                  trf.unit_state_list = [{'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force' : False} ]
               else:

                  # we have units dependant on mergers, etc.
                  trf.unit_inputdata_list = []
                  trf.unit_outputdata_list = []
                  trf.unit_partition_list = []
                  trf.unit_state_list = []
               
                  for req_trf in trf.required_trfs:
                     if not self.transforms[req_trf].merger:
                        trf.unit_inputdata_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ] 
                        trf.unit_outputdata_list += [ self.transforms[req_trf].unit_outputdata_list[uind] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                        trf.unit_partition_list += [ [] for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                        trf.unit_state_list += [ {'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force' : False} for uind in range(len(self.transforms[req_trf].unit_outputdata_list)) ]
                        
                     else:
                        trf.unit_inputdata_list += [[]]
                        trf.unit_outputdata_list += [[]]
                        trf.unit_partition_list += [[]]
                        trf.unit_state_list += [{'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force' : False} ]

   def clearExcludedSites(self):
      for trf in self.transforms:     
         if hasattr(trf.backend, 'requirements') and hasattr(trf.backend.requirements, 'excluded_sites'):
            trf.backend.requirements.excluded_sites = []  

   def activateAllUnits(self):
      for trf in self.transforms:
         if isinstance(trf,MultiTransform):
            for uind in xrange(0,trf.getNumUnits()):                                    
               trf.activateUnit(uind)   

   def setAllRunlimits(self, runLimit):
      for trf in self.transforms:
         if hasattr(trf, 'setRunlimit'):
            trf.setRunlimit(runLimit)           

   def checkContainerContents(self):
      for trf in self.transforms: 
         if isinstance(trf,MultiTransform):
            trf.checkContainerContents() 
         
