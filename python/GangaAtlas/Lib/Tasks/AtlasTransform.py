from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset
from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from sets import Set
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy
from GangaAtlas.Lib.Tasks.AtlasUnit import AtlasUnit
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2
from dq2.common.DQException import DQException

class AtlasTransform(ITransform):
   _schema = Schema(Version(1,0), dict(ITransform._schema.datadict.items() + {
      'local_location'     : SimpleItem(defvalue='', doc='Local location to copy output to', typelist=["str"]),
      'include_file_mask'       : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc = 'List of Regular expressions of which files to include in copy'),
      'exclude_file_mask'       : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc = 'List of Regular expressions of which files to exclude from copy'),
      'files_per_job'     : SimpleItem(defvalue=5, doc='files per job (cf DQ2JobSplitter.numfiles)', modelist=["int"]),
      'MB_per_job'     : SimpleItem(defvalue=0, doc='Split by total input filesize (cf DQ2JobSplitter.filesize)', modelist=["int"]),
      'subjobs_per_unit'     : SimpleItem(defvalue=0, doc='split into this many subjobs per unit master job (cf DQ2JobSplitter.numsubjobs)', modelist=["int"]),
    }.items()))

   _category = 'transforms'
   _name = 'AtlasTransform'
   _exportmethods = ITransform._exportmethods + [ 'addUnit', 'getContainerName', 'initializeFromContainer', 'initializeFromDatasets' ]

   def __init__(self):
      super(AtlasTransform,self).__init__()

      # force a delay of 1 minute to ensure DQ2 datasets have been registered properly
      self.chain_delay = 5

   def check(self):
      """Additional checks to base class"""

      # base class first
      super(AtlasTransform,self).check()
      
      # if a local copy has been specified, create the DSs required
      if self.local_location != '':
         self.unit_copy_output = TaskLocalCopy()
         self.unit_copy_output.local_location = self.local_location
         self.unit_copy_output.include_file_mask = self.include_file_mask
         self.unit_copy_output.exclude_file_mask = self.exclude_file_mask
      
   def createUnits(self):
      """Create new units if required given the inputdata"""
      
      # call parent for chaining
      super(AtlasTransform,self).createUnits()
      
      # loop over input data and see if we need to create any more units
      for inds in self.inputdata:

         if inds._name != "DQ2Dataset":
            continue
         
         ok = False
         for unit in self.units:
            if unit.inputdata.dataset == inds.dataset:
               ok = True

         if not ok:
            # new unit required for this dataset
            unit = AtlasUnit()
            unit.name = "Unit %d" % len(self.units)
            self.addUnitToTRF( unit )
            unit.inputdata = inds

   def addUnit(self, outname, dsname, template = None):
      """Create a new unit based on this ds and output"""
      unit = AtlasUnit()
      if not template:
         unit.inputdata = DQ2Dataset()
      else:
         unit.inputdata = stripProxy( template )
      unit.inputdata.dataset = dsname
      unit.name = outname
      self.addUnitToTRF( unit )

   def getContainerName(self):
      """Return the container for this transform"""
      if self.name == "":
         name = "trf"
      else:
         name = self.name
         
      return (self._getParent().getContainerName()[:-1] + ".%s.%i/" % (name, self.getID())).replace(" ", "_")

   def createChainUnit( self, parent ):
      """Create an output unit given this output data"""
      
      if len(parent.active_job_ids) == 0 or GPI.jobs(parent.active_job_ids[0]).outputdata.datasetname == "":
         return None
      
      unit = AtlasUnit()
      unit.inputdata = DQ2Dataset()
      unit.inputdata.dataset = GPI.jobs(parent.active_job_ids[0]).outputdata.datasetname
      return unit
   
   def initializeFromContainer(self, dset, template = None):
      """Initialise the trf with given container, creating a unit for each DS"""
      if dset[-1] != "/":
         logger.error("Please supply a container!")
         return
      
      try:
         tid_datasets = dq2.listDatasetsInContainer(dset)
      except DQUnknownDatasetException:
         logger.error("dataset container %s not found" % dset)
         return
         
      logger.info("Found %i datasets matching %s..." % (len(tid_datasets), dset))
         
      for ds in tid_datasets:
         self.addUnit('.'.join( ds.split(".")[1:-1] ), ds, template)


   def initializeFromDatasets(self, dset_list, template = None):
      """Initialise the trf with the given dataset list, creating a unit for each DS"""

      for ds in dset_list:
         
         if ds[-1] == "/":
            try:
               tid_datasets = dq2.listDatasetsInContainer(ds)
            except DQUnknownDatasetException:
               logger.error("dataset container %s not found" % ds)
         
            logger.info("Found %i datasets matching %s..." % (len(tid_datasets), ds))
         
            for ds2 in tid_datasets:
               self.addUnit('.'.join( ds.split(".")[1:-1] ), ds2, template)
         else:
            self.addUnit('.'.join( ds.split(".")[1:-1] ), ds, template)
            
