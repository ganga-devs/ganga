from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset
from GangaCore.GPIDev.Lib.Tasks.common import *
from GangaCore.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaCore.GPIDev.Lib.Job.Job import JobError
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaCore.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy
from GangaAtlas.Lib.Tasks.AtlasUnit import AtlasUnit
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.ATLASDataset.ATLASDataset import ATLASLocalDataset, ATLASOutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2
from dq2.common.DQException import DQException
from GangaCore.GPIDev.Schema import *
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy
import GangaCore.GPI as GPI
import os

from GangaCore.Utility.Config import getConfig
configDQ2 = getConfig('DQ2')

from GangaCore.Utility.logging import getLogger
logger = getLogger()

class AtlasTransform(ITransform):
   _schema = Schema(Version(1,0), dict(ITransform._schema.datadict.items() + {
      'local_location'     : SimpleItem(defvalue='', doc='Local location to copy output to', typelist=["str"]),
      'include_file_mask'       : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc = 'List of Regular expressions of which files to include in copy'),
      'exclude_file_mask'       : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc = 'List of Regular expressions of which files to exclude from copy'),
      'files_per_job'     : SimpleItem(defvalue=-1, doc='files per job (cf DQ2JobSplitter.numfiles)', modelist=["int"]),
      'MB_per_job'     : SimpleItem(defvalue=0, doc='Split by total input filesize (cf DQ2JobSplitter.filesize)', modelist=["int"]),
      'subjobs_per_unit'     : SimpleItem(defvalue=0, doc='split into this many subjobs per unit master job (cf DQ2JobSplitter.numsubjobs)', modelist=["int"]),
      'rebroker_fraction'    : SimpleItem(defvalue=0.6, doc='Fraction of failed subjobs to complete subjobs above which the job will be rebrokered', modelist=["float"]),
      'num_dq2_threads'     : SimpleItem(defvalue=1, copyable=1, doc='Number of DQ2 download threads to run simultaneously (use setNumDQ2Threads to modify after submission)', typelist=["int"]),
      'files_per_unit' : SimpleItem(defvalue=-1, copyable=1, doc='Maximum number of files to assign to each unit from the given local files (i.e. AtlasLocalDataset). If < 1, use all files. At present, does not apply to DQ2Datasets', typelist=["int"]),
    }.items()))

   _category = 'transforms'
   _name = 'AtlasTransform'
   _exportmethods = ITransform._exportmethods + [ 'addUnit', 'getContainerName', 'initializeFromContainer', 'initializeFromDatasets', 'checkOutputContainers', 'setNumDQ2Threads', 'checkInputDatasets', 'changeDownloadLocation' ]

   def __init__(self):
      super(AtlasTransform,self).__init__()

      # force a delay of 1 minute to ensure DQ2 datasets have been registered properly
      self.chain_delay = 5

   def setNumDQ2Threads(self, num_threads):
      """Set the number of threads"""
      self.num_dq2_threads = num_threads
      
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

   def checkOutputContainers(self):
      """Go through all completed units and make sure datasets are registered as required"""
      logger.info("Cleaning out transform %d container..." % self.getID())

      try:
         dslist = []
         dq2_lock.acquire()
         try:
            dslist = dq2.listDatasetsInContainer(self.getContainerName())
         except:
            dslist = []

         try:
            dq2.deleteDatasetsFromContainer(self.getContainerName(), dslist )

         except DQContainerDoesNotHaveDataset:
            pass
         except Exception as x:
            logger.error("Problem cleaning out Transform container: %s %s", x.__class__, x)
         except DQException as x:
            logger.error('DQ2 Problem cleaning out Transform container: %s %s' %( x.__class__, x))
      finally:
         dq2_lock.release()

      logger.info("Checking output data has been registered for Transform %d..." % self.getID())
      for unit in self.units:
         
         if len(unit.active_job_ids) == 0:
            continue

         if unit.status == "completed" and GPI.jobs(unit.active_job_ids[0]).outputdata and GPI.jobs(unit.active_job_ids[0]).outputdata._impl._name == "DQ2OutputDataset":
            logger.info("Checking containers in Unit %d..." % unit.getID() )
            unit.registerDataset()            

   def createUnits(self):
      """Create new units if required given the inputdata"""
      
      # call parent for chaining
      super(AtlasTransform,self).createUnits()
      
      # if there is no input data, just create a single unit given the application
      if len(self.inputdata) == 0 and len(self.units) == 0:
         unit = AtlasUnit()
         unit.name = "Unit %d" % len(self.units)
         self.addUnitToTRF( unit )
      
      # loop over input data and see if we need to create any more units
      for inds in self.inputdata:
         
         ok = True

         if inds._name == "DQ2Dataset":
            # check if this data is being run over
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

         elif inds._name == "ATLASLocalDataset":

            # different behaviour depending on files_per_unit
            if self.files_per_unit < 0:
               # check if this data is being run over
               ok = False
               for unit in self.units:
                  if set(unit.inputdata.names) == set(inds.names):
                     ok = True
                     
               if not ok:
                  # new unit required for this dataset
                  unit = AtlasUnit()
                  unit.name = "Unit %d" % len(self.units)
                  self.addUnitToTRF( unit )
                  unit.inputdata = inds
            
            else:

               ok = False
               curr_data = []
               for unit in self.units:
                  curr_data.extend( unit.inputdata.names )

               if set(inds.names) in set( curr_data ) or set(inds.names) == set( curr_data ):
                  ok = True

               if not ok:
                  # new unit(s) required for this dataset
                  num = 0
                  while num < len( inds.names ):
                     unit = AtlasUnit()
                     unit.name = "Unit %d" % len(self.units)
                     self.addUnitToTRF( unit )
                     unit.inputdata = inds.clone()
                     unit.inputdata.names = inds.names[num:num + self.files_per_unit]
                     num += self.files_per_unit


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

   def getContainerName(self, max_length = configDQ2['OUTPUTDATASET_NAMELENGTH'] - 2):
      """Return the container for this transform"""
      if self.name == "":
         name = "trf"
      else:
         name = self.name
         
      dsn = [self._getParent().getContainerName()[:-1], name, "%i/" % self.getID() ] 
      if len(".".join(dsn)) > max_length:
         # too big so force a reduction of Task Name and compress trf name
         dsn2 = [self._getParent().getContainerName(max_length / 2)[:-1], "", "%i/" % self.getID() ]
         dsn =  [self._getParent().getContainerName(max_length / 2)[:-1],
                 name[: - (len(".".join(dsn2)) - max_length) ], "%i/" % self.getID() ]

      return (".".join(dsn)).replace(":", "_").replace(" ", "").replace(",","_")

   def createChainUnit( self, parent_units, use_copy_output = True ):
      """Create an output unit given this output data"""
      
      # we need valid parent jobs
      for parent in parent_units:
         # need datasetname filled for Panda jobs
         if len(parent.active_job_ids) == 0 or \
                (GPI.jobs(parent.active_job_ids[0]).application._impl._name != "TagPrepare" and \
                 GPI.jobs(parent.active_job_ids[0]).outputdata and \
                 GPI.jobs(parent.active_job_ids[0]).backend._impl._name == "Panda" and \
                 GPI.jobs(parent.active_job_ids[0]).outputdata.datasetname == ""):
            return None

         # need datasetList filled for Jedi jobs
         if len(parent.active_job_ids) == 0 or \
                (GPI.jobs(parent.active_job_ids[0]).application._impl._name != "TagPrepare" and \
                 GPI.jobs(parent.active_job_ids[0]).outputdata and \
                 GPI.jobs(parent.active_job_ids[0]).backend._impl._name == "Jedi" and \
                 len(GPI.jobs(parent.active_job_ids[0]).outputdata.datasetList) == 0):
            return None

         # for local jobs, make sure units are complete
         if GPI.jobs(parent_units[0].active_job_ids[0]).outputdata._impl._name == "ATLASOutputDataset" and \
                parent.status != "completed":
            return None
                

      # Are we doing Local -> Local? i.e. are we going from ATLASOutputDataset?
      # Problem: Doesn't take into account merger locations...
      if GPI.jobs(parent_units[0].active_job_ids[0]).outputdata._impl._name == "ATLASOutputDataset":
         unit = AtlasUnit()
         unit.inputdata = ATLASLocalDataset()

         for parent in parent_units:
            for l in GPI.jobs(parent.active_job_ids[0]).outputdata.output:
               unit.inputdata.names += l
            
      # should we use the copy_output (ie. local output). Special case for TagPrepare
      elif GPI.jobs(parent_units[0].active_job_ids[0]).application._impl._name == "TagPrepare":
         
         # make sure all have completed before taking the tag-info
         if parent_units[0].status != "completed":
            return None
         
         unit = AtlasUnit()
         unit.inputdata = DQ2Dataset()
         unit.inputdata.tag_info = GPI.jobs(parent_units[0].active_job_ids[0]).application.tag_info
         
      elif not use_copy_output or not parent.copy_output:
         unit = AtlasUnit()
         unit.inputdata = DQ2Dataset()
         ds_list = []
         for parent in parent_units:
            
            # Don't just use the main datasetname as Jedi introduces separate containers for logs and output files
            if GPI.jobs(parent.active_job_ids[0]).backend._impl._name == "Jedi":
               for ds in GPI.jobs(parent.active_job_ids[0]).outputdata.datasetList:
                  if not ds.endswith(".log/"):
                     unit.inputdata.dataset.append( ds )
            else:
               unit.inputdata.dataset.append( GPI.jobs(parent.active_job_ids[0]).outputdata.datasetname )
         
      else:

         unit = AtlasUnit()
         unit.inputdata = ATLASLocalDataset()

         for parent in parent_units:
            # unit needs to have completed and downloaded
            if parent.status != "completed":
               return None

            # we should be OK so copy all output to an ATLASLocalDataset
            for f in parent.copy_output.files:
               unit.inputdata.names.append( os.path.join( parent.copy_output.local_location, f ) )
         
      return unit
   
   def initializeFromContainer(self, dset, template = None, using_jedi = True):
      """Initialise the trf with given container, creating a unit for each DS"""
      if dset[-1] != "/":
         logger.error("Please supply a container!")
         return
      
      if not using_jedi:
         try:
            tid_datasets = dq2.listDatasetsInContainer(dset)
         except DQUnknownDatasetException:
            logger.error("dataset container %s not found" % dset)
            return
         
         logger.info("Found %i datasets matching %s..." % (len(tid_datasets), dset))
         
         for ds in tid_datasets:
            self.addUnit('.'.join( ds.split(".")[1:-1] ), ds, template)
      else:
         self.addUnit('.'.join( dset[:-1].split(".")[1:] ), dset, template)


   def initializeFromDatasets(self, dset_list, template = None, using_jedi = True):
      """Initialise the trf with the given dataset list, creating a unit for each DS"""

      for ds in dset_list:
         
         if ds[-1] == "/":
            if not using_jedi:
               try:
                  tid_datasets = dq2.listDatasetsInContainer(ds)
               except DQUnknownDatasetException:
                  logger.error("dataset container %s not found" % ds)
         
               logger.info("Found %i datasets matching %s..." % (len(tid_datasets), ds))
         
               for ds2 in tid_datasets:
                  self.addUnit('.'.join( ds.split(".")[1:-1] ), ds2, template)
            else:
               self.addUnit('.'.join( ds[:-1].split(".")[1:] ), ds, template)
         else:
            self.addUnit('.'.join( ds.split(".")[1:-1] ), ds, template)
            
   def checkInputDatasets(self):
      """Check the distribution of the input datasets"""
      if self.backend._name != "Panda" and self.backend._name != "Jedi":
         return

      for unit in self.units:
         print "Checking %s..." % ','.join( unit.inputdata.dataset )
         loc = unit.inputdata.get_locations()
         non_tape = []
         ok_sites = []
         for s in loc:
            if s.find("TAPE") != -1:
               continue
            non_tape.append(s)

            if s in self.backend.requirements.excluded_sites:
               continue               
            ok_sites.append(s)

         # non tape sites
         if len(non_tape) == 0:
            print "ERROR: No non-tape site available for DS '%s'" % unit.inputdata.dataset
         elif len(non_tape) == 1:
            print "WARNING: Only one non-tape site available for DS '%s'" % unit.inputdata.dataset

         # excluded sites
         if len(ok_sites) == 0:
            print "ERROR: No non-excluded sites available for DS '%s'" % unit.inputdata.dataset
         elif len(ok_sites) == 1:
            print "WARNING: Only one non-excluded site available for DS '%s'" % unit.inputdata.dataset

         # finally check panda brokerage
         from GangaPanda.Lib.Panda import selectPandaSite
         site = "NONE"
         try:
            site = selectPandaSite( self, ':'.join( ok_sites ) )
         except:
            pass

         if site == "NONE":
            print "ERROR: No non-blcklisted, non-Tape, non-excluded sites available for DS '%s'" % unit.inputdata.dataset
              
   def changeDownloadLocation(self, new_location, move_files = True):
      """Change the local download area"""
      import os

      # loop over the units
      self.local_location = new_location
      for unit in self.units:

         # do we have a valid local download?
         if not unit.copy_output or unit.copy_output._name != "TaskLocalCopy":
            logger.error("No locel copy available for unit %d" % unit.getID())
            continue
         
         if unit.copy_output.local_location == new_location:
            continue

         # grab the download lock
         if not unit._acquireDownloadLock():
            logger.error("Could not get download lock after 10s for unit %d - maybe try calling the function again?" % unit.getID())
            continue

         # move any files that have already been copied
         if move_files:
            import shutil
            os.makedirs(new_location)
            for fname in unit.copy_output.files:
               old_path = os.path.join(unit.copy_output.local_location, fname)
               new_path = os.path.join(new_location, fname)
               shutil.move(old_path, new_path)
         else:
            unit.copy_output.files = []

         # change the location
         unit.copy_output.local_location = new_location
         
         # set unit to running so the download is run again
         if unit.status == "completed":
            unit.updateStatus("running")
            
         if self.status == "completed":
            self.updateStatus("running")

         # release download lock
         unit._releaseDownloadLock()

