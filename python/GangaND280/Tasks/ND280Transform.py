from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy
from Ganga.GPIDev.Lib.File.MassStorageFile import MassStorageFile
from Ganga.Utility.Config import getConfig
from ND280Unit import ND280Unit
from GangaND280.ND280Dataset.ND280Dataset import ND280LocalDataset, ND280DCacheDataset
from GangaND280.ND280Splitter.ND280Splitter import splitNbInputFile
import Ganga.GPI as GPI

import os

class ND280Transform(ITransform):
   _schema = Schema(Version(1,0), dict(ITransform._schema.datadict.items() + {
        'nbinputfiles' : SimpleItem(defvalue=1,doc='The max number of files assigned to each unit. Use 0 to put all the available files in each given inputdata in one unit (i.e. N inputdata => N units).'),
        'inputdatasubsets' : SimpleItem(defvalue=[], hidden=1,doc='List of subsets of files. The number of files in each subset is equal to nbinputfiles'),
    }.items()))

   _category = 'transforms'
   _name = 'ND280Transform'
   _exportmethods = ITransform._exportmethods + [ ]


   def __init__(self):
      super(ND280Transform,self).__init__()
      self.inputdatasubsets = []

   def createUnits(self):
      """Create new units if required given the inputdata"""
      
      # call parent for chaining
      super(ND280Transform,self).createUnits()
      
      # loop over input data and see if we need to create any more units
      for idx,inds in enumerate(self.inputdata):
         
        # currently only checking for Local Datasets and DCache
        if inds._name == "ND280LocalDataset" or inds._name == "ND280DCacheDataset":
           # First pass, create all the subsets
           if len(self.inputdatasubsets) == idx:
             subsets = splitNbInputFile(inds.names, self.nbinputfiles)
             self.inputdatasubsets.append(subsets)
           elif len(self.inputdatasubsets) < idx:
             raise Exception('ND280Transform: The inputdata and inputdatasubsets are out of sync. This should not happen.')

           ok = True

           for subset in self.inputdatasubsets[idx]:
              # check if this data is being run over by checking all the names listed
              ok = False
              for unit in self.units:
                 if (set(unit.inputdata.names) == set(subset)):
                    ok = True
                    break

              if not ok:
                 # new unit required for this subset
                 unit = ND280Unit()
                 unit.name = "Unit %d" % len(self.units)
                 self.addUnitToTRF( unit )
                 if inds._name == "ND280LocalDataset":
                    unit.inputdata = ND280LocalDataset()
                 elif inds._name == "ND280DCacheDataset":
                    unit.inputdata = ND280DCacheDataset()
                 unit.inputdata.names = subset

      # For special cases where there is no inputdata given,
      # just create one unit.
      if len(self.inputdata) == 0:
         unit = ND280Unit()
         unit.name = "Unit %d" % len(self.units)
         self.addUnitToTRF( unit )


   def addUnit(self, filelist):
      """Create a new unit based on this file list"""
      unit = ND280Unit()
      unit.inputdata = ND280LocalDataset()
      unit.inputdata.names = filelist
      self.addUnitToTRF( unit )

   def createChainUnit( self, parent_units, use_copy_output = True ):
      """Create a chained unit using the output data from the given units"""

      # check all parent units for copy_output
      copy_output_ok = True
      for parent in parent_units:
         if not parent.copy_output:
            copy_output_ok = False

      # all parent units must be completed so the outputfiles are filled correctly
      for parent in parent_units:
         if parent.status != "completed":
           return None

      if len(parent_units) == 0:
         return None

      if not use_copy_output or not copy_output_ok:
         unit = ND280Unit()
         unit.inputdata = ND280LocalDataset()
         for parent in parent_units:
            # loop over the output files and add them to the ND280LocalDataset - THIS MIGHT NEED SOME WORK!
            job = GPI.jobs(parent.active_job_ids[0])

            # if TaskChainInput.include_file_mask is not used go old way (see below)
            # otherwise add all file matching include_file_mask(s) to the unit.inputdata. DV.
            inc_file_mask = False
            for p in self.inputdata[0].include_file_mask:
               unit.inputdata.get_dataset(job.outputdir, p)
               inc_file_mask = True

            if not inc_file_mask:
               for f in job.outputfiles:
                  # should check for different file types and add them as appropriate to the dataset
                  # self.inputdata (== TaskChainInput).include/exclude_file_mask could help with this
                  # This will be A LOT easier with Ganga 6.1 as you can easily map outputfiles -> inputfiles!
                  # TODO: implement use of include/exclude_file_mask
                  #       
                  try:
                     outputfilenameformat = f.outputfilenameformat
                  except:
                     inputdir = job.outputdir
                  else:
                     #### WARNING: The following will work only if the MassStorageFile puts the files in local directories !
                     inputdir = '/'.join( [getConfig('Output')['MassStorageFile']['uploadOptions']['path'], f.outputfilenameformat.replace('{fname}','')])
                  unit.inputdata.get_dataset( inputdir, f.namePattern )
      else:

         unit = ND280Unit()
         unit.inputdata = ND280LocalDataset()

         for parent in parent_units:
            # unit needs to have completed and downloaded before we can get file list
            if parent.status != "completed":
               return None

            # we should be OK so copy all output to the dataset
            for f in parent.copy_output.files:
               unit.inputdata.names.append( os.path.join( parent.copy_output.local_location, f ) )
         
      return unit
   
