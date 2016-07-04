from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy
from Ganga.Utility.logging import getLogger
from ND280Unit_CSVEvtList import ND280Unit_CSVEvtList
from GangaND280.ND280Dataset.ND280Dataset import ND280LocalDataset
from GangaND280.ND280Splitter.ND280Splitter import splitCSVFile
import Ganga.GPI as GPI

import os

logger = getLogger()

class ND280Transform_CSVEvtList(ITransform):
   _schema = Schema(Version(1,0), dict(ITransform._schema.datadict.items() + {
        'nbevents' : SimpleItem(defvalue=-1,doc='The number of events for each unit'),
    }.items()))

   _category = 'transforms'
   _name = 'ND280Transform_CSVEvtList'
   _exportmethods = ITransform._exportmethods + [ ]

   def __init__(self):
      super(ND280Transform_CSVEvtList,self).__init__()

   def createUnits(self):
      """Create new units if required given the inputdata"""
      
      # call parent for chaining
      super(ND280Transform_CSVEvtList,self).createUnits()
      
      # Look at the application schema and check if there is a csvfile variable
      try:
        csvfile = self.application.csvfile
      except AttributeError:
        logger.error('This application doesn\'t contain a csvfile variable. Use another Transform !')
        return

      subsets = splitCSVFile(self.application.csvfile, self.nbevents)

      for s,sub in enumerate(subsets):
        
        # check if this data is being run over by checking all the names listed
        ok = False
        for unit in self.units:
          if unit.subpartid == s:
            ok = True

        if ok:
          continue
        # new unit required for this dataset
        unit = ND280Unit_CSVEvtList()
        unit.name = "Unit %d" % len(self.units)
        unit.subpartid = s
        unit.eventswanted = sub
        unit.inputdata = self.inputdata[0]
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

      if not use_copy_output or not copy_output_ok:
         unit = ND280Unit_CSVEvtList()
         unit.inputdata = ND280LocalDataset()
         for parent in parent_units:
            # loop over the output files and add them to the ND280LocalDataset - THIS MIGHT NEED SOME WORK!
            job = GPI.jobs(parent.active_job_ids[0])
            for f in job.outputfiles:
               # should check for different file types and add them as appropriate to the dataset
               # self.inputdata (== TaskChainInput).include/exclude_file_mask could help with this
               # This will be A LOT easier with Ganga 6.1 as you can easily map outputfiles -> inputfiles!
               unit.inputdata.names.append( os.path.join( job.outputdir, f.namePattern ) )
      else:

         unit = ND280Unit_CSVEvtList()
         unit.inputdata = ND280LocalDataset()

         for parent in parent_units:
            # unit needs to have completed and downloaded before we can get file list
            if parent.status != "completed":
               return None

            # we should be OK so copy all output to the dataset
            for f in parent.copy_output.files:
               unit.inputdata.names.append( os.path.join( parent.copy_output.local_location, f ) )
         
      return unit
   
