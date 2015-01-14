from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaLHCb.Lib.Tasks.LHCbUnit import LHCbUnit
from Ganga.GPIDev.Base.Proxy import isType
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
from GangaLHCb.Lib.LHCbDataset import LHCbDataset

class LHCbTransform(ITransform):
   _schema = Schema(Version(1,0), dict(ITransform._schema.datadict.items() + {
      'splitter'      : ComponentItem('splitters', defvalue=None, optional=1, load_default=False,doc='Splitter to be used for units'),
      'queries'         : ComponentItem('query', defvalue=[], sequence=1, protected=1, optional=1, load_default=False,doc='Queries managed by this Transform'),
    }.items()))

   _category = 'transforms'
   _name = 'LHCbTransform'
   _exportmethods = ITransform._exportmethods + [ 'updateQuery', 'addQuery' ]
   
   def __init__(self):
      super(LHCbTransform,self).__init__()

      # generally no delay neededd
      self.chain_delay = 0

   def addQuery(self, bk):
      """Add a BK query to this transform"""
      ## Check if the BKQuery input is correct and append/update
      if not isType(bk,BKQuery):
         raise GangaAttributeError(None,'LHCbTransform expects a BKQuery object passed to the addQuery method')
      self.queries.append(bk)
      self.updateQuery()
      
   def createUnits(self):
      """Create new units if required given the inputdata"""

      # call parent for chaining
      super(LHCbTransform,self).createUnits()

      # create units for queries if required
      if len(self.queries) != 0:
         new_files = []
         for f in self.inputdata[0].files:
            file_ok = False
            for u in self.units:
               if f in u.inputdata.files:
                  file_ok = True
                  break

            if not file_ok:
               new_files.append(f)

         if len(new_files) > 0:
            unit = LHCbUnit()
            unit.name = "Unit %d" % len(self.units)
            self.addUnitToTRF( unit )
            unit.inputdata = LHCbDataset(files = new_files)
                                                
         return
      
      # loop over input data and see if we need to create any more units
      for inds in self.inputdata:

         if inds._name != "LHCbDataset":
            continue

         ok = False
         for unit in self.units:
            if len(unit.inputdata.files) == len(inds.files):
               # has this DS been associated with this unit?
               ok = True
               for f in unit.inputdata.files:
                  if not f in inds.files:
                     ok = False

         if not ok:
            # new unit required for this dataset
            unit = LHCbUnit()
            unit.name = "Unit %d" % len(self.units)
            self.addUnitToTRF( unit )
            unit.inputdata = inds

   def createChainUnit( self, parent ):
      """Create an output unit given this output data"""
      
      if len(parent.active_job_ids) == 0 or GPI.jobs(parent.active_job_ids[0]).outputdata.datasetname == "":
         return None
      
      unit = LHCbUnit()
      unit.inputdata = LHCbDataset()
      unit.inputdata.dataset = GPI.jobs(parent.active_job_ids[0]).outputdata.datasetname
      return unit
   
   def _getJobsWithRemovedData(self,lost_dataset):
      redo_jobs = []
      redo_jobs_ids = []
      for f in lost_dataset.files:
         for unit in self.units:
            job = GPI.jobs(unit.active_job_ids[0])
            for sj in job.subjobs:

               if not sj.fqid in redo_jobs_ids and f in job.inputdata.files:
                  del job.inputdata.files[ job.inputdata.files.index(f) ]
                  redo_jobs.append(sj)
                  redo_jobs_ids.append(sj.fqid)
                  
      return redo_jobs
   
   def updateQuery(self, resubmit=False):
      """Update the dataset information of the transforms. This will
      include any new data in the processing or re-run jobs that have data which
      has been removed."""
      if len(self.queries) == 0:
         raise GangaException(None,'Cannot call updateQuery() on an LHCbTransform without any queries')

      if self._getParent() != None:
         logger.info('Retrieving latest bookkeeping information for transform %i:%i, please wait...'%(self._getParent().id,self.getID()))
      else:
         logger.info('Retrieving latest bookkeeping information for transform, please wait...')


      all_files = []
      if not len(self.inputdata):
         self.inputdata.append( LHCbDataset() )
         
      for query in self.queries:

         ## Get the latest dataset
         latest_dataset=query.getDataset()
         all_files += query.getDataset().files
         
         ## Compare to previous inputdata, get new and removed
         logger.info('Checking for new and removed data for query %d, please wait...' % self.queries.index(query))
         dead_data = LHCbDataset()
         new_data = LHCbDataset()
         if self.inputdata is not None:
            ## Get new files
            new_data.files += latest_dataset.difference(self.inputdata[0]).files
            ## Get removed files
            dead_data.files += self.inputdata[0].difference(latest_dataset).files
            ## If nothing to be updated then exit

         ## Carry out actions as needed
         redo_jobs = self._getJobsWithRemovedData(dead_data)
         
         if len(new_data.files) == 0 and len(redo_jobs) == 0:
            logger.info('Query %i from Transform %i:%i is already up to date'%(self.queries.index(query), self._getParent().id,self.getID()))
            continue
        
         if len(redo_jobs) != 0 and not resubmit:
            logger.info('There are jobs with out-of-date datasets, some datafiles must '\
                        'be removed. Updating will mean loss of existing output and mean that merged data '\
                        'will change respectively. Due to the permenant nature of this request please recall '\
                        'update with the True argument as updateQuery(True)')
            return

         if len(redo_jobs) != 0:
            # resubmit these removed data jobs
            for j in redo_jobs:
               if j.status in ['submitting','submitted','running','completing']:
                  logger.warning('Job \'%s\' as it is still running but is marked for resubmission due to removed data. It will be killed first'%j.fqid)
                  j.kill()

               logger.info('Resubmitting job \'%s\' as it\'s dataset is out of date.'%j.fqid)
               j.resubmit()
               if self.status == "completed":               
                  self.updateStatus("running")

         if len(new_data) != 0:
            if self.status == "completed":
               self.updateStatus("running")
            if self._getParent() != None:
               logger.info('Transform %i:%i updated, unit %i will be added containing %i more file(s) for processing'%(self._getParent().id,self.getID(),len(self.units),len(new_data)))
            else:
               logger.info('Transform data updated, unit %i will be added containing %i more file(s) for processing'%(len(self.units),len(new_data)))

      self.inputdata[0].files += all_files

   
       
