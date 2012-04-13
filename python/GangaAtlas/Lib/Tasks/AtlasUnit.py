from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
from sets import Set
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2
from dq2.common.DQException import DQException

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset

class AtlasUnit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
    }.items()))

   _category = 'units'
   _name = 'AtlasUnit'
   _exportmethods = IUnit._exportmethods + [ ]
   
   def registerDataset(self):
      """Register in the transform container"""
      trf = self._getParent()
      trf_container = trf.getContainerName()
      fail = False
      try:
         containerinfo = {}
         dq2_lock.acquire()
         try:
            containerinfo = dq2.listDatasets(trf_container)
         except:
            containerinfo = {}
            
         if containerinfo == {}:
            try:
               dq2.registerContainer(trf_container)
               logger.info('Registered container for Unit %i of Transform %i: %s' % (self.getID(), trf.getID(), trf_container))
               
            except Exception, x:
               logger.error('Problem registering container for Unit %i of Transform %i, %s : %s %s' % (self.getID(), trf.getID(), trf_container,x.__class__, x))
               fail = True
            except DQException, x:
               logger.error('DQ2 Problem registering container for Unit %i of Transform %i, %s : %s %s' % (self.getID(), trf.getID(), trf_container,x.__class__, x))
               fail = True
               
         job = GPI.jobs(self.active_job_ids[0])
         ds_list = dq2.listDatasetsInContainer(job.outputdata.datasetname)
         for ds in ds_list:
            try:
               dq2.registerDatasetsInContainer(trf_container, [ ds ] )
            except DQContainerAlreadyHasDataset:
               pass
            except Exception, x:
               logger.error('Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, trf_container, x.__class__, x))
               fail = True
            except DQException, x:
               logger.error('DQ2 Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, trf_container, x.__class__, x))
               fail = True
      finally:
         dq2_lock.release()

      # add dataset to the task container
      task = trf._getParent()
      task_container = task.getContainerName()
      
      try:
         containerinfo = {}
         dq2_lock.acquire()
         try:
            containerinfo = dq2.listDatasets(task_container)
         except:
            containerinfo = {}
         if containerinfo == {}:
            try:
               dq2.registerContainer(task_container)
               logger.info('Registered container for Unit %i of Transform %i: %s' % (self.getID(), trf.getID(), task_container))
                  
            except Exception, x:
               logger.error('Problem registering container for Unit %i of Transform %i in Task %i, %s : %s %s' %
                            (self.getID(), trf.getID(), task.getID(), task_container, x.__class__, x))
               fail = True
            except DQException, x:
               logger.error('DQ2 Problem registering container for Unit %i of Transform %i in Task %i, %s : %s %s' %
                            (self.getID(), trf.getID(), task.getID(), task_container, x.__class__, x))
               fail = True 

         ds_list = dq2.listDatasetsInContainer(job.outputdata.datasetname)
         for ds in ds_list:
            try:
               dq2.registerDatasetsInContainer(task_container, [ ds ] )
            except DQContainerAlreadyHasDataset:
               pass
            except Exception, x:
               logger.error('Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, trf_container, x.__class__, x))
               fail = True
            except DQException, x:
               logger.error('DQ2 Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, trf_container, x.__class__, x))
               fail = True
      finally:
          dq2_lock.release()

      return not fail

   def createNewJob(self):
      """Create any jobs required for this unit"""      
      j = GPI.Job()
      j._impl.backend = self._getParent().backend.clone()
      j._impl.application = self._getParent().application.clone()
      j.inputdata = self.inputdata.clone()

      trf = self._getParent()
      task = trf._getParent()
      if trf.outputdata:
         j.outputdata = trf.outputdata.clone()
      else:
         j.outputdata = GPI.DQ2OutputDataset()
               
      dsn = [trf.getContainerName()[:-1], self.name, "j%i.t%i.trf%i.u%i" %
             (j.id, task.id, trf.getID(), self.getID())]
            
      j.outputdata.datasetname = '.'.join(dsn)
                           
      j.inputsandbox = self._getParent().inputsandbox
      j.outputsandbox = self._getParent().outputsandbox
      j.splitter = DQ2JobSplitter()
      if trf.MB_per_job > 0:
         j.splitter.filesize = trf.MB_per_job
      elif trf.subjobs_per_unit > 0:
         j.splitter.numsubjobs = trf.subjobs_per_unit
      else:
         j.splitter.numfiles = trf.files_per_job

      return j

   def majorResubmit(self, job):
      """perform a major resubmit/rebroker"""
      if not s in self._getParent().backend.requirements.excluded_sites:
         self._getParent().backend.requirements.excluded_sites.append(s)

      super(IUnit,self).majorResubmit(job)
