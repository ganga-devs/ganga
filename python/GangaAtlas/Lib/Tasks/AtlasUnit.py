from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
from sets import Set
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2
from dq2.common.DQException import DQException
from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname

from GangaAtlas.Lib.ATLASDataset.ATLASDataset import Download
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset

from Ganga.Utility.Config import getConfig
configDQ2 = getConfig('DQ2')

import os

class AtlasUnit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
      'output_file_list'     : SimpleItem(hidden=1, transient=1, defvalue={}, doc='list of output files copied')     
    }.items()))

   _category = 'units'
   _name = 'AtlasUnit'
   _exportmethods = IUnit._exportmethods + [ ]

   def __init__(self):
      super(AtlasUnit, self).__init__()
      self.output_file_list = {}
      
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
         ds_list = self.getOutputDatasetList()
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
         
      if fail:
         return not fail
      
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

         ds_list = self.getOutputDatasetList()

         for ds in ds_list:
            try:
               dq2.registerDatasetsInContainer(task_container, [ ds ] )
            except DQContainerAlreadyHasDataset:
               pass
            except Exception, x:
               logger.error('Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, task_container, x.__class__, x))
               fail = True
            except DQException, x:
               logger.error('DQ2 Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, task_container, x.__class__, x))
               fail = True
      finally:
          dq2_lock.release()

      return not fail

   def unregisterDataset(self):
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
            
         if containerinfo != {}:
            job = GPI.jobs(self.active_job_ids[0])
            ds_list = self.getOutputDatasetList()
            for ds in ds_list:
               
               try:
                  dq2.deleteDatasetsFromContainer(trf_container, [ ds ] )
               except DQContainerDoesNotHaveDataset:
                  pass
               except Exception, x:
                  logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                  fail = True
               except DQException, x:
                  logger.error('DQ2 Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                  fail = True
      finally:
         dq2_lock.release()

      if fail:
         return not fail
      
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
            
         if containerinfo != {}:
            job = GPI.jobs(self.active_job_ids[0])
            ds_list = self.getOutputDatasetList()
            for ds in ds_list:
               
               try:
                  dq2.deleteDatasetsFromContainer(task_container, [ ds ] )
               except DQContainerDoesNotHaveDataset:
                  pass
               except Exception, x:
                  logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, task_container, x.__class__, x))
                  fail = True
               except DQException, x:
                  logger.error('DQ2 Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, task_container, x.__class__, x))
                  fail = True
      finally:
         dq2_lock.release()

      return not fail

   def getOutputDatasetList(self):
      """Return a list of the output datasets associated with this unit"""
      
      job = GPI.jobs(self.active_job_ids[0])
      
      if job.backend.individualOutDS:
         # find all the individual out ds's
         cont_list = []
         for ds in job.subjobs(0).outputdata.output:
            cont_name = ds.split(",")[0]
            if not cont_name in cont_list:
               cont_list.append(cont_name)

         ds_list = []
         for cont in cont_list:
            ds_list += dq2.listDatasetsInContainer(cont)
            
         return ds_list
      else:
         return dq2.listDatasetsInContainer(job.outputdata.datasetname)
      
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

      # check for ds name specified and length
      if j.outputdata._impl._name == "DQ2OutputDataset" and j.outputdata.datasetname != "":
         dsn = [j.outputdata.datasetname, "j%i.t%i.trf%i.u%i" %
                (j.id, task.id, trf.getID(), self.getID())]

         if len(".".join(dsn)) > configDQ2['OUTPUTDATASET_NAMELENGTH'] - 2:
            dsn = [j.outputdata.datasetname[: - (len(".".join(dsn)) - configDQ2['OUTPUTDATASET_NAMELENGTH'] + 2)], "j%i.t%i.trf%i.u%i" %
                   (j.id, task.id, trf.getID(), self.getID())]
      else:
         dsn = [trf.getContainerName()[:-1], self.name, "j%i.t%i.trf%i.u%i" %
                (j.id, task.id, trf.getID(), self.getID())]

         if len(".".join(dsn)) > configDQ2['OUTPUTDATASET_NAMELENGTH'] - 2:
            dsn = [trf.getContainerName()[:-1], self.name[: - (len(".".join(dsn)) - configDQ2['OUTPUTDATASET_NAMELENGTH'] + 2)], "j%i.t%i.trf%i.u%i" %
                   (j.id, task.id, trf.getID(), self.getID())]
            
      j.outputdata.datasetname = '.'.join(dsn).replace(":", "_").replace(" ", "").replace(",","_")
                           
      j.inputsandbox = self._getParent().inputsandbox
      j.outputsandbox = self._getParent().outputsandbox

      # check for splitter
      if not trf.splitter:
         j.splitter = DQ2JobSplitter()
         if trf.MB_per_job > 0:
            j.splitter.filesize = trf.MB_per_job
         elif trf.subjobs_per_unit > 0:
            j.splitter.numsubjobs = trf.subjobs_per_unit
         else:
            j.splitter.numfiles = trf.files_per_job
      else:
         j.splitter = trf.splitter.clone()
         
      return j

   def checkMajorResubmit(self, job):
      """check if this job needs to be fully rebrokered or not"""

      # check for failed build jobs (killed)
      if job.status == "killed":
         return True

      for j in job.subjobs:
         if j.status == "killed":
            return True

      return False

   def majorResubmit(self, job):
      """perform a major resubmit/rebroker"""
      for sj in job.subjobs:
         if not sj.backend.site in self._getParent().backend.requirements.excluded_sites:
            self._getParent().backend.requirements.excluded_sites.append(sj.backend.site)

      super(AtlasUnit,self).majorResubmit(job)

   def reset(self):
      """Reset the unit completely"""
      if self.status == "completed":
         self.unregisterDataset() 

      super(AtlasUnit,self).reset()

   def updateStatus(self, status):
      """Update status hook"""

      # register the dataset
      if status == "completed":
         if not self.registerDataset():
            return

      super(AtlasUnit,self).updateStatus(status)

   def checkForSubmission(self):
      """Additional checks for unit submission"""

      # call the base class
      if not super(AtlasUnit,self).checkForSubmission():
         return False
      
      # Add a check for chain units to have frozen their input DS
      if len(self.req_units) > 0 and self.inputdata._name == "DQ2Dataset":
         for uds in self.inputdata.dataset:
            try:
               dq2_lock.acquire()

               try:
                  # list datasets in container
                  ds_list = dq2.listDatasetsInContainer(uds)

                  cont_ok = True
                  for ds in ds_list:
                     # find locations and check if frozen
                     loc_dict = dq2.listDatasetReplicas(ds)
                     locations = []
                     for loc in loc_dict[ loc_dict.keys()[0] ]:
                        locations += loc_dict[ loc_dict.keys()[0] ][loc]

                     ds_ok = False
                     for loc in locations:
                        if loc == "":
                           continue
                        datasetsiteinfo = dq2.listFileReplicas(loc, ds)
                        if datasetsiteinfo[0]['found'] != None:
                           ds_ok = True
                           break

                     if not ds_ok:
                        cont_ok = False
                        break
               except:
                  logger.warning("Unable to check if datasets are frozen")
                  cont_ok = False
            finally:
               dq2_lock.release()


         # at least one dataset wasn't frozen
         if not cont_ok:
            return False

      return True

   def copyOutput(self):
      """Copy the output data to local storage"""

      job = GPI.jobs(self.active_job_ids[0])
      
      if self.copy_output._name != "TaskLocalCopy" or job.outputdata._impl._name != "DQ2OutputDataset":
         logger.error("Cannot transfer from DS type '%s' to '%s'. Please contact plugin developer." % (job.outputdata._name, self.copy_output._name))
         return False

      # get list of output files
      if len(self.output_file_list) == 0:
         dq2_list = dq2.listFilesInDataset(job.outputdata.datasetname)
                 
         for guid in dq2_list[0].keys():
            self.output_file_list[ dq2_list[0][guid]['lfn'] ] = job.outputdata.datasetname
         
      # check which ones still need downloading
      to_download = {}
      for f in self.output_file_list.keys():
         
         # check for REs
         if self.copy_output.isValid(f) and not self.copy_output.isDownloaded(f):            
            to_download[ f ] = self.output_file_list[f]


      # is everything downloaded?
      if len(to_download.keys()) == 0:
         return True

      # nope, so pick the first and grab it
      fname = to_download.keys()[0]
      dsname = to_download[fname]
      exe = 'dq2-get -L ROAMING -a -d -H %s -f %s %s' % (self.copy_output.local_location, fname, dsname)
      logger.info("Downloading '%s' to %s..." % (fname, self.copy_output.local_location))

      thread = Download.download_dq2(exe)
      thread.start()
      thread.join()

      # check for valid download - SHOULD REALLY BE A HASH CHECK
      full_path = os.path.join(self.copy_output.local_location, fname)
      if not os.path.exists(full_path) or os.path.getsize( full_path ) < 4:
         logger.error("Error downloading '%s'" % full_path)
      else:
         self.copy_output.files.append(fname)
         logger.info("File '%s' downloaded successfully" % full_path)
         
      return False
