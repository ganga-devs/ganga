from GangaCore.GPIDev.Lib.Tasks.common import *
from GangaCore.GPIDev.Lib.Tasks.IUnit import IUnit
from GangaCore.GPIDev.Lib.Job.Job import JobError
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2
from dq2.common.DQException import DQException

from GangaAtlas.Lib.ATLASDataset.ATLASDataset import Download, ATLASOutputDataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from GangaAtlas.Lib.Athena.Athena import AthenaSplitterJob
from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset
from GangaCore.GPIDev.Schema import *
import GangaCore.GPI as GPI

from GangaCore.Utility.Config import getConfig
configDQ2 = getConfig('DQ2')

from GangaCore.Utility.logging import getLogger
logger = getLogger()

import os
import threading

class AtlasUnit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
      'output_file_list'     : SimpleItem(hidden=1, transient=1, defvalue={}, doc='list of output files copied'),
    }.items()))

   _category = 'units'
   _name = 'AtlasUnit'
   _exportmethods = IUnit._exportmethods + [ ]
   _download_lock = threading.Lock()

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
               
            except Exception as x:
               logger.error('Problem registering container for Unit %i of Transform %i, %s : %s %s' % (self.getID(), trf.getID(), trf_container,x.__class__, x))
               fail = True
            except DQException as x:
               logger.error('DQ2 Problem registering container for Unit %i of Transform %i, %s : %s %s' % (self.getID(), trf.getID(), trf_container,x.__class__, x))
               fail = True
               
         job = GPI.jobs(self.active_job_ids[0])
         ds_list = self.getOutputDatasetList()

         for ds in ds_list:
            try:
               dq2.registerDatasetsInContainer(trf_container, [ ds ] )
            except DQContainerAlreadyHasDataset:
               pass
            except Exception as x:
               logger.error('Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, trf_container, x.__class__, x))
               fail = True
            except DQException as x:
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
                  
            except Exception as x:
               logger.error('Problem registering container for Unit %i of Transform %i in Task %i, %s : %s %s' %
                            (self.getID(), trf.getID(), task.getID(), task_container, x.__class__, x))
               fail = True
            except DQException as x:
               logger.error('DQ2 Problem registering container for Unit %i of Transform %i in Task %i, %s : %s %s' %
                            (self.getID(), trf.getID(), task.getID(), task_container, x.__class__, x))
               fail = True 

         ds_list = self.getOutputDatasetList()

         for ds in ds_list:
            try:
               dq2.registerDatasetsInContainer(task_container, [ ds ] )
            except DQContainerAlreadyHasDataset:
               pass
            except Exception as x:
               logger.error('Problem registering dataset %s in container %s: %s %s' %( job.outputdata.datasetname, task_container, x.__class__, x))
               fail = True
            except DQException as x:
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
               except Exception as x:
                  logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                  fail = True
               except DQException as x:
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
               except Exception as x:
                  logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, task_container, x.__class__, x))
                  fail = True
               except DQException as x:
                  logger.error('DQ2 Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, task_container, x.__class__, x))
                  fail = True
      finally:
         dq2_lock.release()

      return not fail

   def getContainerList(self):
      """Return a list of the output containers assocaited with this unit"""
      job = GPI.jobs(self.active_job_ids[0])
      cont_list = []
      if job.backend._impl._name == "Jedi":
         # Jedi jobs have their datasets stored in datasetList
         for ds in job.outputdata.datasetList:
            cont_list.append(ds)

      elif job.backend.individualOutDS:
         # find all the individual out ds's
         for ds in job.subjobs(0).outputdata.output:

            # find all containers listed
            for cont_name in ds.split(","):
               if not cont_name.endswith("/"):
                  continue

               if not cont_name in cont_list:
                  cont_list.append(cont_name)
      else:
         cont_list.append(job.outputdata.datasetname)

      return cont_list

   def getOutputDatasetList(self):
      """Return a list of the output datasets associated with this unit"""
      
      ds_list = []
      for cont in self.getContainerList():
         ds_list += dq2.listDatasetsInContainer(cont)

      return ds_list
      
   def createNewJob(self):
      """Create any jobs required for this unit"""      
      j = GPI.Job()
      j._impl.backend = self._getParent().backend.clone()
      j._impl.application = self._getParent().application.clone()
      if self.inputdata:
         j.inputdata = self.inputdata.clone()

      trf = self._getParent()
      task = trf._getParent()
      if trf.outputdata:
         j.outputdata = trf.outputdata.clone()
      elif j.inputdata and j.inputdata._impl._name == "ATLASLocalDataset" and j.application._impl._name != "TagPrepare":
         j.outputdata = GPI.ATLASOutputDataset()
      elif j.application._impl._name != "TagPrepare":
         j.outputdata = GPI.DQ2OutputDataset()

      # check for ds name specified and length
      if j.outputdata and j.outputdata._impl._name == "DQ2OutputDataset":
         max_length = configDQ2['OUTPUTDATASET_NAMELENGTH'] - 11

         # merge names need to be shorter
         if (j.backend._impl._name == "Panda" or j.backend._impl._name == "Jedi"):
            if j.backend.requirements.enableMerge:
               max_length -= 12

            if j.backend._impl._name == "Jedi":
               # go over the outputdata and check for output names that Jedi appends to the outDS name
               tmp_len_chg = 8
               for o in j.outputdata.outputdata:
                  if (len(o)+1) > tmp_len_chg:
                     tmp_len_chg = len(o)+1

               max_length -= tmp_len_chg

            elif j.backend.individualOutDS:
               max_length -= 8

         if j.outputdata.datasetname != "":
            dsn = [j.outputdata.datasetname, "j%i.t%i.trf%i.u%i" %
                   (j.id, task.id, trf.getID(), self.getID())]

            if len(".".join(dsn)) > max_length:
               dsn = [j.outputdata.datasetname[: - (len(".".join(dsn)) - max_length)], "j%i.t%i.trf%i.u%i" %
                      (j.id, task.id, trf.getID(), self.getID())]
         else:
            dsn = [trf.getContainerName()[:-1], self.name, "j%i.t%i.trf%i.u%i" %
                   (j.id, task.id, trf.getID(), self.getID())]

            if len(".".join(dsn)) > max_length:
               dsn2 = [trf.getContainerName(2 * max_length / 3)[:-1], "", "j%i.t%i.trf%i.u%i" % (j.id, task.id, trf.getID(), self.getID())]
               dsn = [trf.getContainerName(2 * max_length / 3)[:-1], self.name[: - (len(".".join(dsn2)) - max_length)], "j%i.t%i.trf%i.u%i" %
                      (j.id, task.id, trf.getID(), self.getID())]
            
         j.outputdata.datasetname = '.'.join(dsn).replace(":", "_").replace(" ", "").replace(",","_")
                           
      j.inputsandbox = self._getParent().inputsandbox
      j.outputsandbox = self._getParent().outputsandbox

      # check for splitter - TagPrepare and Jedi don't user splitters
      if j.application._impl._name == "TagPrepare":
         return j
      
      if j.backend._impl._name == "Jedi":
         if trf.files_per_job > 0:
            j.backend.requirements.nFilesPerJob = trf.files_per_job
         elif trf.MB_per_job > 0:
            j.backend.requirements.nGBPerJob = trf.MB_per_job / 1000

         return j

      if not trf.splitter:
         # provide a default number of files if there's nothing else given
         nfiles = trf.files_per_job
         if nfiles < 1:
            nfiles = 5

         if j.inputdata._impl._name == "ATLASLocalDataset":
            j.splitter = AthenaSplitterJob()
            if trf.subjobs_per_unit > 0:
               j.splitter.numsubjobs = trf.subjobs_per_unit
            else:
               import math 
               j.splitter.numsubjobs = int( math.ceil( len(j.inputdata.names) / float(nfiles) ) )
         else:
            j.splitter = DQ2JobSplitter()
            if trf.MB_per_job > 0:
               j.splitter.filesize = trf.MB_per_job
            elif trf.subjobs_per_unit > 0:
               j.splitter.numsubjobs = trf.subjobs_per_unit
            else:
               j.splitter.numfiles = nfiles
      else:
         j.splitter = trf.splitter.clone()

      # postprocessors
      if len(self._getParent().postprocessors.process_objects) > 0:
         import copy
         j.postprocessors = copy.deepcopy( addProxy(self._getParent()).postprocessors )
         
      return j

   def checkMajorResubmit(self, job):
      """check if this job needs to be fully rebrokered or not"""

      # check for failed build jobs (killed)
      if job.status == "killed":
         return True

      for j in job.subjobs:
         if j.status == "killed":
            return True

      # are most subjobs failed?
      if job.subjobs:
         num = 0
         for j in job.subjobs:
            if j.status == "failed":
               num += 1

         if float(num) / len(job.subjobs) > self._getParent().rebroker_fraction:
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

   def checkCompleted(self, job):
      """Check if this unit is complete"""
      if job.status == "completed":
         if job.outputdata and job.outputdata._impl._name == "DQ2OutputDataset" and job.backend.__class__.__name__ != "Jedi":

            # make sure all datasets are complete
            if job.backend.requirements.enableMerge:

               # get container list
               cont_list = self.getContainerList()

               for mj in job.backend.mergejobs:
                  if mj.status != "finished":
                     # merge jobs failed - reset the unit for the moment
                     logger.error("Merge jobs failed. Resetting unit...")
                     self._getParent().resetUnit(self.getID())
                     return False

               for cont in cont_list:
                  dq2_list = dq2.listFilesInDataset(cont)
                  for guid in dq2_list[0].keys():                  
                     if dq2_list[0][guid]['lfn'].find("merge") == -1:
                        logger.warning("Merged files not transferred to out DS by Panda yet. Waiting...")
                        return False

         return True
      else:
         return False

   def updateStatus(self, status):
      """Update status hook"""

      # register the dataset if applicable
      if status == "completed":
         job = GPI.jobs(self.active_job_ids[0])
         if job.outputdata and job.outputdata._impl._name == "DQ2OutputDataset" and not self.registerDataset():
            return
         
      super(AtlasUnit,self).updateStatus(status)

   def checkForSubmission(self):
      """Additional checks for unit submission"""

      # call the base class
      if not super(AtlasUnit,self).checkForSubmission():
         return False

      # check that parent units are complete because otherwise, when we check for submission to do submissions first (ITransform.update)
      # datasets may not have been created yet
      if not self.checkParentUnitsAreComplete():
         return False

      # Add a check for chain units to have frozen their input DS
      if len(self.req_units) > 0 and self.inputdata._name == "DQ2Dataset" and not self.inputdata.tag_info:

         # check datasets are frozen
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
                        if datasetsiteinfo[0]['found'] is not None:
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

   def _acquireDownloadLock(self, timeout = 10):
      """Grab the download lock"""
      import time
      t = 0
      while t < timeout:
         if self._download_lock.acquire(False):
            return True

         time.sleep(0.1)
         t += 0.1

      return False

   def _releaseDownloadLock(self, timeout = 10):
      """Release the download lock"""
      self._download_lock.release()

   def copyOutput(self):
      """Copy the output data to local storage"""

      job = GPI.jobs(self.active_job_ids[0])
      
      if self.copy_output._name != "TaskLocalCopy" or job.outputdata._impl._name != "DQ2OutputDataset":
         logger.error("Cannot transfer from DS type '%s' to '%s'. Please contact plugin developer." % (job.outputdata._name, self.copy_output._name))
         return False

      # get list of output files
      self._acquireDownloadLock()
      dq2_list = []
      if len(self.output_file_list) == 0:
         for ds in self.getOutputDatasetList():
            dq2_list = dq2.listFilesInDataset(ds)
            
            # merge job DSs leave empty non-merged DSs around
            if job.backend.__class__.__name__ == "Panda" and job.backend.requirements.enableMerge and not ds.endswith("merge") and len(dq2_list) == 0:
               continue

            for guid in dq2_list[0].keys():
               self.output_file_list[ dq2_list[0][guid]['lfn'] ] = ds
         
      # check which ones still need downloading
      to_download = {}
      for f in self.output_file_list.keys():
         
         # check for REs
         if self.copy_output.isValid(f) and not self.copy_output.isDownloaded(f):            
            to_download[ f ] = self.output_file_list[f]

      # store download location in case it's changed while downloading
      download_loc = self.copy_output.local_location
      self._releaseDownloadLock()

      # is everything downloaded?
      if len(to_download.keys()) == 0:
         return True

      # nope, so pick the requested number and off we go
      thread_array = []
      for fname in to_download.keys()[:self._getParent().num_dq2_threads]:
         dsname = to_download[fname]
         exe = 'dq2-get -L ROAMING -a -d -H %s -f %s %s' % (download_loc, fname, dsname)
         logger.info("Downloading '%s' to %s..." % (fname, download_loc))

         thread = Download.download_dq2(exe)
         thread.start()
         thread_array.append(thread)

      for t in thread_array:
         t.join()

      self._acquireDownloadLock()
      
      # check for valid download - SHOULD REALLY BE A HASH CHECK
      for fname in to_download.keys()[:self._getParent().num_dq2_threads]:
         full_path = os.path.join(self.copy_output.local_location, fname)
         if not os.path.exists(full_path):
            logger.error("Error downloading '%s'. File doesn't exist after download." % full_path)
         elif os.path.getsize( full_path ) < 4:
            logger.error("Error downloading '%s'. File size smaller than 4 bytes (%d)" % (full_path, os.path.getsize( full_path ) ))
         else:
            self.copy_output.files.append(fname)
            logger.info("File '%s' downloaded successfully" % full_path)

      self._releaseDownloadLock()

      return False
