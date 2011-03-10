from common import *
from Transform import Transform
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from TaskApplication import AthenaTask, AnaTaskSplitterJob
from GangaAtlas.Lib.ATLASDataset.ATLASDataset import ATLASLocalDataset,ATLASOutputDataset
from GangaAtlas.Lib.Athena.Athena import AthenaSplitterJob

from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2

#config.addOption('cloudPreference',[],'list of preferred clouds to choose for AnaTask analysis')
#config.addOption('backendPreference',["LCG","Panda","NG"],'order of preferred backends (LCG, Panda, NG) for AnaTask analysis')

import time
import threading

from dq2.info import TiersOfATLAS
from GangaAtlas.Lib.ATLASDataset import whichCloud
from Ganga.Core.exceptions import ApplicationConfigurationError

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 

PandaClient = None
def getPandaClient():
    global PandaClient
    if PandaClient is None:
        from pandatools import Client
        PandaClient = Client
    return PandaClient

import random

def whichCloudExt(site):
   if site.startswith("NDGF"):
      return "NG"
   return whichCloud(site)

def stripSite(site):
   dq2alternatename = TiersOfATLAS.getSiteProperty(site,'alternateName')
   if not dq2alternatename:
      return site
   else: 
      return dq2alternatename[0]

def stripSites(sites):
   newsites = {}
   for site in sites:
      newsites[stripSite(site)] = 1
   return newsites.keys()


class MultiTransform(Transform):
   """ Analyzes Events """
   _schema = Schema(Version(1,0), dict(Transform._schema.datadict.items() + {
       'files_per_job'     : SimpleItem(defvalue=5, doc='files per job', modelist=["int"]),
       'partitions_data'   : ComponentItem('datasets', defvalue=[], optional=1, sequence=1, hidden=1, doc='Input dataset for each partition'),
       'partitions_sites'  : SimpleItem(defvalue=[], hidden=1, modelist=["str","list"],doc='Input site for each partition'),
       'partitions_fails'  : SimpleItem(defvalue=[], hidden=1, modelist=["list"],doc='Number of failures for each partition'),
       'partition_lock'    : SimpleItem( defvalue=None, typelist=None, hidden=1, transient=1, doc='Lock for partition info'),
       'required_trfs'        :SimpleItem(defvalue=[-1], doc='ID of Transform that this should follow'),
       'unit_partition_list': SimpleItem(defvalue=[], hidden=1, doc='Map from unit to partitions'),
       'unit_inputdata_list': SimpleItem(defvalue=[], hidden=1, doc='Map from unit to inputdata'),
       'unit_outputdata_list': SimpleItem(defvalue=[], hidden=1, doc='Map from unit to outputdata'),
       'do_auto_download'   : SimpleItem(defvalue=False, doc='Trigger automatic dq2 download of related datasets'),
       'individual_unit_merger'   : SimpleItem(defvalue=False, doc='Run the merger per unit rather than on the whole transform'),
       'single_unit'   : SimpleItem(defvalue=False, doc='Reduce to a single unit that runs over the all outputs from all required trfs'),
       'local_files'       :  SimpleItem(defvalue={'dq2':[], 'merge':[]}, doc='Local files downloaded/merged by the completed transform'),
       'merger'            : ComponentItem('mergers', defvalue=None, load_default=0,optional=1, doc='Local merger to be done over all units when complete.')
       #'outputdata'        : ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset'),
       #'dataset_name'      : SimpleItem(defvalue="", transient=1, getter="get_dataset_name", doc='name of the output dataset'),
       }.items()))
   _category = 'transforms'
   _name = 'MultiTransform'
   _exportmethods = Transform._exportmethods + ['getID', 'addUnit']
   
   def initialize(self):
       super(MultiTransform, self).initialize()
       self.backend = None

   def getID(self):
       """Return the index of this trf in the parent task"""
       task = self._getParent()
       if not task:
           raise ApplicationConfigurationError(None, "This transform has not been associated with a task and so there is no ID available")
       
       return task.transforms.index(self)
       
   def check(self):
      super(MultiTransform,self).check()

      # create the partition lock if required
      if not self.partition_lock:
          self.partition_lock = threading.Lock()

      # make sure the unit list hasn't already been determined
      if len(self.required_trfs) != 0 and len(self.partitions_sites) > 0:
          return
      
      if True: #len(self.required_trfs) != 0:
          # add a dummy partition per unit 
          self.partition_lock.acquire()          
          self.setPartitionsLimit( 1 )
          self.setPartitionsStatus([1], "hold")
          self.partition_lock.release()
          return
      
      self.partitions_data = []
      self.partitions_fails = []
      self.partitions_sites = []

      # set the first unit off - the others will be sorted in the monitoring
      self.createPartitionList(0)

   def getUnitsFromPartitions(self, partitions):
      """Get the units referenced by this partition list"""
      unit_list = []

      for uind in range(0, len(self.unit_partition_list)):
          for p in self.unit_partition_list[uind]:
              if p in partitions and not uind in unit_list:
                  unit_list.append(uind)

      return unit_list
      
   def getJobsForPartitions(self, partitions):
      """Return the appropriate job definitions for the given partitions"""
      
      alljobs = []
      unit_list = self.getUnitsFromPartitions(partitions)
      
      for uind in unit_list:
          if len(self.unit_partition_list[uind]) == 0:
              continue
          
          j = self.createNewJob(self.unit_partition_list[uind][0])

          # construct the out DS name
          task = self._getParent()
          dsn = ["user", getNickname(), task.creation_date,
                 self.unit_outputdata_list[uind],
                 "j%i.t%i.trf%i.u%i" % (j.id, task.id,
                                    task.transforms.index(self),
                                    uind)]

          # sort out the output data
          if not self.outputdata:
              if j.backend._impl._name in ['Panda']:
                  j.outputdata = GPI.DQ2OutputDataset()
              else:
                  j.outputdata = GPI.ATLASOutputDataset()
              
          j.splitter = AnaTaskSplitterJob()
          j.splitter.subjobs = self.unit_partition_list[uind]

          j.inputdata = self.partitions_data[ self.unit_partition_list[uind][0]-1]

          if j.backend._impl._name in ['Panda']:
              j.backend.site = self.partitions_sites[self.unit_partition_list[uind][0]-1]
              if j.application.atlas_exetype == "ATHENA":
                  j.outputdata.outputdata=[]
              
          if j.outputdata._impl._name == 'DQ2OutputDataset' and j.outputdata.datasetname == '':
              j.outputdata.datasetname = ".".join(dsn)
              
          alljobs.append(j)

      return alljobs

   def isLocalTRF(self):
       if self.backend and not self.backend._name in ['Panda']:
           return True
       else:
           return False

   def isUnitComplete(self, uind):
       """Return if this unit is complete"""
       for c in self.unit_partition_list[uind]:
           if self.getPartitionStatus(c) != "completed":
               return False

       if len(self.unit_partition_list[uind]) == 0:
           return False
       
       return True

   def getLocalDQ2FileList(self, uind):
       """List the local files after a dq2 retrieve"""
       import os
       filelist = []
       mj = self.getUnitMasterJob( uind )
       for sj in mj.subjobs:
           for fstr in sj.outputdata.output:
               f = fstr.split(',')[1]
               
               # ignore log files
               if f.find('.root') == -1:
                   continue

               # construct path
               if mj.outputdata.local_location:
                   #file_path = os.path.join(mj.outputdata.local_location, "%s/%s" % (mj.id, sj.id), f)
                   file_path = os.path.join(mj.outputdata.local_location, sj.outputdata.datasetname, f)
               else:
                   file_path = os.path.join(sj.outputdir, f)

               filelist.append(file_path)

       return filelist

   def getUnitMasterJob(self, uind):
       """Return the master job corresponding to this unit"""
       sj = self.getPartitionJobs( self.unit_partition_list[uind][0] )
       mj = sj[-1]._impl._getParent()
       return mj
  
   def finalise(self):
      """DQ2 get anything appropriate and merge it as well"""
      
      # find the next trfs
      task = self._getParent()
      next_trfs = []
      this_trf_id = self.getID()
      for trf in task.transforms:
          if this_trf_id in trf.required_trfs:
              next_trfs.append(trf)

      do_download = self.do_auto_download
      
      for trf in next_trfs:
          if trf.isLocalTRF():
              do_download = True

      # loop over any complete units
      for uind in range(0, len(self.unit_partition_list)):

          if not self.isUnitComplete(uind):
              continue

          # check for dq2
          if do_download or self.merger:

              # if unit is complete - dq2-get
              import os
              filelist = self.getLocalDQ2FileList( uind )

              do_dq2 = False
              for f in filelist:
                  if not os.path.exists( f ):
                      do_dq2 = True

              if do_dq2:
                  # not got all files - try the retrieve
                  mj = self.getUnitMasterJob( uind )
                  if mj.outputdata.local_location:
                      mj.outputdata.retrieve(blocking=True, subjobDownload=True, useDSNameForDir=True, outputNamesRE=".root")
                  else:
                      mj.outputdata.retrieve(blocking=True, subjobDownload=True, outputNamesRE=".root")

                  # check if the download worked
                  do_dq2 = False
                  for f2 in filelist:
                      if not os.path.exists( f2 ):
                          logger.error("Couldn't download file %s. Will retry..." % f2)
                          do_dq2 = True
                          break

              # if the dq2-get is complete, attempt the merger
              if not do_dq2 and self.merger:
                  do_merger = True
                  joblist = []

                  if self.individual_unit_merger:

                      # merging individual units
                      do_merger =  self.isUnitComplete(uind)

                      # check if this unit has been merged
                      for f in self.local_files['merge']:
                          if self.unit_outputdata_list[uind] in f:
                              do_merger = False

                      if do_merger:
                          mj = self.getUnitMasterJob( uind )
                          for sj in mj.subjobs:
                              joblist.append(sj)
                  else:

                      if len(self.local_files['merge']) != 0:
                          do_merger = False

                      # merging the whole transform
                      for uind2 in range(0, len(self.unit_partition_list)):
                          if not self.isUnitComplete(uind2):
                              do_merger = False

                      # construct a joblist for the merger
                      if do_merger:
                          for uind2 in range(0, len(self.unit_partition_list)):
                              mj = getUnitMasterJob( uind2 )
                              for sj in mj.subjobs:
                                  joblist.append(sj)


                  if do_merger:

                      # set the output directory
                      if not self.merger.sum_outputdir:
                          local_location = joblist[0]._impl._getParent().outputdir
                      else:
                          local_location = self.merger.sum_outputdir

                          # add the unit name if required
                          if self.individual_unit_merger:
                              local_location = os.path.join(self.merger.sum_outputdir, self.unit_outputdata_list[uind])

                      logger.warning("Running merger for transform %d, unit %d..." % (self.getID(), uind))

                      if not os.path.exists(local_location):
                          os.makedirs(local_location)

                      self.merger.merge( subjobs = joblist, local_location = local_location)

                      # check files are there
                      if len(os.listdir(local_location)) == 0:
                          logger.error("Problem with merger. Will retry...")
                          return
                      else:
                          logger.info("Merged unit %d from transform %d" % ( uind, self.getID()))
                          for f in os.listdir(local_location):
                              full_path = os.path.join(local_location, f)
                              self.local_files['merge'].append(full_path)

          # notify the next transforms
          for trf in next_trfs:
              if not trf.isLocalTRF() or ((not self.merger and len(self.getLocalDQ2FileList( uind )) == len(self.local_files['dq2'])) or
                                          (self.merger and len(self.local_files['merge']) > 0)):
                  if trf.status in ["running", "completed"]:
                      trf.updateInputStatus(self, uind)
              
      
   def updateInputStatus(self, ltf, uind):
      """We have a completed unit - set this one off if required"""

      # change the unit index with the offset from other required trfs
      task = self._getParent()
      full_uind = 0
      for ltf_id in self.required_trfs:

          if ltf.getID() == ltf_id:
              full_uind += uind
              break

          full_uind += len( task.transforms[ltf_id].unit_partition_list )

      # if we aren't a single unit, just copy outputdata to inputdata
      if not self.single_unit:
          
          # check if this unit is already running
          if len(self.unit_partition_list[full_uind]) > 0:
              return
          
          if not self.isLocalTRF():
              # grid job
              self.unit_inputdata_list[full_uind] = [ltf.getUnitMasterJob(uind).outputdata.datasetname]
              self.unit_outputdata_list[full_uind] = ltf.unit_outputdata_list[uind]
          else:
              # local job
              if ltf.merger:
                  self.unit_inputdata_list[full_uind] = ltf.local_files['merge']
              else:
                  self.unit_inputdata_list[full_uind] = ltf.local_files['dq2']
                  
              self.unit_outputdata_list[full_uind] = ltf.unit_outputdata_list[uind]
              
          self.createPartitionList(full_uind)
      else:
                
          # check if this unit is already running
          if len(self.unit_partition_list[0]) > 0:
              return

          # check if all required trfs are complete
          task = self._getParent()
          done = True
          self.unit_inputdata_list[0] = []
          self.unit_outputdata_list[0] = "unit"
          for ltf_id in self.required_trfs:
              for uind2 in range(0, len(task.transforms[ltf_id].unit_partition_list)):
                  if not task.transforms[ltf_id].isUnitComplete(uind2):
                      done = False
                  else:
                      if not self.isLocalTRF:
                          # grid job              
                          self.unit_inputdata_list[0].append( task.transforms[ltf_id].getUnitMasterJob(uind2).outputdata.datasetname)

              if self.isLocalTRF():
                  if task.transforms[ltf_id].merger:
                      self.unit_inputdata_list[0] += task.transforms[ltf_id].local_files['merge'] 
                  else:
                      self.unit_inputdata_list[0] += task.transforms[ltf_id].local_files['dq2'] 

          if done:
              self.createPartitionList(0)
      
   def getNextPartitions(self, n):
       """Returns the N next partitions to process"""

       # find the partitions that are available
       if not self.partition_lock:
          self.partition_lock = threading.Lock()
          
       full_partition_list = []
       new_unit_sub = False
       
       # go through each unit and return all partitions for this unit if available
       for uind in range(0, len(self.unit_partition_list)):

           # create new partition list if required
           if not new_unit_sub and len(self.required_trfs) == 0 and len(self.unit_partition_list[uind]) == 0:
               self.createPartitionList(uind)
               full_partition_list += self.unit_partition_list[uind]
               new_unit_sub = True
               continue

           # avoid this unit if complete or waiting for an upstream trf
           if len(self.unit_partition_list[uind]) == 0 or self.isUnitComplete(uind):
               continue

           # first check for completely new units
           self.partition_lock.acquire()
           partition_status_dict = {'ready':[], 'attempted':[], 'completed':[], 'killed':[], 'running':[]}
           for p in self.unit_partition_list[uind]:
               if self._partition_status[p] in partition_status_dict.keys():
                   partition_status_dict[self._partition_status[p]].append(p)
               else:
                   partition_status_dict[self._partition_status[p]] = [p]
                   
           self.partition_lock.release()

           # check for any running jobs
           if len(partition_status_dict['running']) > 0:
               continue
           
           # check for full unit submission
           if len(partition_status_dict['ready']) == len(self.unit_partition_list[uind]):
               full_partition_list += partition_status_dict['ready']
               continue

           # check for full killed units (i.e. failed build job)
           if not self.isLocalTRF() and len(partition_status_dict['killed']) == len(self.unit_partition_list[uind]):
               full_partition_list += partition_status_dict['killed']
               
               for p in partition_status_dict['killed']:
                   self.partitions_fails[p-1] += 1
                   
               # don't need to add partition fails as the number of apps will give the value

               # exclude this site
               for sj in self.getUnitMasterJob(uind).subjobs:
                   if not sj.backend.actualCE in self.backend.requirements.excluded_sites:
                       self.backend.requirements.excluded_sites.append( sj.backend.actualCE )

               logger.warning("All partitions failed in unit %d of transform %d. Rebrokering to avoid possible bad sites %s" %
                              (uind, self.getID(), self.backend.requirements.excluded_sites) )
               for p in self.unit_partition_list[ uind ]:
                   self.setPartitionStatus(p, 'bad')

               self.createPartitionList( uind )
               continue

           # check for full failed units (dodgy site?)
           if not self.isLocalTRF() and len(partition_status_dict['attempted']) == len(self.unit_partition_list[uind]):
               #full_partition_list += partition_status_dict['attempted']
               
               for p in partition_status_dict['attempted']:
                   self.partitions_fails[p-1] += 1
                   
               # don't need to add partition fails as the number of apps will give the value
           
               # exclude this site
               for sj in self.getUnitMasterJob(uind).subjobs:
                   if not sj.backend.actualCE in self.backend.requirements.excluded_sites:
                       self.backend.requirements.excluded_sites.append( sj.backend.actualCE )

               logger.warning("All partitions failed in unit %d of transform %d. Rebrokering to avoid possible bad sites %s" %
                              (uind, self.getID(), self.backend.requirements.excluded_sites) )
               
               for p in self.unit_partition_list[ uind ]:
                   self.setPartitionStatus(p, 'bad')

               self.createPartitionList( uind )
               continue
           
           if len(partition_status_dict['completed']) + len(partition_status_dict['attempted']) == len(self.unit_partition_list[uind]):
               for p in partition_status_dict['attempted']:
                   self.partitions_fails[p-1] += 1

               # check if one site failed all jobs
               mj = self.getUnitMasterJob(uind)

               if not self.isLocalTRF():
                   completed_sites = []
                   failed_sites = []
                   for sj in mj.subjobs:
                       if (sj.status == 'failed' or sj.status == 'killed') and not sj.backend.site in failed_sites:
                           failed_sites.append(sj.backend.site)
                       elif sj.status == 'completed' and not sj.backend.site in completed_sites:
                           completed_sites.append(sj.backend.site)

                   full_resubmit = False
                   for f in failed_sites:
                       if not f in completed_sites and not f in self.backend.requirements.excluded_sites:
                           self.backend.requirements.excluded_sites.append( f )
                           full_resubmit = True
                           
                   if full_resubmit:
                       full_partition_list += partition_status_dict['completed']
                       full_partition_list += partition_status_dict['attempted']

                       for p in self.unit_partition_list[ uind ]:
                           self.setPartitionStatus(p, 'bad')

                       self.createPartitionList( uind )
                       continue
               
               # resubmit failed jobs
               if mj.status != 'failed':
                   continue
               
               try:
                   mj.resubmit()

                   for p in self.unit_partition_list[uind]:
                       if self._partition_status[p] in ["attempted"]:
                           self._partition_status[p] = 'running'
                           self._app_status[ self.getPartitionApps()[p][-1] ] = 'submitting'

               except:
                   logger.error("Error attempting to resubmit master job %i. Pausing transform." % mj.id)
                   self.pause()
                   

       return full_partition_list
                     
   def getPartitionFailures(self, partition):
       """Get the number of failures from the _r? flag at the end of the DS name"""
       total = len([1 for app in [self.getPartitionApps()[partition][-1]] if app in self._app_status and self._app_status[app] in ["new","failed","killed"]])
       return self.partitions_fails[partition-1] + total

   def addUnit(self, outName, inDSList):
       """Specify the output datset name and inputDS lists for a unit"""
       self.unit_outputdata_list.append(outName)
       self.unit_inputdata_list.append(inDSList)
       self.unit_partition_list.append([])

   def createPartitionList( self, unit_num ):

      if not self.partition_lock:
          self.partition_lock = threading.Lock()
          
      # create partitions as given by the unit lists
      part_num = len(self.partitions_data) + 1
      
      if not self.backend:
          self.backend = stripProxy(GPI.Panda())
      else:
          self.backend = stripProxy(self.backend)

      if self.backend._name in ['Panda']:
          self.inputdata = DQ2Dataset()
          self.inputdata.dataset = self.unit_inputdata_list[unit_num]
          splitter = DQ2JobSplitter()
          splitter.numfiles = self.files_per_job
          logger.warning("Determining partition splitting for dataset(s) %s..." % self.inputdata.dataset )

      elif self.backend._name in ['Local', 'PBS', 'LSF']:
          self.inputdata = ATLASLocalDataset()
          self.inputdata.names = self.unit_inputdata_list[unit_num]

          splitter = AthenaSplitterJob()
          splitter.numsubjobs = int( len(self.inputdata.names) / self.files_per_job ) + 1
      else:
          self.pause()
          raise ApplicationConfigurationError(None, "Backend '%s' not supported in MultiTask mode" % self.backend._name)

      try:
          sjl = splitter.split(self)
      except:
          logger.error("Exception during split - pausing transform. Maybe no valid sites found?")
          self.pause()
          return
      
      if len(sjl) == 0:
          logger.error("Splitter didn't produce any subjobs - pausing this transform")
          self.pause()
          return
      
      self.partition_lock.acquire()
      if self.backend._name in ['Panda']:
          try:
              self.partitions_sites += [sj.backend.requirements.sites for sj in sjl]
          except AttributeError:
              self.partitions_sites += [sj.backend.site for sj in sjl]
              pass

      self.partitions_data += [sj.inputdata for sj in sjl]
      self.partitions_fails += [0 for sj in sjl]
      
      self.unit_partition_list[unit_num] = range(part_num, part_num+len(sjl) )
      self.setPartitionsLimit(len(self.partitions_data)+1)
      self.setPartitionsStatus([c for c in range(part_num,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")

      # check if we need a dummy partition to keep the transform running
      for part_list in self.unit_partition_list:
          if len(part_list) == 0:
              self.setPartitionsLimit( len(self.partitions_data)+2 )
              self.setPartitionsStatus([len(self.partitions_data)+1], "hold")
              break
                  
      self.partition_lock.release()
      
   def updateStatus(self, status):
      """Update the transform status"""
      if status != 'completed':
          self.status = status
          return

      # check that dq2-get and merger has completed
      if (self.do_auto_download or self.merger):
          import os
          for uind in range(0, len(self.unit_partition_list)):
              filelist = self.getLocalDQ2FileList( uind )

              do_dq2 = False
              for f in filelist:
                  if not os.path.exists( f ):
                      do_dq2 = True

              if do_dq2:
                  # still need to dq2-get
                  return

      if self.merger:

          if self.individual_unit_merger:

              for uind in range(0, len(self.unit_partition_list)):
                  
                  # check if this unit has been merged
                  merge_done = False
                  for f in self.local_files['merge']:
                      if self.unit_outputdata_list[uind] in f:
                          merge_done = True

                  if not merge_done:
                      # still need to merge
                      return

          else:

              if len(self.local_files['merge']) == 0:
                  return

      self.status = status

   def notifyNextTransform(self, partition):
       """ Notify any dependant transforms of the input update """
       return
