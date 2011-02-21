from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Transform
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
       'tid_list'           : SimpleItem(defvalue=[], hidden=1, modelist=["str","list"],doc='Input TID Datasets'),
       'partition_lock'    : SimpleItem( defvalue=None, typelist=None, hidden=1, transient=1, doc='Lock for partition info'),
       'required_trf'        :SimpleItem(defvalue=-1, doc='ID of Transform that this should follow'),
       'unit_partition_list': SimpleItem(defvalue=[], hidden=1, doc='Map from unit to partitions'),
       'do_auto_download'   : SimpleItem(defvalue=False, doc='Trigger automatic dq2 download of related datasets'),
       'local_files'       :  SimpleItem(defvalue={'dq2':[], 'merge':[]}, doc='Local files downloaded/merged by the completed transform'),
       'merger'            : ComponentItem('mergers', defvalue=None, load_default=0,optional=1, doc='Local merger to be done over all units when complete.')
       #'outputdata'        : ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset'),
       #'dataset_name'      : SimpleItem(defvalue="", transient=1, getter="get_dataset_name", doc='name of the output dataset'),
       }.items()))
   _category = 'transforms'
   _name = 'MultiTransform'
   _exportmethods = Transform._exportmethods + ['getID']
   
   def initialize(self):
       super(MultiTransform, self).initialize()

   def getID(self):
       """Return the index of this trf in the parent task"""
       task = self._getParent()
       if not task:
           raise ApplicationConfigurationError(None, "This transform has not been associated with a task and so there is no ID available")
       
       return task.transforms.index(self)
       
   def check(self):
      super(MultiTransform,self).check()

      if not self.partition_lock:
          self.partition_lock = threading.Lock()

      if len(self.tid_list) == 0:
          # add a dummy partition per unit 
          self.partition_lock.acquire()          
          self.setPartitionsLimit( 1 )
          self.setPartitionsStatus([1], "hold")
          self.partition_lock.release()            
          return

      # create a unit per dataset listed
      self.partitions_data = []
      self.partitions_sites = []
      part_num = 1
      self.unit_partition_list = []
      for ind in self.tid_list:
          logger.warning("Determining partition splitting for dataset %s..." % ind )
          
          self.backend = stripProxy(GPI.Panda())

          self.inputdata = DQ2Dataset()
          self.inputdata.dataset = ind
          
          splitter = DQ2JobSplitter()
          splitter.numfiles = self.files_per_job
          
          sjl = splitter.split(self) # This works even for Panda, no special "Job" properties are used anywhere.
          self.partitions_data += [sj.inputdata for sj in sjl]
          
          try:
              self.partitions_sites += [sj.backend.requirements.sites for sj in sjl]
          except AttributeError:
              self.partitions_sites += [sj.backend.site for sj in sjl]
              pass

          self.unit_partition_list.append(range(part_num, part_num+len(sjl) ) )
          part_num += len(sjl)
              
      self.partition_lock.acquire()
      self.setPartitionsLimit(len(self.partitions_data)+1)
      self.setPartitionsStatus([c for c in range(1,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")
      self.partition_lock.release()
      
   def getJobsForPartitions(self, partitions):

      # divide into site boundaries
      alljobs = []

      for unit in self.unit_partition_list:
          if len(unit) == 0:
              continue

          # find the unit that is being requested
          done = False
          for p in unit:
              if p in partitions:
                  done = True

          if not done:
              continue

          j = self.createNewJob(unit[0])
          j.backend = self.backend #= stripProxy(GPI.Panda())
          if self.outputdata:
              j.outputdata = self.outputdata
          else:
              j.outputdata = DQ2OutputDataset()
          
          if len(unit) >= 1:
              j.splitter = AnaTaskSplitterJob()
              j.splitter.subjobs = unit

          j.inputdata = self.partitions_data[ unit[0]-1]

          if self.backend._name in ['LCG', 'Panda', 'CREAM']:
              j.backend.site = self.partitions_sites[ unit[0]-1]

          if stripProxy(j.backend)._name == "Panda" and j.application.atlas_exetype == "ATHENA":
              j.outputdata.outputdata=[]
              
          task = self._getParent()

          if j.outputdata._impl._name == 'DQ2OutputDataset':
              dsn = ["user",getNickname(),task.creation_date,"%i.t_%s_%s_%s" % (j.id, task.id,
                                                                                task.transforms.index(self),
                                                                                self.unit_partition_list.index(unit))]
              j.outputdata.datasetname = ".".join(dsn)
              
          alljobs.append(j)

      return alljobs

   def notifyNextTransform(self, partition):
      """ Notify any dependant transforms of the input update """

      # find the next trfs
      task = self._getParent()
      next_trfs = []
      do_download = self.do_auto_download
      if task:
          trf_index = task.transforms.index(self)
          for trf in task.transforms:
              if trf.required_trf == trf_index:
                  next_trfs.append( trf )
                  if not trf.backend._name in ['LCG', 'Panda', 'CREAM']:
                      do_download = True
                      
      if len(next_trfs) == 0:
          return
      
      # first, perform any dq2-gets required
      if do_download or self.merger:
          unit_list = []
          unit_index = 0
          for ul in self.unit_partition_list:
              if partition in ul:
                  unit_list = ul
                  break
              unit_index += 1

          if len(unit_list) > 0:
              done = True
              for c in unit_list:
                  if self.getPartitionStatus(c) != "completed":
                      done = False

              if done:
                  # this unit is now complete, try to download
                  import os
                  sj = self.getPartitionJobs( unit_list[0] )
                  mj = sj[-1]._impl._getParent()

                  # check that we need to do anything
                  dq2_get_done = True
                  for sj in mj.subjobs:
                      for fstr in sj.outputdata.output:
                          f = fstr.split(',')[1]
                          
                          # ignore log files
                          if f.endswith('log.tgz'):
                              continue
                          
                          if mj.outputdata.local_location:
                              file_path = os.path.join(mj.outputdata.local_location, "%s/%s" % (mj.id, sj.id), f)
                          else:
                              file_path = os.path.join(sj.outputdir, f)
                              
                          if not os.path.exists( file_path ):
                              # trigger download
                              dq2_get_done = False

                  if not dq2_get_done:
                      mj.outputdata.retrieve(blocking=True, subjobDownload=True)

                      # check for complete download
                      for sj in mj.subjobs:
                          for fstr in sj.outputdata.output:
                              f = fstr.split(',')[1]

                              # ignore log files
                              if f.endswith('log.tgz'):
                                  continue

                              if mj.outputdata.local_location:
                                  file_path = os.path.join(mj.outputdata.local_location, "%s" % mj.id, f)
                              else:
                                  file_path = os.path.join(sj.outputdir, f)
                                                   
                              if os.path.exists( file_path ):
                                  self.local_files['dq2'].append(file_path)
                              else:
                                  logger.error("Couldn't download file %s" % file_path)
                              
                              
      # now perform the merger if all units complete
      if self.merger and len(self.unit_partition_list) > 0:
          done = True
          joblist = []
          for ul in self.unit_partition_list:
              for c in ul:
                  if self.getPartitionStatus(c) != "completed":
                      done = False
                  else:
                      joblist.append( self.getPartitionJobs( c )[-1] )

          for j in joblist:
              if j.status != 'completed':
                  done = False
                  
          if done:
              # this transform is now complete, try to merge
              if not self.merger.sum_outputdir:
                  local_location = joblist[0]._impl._getParent().outputdir
              else:
                  local_location = self.merger.sum_outputdir

              # check if the merger has already happened
              for f in joblist[0].outputdata.outputdata:
                  full_path = os.path.join(local_location, f)
                  if not full_path in self.local_files['merge']:
                      logger.warning("Running merger for transform %d..." % self.getID())
                      self.merger.merge( subjobs = joblist, local_location = local_location)
                      break

              # check files are there
              for f in joblist[0].outputdata.outputdata:
                  full_path = os.path.join(local_location, f)
                  if not os.path.exists( full_path ):
                      logger.error("Merger failed. Will retry.")
                  else:
                      self.local_files['merge'].append(full_path)
                      
              
      # now notify the next transform now all inputs are ready
      if not self.merger or len(self.local_files['merge']) > 0:
          for trf in next_trfs:
              trf.updateInputStatus(self, partition)
            
   def updateInputStatus(self, ltf, partition):
      # Check units that use this partition

      # Find the unit this partition belongs to
      unit_list = []
      unit_index = 0

      if not ltf.merger:
          for ul in ltf.unit_partition_list:
              if partition in ul:
                  unit_list = ul
                  break
              unit_index += 1

          if len(unit_list) == 0:
              logger.warning("Could not find unit that partition %d belongs to." % partition)
              return
      else:
          # last trf had a merger so we down to one unit - all units must therefore have completed
          for ul in ltf.unit_partition_list:
              unit_list += ul

          unit_index = 0
          
      done = True
      for c in unit_list:
          if ltf.getPartitionStatus(c) != "completed":
              done = False

          
      # check if this partition has been started
      if unit_index < len(self.unit_partition_list) and len(self.unit_partition_list[ unit_index ]) == 0 and done:
          if not self.partition_lock:
              self.partition_lock = threading.Lock()
                        
          # find outputdataset name for this unit and add to the tid list - unit => 1 site => output dataset
          jobs = ltf.getPartitionJobs(unit_list[0])

          # create a unit per dataset listed - note that we select the last job in the list in case to retries
          mj = jobs[-1]._impl._getParent()
          if not mj:
              logger.error("Could not get master job of job id %d - can not continue setting up transform." % jobs[-1].id)
              return
          
          part_num = len(self.partitions_data)+1
          ind = mj.outputdata.datasetname
          logger.warning("Determining partition splitting for dataset %s..." % ind )

          if not self.backend:
              self.backend = stripProxy(GPI.Panda())

          if self.backend._name in ['LCG', 'Panda', 'CREAM']:
              self.inputdata = DQ2Dataset()
              self.inputdata.dataset = ind

              splitter = DQ2JobSplitter()
              splitter.numfiles = self.files_per_job

              sjl = splitter.split(self) # This works even for Panda, no special "Job" properties are used anywhere.

              
          else:
              self.inputdata = ATLASLocalDataset()
              if len(ltf.local_files['merge']) > 0:
                  self.inputdata.names = ltf.local_files['merge']
              else:
                  self.inputdata.names = ltf.local_files['dq2']
                  
              splitter = AthenaSplitterJob()
              splitter.numsubjobs = int( len(self.inputdata.names) / self.files_per_job ) + 1
              sjl = splitter.split(self) # This works even for Panda, no special "Job" properties are used anywhere.
              
          self.partition_lock.acquire()
          self.partitions_data += [sj.inputdata for sj in sjl]

          if self.backend._name in ['LCG', 'Panda', 'CREAM']:
              try:
                  self.partitions_sites += [sj.backend.requirements.sites for sj in sjl]
              except AttributeError:
                  self.partitions_sites += [sj.backend.site for sj in sjl]
                  pass

          self.unit_partition_list[ unit_index ] = range(part_num, part_num+len(sjl) )
                                          
          self.setPartitionsLimit(len(self.partitions_data)+1)
          self.setPartitionsStatus([c for c in range(part_num,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")

          # check if we need a dummy partition to keep the transform running
          for part_list in self.unit_partition_list:
              if len(part_list) == 0:
                  self.setPartitionsLimit( len(self.partitions_data)+2 )
                  self.setPartitionsStatus([len(self.partitions_data)+1], "hold")
                  break
                  
          self.partition_lock.release()
      
          
   def getNextPartitions(self, n):
       """Returns the N next partitions to process"""
       # find the partitions that are available
       if not self.partition_lock:
          self.partition_lock = threading.Lock()
          
       self.partition_lock.acquire()
       full_partition_list = []

       # go through each unit and return all partitions for this unit if available
       for unit in self.unit_partition_list:
           if len(unit) == 0:
               continue

           # first check for completely new units
           partition_list = []
           for p in unit:
               if self._partition_status[p] in ["ready"]:
                   partition_list.append(p)
           
           if len(partition_list) != len(unit):
               
               # not all partitions in this unit are avilable - check for failed attempts
               partition_list = []
               attempted = False
               for p in unit:
                   if self._partition_status[p] in ["attempted"]:
                       attempted = True
                       
                   if self._partition_status[p] in ["attempted", "completed"]:
                       partition_list.append(p)

               if not attempted or len(partition_list) != len(unit):
                   continue

               # resubmit the failed jobs               
               sj = self.getPartitionJobs( unit[0] )
               mj = sj[-1]._impl._getParent()
               if mj.status == 'failed':
                   for p in unit:
                       if self._partition_status[p] in ["attempted"]:
                           self._partition_status[p] = 'ready'
                           self._app_status[ self.getPartitionApps()[p][-1] ] = 'submitting'
                           
                   mj.resubmit()

               continue
               
           full_partition_list += partition_list

           if len(full_partition_list) > n:
               break
           
       self.partition_lock.release()

       return full_partition_list
                     
   def getPartitionFailures(self, partition):
       """Get the number of failures from the _r? flag at the end of the DS name"""
       if not hasattr(self.getPartitionJobs( partition )[-1].outputdata, "datasetname"):
           return 0
       
       outds = self.getPartitionJobs( partition )[-1].outputdata.datasetname
       pos = outds.rfind('_r')
       if pos > -1:
           return int(outds[pos+2:])
       else:
           return 0
