from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Transform
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from TaskApplication import AthenaTask, AnaTaskSplitterJob
from GangaAtlas.Lib.ATLASDataset.ATLASDataset import ATLASLocalDataset,ATLASOutputDataset
from GangaAtlas.Lib.Athena.Athena import AthenaSplitterJob
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy

from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2
from dq2.common.DQException import DQException

#config.addOption('cloudPreference',[],'list of preferred clouds to choose for AnaTask analysis')
#config.addOption('backendPreference',["LCG","Panda","NG"],'order of preferred backends (LCG, Panda, NG) for AnaTask analysis')

import time
import threading
import copy
import os

from dq2.info import TiersOfATLAS
from GangaAtlas.Lib.ATLASDataset import whichCloud
from Ganga.Core.exceptions import ApplicationConfigurationError

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 
from Ganga.Utility.Config import getConfig

configDQ2 = getConfig('DQ2')


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
       'files_per_job'     : SimpleItem(defvalue=5, doc='files per job (cf DQ2JobSplitter.numfiles)', modelist=["int"]),
       'MB_per_job'     : SimpleItem(defvalue=0, doc='Split by total input filesize (cf DQ2JobSplitter.filesize)', modelist=["int"]),
       'subjobs_per_unit'     : SimpleItem(defvalue=0, doc='split into this many subjobs per unit master job (cf DQ2JobSplitter.numsubjobs)', modelist=["int"]),
       'partitions_data'   : ComponentItem('datasets', defvalue=[], optional=1, sequence=1, hidden=1, doc='Input dataset for each partition'),
       'partitions_sites'  : SimpleItem(defvalue=[], hidden=1, modelist=["str","list"],doc='Input site for each partition'),
       'partitions_fails'  : SimpleItem(defvalue=[], hidden=1, modelist=["list"],doc='Number of failures for each partition'),
       'partition_lock'    : SimpleItem( defvalue=None, typelist=None, hidden=1, transient=1, doc='Lock for partition info'),
       'required_trfs'        :SimpleItem(defvalue=[], doc='ID of Transform that this should follow'),
       'unit_partition_list': SimpleItem(defvalue=[], hidden=1, doc='Map from unit to partitions'),
       'unit_inputdata_list': SimpleItem(defvalue=[], hidden=1, doc='Map from unit to inputdata'),
       'unit_outputdata_list': SimpleItem(defvalue=[], hidden=1, doc='Map from unit to outputdata'),
       'unit_state_list': SimpleItem(defvalue=[], hidden=1, doc='An array storing the unit states'),
       'unit_job_list': SimpleItem(defvalue=[], transient=1,hidden=1, doc='An array mapping from units to jobs'),
       'do_auto_download'   : SimpleItem(defvalue=False, doc='Trigger automatic dq2 download of related datasets'),
       'individual_unit_merger'   : SimpleItem(defvalue=False, doc='Run the merger per unit rather than on the whole transform'),
       'single_unit'   : SimpleItem(defvalue=False, doc='Reduce to a single unit that runs over the all outputs from all required trfs'),
       'local_files'       :  SimpleItem(defvalue={'dq2':[], 'merge':[]}, doc='Local files downloaded/merged by the completed transform'),
       'merger'            : ComponentItem('mergers', defvalue=None, load_default=0,optional=1, doc='Local merger to be done over all units when complete.'),
       'rebroker_on_job_fail' : SimpleItem(defvalue=False, doc='If the maximum number of job failures is hit, rebroker the unit'),
       'splitter'            : ComponentItem('splitters', defvalue=None, load_default=0,optional=1, doc='Splitter to be used for this transform. Note that split options specified in the transform WILL NOT overwrite these settings.')
       #'outputdata'        : ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset'),
       #'dataset_name'      : SimpleItem(defvalue="", transient=1, getter="get_dataset_name", doc='name of the output dataset'),
       }.items()))
   _category = 'transforms'
   _name = 'MultiTransform'
   _exportmethods = Transform._exportmethods + ['getID', 'addUnit', 'activateUnit', 'deactivateUnit', 'getUnitJob', 'forceUnitCompletion', 'resetUnit', 'getContainerName', 'getNumUnits', 'getUnitsFromPartitions', 'isLocalTRF', 'isUnitComplete', 'getLocalDQ2FileList', 'getAllUnitJobs', 'listUnitDatasets', 'listAllDatasets', 'getUnitContainerName', 'checkContainerContents', 'unitOverview', 'getUnitInputList','getUnitOutputList' ]
   
   def initialize(self):
       super(MultiTransform, self).initialize()
       self.backend = None

   def getUnitInputList(self):
       return copy.deepcopy(self.unit_inputdata_list)

   def getUnitOutputList(self):
       return copy.deepcopy(self.unit_outputdata_list)
   
   def getNumUnits(self):
       return len(self.unit_partition_list)
   
   def getID(self):
       """Return the index of this trf in the parent task"""
       task = self._getParent()
       if not task:
           raise ApplicationConfigurationError(None, "This transform has not been associated with a task and so there is no ID available")
       
       return task.transforms.index(self)

   def unitOverview(self, status = ''):
       """User function to show the unit status"""
       self.unit_overview(status)
       
   def unit_overview(self, status):
       """Show the status of the units in this transform"""
       for uind in range(0, len(self.unit_outputdata_list)):

           is_complete = self.isUnitComplete(uind)
           
           # display colour given state
           o = ""
           o += ("%d:  " % uind) + self.unit_outputdata_list[uind]
           
           # is unit active?          
           if self.unit_state_list[uind]['active']:
               o += " " * (40-len(o) + 3) + "*"
           else:
               o += " " * (40-len(o) + 3) + "-"

           # is unit configured?
           if self.unit_state_list[uind]['configured']:
               o += "\t"+" " * 3 + "*"
           else:
               o += "\t"+" " * 3 + "-"
               
           # is unit submitted?
           if self.unit_state_list[uind]['submitted']:
               o += "\t"+" " * 3 + "*"
           else:
               o += "\t"+" " * 3 + "-"

           # is unit downloaded?
           if self.unit_state_list[uind]['download']:
               o += "\t"+" " * 3 + "*"
           else:
               o += "\t"+" " * 3 + "-"

           # is unit Merged?
           if self.unit_state_list[uind]['merged']:
               o += "\t"+" " * 3 + "*"
           else:
               o += "\t"+" " * 3 + "-"

           # is unit Complete?
           if is_complete:
               o += "\t"+" " * 2 + "*"
           else:
               o += "\t"+" " * 2 + "-"    
               
           # Number of exceptions
           o += "\t" +" " * 3 + "%d" % self.unit_state_list[uind]['exceptions']
           
           # Any reasons?
           o += "\t" + self.unit_state_list[uind]['reason']

           # change colour on state
           if is_complete:
               o = markup(o,overview_colours["completed"])
           elif not self.unit_state_list[uind]['active']:
               o = markup(o,overview_colours["bad"])
           elif not self.unit_state_list[uind]['configured']:
               o = markup(o,overview_colours["hold"])
           elif not self.unit_state_list[uind]['submitted']:
               o = markup(o,overview_colours["ready"])  
           else:
               o = markup(o,overview_colours["running"])

           # print depending on status
           if is_complete and status == 'completed':
               print o
           elif not self.unit_state_list[uind]['active'] and status == 'bad':
               print o
           elif not self.unit_state_list[uind]['configured'] and status == 'hold':
               print o
           elif not self.unit_state_list[uind]['submitted'] and status == 'ready':
               print o
           elif status == 'running' and self.unit_state_list[uind]['active'] and self.unit_state_list[uind]['configured'] and self.unit_state_list[uind]['submitted']:
               print o
           elif status == '':
               print o

                                                                                               
   def check(self):
      super(MultiTransform,self).check()

      # create the partition lock if required
      if not self.partition_lock:
          self.partition_lock = threading.Lock()

      # backwards compatibility
      if len(self.unit_state_list) == 0:
          for uind in range(0, len(self.unit_partition_list)):
              self.unit_state_list.append({'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force':False})

      for uind in range(0, len(self.unit_partition_list)):
          if not 'active' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['active'] = True

          if not 'configured' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['configured'] = False

          if not 'submitted' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['submitted'] = False

          if not 'download' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['download'] = False

          if not 'merged' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['merged'] = False

          if not 'reason' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['reason'] = ''
              
          if not 'exceptions' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['exceptions'] = 0

          if not 'force' in self.unit_state_list[uind].keys():
              self.unit_state_list[uind]['force'] = False
              

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
          dsn = [self.getContainerName()[:-1],
                 self.unit_outputdata_list[uind],
                 "j%i.t%i.trf%i.u%i" % (j.id, task.id,
                                    task.transforms.index(self),
                                    uind)]

          # check for DS name too long
          if len(".".join(dsn)) > configDQ2['OUTPUTDATASET_NAMELENGTH'] - 2:
              dsn = [self.getContainerName()[:-1],
                     self.unit_outputdata_list[uind][: - (len(".".join(dsn)) - configDQ2['OUTPUTDATASET_NAMELENGTH'] + 2)],
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

       # check for force complete
       if self.unit_state_list[uind]['force']:
           return True
           
       for c in self.unit_partition_list[uind]:
           if self.getPartitionStatus(c) != "completed":
               return False

       if len(self.unit_partition_list[uind]) == 0:
           return False

       # check for merged job completion
       mj = self.getUnitMasterJob( uind )

       if not mj or mj.status != "completed":
           return False

       return True

   def getLocalDQ2FileList(self, uind):
       """List the local files after a dq2 retrieve"""
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

   def getUnitMasterJob(self, uind, proxy=False):
       """Return the master job corresponding to this unit"""
       if uind < 0 or uind > len(self.unit_partition_list):
           return None

       if len(self.unit_partition_list[uind]) == 0:
           return None
       
       # update the unit_job_list if necessary
       if len(self.unit_job_list) == 0:
           for uind2 in range(0, len(self.unit_partition_list)):
               self.unit_job_list.append(None)
       
       if self.unit_job_list[uind]:
           if proxy:
               return self.unit_job_list[uind].master
           else:
               return self.unit_job_list[uind]._impl._getParent()
       
       sj = self.getPartitionJobs( self.unit_partition_list[uind][0] )

       if not sj:
           return None

       # check for valid subjob
       if sj[-1].master:
           self.unit_job_list[uind] = sj[-1]
       
       if proxy:
           mj = sj[-1].master
       else:
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

      if self.merger:
          do_download = True

      # check for forced units and set partitions within to 'bad'
      for uind2 in range(0, len(self.unit_partition_list)):
          if self.unit_state_list[uind2]['force']:
              for p in self.unit_partition_list[uind2]:
                  if self._partition_status[p] in ['attempted','failed']:
                      self.setPartitionStatus(p, 'bad')

      # find unit with least exceptions raised
      min_excep = 5
      uind = -1
      for uind2 in range(0, len(self.unit_partition_list)):

          # update hte submitted flag
          try:
              mj = self.getUnitMasterJob( uind2 )
              if mj and len(self.unit_partition_list[uind2]) > 0:
                  self.unit_state_list[uind2]['submitted'] = True
          except:
              continue

          if not self.isUnitComplete(uind2) or not self.unit_state_list[uind2]['active']:
              continue

          # deactivate any units with greater than 3 exceptions
          if self.unit_state_list[uind2]['exceptions'] > 5:
              logger.error("Too many exceptions downloading and/or merging for unit '%s' in Transform %d, Task %d. Deactivating unit." % (self.unit_outputdata_list[uind2], self.getID(), self._getParent().id))
              self.unit_state_list[uind2]['reason'] = 'Too many exceptions.'
              self.unit_state_list[uind2]['active'] = False
              continue

          # notify the next transforms
          for trf in next_trfs:
              if not trf.isLocalTRF() or ((not self.merger and len(self.getLocalDQ2FileList( uind2 )) == len(self.local_files['dq2'])) or
                                          (self.merger and len(self.local_files['merge']) > 0)):
                  if trf.status in ["running", "completed"]:
                      trf.updateInputStatus(self, uind2)


          # Find the least error-prone unit to dq2-get/merge from
          if do_download and self.unit_state_list[uind2]['exceptions'] < min_excep:
              
              # check if this needs DQ2 or merger
              filelist = self.getLocalDQ2FileList( uind2 )
              do_dq2 = False
              for f in filelist:
                  if not os.path.exists( f ) or os.path.getsize( f ) < 10:
                      do_dq2 = True

              do_merger = False
              if self.merger and self.individual_unit_merger:
                  do_merger = True
                  for f in self.local_files['merge']:
                      if self.unit_outputdata_list[uind2] in f:
                          do_merger = False
                      
              if do_dq2 or do_merger:
                  min_excep = self.unit_state_list[uind2]['exceptions']
                  uind = uind2
              
      # Perform required dq2-get/merge
      if uind > -1:

          # check for dq2
          if do_download or self.merger:

              # if unit is complete - dq2-get
              filelist = self.getLocalDQ2FileList( uind )

              do_dq2 = False
              for f in filelist:
                  if not os.path.exists( f ):
                      do_dq2 = True
                  else:
                      # check if the file was OK
                      if os.path.getsize( f ) < 10:
                          logger.warning("Previous problem with dq2-get. Removing file %s." % f)
                          self.unit_state_list[uind]['reason'] = "Problem with dq2-get."
                          self.unit_state_list[uind]['exceptions'] += 1
                          os.remove(f)
                          do_dq2 = True

              if do_dq2:
                  # not got all files - try the retrieve
                  mj = self.getUnitMasterJob( uind )
                  try:
                      if mj.outputdata.local_location:
                          mj.outputdata.retrieve(blocking=True, subjobDownload=True, useDSNameForDir=True, outputNamesRE=".root")
                      else:
                          mj.outputdata.retrieve(blocking=True, subjobDownload=True, outputNamesRE=".root")

                  except Exception, x:
                      logger.error("Exception during retrieve %s %s" % (x.__class__,x))
                  
                  # check if the download worked
                  do_dq2 = False
                  for f2 in filelist:
                      if not os.path.exists( f2 ):
                          logger.warning("Couldn't download file %s." % f2)
                          do_dq2 = True
                          self.unit_state_list[uind]['reason'] = "Problem with dq2-get."
                          self.unit_state_list[uind]['exceptions'] += 1
                          break
                      else:
                          # check if the file was OK
                          if os.path.getsize( f2 ) < 10:
                              logger.warning("Problem with dq2-get - removing file %s." % f2)
                              self.unit_state_list[uind]['reason'] = "Problem with dq2-get."
                              self.unit_state_list[uind]['exceptions'] += 1
                              os.remove(f2)
                              do_dq2 = True
                              break

                  if not do_dq2:
                      self.unit_state_list[uind]['reason'] = ''
                      self.unit_state_list[uind]['download'] = True

                  # only do the dq2-get in each cycle
                  return
              
              # if the dq2-get is complete, attempt the merger
              if not do_dq2 and self.merger:
                  
                  do_merger = True
                  joblist = []

                  if self.individual_unit_merger:

                      # merging individual units
                      do_merger =  self.isUnitComplete(uind)

                      if do_merger and not self.unit_state_list[0]['merged']:
                          mj = self.getUnitMasterJob( uind )
                          for sj in mj.subjobs:
                              joblist.append(sj)


                  if do_merger:

                      # set the output directory
                      if not self.merger.sum_outputdir:
                          local_location = joblist[0]._getParent().outputdir
                      else:
                          local_location = self.merger.sum_outputdir

                      logger.warning("Running merger for transform %d, unit %d..." % (self.getID(), uind))

                      if not os.path.exists(local_location):
                          os.makedirs(local_location)

                      try:
                          self.merger.merge( subjobs = joblist, local_location = local_location)
                      except Exception, x:
                          logger.error("Exception during merger %s %s" % (x.__class__,x))
                      
                      # check files are there
                      if len(os.listdir(local_location)) == 0:
                          logger.warning("Problem with merger.")
                          self.unit_state_list[uind]['reason'] = "Problem with merger."
                          self.unit_state_list[uind]['exceptions'] += 1
                          return
                      else:
                          logger.info("Merged unit %d from transform %d" % ( uind, self.getID()))
                          self.unit_state_list[uind]['merged'] = True
                          for f in os.listdir(local_location):
                              full_path = os.path.join(local_location, f)
                              self.local_files['merge'].append(full_path)


      # do full unit merger if requested
      if self.merger and not self.individual_unit_merger and not self.unit_state_list[0]['merged']:

          # check all downloads are complete
          do_merger = True
          for uind in range(0, len(self.unit_partition_list)):              
              if self.unit_state_list[uind]['active'] and not self.unit_state_list[uind]['download']:
                  do_merger = False

          joblist = []
          
          if do_merger:

              # construct a joblist for the merger
              for uind2 in range(0, len(self.unit_partition_list)):
                  if not self.unit_state_list[uind]['active']:
                      continue
                  
                  mj = self.getUnitMasterJob( uind2 )
                  for sj in mj.subjobs:
                      joblist.append(sj)
              
              # set the output directory
              if not self.merger.sum_outputdir:
                  local_location = joblist[0]._getParent().outputdir
              else:
                  local_location = self.merger.sum_outputdir

              # add the unit name if required
              logger.warning("Running merger across whole of transform %d ..." % (self.getID()))

              if not os.path.exists(local_location):
                  os.makedirs(local_location)

              try:
                  self.merger.merge( subjobs = joblist, local_location = local_location)
              except Exception, x:
                  logger.error("Exception during merger %s %s" % (x.__class__,x))
                      
              # check files are there
              if len(os.listdir(local_location)) == 0:
                  logger.warning("Problem with merger.")
                  for uind in range(0, len(self.unit_partition_list)):
                      self.unit_state_list[uind]['reason'] = "Problem with merger."
                      self.unit_state_list[uind]['exceptions'] += 1
                  return
              else:
                  logger.info("Merged transform %d" % ( self.getID()))
                  for uind in range(0, len(self.unit_partition_list)):                      
                      self.unit_state_list[uind]['merged'] = True
                      
                  for f in os.listdir(local_location):
                      full_path = os.path.join(local_location, f)
                      self.local_files['merge'].append(full_path)
          
   def updateInputStatus(self, ltf, uind):
      """We have a completed unit - set this one off if required"""

      # change the unit index with the offset from other required trfs
      task = self._getParent()
      full_uind = 0
      for ltf_id in self.required_trfs:

          if ltf.getID() == ltf_id:
              if not ltf.merger or ltf.individual_unit_merger:                  
                  full_uind += uind
              break

          full_uind += len( task.transforms[ltf_id].unit_partition_list )

      # if we aren't a single unit, just copy outputdata to inputdata
      if not self.single_unit:
          
          # check if this unit is already running
          if len(self.unit_partition_list[full_uind]) > 0 or not self.unit_state_list[full_uind]['active']:
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
          if len(self.unit_partition_list[0]) > 0 or not self.unit_state_list[0]['active']:
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

       #logger.warning("--------    Entering getNextPartitions for Task %d, trf %d (%s)" % (self._getParent().id, self.getID(), time.time()))
       
       # find the partitions that are available
       if not self.partition_lock:
          self.partition_lock = threading.Lock()
          
       full_partition_list = []

       # go through each unit and return all partitions for this unit if available
       for uind in range(0, len(self.unit_partition_list)):

           #logger.warning("Checking Unit %d (%s)" % (uind, time.time()))
           
           mj = self.getUnitMasterJob(uind)

           # if active, ignore
           if not self.unit_state_list[uind]['active']:
               continue
           
           #logger.warning("Unit %d is active (%s)" % (uind, time.time()))
           
           # create new partition list if required
           if len(self.required_trfs) == 0 and len(self.unit_partition_list[uind]) == 0:
               self.createPartitionList(uind)
               full_partition_list += self.unit_partition_list[uind]
               break

           #logger.warning("Unit %d did not create partition list (%s)" % (uind, time.time()))
                      
           # avoid this unit if complete or waiting for an upstream trf
           if len(self.unit_partition_list[uind]) == 0 or self.isUnitComplete(uind):
               continue

           #logger.warning("Unit %d is NOT complete and has a valid partition list (%s)" % (uind, time.time()))
           
           # first check for completely new units
           self.partition_lock.acquire()
           partition_status_dict = {'ready':[], 'attempted':[], 'completed':[], 'killed':[], 'running':[]}
           for p in self.unit_partition_list[uind]:
               if self._partition_status[p] in partition_status_dict.keys():
                   partition_status_dict[self._partition_status[p]].append(p)
               else:
                   partition_status_dict[self._partition_status[p]] = [p]

           self.partition_lock.release()

           # check for too many failures
           for p in self.unit_partition_list[uind]:
               if self._partition_status[p] in ["failed"]:
                   if not self.rebroker_on_job_fail:
                       logger.error("Too many failures for partition %s in Transform %d, Task %d. Deactivating unit '%s'." % (p, self.getID(), self._getParent().id, self.unit_outputdata_list[uind]))
                       self.unit_state_list[uind]['reason'] = "Too many job failures in unit"
                       self.unit_state_list[uind]['active'] = False
                       continue
                   else:
                       logger.warning("Too many failures for partition %s in Transform %d, Task %d. Rebrokering unit '%s'." % (p, self.getID(), self._getParent().id, self.unit_outputdata_list[uind]))
                       # add sites to excluded
                       for sj in mj.subjobs:
                           if sj.status == 'failed':
                               self.backend.requirements.excluded_sites.append( sj.backend.actualCE )
                   
                       # mark partitions as bad
                       for p in self.unit_partition_list[ uind ]:
                           self.setPartitionStatus(p, 'bad')                       

                       self.removeDatasetsFromContainers(mj)
               
                       # recreate partition list
                       self.unit_partition_list[uind] = []
                       if len(self.unit_job_list) > 0:
                           self.unit_job_list[uind] = None
                       self.unit_state_list[uind] = {'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force':False}
                       self.createPartitionList( uind )
                       break

           #logger.warning("Unit %d hasn't had too many failures (%s)" % (uind, time.time()))
               
           # check for full unit submission
           if len(partition_status_dict['ready']) == len(self.unit_partition_list[uind]):
               full_partition_list += partition_status_dict['ready']
               #continue
               break

           #logger.warning("Unit %d does not need full submission (%s)" % (uind, time.time()))

           #if mj:
           #    logger.warning("Checks for Unit %d: %d, %d, %s (%s)" % (uind, len(partition_status_dict['running']), mj.id, mj.status, time.time()))
           #else:
           #    logger.warning("Unit %d has no master job associated (%s)" % (uind, time.time()))
           
           # check for any running jobs
           if len(partition_status_dict['running']) > 0 and mj and not mj.status in ['failed', 'killed']:
               continue

           
           if not mj:
               continue
           
           # --------------------------------------
           # check for failed build jobs within unit
           build_fail = False
           for sj in mj.subjobs:
               if sj.status == 'killed':
                   self.backend.requirements.excluded_sites.append( sj.backend.actualCE )
                   build_fail = True

           if build_fail:
               logger.warning("killed subjobs found in unit %d. Assuming failed build job - will rebroker unit." % uind)
               
               # mark partitions as bad
               for p in self.unit_partition_list[ uind ]:
                   self.setPartitionStatus(p, 'bad')

               self.removeDatasetsFromContainers(mj)
               
               # recreate partition list
               self.unit_state_list[uind]['configured'] = False
               self.unit_state_list[uind]['submitted'] = False
               self.createPartitionList( uind )
               break

           #logger.warning("Unit %d didn't not have a failed build job (%s)" % (uind, time.time()))
           
           # --------------------------------------
           # check for full failed units (dodgy site?)
           if not self.isLocalTRF() and len(partition_status_dict['attempted']) == len(self.unit_partition_list[uind]) and len(self.unit_partition_list[uind]) > 2:
               #full_partition_list += partition_status_dict['attempted']
               
               #logger.warning("Unit %d has been flagged as fully failed (%s)" % (uind, time.time()))
               
               if not mj.status in ['failed', 'killed']:
                   continue
               
               #for p in partition_status_dict['attempted']:
               #    self.partitions_fails[p-1] += 1
                   
               # don't need to add partition fails as the number of apps will give the value
           
               # exclude this site
               for sj in self.getUnitMasterJob(uind).subjobs:
                   if not sj.backend.actualCE in self.backend.requirements.excluded_sites:
                       self.backend.requirements.excluded_sites.append( sj.backend.actualCE )

               logger.warning("All partitions failed in unit %d of transform %d. Rebrokering to avoid possible bad sites %s" %
                              (uind, self.getID(), self.backend.requirements.excluded_sites) )
               
               for p in self.unit_partition_list[ uind ]:
                   self.setPartitionStatus(p, 'bad')
                   
               self.removeDatasetsFromContainers(mj)
                   
               self.unit_state_list[uind]['configured'] = False
               self.unit_state_list[uind]['submitted'] = False
               self.createPartitionList( uind )
               #continue
               break

           if len(partition_status_dict['completed']) + len(partition_status_dict['attempted']) == len(self.unit_partition_list[uind]):

               #logger.warning("Unit %d has been flagged as partially failed (%s)" % (uind, time.time()))
               
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
                           num_fails = 0
                           for sj in mj.subjobs:
                               if sj.backend.site == f:
                                   num_fails += 1

                           if num_fails > 2:
                               self.backend.requirements.excluded_sites.append( f )
                               full_resubmit = True

                   if full_resubmit:
                       #full_partition_list += partition_status_dict['completed']
                       #full_partition_list += partition_status_dict['attempted']

                       for p in self.unit_partition_list[ uind ]:
                           self.setPartitionStatus(p, 'bad')

                       #for p in partition_status_dict['attempted']:
                       #    self.partitions_fails[p-1] += 1
                   
                       self.removeDatasetsFromContainers(mj)
                       self.unit_state_list[uind]['configured'] = False
                       self.unit_state_list[uind]['submitted'] = False
                       self.createPartitionList( uind )
                       #continue
                       break

               #logger.warning("Unit %d has not had full resubmit and is at status %s (%s)" % (uind, mj.status, time.time()))
               
               # resubmit failed jobs
               if mj.status != 'failed':
                   continue

               for p in partition_status_dict['attempted']:
                   self.partitions_fails[p-1] += 1

               try:
                   mj.resubmit()
                   #logger.warning("Unit %d attempting resubmit (%s)" % (uind, time.time()))
                   
                   for p in self.unit_partition_list[uind]:
                       if self._partition_status[p] in ["attempted"]:
                           self._partition_status[p] = 'running'
                           self._app_status[ self.getPartitionApps()[p][-1] ] = 'submitting'
                   break
               except:
                   logger.error("Error attempting to resubmit master job %i in Transform %d, Task %d. Deactivating unit '%s'." % (mj.id, self.getID(), self._getParent().id, self.unit_outputdata_list[uind]))
                   self.unit_state_list[uind]['active'] = False
                   self.unit_state_list[uind]['reason'] = 'Error on resubmission'
                   #self.pause()
                   

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
       self.unit_state_list.append({'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force' : False})

   def getUnit(self, unit):
       """get the unit number by number or name"""       
       if isinstance(unit, str):
           for uind in range(0, len(self.unit_outputdata_list)):
               if unit == self.unit_outputdata_list[uind]:
                   return uind
           logger.warning("Couldn't find unit with name '%s'." % unit)
       elif isinstance(unit, int):
           if unit < 0 or unit > len(self.unit_outputdata_list)-1:
               logger.warning("Unit number '%d' out of range" % unit)
           else:
               return unit
       else:
           logger.warning('Incorrect type for unit referral. Allowed types are int or string.')

       return -1
           
   def getUnitJob(self, unit):
       """Return the Master job associated with this unit (takes either unit name or number)"""
       unit = self.getUnit(unit)
       if unit != -1:           
           return self.getUnitMasterJob( unit, proxy=True )
       else:
           return None
       
   def activateUnit(self, unit):
       """activate the given unit"""
       unit = self.getUnit(unit)
       if unit != -1:
           self.unit_state_list[unit]['active'] = True
           self.unit_state_list[unit]['exceptions'] = 0
           self.unit_state_list[unit]['reason'] = ''

   def deactivateUnit(self, unit):
       """deactivate the given unit"""
       unit = self.getUnit(unit)
       if unit != -1:
           self.unit_state_list[unit]['active'] = False
           self.unit_state_list[unit]['exceptions'] = 0

   def forceUnitCompletion(self, unit):
       """Set unit to ignore all failed jobs/partitions"""
       unit = self.getUnit(unit)
       if unit != -1:
           self.unit_state_list[unit]['force'] = True
           self.addDatasetsToContainers( self.getUnitMasterJob(unit) )


   def resetUnit(self, unit):
       """Reset a unit completely"""
       unit = self.getUnit(unit)
       if unit != -1:
           # remove any possible DSs already complete
           mj = self.getUnitMasterJob(unit)

           if mj:
               self.removeDatasetsFromContainers(mj)

           # wipe the unit
           self.unit_state_list[unit] = {'active':True, 'configured':False, 'submitted':False, 'download':False, 'merged':False, 'reason':'', 'exceptions' : 0, 'force':False}

           # reset the partitions
           for p in self.unit_partition_list[unit]:
               self.setPartitionStatus(p, 'bad')

           # kill the job
           try:
               mj.kill()
           except:
               pass
           
           self.unit_partition_list[unit] = []          

           # finally, blank the job lookup table
           if len(self.unit_job_list) > 0:
               self.unit_job_list[unit] = None
                                                                           
   def createPartitionList( self, unit_num ):

      if not self.partition_lock:
          self.partition_lock = threading.Lock()

      # reset the partition list
      self.unit_partition_list[unit_num] = []

      # blank entry in job list
      if len(self.unit_job_list) > 0 and self.unit_job_list[unit_num]:
          self.unit_job_list[unit_num] = None
          
      # create partitions as given by the unit lists
      part_num = len(self.partitions_data) + 1      
      
      if not self.backend:
          self.backend = stripProxy(GPI.Panda())
      else:
          self.backend = stripProxy(self.backend)

      temp_inds = None
      if self.backend._name in ['Panda']:
          
          if not self.inputdata:
              self.inputdata = DQ2Dataset()
          else:
              # do a proper copy of the original DS
              temp_inds = copy.deepcopy(self.inputdata)

          if self.inputdata._name == "DQ2Dataset" and self.inputdata.tag_info:
              # copy tag info
              fname = self.unit_inputdata_list[unit_num].split(":")[0]
              start = int(self.unit_inputdata_list[unit_num].split(":")[1])
              stop = int(self.unit_inputdata_list[unit_num].split(":")[2])

              self.inputdata = DQ2Dataset()
              self.inputdata.tag_info = { fname : copy.deepcopy(temp_inds.tag_info[fname]) }
              self.inputdata.tag_info[fname]['refs'] = []

              if stop >= len(temp_inds.tag_info[fname]['refs']):
                  stop = len(temp_inds.tag_info[fname]['refs'])
                  
              logger.warning("Creating unit with file %s and refs %d to %d" % (fname, start, stop))
              for i in range(start, stop):
                  self.inputdata.tag_info[fname]['refs'].append( copy.deepcopy( temp_inds.tag_info[fname]['refs'][i]) )                                
              
          else:
              self.inputdata.dataset = self.unit_inputdata_list[unit_num]
              
          splitter = self.splitter
          if not splitter:
              splitter = DQ2JobSplitter()
              
              if self.MB_per_job > 0:
                  splitter.filesize = self.MB_per_job
              elif self.subjobs_per_unit > 0:
                  splitter.numsubjobs = self.subjobs_per_unit
              else:              
                  splitter.numfiles = self.files_per_job
              
          logger.warning("Determining partition splitting for dataset(s) %s..." % self.inputdata.dataset )

      elif self.backend._name in ['Local', 'PBS', 'LSF']:
          self.inputdata = ATLASLocalDataset()
          self.inputdata.names = self.unit_inputdata_list[unit_num]

          splitter = self.splitter
          if not splitter:
              splitter = AthenaSplitterJob()
              splitter.numsubjobs = int( len(self.inputdata.names) / self.files_per_job ) + 1
      else:
          self.pause()
          raise ApplicationConfigurationError(None, "Backend '%s' not supported in MultiTask mode" % self.backend._name)

      #sjl = splitter.split(self)
      try:
          sjl = splitter.split(self)
      except Exception, x:          
          logger.error('Problem in Task %i, Transform %i, Unit %i: General Exception during split %s %s\nDeactivating unit.' %
                       (self._getParent().id, self.getID(), unit_num, x.__class__,x, ))
          self.unit_state_list[unit_num]['active'] = False
          self.unit_state_list[unit_num]['configured'] = False
          self.unit_state_list[unit_num]['reason'] = "Error during split. No valid site?"
          self.inputdata = temp_inds
          return
      except DQException, x:
          logger.error("Problem in Task %i, Transform %i, Unit %i: Exception in DQ2 during split %s %s\nDeactivating unit. Maybe no valid sites found?" %
                       (self._getParent().id, self.getID(), unit_num, x.__class__,x))
          self.unit_state_list[unit_num]['active'] = False
          self.unit_state_list[unit_num]['configured'] = False
          self.unit_state_list[unit_num]['reason'] = "Error during split. No valid site?"
          self.inputdata = temp_inds
          return

      #restore old inds
      self.inputdata = temp_inds
      
      if len(sjl) == 0:
          logger.error("Problem in Task %i, Transform %i, Unit %i: Splitter didn't produce any subjobs - deactivating this unit" % (self._getParent().id, self.getID(), unit_num))
          self.unit_state_list[unit_num]['active'] = False
          self.unit_state_list[unit_num]['configured'] = False
          self.unit_state_list[unit_num]['reason'] = "No subjobs produced in split. Brokering issue?"
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
          
      self.unit_state_list[unit_num]['configured'] = True
      self.unit_state_list[unit_num]['reason'] = ""
                  
      self.partition_lock.release()
      
   def updateStatus(self, status):
      """Update the transform status"""
      if status != 'completed':
          self.status = status
          return

      # before we do anything, check that all units have completed
      for uind in range(0, len(self.unit_partition_list)):
          if not self.isUnitComplete(uind):
              return

      # check for all DSs in containers (usually because of forced completion)
      for uind in range(0, len(self.unit_partition_list)):
          j = self.getUnitMasterJob(uind)
          self.addDatasetsToContainers(j)
                  
      # check that dq2-get and merger has completed
      if (self.do_auto_download or self.merger):
          for uind in range(0, len(self.unit_partition_list)):
              filelist = self.getLocalDQ2FileList( uind )

              do_dq2 = False
              for f in filelist:
                  if not os.path.exists( f ):
                      do_dq2 = True
                  else:
                      # check if the file was OK
                      if os.path.getsize( f ) < 10:
                          logger.warning("Previous problem with dq2-get. Removing file %s and retrying..." % f)
                          os.remove(f)
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

   def getContainerName(self):
       """Return the parent container for this whole transform"""
       if self.name == "":
           name = "trf"
       else:
           name = self.name
          
       return (self._getParent().getContainerName()[:-1] + ".%s.%i/" % (name, self.getID())).replace(" ", "_")
   
   def checkCompletedApp(self, app):
      j = app._getParent()

      if not j.outputdata or j.outputdata._name != "DQ2OutputDataset":
          return True

      return True

   def setAppStatus(self, app, new_status):
       if app._getParent().subjobs or new_status != "completed" or (app.id in self._app_status and self._app_status[app.id] in ["removed","completed","failed", "bad"]):
           super(MultiTransform,self).setAppStatus(app, new_status)
       else:
           super(MultiTransform,self).setAppStatus(app, new_status)
           self.addDatasetsToContainers(app._getParent())
           
   def setMasterJobStatus(self, job, new_status):
       """hook for a master job status update"""
       
       if new_status != "completed" or job.status == new_status:
           return

       self.addDatasetsToContainers(job)

   def checkContainerContents(self):
      """Check all datasets have been added to the trf and unit containers"""
      for uind in range(0, len(self.unit_partition_list)):
          mj = self.getUnitMasterJob(uind)
          if mj:
              self.addDatasetsToContainers(mj)
      
      
   def addDatasetsToContainers(self, j):
      """add datasets to transform and task containers"""
       
      if not j or not j.outputdata or j.outputdata._name != "DQ2OutputDataset":
          return

      # check if this is a valid job in this trf
      ok = False
      if j.subjobs:
          pj = j
      else:
          pj = j._getParent()
      
      for uind in range(0, len(self.unit_partition_list)):
          mj = self.getUnitMasterJob(uind)
          # check for this master job attached to a unit and only forced subjob check
          if mj and (pj.id == mj.id) and (j.subjobs or self.unit_state_list[uind]['force']):
              ok = True
              break

      if not ok:
          return
      
      # add dataset to the transform container
      trf_container = self.getContainerName()
      
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
                  logger.info('Registered container for Transform %i: %s' % (self.getID(), trf_container))
                  
              except Exception, x:
                  logger.error('Problem registering container for Transform %i, %s : %s %s' % (self.getID(), trf_container,x.__class__, x))
              except DQException, x:
                  logger.error('DQ2 Problem registering container for Transform %i, %s : %s %s' % (self.getID(), trf_container,x.__class__, x))

          if j.subjobs:
              ds_list = dq2.listDatasetsInContainer(j.outputdata.datasetname)
              for ds in ds_list:
                  try:
                      dq2.registerDatasetsInContainer(trf_container, [ ds ] )
                  except DQContainerAlreadyHasDataset:
                      pass
                  except Exception, x:
                      logger.error('Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                  except DQException, x:
                      logger.error('DQ2 Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
          else:
              try:
                  dq2.registerDatasetsInContainer(trf_container, [ j.outputdata.datasetname ] )
                  
              except DQContainerAlreadyHasDataset:
                  pass
              except Exception, x:
                  logger.error('Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
              except DQException, x:
                  logger.error('DQ2 Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
      finally:
          dq2_lock.release()

      # add dataset to the task container
      task_container = self._getParent().getContainerName()
      
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
                  logger.info('Registered container for Task %i: %s' % (self._getParent().id, task_container))
              except Exception, x:
                  logger.error('Problem registering container for Task %i, %s : %s %s' % (self._getParent().id, task_container,x.__class__, x))
              except DQException, x:
                  logger.error('DQ2 Problem registering container for Task %i, %s : %s %s' % (self._getParent().id, task_container,x.__class__, x))

          if j.subjobs:
              ds_list = dq2.listDatasetsInContainer(j.outputdata.datasetname)
              for ds in ds_list:
                  try:
                      dq2.registerDatasetsInContainer(task_container, [ ds ] )
                  except DQContainerAlreadyHasDataset:
                      pass
                  except Exception, x:
                      logger.error('Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                  except DQException, x:
                      logger.error('DQ2 Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
          else:
              try:
                  dq2.registerDatasetsInContainer(task_container, [ j.outputdata.datasetname ] )
                  
              except DQContainerAlreadyHasDataset:
                  pass
              except Exception, x:
                  logger.error('Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
              except DQException, x:
                  logger.error('DQ2 Problem registering dataset %s in container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))

      finally:
          dq2_lock.release()

   def removeDatasetsFromContainers(self, j):
      """add datasets to transform and task containers"""
       
      if not j or not j.outputdata or j.outputdata._name != "DQ2OutputDataset":
          return

      # remove dataset from the transform container
      trf_container = self.getContainerName()
      
      try:
          containerinfo = {}
          dq2_lock.acquire()
          try:
              containerinfo = dq2.listDatasets(trf_container)
          except:
              containerinfo = {}

          if containerinfo != {}:
              if j.subjobs:
                  ds_list = dq2.listDatasetsInContainer(j.outputdata.datasetname)
                  for ds in ds_list:
                      try:
                          dq2.deleteDatasetsFromContainer(trf_container, [ ds ] )
                      except DQContainerDoesNotHaveDataset:
                          pass
                      except Exception, x:
                          logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                      except DQException, x:
                          logger.error('DQ2 Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
              else:
                  
                  try:
                      dq2.deleteDatasetsFromContainer(trf_container, [ j.outputdata.datasetname ] )
                  except DQContainerDoesNotHaveDataset:
                      pass
                  except Exception, x:
                      logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                  except DQException, x:
                          logger.error('DQ2 Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x)) 
      finally:
          dq2_lock.release()


      # remove dataset from the task container
      task_container = self._getParent().getContainerName()
      
      try:
          containerinfo = {}
          dq2_lock.acquire()
          try:
              containerinfo = dq2.listDatasets(task_container)
          except:
              containerinfo = {}

          if containerinfo != {}:
              if j.subjobs:
                  ds_list = dq2.listDatasetsInContainer(j.outputdata.datasetname)
                  for ds in ds_list:
                      try:
                          dq2.deleteDatasetsFromContainer(task_container, [ ds ] )
                      except DQContainerDoesNotHaveDataset:
                          pass
                      except Exception, x:
                          logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                      except DQException, x:
                          logger.error('DQ2 Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
              else:
                  
                  try:
                      dq2.deleteDatasetsFromContainer(task_container, [ j.outputdata.datasetname ] )
                  except DQContainerDoesNotHaveDataset:
                      pass
                  except Exception, x:
                      logger.error('Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x))
                  except DQException, x:
                          logger.error('DQ2 Problem removing dataset %s from container %s: %s %s' %( j.outputdata.datasetname, trf_container, x.__class__, x)) 
      finally:
          dq2_lock.release()


   def getAllUnitJobs(self):
      "Return all unit master jobs associated with this Transform"
      task = self._getParent()
      if not task:
          raise ApplicationConfigurationError(None, "This transform has not been associated with a task and so no jobs are available")

      jobslice = JobRegistrySlice("tasks(%i).transforms[%i].allUnitJobs()"%(task.id, self.getID()))
      
      for uind in xrange(0,self.getNumUnits()):
          mj = self.getUnitMasterJob(uind)
          if mj:
              jobslice.objects[mj.fqid] = mj

      return JobRegistrySliceProxy(jobslice)
  
   def getUnitContainerName(self, unit):
      "Return the container associated with this unit"
      mj = self.getUnitJob(unit)
      if mj and mj.outputdata and mj.outputdata._impl._name == "DQ2OutputDataset":
         return mj.outputdata.datasetname

      return None

   def listAllDatasets(self):
      "List all datasets in container of this transform"
      ds_list = []
      try:
          try:
              dq2_lock.acquire()
              ds_list = dq2.listDatasetsInContainer(self.getContainerName())
          except DQContainerDoesNotHaveDataset:
              pass
          except Exception, x:
              logger.error('Problem finding datasets associated with TRF container %s: %s %s' %( self.getContainerName(), x.__class__, x))
          except DQException, x:
              logger.error('DQ2 Problem finding datasets associated with TRF container %s: %s %s' %( self.getContainerName(), x.__class__, x))
      finally:
          dq2_lock.release()
          
      return ds_list

   def listUnitDatasets(self, unit):
      "List all datasets in container of this transform"
      cont_name = self.getUnitContainerName(unit)
      if not cont_name:
          return []
      
      ds_list = []
      try:
          try:
              dq2_lock.acquire()
              ds_list = dq2.listDatasetsInContainer(cont_name)
          except DQContainerDoesNotHaveDataset:
              pass
          except Exception, x:
              logger.error('Problem finding datasets associated with Unit container %s: %s %s' %( cont_name, x.__class__, x))
          except DQException, x:
              logger.error('DQ2 Problem finding datasets associated with Unit container %s: %s %s' %( cont_name, x.__class__, x))
      finally:
          dq2_lock.release()
          
      return ds_list

  
