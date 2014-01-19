from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks import Transform
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from TaskApplication import AthenaTask, AnaTaskSplitterJob


from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2

import time

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


class AnaTransform(Transform):
   """ Analyzes Events """
   _schema = Schema(Version(1,0), dict(Transform._schema.datadict.items() + {
       'files_per_job'     : SimpleItem(defvalue=5, doc='files per job', modelist=["int"]),
       'partitions_data'   : ComponentItem('datasets', defvalue=[], optional=1, sequence=1, hidden=1, doc='Input dataset for each partition'),
       'partitions_sites'  : SimpleItem(defvalue=[], hidden=1, modelist=["str","list"],doc='Input site for each partition'),
       'outputdata'        : ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset'),
       'dataset_name'      : SimpleItem(defvalue="", transient=1, comparable=False, getter="get_dataset_name", doc='name of the output dataset'),
       }.items()))
   _category = 'transforms'
   _name = 'AnaTransform'
   _exportmethods = Transform._exportmethods

   def initialize(self):
      super(AnaTransform, self).initialize()
      self.application = AthenaTask()
      self.inputdata = DQ2Dataset()

   def get_dataset_name(self):
      task = self._getParent()
      name_base = ["user",getNickname(),task.creation_date,"task_%s" % task.id]
      if self.inputdata.dataset:
          subtask_dsname = ".".join(name_base +["subtask_%s" % task.transforms.index(self), str(self.inputdata.dataset[0].strip("/"))])
      else:
          subtask_dsname = ".".join(name_base +["subtask_%s" % task.transforms.index(self)])
            
      # make sure we keep the name size limit:
      dq2_config = getConfig("DQ2")
      max_length_site = len("ALBERTA-WESTGRID-T2_SCRATCHDISK      ")
      max_length = dq2_config['OUTPUTDATASET_NAMELENGTH'] - max_length_site
      if len(subtask_dsname) > max_length:
          logger.debug("Proposed dataset name longer than limit (%d). Restricting dataset name..." % max_length)

          while len(subtask_dsname) > max_length:
              subtask_dsname_toks = subtask_dsname.split('.')
              subtask_dsname = '.'.join(subtask_dsname_toks[:len(subtask_dsname_toks)-1])
      return subtask_dsname

## Internal methods
   def checkCompletedApp(self, app):
      task = self._getParent()
      j = app._getParent()
      for odat in j.outputdata.outputdata:
          # Look out: if this is changed, there is anothher one like it below!
          if 0==len([f for f in j.outputdata.output if ".".join(odat.split(".")[:-1]) in f]):
              logger.error("Job %s has not produced %s file, only: %s" % (j.id, odat, j.outputdata.output))
              return False
      # if this is the first app to complete the partition...
      if self.getPartitionStatus(self._app_partition[app.id]) != "completed":
          task_container, subtask_dsname = task.container_name, self.dataset_name

          infos = {}
          for oinfo in j.outputdata.output:
              try:
                  dq2_lock.acquire()
                  info = oinfo.split(",")
                  # get master replica from dataset - info not set to SE; but to ANALY_XYZ from panda
                  master_replica = dq2.getMasterReplicaLocation(info[0])
                  if master_replica:
                      info[5] = master_replica
                  else:
                      replicas = dq2.listDatasetReplicas(info[0]).values()
                      if len(replicas) == 0:
                          try:
                              info[5] = getPandaClient().PandaSites[info[5]]["ddm"]
                          except KeyError:
                              pass
                      else:
                          complete, incomplete = replicas[0].values()
                          info[5] = (complete + incomplete)[0]
                  if info[4][:3] == "ad:":
                      info[4] = info[4][3:]

              finally:
                  dq2_lock.release()
                
              datasetname = subtask_dsname + '.' + info[5]
              info[0] = datasetname
              infos.setdefault(datasetname, []).append(",".join(info))

          for ds in infos.keys():
              outputdata = DQ2OutputDataset()
              try:
                  outputdata.create_dataset(ds)
              except DQDatasetExistsException:
                  pass
              try:
                  outputdata.register_datasets_details(None, infos[ds])
              except DQFileExistsInDatasetException:
                  pass

          # Register Container
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
                      logger.debug('Registered container for Task %i: %s' % (task.id, task_container))
                  except Exception, x:
                      logger.error('Problem registering container for Task %i, %s : %s %s' % (task.id, task_container,x.__class__, x))
              for ds in infos.keys():
                  try:
                      dq2.registerDatasetsInContainer(task_container, [ ds ] )
                  except DQContainerAlreadyHasDataset:
                      pass
                  except Exception, x:
                      logger.error('Problem registering dataset %s in container %s: %s %s' %( subtask_dsname, task_container, x.__class__, x))
          finally:
              dq2_lock.release()
      return True

   def rebrokerPanda(self, cloud = None):
      sites = getPandaClient().PandaSites.keys()

      dataset_sites = [getPandaClient().convertDQ2toPandaID(site) for site in self.inputdata.get_locations(complete=1)]
      sites = [site for site in sites if site in dataset_sites]
      sites = [s for s in sites if not s in self.backend.requirements.excluded_sites] 
      sites = [s for s in sites if not s.replace("ANALY_","") in self.backend.requirements.excluded_sites]
      if cloud:
         sites = [s for s in sites if s.replace("ANALY_","") in getPandaClient().PandaClouds[cloud]["sites"]] 
      if len(sites) == 0:
         logger.error("No compatible sites for rebrokering found!")
         return

      from random import shuffle
      shuffle(sites)
      self.backend.site = sites[0]
      self.partitions_sites = [sites[0]]*len(self.partitions_sites)
      logger.warning("Rebrokering transform - %i possible sites; chosing %s at random..." % (len(sites),sites[0]))


   def findCompleteCloudBackend(self,db_sites,allowed_sites,replicas):
      # Sort complete replicas into clouds
      # returns sorted list of tuples (cloud, backend)
      addclouds = TiersOfATLAS.ToACache.dbcloud.keys()
      random.shuffle(addclouds)
      clouds = config["cloudPreference"] + addclouds
      complete_sites = {}
      for c in clouds:
         complete_sites[c] = []
      for s in replicas[1]:
         try:
            cloud = whichCloudExt(s)
         except:
            logger.warning("Could not get cloud of site %s!", s)
            continue
         if db_sites and not stripSite(s) in db_sites:
            continue
         #print cloud, s
         complete_sites[cloud].append(s)

      result = []
      # Check if we find a cloud/backend combination for a complete replica
      backends = [be for be in config["backendPreference"] if be in GPI.__dict__]
      for cloud in clouds:
         for backend in backends:
            if not backend in allowed_sites.keys(): 
               continue
            if backend == "Panda":
                sites = [site for site in complete_sites[cloud] if getPandaClient().convertDQ2toPandaID(site) in allowed_sites["Panda"]]
            else:
                sites = [site for site in complete_sites[cloud] if site in allowed_sites[backend]]
            if len(sites) > 0:
               result.append((cloud,backend))
      return result
 
   def findIncompleteCloudBackend(self,db_sites,allowed_sites,replicas):
      # If no cloud/backend combination is found for complete replicas, 
      # find cloud/backend with maximal number of replicas
      # returns list of tuples sorted by number of sites with replicas: (cloud, backend)
      addclouds = TiersOfATLAS.ToACache.dbcloud.keys()
      random.shuffle(addclouds)
      clouds = config["cloudPreference"] + addclouds
      incomplete_sites = {}
      for c in clouds:
         incomplete_sites[c] = []
      for s in replicas[0]:
         try:
            cloud = whichCloudExt(s)
         except:
            continue
         if db_sites and not stripSite(s) in db_sites:
            continue
         incomplete_sites[cloud].append(s)
      
      using_backend = None
      using_cloud = None
      max_sites = 0
      result = []
      backends = [be for be in config["backendPreference"] if be in GPI.__dict__]
      for cloud in clouds:
         for backend in backends:
            if backend == "Panda":
                from pandatools import Client
                sites = [site for site in incomplete_sites[cloud] if getPandaClient().convertDQ2toPandaID(site) in allowed_sites["Panda"]]
            else:
                sites = [site for site in incomplete_sites[cloud] if site in allowed_sites[backend]]
            if len(sites) > 0:
               result.append((len(sites),cloud,backend))
      result.sort()
      return [(r[1],r[2]) for r in result]

   def check(self):
      super(AnaTransform,self).check()
      if not self.inputdata.dataset:
         return
      if not self.backend:
         logger.warning("Determining backend and cloud...")

         # Get ddm sites of atlas_dbrelease, if present
         db_sites = None
         if self.application.atlas_dbrelease == "LATEST":
            from pandatools import Client
            self.application.atlas_dbrelease = getPandaClient().getLatestDBRelease(False)
         if self.application.atlas_dbrelease:
            try:
               db_dataset = self.application.atlas_dbrelease.split(':')[0] 
               try:
                  dq2_lock.acquire()
                  db_locations = dq2.listDatasetReplicas(db_dataset).values()[0][1] 
               finally:
                  dq2_lock.release()

            except Exception, x:
               raise ApplicationConfigurationError(x, 'Problem in AnaTask - j.application.atlas_dbrelease is wrongly configured ! ')
            db_sites = stripSites(db_locations)
         
         # Get complete/incomplete ddm sites for input dataset
         ds = self.inputdata.dataset[0]
         try:
            dq2_lock.acquire()
            if ds[-1] != "/":
               try:
                  replicas = {ds : dq2.listDatasetReplicas(ds)}
               except DQUnknownDatasetException:
                  ds += "/"
            if ds[-1] == "/":
               replicas = dq2.listDatasetReplicasInContainer(ds)
         finally:
            dq2_lock.release()
        
         # check if replicas are non-empty
         somefound = False
         for tid in replicas: 
            if len(replicas[tid]) == 0:
               raise ApplicationConfigurationError(None, "No replicas for dataset %s found!" % tid)
         replicas = [r.values()[0] for r in replicas.values()] # (dict with only one entry)

         # Get allowed sites for each backend:
         backends = [be for be in config["backendPreference"] if be in GPI.__dict__]
         allowed_sites = {}
         if "LCG" in backends:
            allowed_sites["LCG"] = GPI.LCG().requirements.list_sites(True,True)
         if "Panda" in backends:
            from pandatools import Client
            allowed_sites["Panda"] = getPandaClient().PandaSites.keys()
            #allowed_sites["Panda"] = [site["ddm"] for site in Client.getSiteSpecs()[1].values()]
         if "NG" in backends:
            allowed_sites["NG"] = getConfig("Athena")["AllowedSitesNGDQ2JobSplitter"]
         #if "PBS" in backends:
         #   sites["PBS"] = [] # should be local DQ2 storage element!

         # Get list of cloud-backend pairs (cbl) for complete replicas
         common_cbl = None
         for r in replicas:
            cbl = self.findCompleteCloudBackend(db_sites, allowed_sites, r)
            if common_cbl is None:
               common_cbl = cbl
            else:
               common_cbl = [cb for cb in cbl if cb in common_cbl]

         #print "CLOUD/BACKEND list for COMPLETE replicas: ", common_cbl

         # ..and for incomplete replicas
         if common_cbl is None or len(common_cbl) == 0:
            if len(replicas) > 1:
               raise ApplicationConfigurationError(None, 'Container dataset %s has no complete replica on one site and backend. Please specify individual tid datasets or use t.initializeFromDataset("%s") ' % (ds, ds))
            common_cbl = self.findIncompleteCloudBackend(db_sites, allowed_sites, replicas[0])
            #print "CLOUD/BACKEND list for INCOMPLETE replicas: ", common_cbl
         if common_cbl is None or len(common_cbl) == 0:
            raise ApplicationConfigurationError(None, 'Container dataset %s has no replica on one site and backend. Please specify individual tid datasets!' % (ds))

         cb = common_cbl[0]
         using_cloud = cb[0]
         using_backend = cb[1]

         assert using_cloud, using_backend

         if using_backend == "Panda":
            self.backend = stripProxy(GPI.Panda())
            self.backend.requirements.cloud = using_cloud

         elif using_backend == "NG":
            self.backend = stripProxy(GPI.NG())
         elif using_backend == "LCG":
            self.backend = stripProxy(GPI.LCG())
            self.backend.requirements = stripProxy(GPI.AtlasLCGRequirements())
            self.backend.requirements.cloud = using_cloud
         assert self.backend
         logger.warning("Running on cloud %s using backend %s", using_cloud, using_backend)

      logger.warning("Determining partition splitting...")
      try:
         if not self.backend.requirements.cloud:
            self.backend.requirements.cloud = "DE"
      except:
         pass
      if not self.inputdata.dataset:
         return
      splitter = DQ2JobSplitter()
      splitter.numfiles = self.files_per_job
      #splitter.update_siteindex = False # commented to use default value
      #splitter.use_lfc = True
      sjl = splitter.split(self) # This works even for Panda, no special "Job" properties are used anywhere.
      self.partitions_data = [sj.inputdata for sj in sjl]
      try:
         self.partitions_sites = [sj.backend.requirements.sites for sj in sjl]
      except AttributeError:
         self.partitions_sites = [sj.backend.site for sj in sjl]
         pass
      self.setPartitionsLimit(len(self.partitions_data)+1)
      self.setPartitionsStatus([c for c in range(1,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")
   
   def getJobsForPartitions(self, partitions):
      j = self.createNewJob(partitions[0])
      if len(partitions) >= 1:
          j.splitter = AnaTaskSplitterJob()
          j.splitter.subjobs = partitions
      j.inputdata = self.partitions_data[partitions[0]-1]
      if self.partitions_sites:
         if stripProxy(j.backend)._name == "Panda":
            if j.backend.site == "AUTO":
               j.backend.site = self.partitions_sites[partitions[0]-1]
         else:
            j.backend.requirements.sites = self.partitions_sites[partitions[0]-1]
      j.outputdata = self.outputdata
      if stripProxy(j.backend)._name == "Panda" and j.application.atlas_exetype == "ATHENA":
          j.outputdata.outputdata=[]
      #j.outputdata.datasetname = ""
      task = self._getParent()
      dsn = ["user",getNickname(),task.creation_date,"%i.t_%s_%s" % (j.id, task.id, task.transforms.index(self))]
      j.outputdata.datasetname = ".".join(dsn)

      #if j.outputdata.datasetname:
         #today = time.strftime("%Y%m%d",time.localtime())
         #j.outputdata.datasetname = "%s.%i.%s" % (j.outputdata.datasetname, j.id, today)
      return [j]

   def info(self):
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      print "* dataset: %s " % self.inputdata.dataset
      print "* processing %s per job" % say(self.files_per_job,"file")
      print "* backend: %s" % self.backend.__class__.__name__
      print "* application:"
      self.application.printTree() 


