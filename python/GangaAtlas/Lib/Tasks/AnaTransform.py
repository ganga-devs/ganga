from common import *
from Transform import Transform
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from TaskApplication import AthenaTask, AnaTaskSplitterJob


from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException 
from dq2.container.exceptions import DQContainerAlreadyHasDataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2

config.addOption('cloudPreference',[],'list of preferred clouds to choose for AnaTask analysis')
config.addOption('backendPreference',["LCG","Panda","NG"],'order of preferred backends (LCG, Panda, NG) for AnaTask analysis')

import time

from dq2.info import TiersOfATLAS
from GangaAtlas.Lib.ATLASDataset import whichCloud
from Ganga.Core.exceptions import ApplicationConfigurationError

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
       'partitions_data'   : ComponentItem('datasets', defvalue=[], sequence=1, hidden=1, doc='Input dataset for each partition'),
       'partitions_sites'  : SimpleItem(defvalue=[], hidden=1, modelist=["str","list"],doc='Input site for each partition'),
       'outputdata'        : ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset'),
       }.items()))
   _category = 'transforms'
   _name = 'AnaTransform'
   _exportmethods = Transform._exportmethods

   def initialize(self):
      super(AnaTransform, self).initialize()
      self.application = AthenaTask()
      self.inputdata = DQ2Dataset()

## Internal methods
   def checkCompletedApp(self, app):
      j = app._getParent()
      for odat in j.outputdata.outputdata:
          if 0==len([f for f in j.outputdata.output if odat in f]):
              logger.error("Job %s has not produced %s file, only: %s" % (j.id, odat, j.outputdata.output))
              return False
      # if this is the first app to complete the partition...
      if self.getPartitionStatus(self._app_partition[app.id]) != "completed":
          prefix = ".".join(j.outputdata.datasetname.split(".")[:3])
          task = self._getParent()
          datasetname = "%s.task_%i.subtask_%i.%s"%(prefix,task.id,task.transforms.index(self),self.outputdata.datasetname)
          outputdata = DQ2OutputDataset()
          try:
              outputdata.create_dataset(datasetname)
          except DQDatasetExistsException:
              pass
          infos = []
          for odat in j.outputdata.outputdata:
              info = [f for f in j.outputdata.output if odat in f][0].split(",")
              info[0] = datasetname
              infos.append(",".join(info))
          outputdata.register_datasets_details(None, infos)

          container = "%s.task_%i.%s/"%(prefix,task.id,self.outputdata.datasetname)
          # Register Container
          try:
              containerinfo = {}
              dq2_lock.acquire()
              try:
                  containerinfo = dq2.listDatasets(container)
              except:
                  containerinfo = {}
              if containerinfo == {}:
                  try:
                      dq2.registerContainer(container)
                      logger.debug('Registered container for Task %i: %s' % (task.id, container))
                  except Exception, x:
                      logger.error('Problem registering container for Task %i, %s : %s %s' % (task.id, container,x.__class__, x))
              try:
                  dq2.registerDatasetsInContainer(container, [ datasetname ])
              except DQContainerAlreadyHasDataset:
                  pass
              except Exception, x:
                  logger.error('Problem registering dataset %s in container %s: %s %s' %(datasetname, container, x.__class__, x))
          finally:
              dq2_lock.release()
      return True


   def findCompleteCloudBackend(self,db_sites,allowed_sites,replicas):
      # Sort complete replicas into clouds
      # returns sorted list of tuples (cloud, backend)
      clouds = config["cloudPreference"] + TiersOfATLAS.ToACache.dbcloud.keys()
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
            sites = [site for site in complete_sites[cloud] if site in allowed_sites[backend]]
            if len(sites) > 0:
               result.append((cloud,backend))
      return result
 
   def findIncompleteCloudBackend(self,db_sites,allowed_sites,replicas):
      # If no cloud/backend combination is found for complete replicas, 
      # find cloud/backend with maximal number of replicas
      # returns list of tuples sorted by number of sites with replicas: (cloud, backend)
      clouds = config["cloudPreference"] + TiersOfATLAS.ToACache.dbcloud.keys()
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
         if self.application.atlas_dbrelease: 
            try:
               db_dataset = self.application.atlas_dbrelease.split(':')[0] 
               db_locations = dq2.listDatasetReplicas(db_dataset).values()[0][1] 
            except Exception, x:
               raise ApplicationConfigurationError(x, 'Problem in AnaTask - j.application.atlas_dbrelease is wrongly configured ! ')
            db_sites = stripSites(db_locations)
         
         # Get complete/incomplete ddm sites for input dataset
         ds = self.inputdata.dataset[0]
         if ds[-1] != "/":
            try:
               replicas = {ds : dq2.listDatasetReplicas(ds)}
            except DQUnknownDatasetException:
               ds += "/"
         if ds[-1] == "/":
            replicas = dq2.listDatasetReplicasInContainer(ds)
        
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
            allowed_sites["Panda"] = [site["ddm"] for site in Client.getSiteSpecs()[1].values()]
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
            
         # ..and for incomplete replicas
         if common_cbl is None or len(common_cbl) == 0:
            if len(replicas) > 1:
               raise ApplicationConfigurationError(None, 'Container dataset %s has no complete replica on one site and backend. Please specify individual tid datasets or use t.initializeFromDataset("%s") ' % (ds, ds))
            common_cbl = self.findIncompleteCloudBackend(db_sites, allowed_sites, replicas[0])
         if common_cbl is None or len(common_cbl) == 0:
            raise ApplicationConfigurationError(None, 'Container dataset %s has no replica on one site and backend. Please specify individual tid datasets!' % (ds))

         using_cloud = common_cbl[0][0]
         using_backend = common_cbl[0][1]

         assert using_cloud, using_backend

         if using_backend == "Panda":
            self.backend = stripProxy(GPI.Panda())
            self.backend.requirements.cloud = using_cloud
            self.outputdata.outputdata=[]
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
      #splitter.use_lfc = True
      sjl = splitter.split(self) # This works even for Panda, no special "Job" properties are used anywhere.
      self.partitions_data = [sj.inputdata for sj in sjl]
      try:
         self.partitions_sites = [sj.backend.requirements.sites for sj in sjl]
      except AttributeError:
         self.partitions_sites = None
         pass
      self.setPartitionsLimit(len(self.partitions_data)+1)
      self.setPartitionsStatus([c for c in range(1,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")
   
   def getJobsForPartitions(self, partitions):
      j = self.createNewJob(partitions[0])
      if len(partitions) > 1:
          j.splitter = AnaTaskSplitterJob()
          j.splitter.subjobs = partitions
      j.inputdata = self.partitions_data[partitions[0]-1]
      if self.partitions_sites:
         j.backend.requirements.sites = self.partitions_sites[partitions[0]-1]
      j.outputdata = self.outputdata
      if j.outputdata.datasetname:
         today = time.strftime("%Y%m%d",time.localtime())
         j.outputdata.datasetname = "%s.%i.%s" % (j.outputdata.datasetname, j.id, today)
      return [j]

   def info(self):
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      print "* dataset: %s " % self.inputdata.dataset
      print "* processing %s per job" % say(self.files_per_job,"file")
      print "* backend: %s" % self.backend.__class__.__name__
      print "* application:"
      self.application.printTree() 


