from common import *
from Transform import Transform
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from TaskApplication import AthenaTask, AnaTaskSplitterJob


class AnaTransform(Transform):
   """ Analyzes Events """
   _schema = Schema(Version(1,0), dict(Transform._schema.datadict.items() + {
       'files_per_job'   : SimpleItem(defvalue=5, doc='files per job', modelist=["int"]),
       'partitions_data'   : ComponentItem('datasets', defvalue=[], sequence=1, hidden=1, doc='Input dataset for each partition'),
       'partitions_sites'  : SimpleItem(defvalue=[], hidden=1, modelist=["str","list"],doc='Input site for each partition'),
       'outputdata'      : ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset'),
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
      for f in j.outputdata.output:
         if "root" in f:
            return True
      logger.error("Job %s has not produced %s file, only: %s" % (j.id, "root", j.outputdata.output))
      return False

   def getOutputDataset(self):
      outname = ""
      if self.inputdata.dataset:
         ds = self.inputdata.dataset[0]
         sds = ds.split(".")
         if ds.startswith("user"):
            if len(sds) >= 7:
               outname = ".".join(sds[4:-2])
         else:
            if len(sds) >= 5:
               outname = ".".join(sds[1:-3])
      if outname:
         return "analysis." + outname + ".%s.ROOT" % (self._getParent().id)
      else:
         return "analysis.%s.ROOT" % (self._getParent().id)

   def check(self):
      super(AnaTransform,self).check()
      if self.inputdata.dataset == "":
         return
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
      self.inputsandbox = []
      self.outputsandbox = []
      sjl = splitter.split(self) # This works even for Panda, no special "Job" properties are used anywhere.
      self.partitions_data = [sj.inputdata for sj in sjl]
      self.partitions_sites = [sj.backend.requirements.sites for sj in sjl]
      self.setPartitionsLimit(len(self.partitions_data)+1)
      self.setPartitionsStatus([c for c in range(1,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")
      ods = self.getOutputDataset()
      logger.warning("Output will be saved in dataset '%s'.", ods)
      self.outputdata.output_dataset = ods
   
   def getJobsForPartitions(self, partitions):
      j = self.createNewJob(partitions[0])
      if len(partitions) > 1:
          j.splitter = AnaTaskSplitterJob()
          j.splitter.subjobs = partitions
      j.inputdata = self.partitions_data[partitions[0]-1]
      if stripProxy(j.backend)._name == 'LCG':
         j.backend.requirements.sites = self.partitions_sites[partitions[0]-1]
      j.outputdata = self.outputdata
      return [j]

   def info(self):
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      print "* dataset: %s " % self.inputdata.dataset
      print "* processing %s per job" % say(self.files_per_job,"file")
      print "* backend: %s" % self.backend.__class__.__name__
      print "* application:"
      self.application.printTree() 


