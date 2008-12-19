from common import *
from Transform import Transform
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import DQ2Dataset, DQ2OutputDataset
from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter
from TaskApplication import AthenaTask

class AnaTransform(Transform):
   """ Analyzes Events """
   _schema = Schema(Version(1,0), dict(Transform._schema.datadict.items() + {
       'files_per_job'   : SimpleItem(defvalue=5, doc='files per job',    checkset="reSetup", modelist=["int"]),
       'partitions_data'     : ComponentItem('datasets', defvalue=[], sequence=1, hidden=1, doc='Input dataset for each partition'),
       'outputdata'      : ComponentItem('datasets', defvalue=DQ2OutputDataset(), doc='Output dataset'),
       }.items()))
   _category = 'transforms'
   _name = 'AnaTransform'
   _exportmethods = Transform._exportmethods

   def initialize(self):
      super(AnaTransform, self).initialize()
      self.application = AthenaTask()
      self.inputdata = DQ2Dataset()

   def checkCompletedApp(self, app):
      j = app._getParent()
      for f in j.outputdata.actual_output:
         if "root" in f:
            return True
      logger.error("Job %s has not produced %s file, only: %s" % (j.id, "root", j.outputdata.actual_output))
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
         return "analysis." + outname + ".%s.ROOT" % (self.getTask().id)
      else:
         return "analysis.%s.ROOT" % (self.getTask().id)

   def setup(self):
      super(AnaTransform,self).setup()
      if self.inputdata.dataset == "":
         return
      if self.status == "running":
         logger.warning("AnaTransform already running. This operation is no longer possible!") # This has to be generalized
         return
      logger.warning("Determining partition splitting...")
      try:
         if not self.backend.requirements.cloud:
            self.backend.requirements.cloud = "DE"
      except:
         pass
      if not self.inputdata.dataset:
         return
      j = GPI.Job()
      try:
         j.backend = self.backend
         j.application = self.application
         j.application._impl.tasks_id = "00"
         j.inputdata = self.inputdata
         j.outputdata = DQ2OutputDataset()
         j.splitter = DQ2JobSplitter()
         j.splitter.numfiles = self.files_per_job
         j.splitter.use_lfc = True
         sjl = j.splitter._impl.split(j._impl)
         self.partitions_data = [sj.inputdata for sj in sjl]
         self.setPartitionsLimit(len(self.partitions_data)+2)
         self.setPartitionsStatus([c for c in range(1,len(self.partitions_data)+1) if self.getPartitionStatus(c) != "completed"], "ready")
         ods = self.getOutputDataset()
         logger.warning("Output will be saved in dataset '%s'.", ods)
         self.outputdata.output_dataset = ods
      finally:
         j.remove()
   
   def createPartitionJobs(self, partition, number=1):
      if number > 1:
         jl = []
         for i in range(partition,partition+number):
            jl.extend(self.createPartitionJobs(i))
         return jl

      if self.partitions_data == []:
         self.reSetup()
         self.checkSetup()
      j = self.createNewJob(partition)
      j.inputdata = self.partitions_data[partition-1]
      j.outputdata = self.outputdata
      j.application.atlas_environment.append("OUTPUT_FILE_NUMBER=%i" % partition)
      return [j]


   def info(self):
      self.checkSetup()
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      print "* dataset: " % self.dataset
      if self.skip_input_files > 0:
         print "* skipping " + say(self.skip_input_files,"file")
      print "* processing %s per job" % say(self.files_per_job,"files")
      print "* backend: %s" % self.backend.__class__.__name__
      print "* application:"
      self.application.printTree() 
