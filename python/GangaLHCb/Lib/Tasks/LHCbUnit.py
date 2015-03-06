from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from GangaLHCb.Lib.Splitters.SplitByFiles import SplitByFiles
from Ganga.GPIDev.Base.Proxy import addProxy

class LHCbUnit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
      'input_datset_index'     : SimpleItem(defvalue=-1, protected=1, hidden=1, doc='Index of input dataset from parent Transform', typelist=["int"]),
    }.items()))

   _category = 'units'
   _name = 'LHCbUnit'
   _exportmethods = IUnit._exportmethods + [ ]
   
   def createNewJob(self):
      """Create any jobs required for this unit"""      
      j = GPI.Job()
      j._impl.backend = self._getParent().backend.clone()
      j._impl.application = self._getParent().application.clone()
      if self.inputdata:
         j.inputdata = self.inputdata.clone()

      trf = self._getParent()
      task = trf._getParent()
      j.inputsandbox = self._getParent().inputsandbox

      import copy
      j.outputfiles = copy.deepcopy(self._getParent().outputfiles)
      if len(self._getParent().postprocessors.process_objects) > 0:
         j.postprocessors = copy.deepcopy( addProxy(self._getParent()).postprocessors )
      
      if trf.splitter:
         j.splitter = trf.splitter.clone()
         
         # change the first event for GaussSplitter
         if trf.splitter._name == "GaussSplitter":
            events_per_unit = j.splitter.eventsPerJob * j.splitter.numberOfJobs
            j.splitter.firstEventNumber = self.getID() * events_per_unit
            
      else:
         j.splitter = SplitByFiles()

      return j

   def checkMajorResubmit(self, job):
      """check if this job needs to be fully rebrokered or not"""
      return False

   def majorResubmit(self, job):
      """perform a major resubmit/rebroker"""
      super(LHCbUnit,self).majorResubmit(job)

   def reset(self):
      """Reset the unit completely"""
      super(LHCbUnit,self).reset()

   def updateStatus(self, status):
      """Update status hook"""

      # check for input data deletion of chain data
      if status == "completed" and self._getParent().delete_chain_input and len(self.req_units) > 0:

         # the inputdata field *must* be filled from the parent task
         # NOTE: When changing to inputfiles, will probably need to check for any specified in trf.inputfiles
         job = GPI.jobs(self.active_job_ids[0])
         for f in job.inputdata.files:
            logger.warning("Removing chain inputdata file '%s'..." % f.name)
            f.remove()
            
      super(LHCbUnit,self).updateStatus(status)

   def checkForSubmission(self):
      """Additional checks for unit submission"""

      # call the base class
      if not super(LHCbUnit,self).checkForSubmission():
         return False
      
      return True
