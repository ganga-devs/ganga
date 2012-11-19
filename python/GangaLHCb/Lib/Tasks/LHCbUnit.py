from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy

class LHCbUnit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
    }.items()))

   _category = 'units'
   _name = 'LHCbUnit'
   _exportmethods = IUnit._exportmethods + [ ]
   _hidden = 1
   
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
                           
      j.inputsandbox = self._getParent().inputsandbox
      j.outputsandbox = self._getParent().outputsandbox

      if trf.splitter:
         j.splitter = trf.splitter.clone()
      else:
         j.splitter = SplitByFiles(bulksubmit = True)
         
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
      super(LHCbUnit,self).updateStatus(status)

   def checkForSubmission(self):
      """Additional checks for unit submission"""

      # call the base class
      if not super(LHCbUnit,self).checkForSubmission():
         return False
      
      return True
