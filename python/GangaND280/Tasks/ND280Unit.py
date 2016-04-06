from Ganga.GPIDev.Lib.Tasks.common import makeRegisteredJob, getJobByID
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy

import os
from copy import deepcopy

class ND280Unit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
    }.items()))

   _category = 'units'
   _name = 'ND280Unit'
   _exportmethods = IUnit._exportmethods + [ ]

   def __init__(self):
      super(ND280Unit, self).__init__()
      
   def createNewJob(self):
      """Create any jobs required for this unit"""      
      j = makeRegisteredJob()
      j._impl.backend = self._getParent().backend.clone()
      j._impl.application = self._getParent().application.clone()
      if not self.inputdata == None:
        j.inputdata = self.inputdata.clone()

      trf = self._getParent()
      task = trf._getParent()

      # copy across the outputfiles
      for f in trf.outputfiles:
         j.outputfiles += [f.clone()]

      j.inputsandbox = trf.inputsandbox

      # Sort out the splitter
      if trf.splitter:
         j.splitter = trf.splitter.clone()

      # Postprocessors
      for pp in trf.postprocessors:
         j.postprocessors.append(deepcopy(pp))
         
      return j

   def checkMajorResubmit(self, job):
      """Check if a failed job shold be 'rebrokered' (new job created) rather than just resubmitted"""
      return False

   def majorResubmit(self, job):
      """Do any bookkeeping before a major resubmit is done"""
      super(ND280Unit,self).majorResubmit(job)

   def reset(self):
      """Reset the unit completely"""
      super(ND280Unit,self).reset()

   def updateStatus(self, status):
      """Update status hook"""
      super(ND280Unit,self).updateStatus(status)

   def checkForSubmission(self):
      """Additional checks for unit submission"""

      # call the base class
      if not super(ND280Unit,self).checkForSubmission():
         return False

      return True

   def copyOutput(self):
      """Copy the output data to local storage"""

      job = getJobByID(self.active_job_ids[0])
      
      if self.copy_output._name != "TaskLocalCopy" or job.outputdata._impl._name != "DQ2OutputDataset":
         logger.error("Cannot transfer from DS type '%s' to '%s'. Please contact plugin developer." % (job.outputdata._name, self.copy_output._name))
         return False

      # check which fies still need downloading
      to_download = []
      for f in job.outputfiles:
         
         # check for REs
         if self.copy_output.isValid( os.path.join(f.localDir, f.namePattern)) and not self.copy_output.isDownloaded( os.path.join(f.localDir, f.namePattern)):
            to_download.append(f)

      # is everything downloaded?
      if len(to_download) == 0:
         return True

      # nope, so pick the requested number and off we go
      for f in to_download:
         f.get()

      return False
