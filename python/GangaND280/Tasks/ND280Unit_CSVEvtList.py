from Ganga.GPIDev.Lib.Tasks.common import makeRegisteredJob, getJobByID
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy

import os

class ND280Unit_CSVEvtList(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
      'subpartid' : SimpleItem(defvalue=-1,typelist=['int'],doc='Index of this unit which is important when the original CSV file was split by the transform. Otherwise leave it at -1'),
      'eventswanted' : SimpleItem(defvalue='',typelist=['str','list'],doc='CSV list of run, subrun, and event numbers wanted.'),
    }.items()))

   _category = 'units'
   _name = 'ND280Unit_CSVEvtList'
   _exportmethods = IUnit._exportmethods + [ ]

   def __init__(self):
      super(ND280Unit_CSVEvtList, self).__init__()
      
   def createNewJob(self):
      """Create any jobs required for this unit"""      
      j = makeRegisteredJob()
      j._impl.backend = self._getParent().backend.clone()
      j._impl.application = self._getParent().application.clone()
      j.inputdata = self.inputdata.clone()

      trf = self._getParent()
      task = trf._getParent()

      # copy across the outputfiles
      for f in trf.outputfiles:
         j.outputfiles += [f.clone()]

      j.inputsandbox = trf.inputsandbox

      if type(self.eventswanted) == type(''):
        subLines = self.eventswanted
      else:
        subLines = '\n'.join(self.eventswanted)
      # Base for the naming of each subjob's CSV file
      incsvfile = j._impl.application.csvfile
      tmpname = os.path.basename(incsvfile)
      if len(tmpname.split('.')) > 1:
        patterncsv = '.'.join(tmpname.split('.')[0:-1])+"_sub%d."+ tmpname.split('.')[-1]
      else:
        patterncsv = tmpname+"_sub%d"

      from Ganga.GPIDev.Lib.File import FileBuffer
      thiscsv = patterncsv % self.subpartid

      # Create the CSV file for this Unit
      j._impl.getInputWorkspace().writefile(FileBuffer(thiscsv,subLines),executable=0)
      j._impl.application.csvfile = j._impl.getInputWorkspace().getPath()+thiscsv
      j.inputsandbox.append(j._impl.getInputWorkspace().getPath()+thiscsv)

      # Base for the naming of each subjob's output file
      tmpname = os.path.basename(j._impl.application.outputfile)
      if len(tmpname.split('.')) > 1:
        patternout = '.'.join(tmpname.split('.')[0:-1])+"_sub%d."+ tmpname.split('.')[-1]
      else:
        patternout = tmpname+"_sub%d"
      j._impl.application.outputfile = patternout % self.subpartid

      # Sort out the splitter
      if trf.splitter:
         j.splitter = trf.splitter.clone()
         
      return j

   def checkMajorResubmit(self, job):
      """Check if a failed job shold be 'rebrokered' (new job created) rather than just resubmitted"""
      return False

   def majorResubmit(self, job):
      """Do any bookkeeping before a major resubmit is done"""
      super(ND280Unit_CSVEvtList,self).majorResubmit(job)

   def reset(self):
      """Reset the unit completely"""
      super(ND280Unit_CSVEvtList,self).reset()

   def updateStatus(self, status):
      """Update status hook"""
      super(ND280Unit_CSVEvtList,self).updateStatus(status)

   def checkForSubmission(self):
      """Additional checks for unit submission"""

      # call the base class
      if not super(ND280Unit_CSVEvtList,self).checkForSubmission():
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
