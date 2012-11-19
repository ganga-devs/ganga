from common import *
from Transform import Transform
from Ganga.Utility.util import containsGangaObjects,isNestedList
from TaskApplication import ArgSplitterTask

class ArgTransform(Transform):
   _schema = Schema(Version(1,0), dict(Transform._schema.datadict.items() + {
        'args' : SimpleItem(defvalue=[],typelist=['list','Ganga.GPIDev.Lib.GangaList.GangaList.GangaList'],sequence=1,checkset='_checksetNestedLists',doc='A list of lists of arguments to pass to script'),
}.items()))
   _category = 'transforms'
   _name = 'ArgTransform'
   _exportmethods = Transform._exportmethods

   # Copied from ISplitter
   def _checksetNestedLists(self,value):
      """The rule is that if there are nested lists then they 
      must not contain GangaObjects, as this corrupts the repository"""
      if isNestedList(value) and containsGangaObjects(value):
         raise TypeMismatchError('Assigning nested lists which contain Ganga GPI Objects is not supported.')

   def check(self):
      nargs = len(self.args)
      self.setPartitionsStatus(range(1,nargs+1), "ready")
      if "_partition_status" in self._data:
         self.setPartitionsLimit(nargs+1)

   def getJobsForPartitions(self, partitions):
      """Create Ganga Jobs for the next N partitions that are ready and submit them."""
      j = self.createNewJob(partitions[0])
      if len(partitions) > 1:
         j.splitter = ArgSplitterTask()
         j.splitter.args = [self.args[p-1] for p in partitions]
         j.splitter.task_partitions = partitions
      else:
         p = partitions[0]
         if (p < 1 or p > len(self.args)):
            raise ApplicationConfigurationError("Partition %i did not find a corresponding argment!", p)
         j.application.args = self.args[p-1]
      return [j]

