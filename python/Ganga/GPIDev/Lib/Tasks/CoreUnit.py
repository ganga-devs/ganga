from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit

class CoreUnit(IUnit):
   _schema = Schema(Version(1,0), dict(IUnit._schema.datadict.items() + {
    }.items()))

   _category = 'units'
   _name = 'CoreUnit'
   _exportmethods = IUnit._exportmethods + [ ]

   def __init__(self):
      super(CoreUnit, self).__init__()

   def createNewJob(self):
      """Create any jobs required for this unit"""
      j = GPI.Job()
      j._impl.backend = self._getParent().backend.clone()
      
      if self.application != None:
         j._impl.application = self._getParent().application.clone()

      import copy
      j.inputfiles = copy.deepcopy(self._getParent().inputfiles)
      j.inputdata = copy.deepcopy(self._getParent().inputdata)
      j.inputsandbox = copy.deepcopy(self._getParent().inputsandbox)
      j.outputfiles = copy.deepcopy(self._getParent().outputfiles)
      j.outputsandbox = copy.deepcopy(self._getParent().outputsandbox)
      
      if self._getParent().splitter:
         j.splitter = self._getParent().splitter.clone()
      #j.postprocessor = copy.deepcopy(self._getParent().postprocessor)

      return j
