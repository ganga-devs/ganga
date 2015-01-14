from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
import copy

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
      
      j.backend = self._getParent().backend.clone()

      # copy form ourselves or the parent transform depending on what's specified
      fields = ['application', 'splitter', 'inputfiles', 'inputdata', 'inputsandbox', 'outputfiles', 'postprocessors']

      for f in fields:

         if ( f == "postprocessors" and len(getattr(self, f).process_objects) > 0 ) or (f != "postprocessors" and getattr(self, f) != None):
            setattr(j, f, copy.deepcopy(getattr(self, f)) )
         elif ( f == "postprocessors" and len(getattr(self._getParent(), f).process_objects) > 0 ) or (f != "postprocessors" and getattr(self._getParent(), f) != None):
            setattr(j, f, copy.deepcopy(getattr(self._getParent(), f)) )

      return j
