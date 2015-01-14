from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Tasks.CoreUnit import CoreUnit
from Ganga.GPIDev.Lib.Job.Job import JobError, Job

class CoreTransform(ITransform):
   _schema = Schema(Version(1,0), dict(ITransform._schema.datadict.items() + {
            'unit_splitter'     :  ComponentItem('splitters', defvalue=None, optional=1,load_default=False, doc='Splitter to be used to create the units'),
            'fields_to_copy': SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc = 'A list of fields that should be copied when creating units, e.g. application, inputfiles. Empty (default) implies all fields are copied unless the GeenricSplitter is used '),
    }.items()))
   
   _category = 'transforms'
   _name = 'CoreTransform'
   _exportmethods = ITransform._exportmethods + [ ]

   def __init__(self):
      super(CoreTransform,self).__init__()

   def createUnits(self):
      """Create new units if required given the inputdata"""

      # call parent for chaining
      super(CoreTransform,self).createUnits()

      
      # Use the given splitter to create the unit definitions
      if len(self.units) > 0:
         # already have units so return
         return

      if self.unit_splitter == None:
         raise ApplicationConfigurationError(None, "No unit splitter provided for CoreTransform unit creation, Transform %d (%s)" % 
                                             (self.getID(), self.name))

      # create a dummy job, assign everything and then call the split
      j = Job()
      j.backend = self.backend.clone()
      j.application = self.application.clone()

      subjobs = self.unit_splitter.split(j)

      if len(subjobs) == 0:
         raise ApplicationConfigurationError(None, "Unit splitter gave no subjobs after split for CoreTransform unit creation, Transform %d (%s)" % 
                                             (self.getID(), self.name))

      # now create the units from these jobs
      for sj in subjobs:
         unit = CoreUnit()
         unit.application = sj.application.clone()
         self.addUnitToTRF( unit )
