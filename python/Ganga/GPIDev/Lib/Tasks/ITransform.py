from common import *
from TaskApplication import ExecutableTask, taskApp
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.Job.MetadataDict import *
from IUnit import IUnit
import time

class ITransform(GangaObject):
   _schema = Schema(Version(1,0), {
        'status'         : SimpleItem(defvalue='new', protected=1, copyable=0, doc='Status - running, pause or completed', typelist=["str"]),
        'name'           : SimpleItem(defvalue='Simple Transform', doc='Name of the transform (cosmetic)', typelist=["str"]),
        'application'    : ComponentItem('applications', defvalue=None, optional=1, load_default=False, doc='Application of the Transform.'),
        'inputsandbox'   : FileItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File'],sequence=1,doc="list of File objects shipped to the worker node "),
        'outputsandbox'  : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of filenames or patterns shipped from the worker node"),
        'backend'        : ComponentItem('backends', defvalue=None, optional=1,load_default=False, doc='Backend of the Transform.'),
        'splitter'       : ComponentItem('splitters', defvalue=None, optional=1,load_default=False, doc='Splitter used on each unit of the Transform.'),
        'merger'         : ComponentItem('mergers', defvalue=None, load_default=0,optional=1, doc='Local merger to be done over all units when complete.'),
        'run_limit'      : SimpleItem(defvalue=8, doc='Number of times a partition is tried to be processed.', protected=1, typelist=["int"]),
        'minor_run_limit'      : SimpleItem(defvalue=4, doc='Number of times a unit can be resubmitted', protected=1, typelist=["int"]),
        'major_run_limit'      : SimpleItem(defvalue=4, doc='Number of times a junit can be rebrokered', protected=1, typelist=["int"]),
        'units'          : ComponentItem('units',defvalue=[],sequence=1,copyable=1,doc='list of units'),
        'inputdata'      : ComponentItem('datasets', defvalue=[], sequence=1, protected=1, optional=1, load_default=False,doc='Input datasets to run over'),
        'outputdata'     : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Output dataset template'),
        'metadata'       : ComponentItem('metadata',defvalue = MetadataDict(), doc='the metadata', protected =1),
        'rebroker_on_job_fail'     : SimpleItem(defvalue=False, doc='Rebroker if too many minor resubs'),
        'abort_loop_on_submit'     : SimpleItem(defvalue=True, doc='Break out of the Task Loop after submissions'),
        'required_trfs'  : SimpleItem(defvalue=[],typelist=['int'],sequence=1,doc="IDs of transforms that must complete before this unit will start. NOTE DOESN'T COPY OUTPUT DATA TO INPUT DATA. Use TaskChainInput Dataset for that."),
        'chain_delay'    : SimpleItem(defvalue=0, doc='Minutes delay between a required/chained unit completing and starting this one', protected=1, typelist=["int"]),
    })

   _category = 'transforms'
   _name = 'ITransform'
   _exportmethods = [ 'addInputData', 'resetUnit', 'setRunLimit', 'getJobs', 'setMinorRunLimit', 'setMajorRunLimit', 'getID' ]

   def getJobs(self):
      """Return a list of the currently active job ids"""
      joblist = []
      for u in self.units:
         joblist += u.active_job_ids
      return joblist
   
   def setMinorRunLimit(self, newRL):
      """Set the number of times a job will be resubmitted before a major resubmit is attempted"""
      self.minor_run_limit = newRL

   def setMajorRunLimit(self, newRL):
      """Set the number of times a job will be rebrokered before the transform is paused"""
      self.major_run_limit = newRL
      
   def setRunLimit(self, newRL):
      """Set the total (minor+major) number of times a job should be resubmitted before the transform is paused"""
      self.run_limit = newRL

   def overview(self, status = ''):
      """Show the status of the units in this transform"""
      for unit in self.units:
           # display colour given state
           o = ""
           o += ("%d:  " % self.units.index(unit)) + unit.name
           
           # is unit active?          
           if unit.active:
               o += " " * (40-len(o) + 3) + "*"
           else:
               o += " " * (40-len(o) + 3) + "-"

           
           # sub job status
           o += "\t %i" % unit.n_status("submitted")
           o += "\t %i" % unit.n_status("running")
           o += "\t %i" % unit.n_status("completed")
           o += "\t %i" % unit.n_status("failed")
           o += "\t %i" % unit.minor_resub_count
           o += "\t %i" % unit.major_resub_count
           
           # change colour on state
           if unit.status == 'completed':
               o = markup(o,overview_colours["completed"])
           elif not unit.active:
               o = markup(o,overview_colours["bad"])
           elif len(unit.active_job_ids) == 0:
               o = markup(o,overview_colours["hold"])
           else:
               o = markup(o,overview_colours["running"])

           print o

               
## Special methods:
   def __init__(self):
      super(ITransform,self).__init__()
      self.initialize()
      
   def _readonly(self):
      """A transform is read-only if the status is not new."""
      if self.status == "new":
         return 0
      return 1

   def initialize(self):
      self.backend = stripProxy(GPI.Local())
      self.updateStatus("new")

   def check(self):
      """Check this transform has valid data, etc. and has the correct units"""

      # ignore anything but new transforms
      if self.status != "new":
         return
      
      self.updateStatus("running")
      
      # first, validate the transform
      if not self.validate():
         raise ApplicationConfigurationError(None, "Validate failed for Transform %s" % self.name)
      
   def startup(self):
      """This function is used to set the status after restarting Ganga"""
      pass

## Public methods
   def resetUnit(self, uid):
      """Reset the given unit"""
      for u in self.units:
         if u.getID() == uid:
            u.reset()
            
      self.updateStatus("running")
      
      
   def getID(self):
       """Return the index of this trf in the parent task"""
       task = self._getParent()
       if not task:
           raise ApplicationConfigurationError(None, "This transform has not been associated with a task and so there is no ID available")       
       return task.transforms.index(self)
            
   def run(self, check=True):
      """Sets this transform to running status"""
      if self.status == "new" and check:
         self.check()
      if self.status != "completed":
         self.updateStatus("running")
         task = self._getParent()
         if task:
            task.updateStatus()
      else:
         logger.warning("Transform is already completed!")

   def update(self):
      """Called by the parent task to check for status updates, submit jobs, etc."""
      #logger.warning("Entered Transform %d update function..." % self.getID())

      if self.status == "pause" or self.status == "new":
         return 0

      # check for complete required units
      task = self._getParent()
      for trf_id in self.required_trfs:
         if task.transforms[trf_id].status != "completed":
            return 0

      # set the start time if not already set
      if len(self.required_trfs) > 0 and self.units[0].start_time == 0:
         for unit in self.units:
            unit.start_time = time.time() + self.chain_delay * 60 - 1
                        
      # ask the unit splitter if we should create any more units given the current data
      self.createUnits( )

      # loop over units and update them ((re)submits will be called here)
      old_status = self.status
      unit_status_list = []
      for unit in self.units:
         
         if unit.update() and self.abort_loop_on_submit:
            logger.warning("Unit %d of transform %d has aborted the loop" % (unit.getID(), self.getID()))
            return 1

         unit_status_list.append( unit.status )
            
      # update status and check
      old_status = self.status
      for state in ['running', 'completed']:
         if state in unit_status_list:
            if state != self.status:
               self.updateStatus(state)               
            break

   def createUnits(self):
      """Create new units if required given the inputdata"""

      # check for chaining
      for ds in self.inputdata:
         if ds._name == "TaskChainInput" and ds.input_trf_id != -1:

            # loop over units in parent trf and create units as required
            for in_unit in self._getParent().transforms[ds.input_trf_id].units:

               # is there a unit already linked?
               done = False
               for out_unit in self.units:
                  if '%d:%d' % (ds.input_trf_id, in_unit.getID() ) in out_unit.req_units:
                     done = True

               if not done:
                  new_unit = self.createChainUnit( in_unit )
                  if new_unit:
                     self.addChainUnitToTRF( new_unit, ds, in_unit.getID() )

   def createChainUnit( self, outdata ):
      """Create a chained unit given the parent outputdata"""
      return IUnit()
      
   def addChainUnitToTRF( self, unit, inDS, unit_id = -1 ):
      """Add a chained unit to this TRF. Override for more control"""
      unit.req_units.append('%d:%d' % (inDS.input_trf_id, unit_id ) )
      unit.name = "Parent: TRF %d, Unit %d" % (inDS.input_trf_id, unit_id )
      self.addUnitToTRF(unit)
   
   def addInputData(self, inDS):
      """Add the given input dataset to the list"""
      self.inputdata.append(inDS)

   def pause(self):
      """Pause the task - the background thread will not submit new jobs from this task"""
      if self.status != "completed":
         self.updateStatus("pause")
         #self.status = "pause"
         task = self._getParent()
         if task:
            task.updateStatus()
      else:
         logger.debug("Transform is already completed!")

   def setRunlimit(self,newRL):
      """Set the number of times a job should be resubmitted before the transform is paused"""
      self.run_limit = newRL
      logger.debug("Runlimit set to %i", newRL)

## Methods that can/should be overridden by derived classes
   def validate(self):
      """Override this to validate that the transform is OK"""

      # this is a generic trf so assume the application and splitter will do all the work
      return True

   def addUnitToTRF(self, unit):
      """Add a unit to this Transform given the input and output data"""
      if not unit:
         raise ApplicationConfigurationError(None, "addUnitTOTRF failed for Transform %d (%s): No unit specified" %
                                             (self.getID(), self.name))

      unit.updateStatus("hold")
      unit.active = True
      self.units.append(unit)

## Information methods
   def fqn(self):
      task = self._getParent()
      if task:
         return "Task %i Transform %i" % (task.id, task.transforms.index(self))
      else:
         return "Unassigned Transform '%s'" % (self.name)

   def n_active(self):
      return sum([u.n_active() for u in self.units])
   
   def n_all(self):
      return sum([u.n_all() for u in self.units])

   def n_status(self,status):
      return sum([u.n_status(status) for u in self.units])

   def info(self):
      print markup("%s '%s'" % (self.__class__.__name__, self.name), status_colours[self.status])
      print "* backend: %s" % self.backend.__class__.__name__
      print "Application:"
      self.application.printTree() 

   def updateStatus(self, status):
      """Update the transform status"""
      self.status = status

