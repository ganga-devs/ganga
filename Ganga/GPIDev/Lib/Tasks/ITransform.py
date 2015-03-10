from common import *
from TaskApplication import ExecutableTask, taskApp
from Ganga.GPIDev.Lib.Job.Job import JobError, Job
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.Job.MetadataDict import *
from IUnit import IUnit
import time
import os

class ITransform(GangaObject):
   _schema = Schema(Version(1,0), {
        'status'         : SimpleItem(defvalue='new', protected=1, copyable=0, doc='Status - running, pause or completed', typelist=["str"]),
        'name'           : SimpleItem(defvalue='Simple Transform', doc='Name of the transform (cosmetic)', typelist=["str"]),
        'application'    : ComponentItem('applications', defvalue=None, optional=1, load_default=False, doc='Application of the Transform.'),
        'inputsandbox'   : FileItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File'],sequence=1,doc="list of File objects shipped to the worker node "),
        'outputsandbox'  : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of filenames or patterns shipped from the worker node"),
        'backend'        : ComponentItem('backends', defvalue=None, optional=1,load_default=False, doc='Backend of the Transform.'),
        'splitter'       : ComponentItem('splitters', defvalue=None, optional=1,load_default=False, doc='Splitter used on each unit of the Transform.'),
        'postprocessors':ComponentItem('postprocessor',defvalue=None,doc='list of postprocessors to run after job has finished'),
        'merger'         : ComponentItem('mergers', defvalue=None, hidden=1, copyable=0, load_default=0,optional=1, doc='Merger to be done over all units when complete.'),
        'unit_merger'    : ComponentItem('mergers', defvalue=None, load_default=0,optional=1, doc='Merger to be copied and run on each unit separately.'),
        'copy_output' : ComponentItem('datasets', defvalue=None, load_default=0,optional=1, doc='The dataset to copy all units output to, e.g. Grid dataset -> Local Dataset'),
        'unit_copy_output' : ComponentItem('datasets', defvalue=None, load_default=0,optional=1, doc='The dataset to copy each individual unit output to, e.g. Grid dataset -> Local Dataset'),
        'unit_merger'    : ComponentItem('mergers', defvalue=None, load_default=0,optional=1, doc='Merger to be run copied and run on each unit separately.'),
        'run_limit'      : SimpleItem(defvalue=8, doc='Number of times a partition is tried to be processed.', protected=1, typelist=["int"]),
        'minor_run_limit'      : SimpleItem(defvalue=3, doc='Number of times a unit can be resubmitted', protected=1, typelist=["int"]),
        'major_run_limit'      : SimpleItem(defvalue=3, doc='Number of times a junit can be rebrokered', protected=1, typelist=["int"]),
        'units'          : ComponentItem('units',defvalue=[],sequence=1,copyable=1,doc='list of units'),
        'inputdata'      : ComponentItem('datasets', defvalue=[], sequence=1, protected=1, optional=1, load_default=False,doc='Input datasets to run over'),
        'outputdata'     : ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='Output dataset template'),
        'inputfiles' : GangaFileItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.IOutputFile.IOutputFile'],sequence=1,doc="list of file objects that will act as input files for a job"),
        'outputfiles' : GangaFileItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.OutputFile.OutputFile'],sequence=1,doc="list of \
OutputFile objects to be copied to all jobs"),
        'metadata'       : ComponentItem('metadata',defvalue = MetadataDict(), doc='the metadata', protected =1),
        'rebroker_on_job_fail'     : SimpleItem(defvalue=True, doc='Rebroker if too many minor resubs'),
        'abort_loop_on_submit'     : SimpleItem(defvalue=True, doc='Break out of the Task Loop after submissions'),
        'required_trfs'  : SimpleItem(defvalue=[],typelist=['int'],sequence=1,doc="IDs of transforms that must complete before this unit will start. NOTE DOESN'T COPY OUTPUT DATA TO INPUT DATA. Use TaskChainInput Dataset for that."),
        'chain_delay'    : SimpleItem(defvalue=0, doc='Minutes delay between a required/chained unit completing and starting this one', protected=1, typelist=["int"]),
        'submit_with_threads': SimpleItem(defvalue=False, doc='Use Ganga Threads for submission'),
        'max_active_threads': SimpleItem(defvalue=10, doc='Maximum number of Ganga Threads to use. Note that the number of simultaneous threads is controlled by the queue system (default is 5)'),
        #'force_single_unit' : SimpleItem(defvalue=False, doc='Force all input data into one Unit'),
    })

   _category = 'transforms'
   _name = 'ITransform'
   _exportmethods = [ 'addInputData', 'resetUnit', 'setRunLimit', 'getJobs', 'setMinorRunLimit', 'setMajorRunLimit', 'getID', 'overview', 'resetUnitsByStatus', 'removeUnusedJobs' ]
   _hidden = 0

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
           elif unit.status == "recreating":
               o = markup(o,overview_colours["attempted"])
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
      
      # first, validate the transform
      if not self.validate():
         raise ApplicationConfigurationError(None, "Validate failed for Transform %s" % self.name)

            
      self.updateStatus("running")

      
   def startup(self):
      """This function is used to set the status after restarting Ganga"""
      pass

## Public methods
   def resetUnit(self, uid):
      """Reset the given unit"""
      for u in self.units:
         if u.getID() == uid:
            u.reset()
            break

      # find any chained units and mark for recreation
      for trf in self._getParent().transforms:
         for u2 in trf.units:
            for req in u2.req_units:
               if req == "%d:%d" % (self.getID(), u.getID()) or req == "%d:ALL" % (self.getID()):
                  trf.resetUnit(u2.getID())

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

      # find submissions first
      unit_update_list = []
      for unit in self.units:

         if not unit.checkForSubmission() and not unit.checkForResubmission():
            unit_update_list.append(unit)
            continue
            
         if unit.update() and self.abort_loop_on_submit:
            logger.info("Unit %d of transform %d, Task %d has aborted the loop" % (unit.getID(), self.getID(), task.id))
            return 1

         unit_status_list.append( unit.status )

      # now check for download
      for unit in unit_update_list:
         if unit.update() and self.abort_loop_on_submit:
            logger.info("Unit %d of transform %d, Task %d has aborted the loop" % (unit.getID(), self.getID(), task.id))
            return 1
         
         unit_status_list.append( unit.status )

      # check for any TaskChainInput completions
      for ds in self.inputdata:
         if ds._name == "TaskChainInput" and ds.input_trf_id != -1:
            if task.transforms[ds.input_trf_id].status != "completed":
               return 0
                        
      # update status and check
      old_status = self.status
      for state in ['running', 'hold', 'bad', 'completed']:
         if state in unit_status_list:
            if state == 'hold':
               state = "running"
            if state != self.status:
               self.updateStatus(state)               
            break

   def createUnits(self):
      """Create new units if required given the inputdata"""

      # check for chaining
      for ds in self.inputdata:
         if ds._name == "TaskChainInput" and ds.input_trf_id != -1:

            # check for single unit
            if ds.single_unit:
               
               # is there a unit already linked?
               done = False
               rec_unit = None
               for out_unit in self.units:
                  if '%d:ALL' % (ds.input_trf_id) in out_unit.req_units:                     
                     done = True
                     # check if the unit is being recreated
                     if out_unit.status == "recreating":
                        rec_unit = out_unit
                     break

               if not done or rec_unit:
                  new_unit = self.createChainUnit( self._getParent().transforms[ds.input_trf_id].units, ds.use_copy_output )
                  if new_unit:
                     self.addChainUnitToTRF( new_unit, ds, -1, prev_unit = rec_unit )

            else:
                  
               # loop over units in parent trf and create units as required
               for in_unit in self._getParent().transforms[ds.input_trf_id].units:

                  # is there a unit already linked?
                  done = False
                  rec_unit = None
                  for out_unit in self.units:
                     if '%d:%d' % (ds.input_trf_id, in_unit.getID() ) in out_unit.req_units:
                        done = True
                        # check if the unit is being recreated
                        if out_unit.status == "recreating":
                           rec_unit = out_unit
                        break
                                                                  

                  if not done or rec_unit:
                     new_unit = self.createChainUnit( [ in_unit ], ds.use_copy_output )
                     if new_unit:
                        self.addChainUnitToTRF( new_unit, ds, in_unit.getID(), prev_unit = rec_unit )
                        

   def createChainUnit( self, parent_units, use_copy_output = True ):
      """Create a chained unit given the parent outputdata"""
      return IUnit()
      
   def addChainUnitToTRF( self, unit, inDS, unit_id = -1, prev_unit = None ):
      """Add a chained unit to this TRF. Override for more control"""
      if unit_id == -1:
         unit.req_units.append('%d:ALL' % (inDS.input_trf_id ) )
         unit.name = "Parent: TRF %d, All Units" % (inDS.input_trf_id )
      else:
         unit.req_units.append('%d:%d' % (inDS.input_trf_id, unit_id ) )
         unit.name = "Parent: TRF %d, Unit %d" % (inDS.input_trf_id, unit_id )

      self.addUnitToTRF(unit, prev_unit)
   
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

      # make sure a path has been selected for any local downloads
      if self.unit_copy_output != None and self.unit_copy_output._name == "TaskLocalCopy":
         if self.unit_copy_output.local_location == '':
            logger.error("No path selected for Local Output Copy")
            return False

      if self.copy_output != None and self.copy_output._name == "TaskLocalCopy":
         if self.copy_output.local_location == '':
            logger.error("No path selected for Local Output Copy")
            return False
   
      # this is a generic trf so assume the application and splitter will do all the work
      return True

   def addUnitToTRF(self, unit, prev_unit = None):
      """Add a unit to this Transform given the input and output data"""
      if not unit:
         raise ApplicationConfigurationError(None, "addUnitTOTRF failed for Transform %d (%s): No unit specified" %
                                             (self.getID(), self.name))

      unit.updateStatus("hold")
      unit.active = True
      if prev_unit:
         unit.prev_job_ids += prev_unit.prev_job_ids
         self.units[ prev_unit.getID() ] = unit
      else:
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

   def createUnitCopyOutputDS(self, unit_id):
      """Create a the Copy Output dataset to use with this unit. Overload to handle more than the basics"""
      
      if self.unit_copy_output._name != "TaskLocalCopy":
         logger.warning("Default implementation of createUnitCopyOutputDS can't handle datasets of type '%s'" % self.unit_copy_output._name)
         return

      # create copies of the Copy Output DS and add Unit name to path
      self.units[unit_id].copy_output = self.unit_copy_output.clone()
      self.units[unit_id].copy_output.local_location = os.path.join( self.unit_copy_output.local_location, self.units[unit_id].name.replace(":", "_").replace(" ", "").replace(",","_") )
    
                    
   def __setattr__(self, attr, value):

      if attr == 'outputfiles':

         if value != []:     
            if self.outputdata is not None:
               logger.error('ITransform.outputdata is set, you can\'t set ITransform.outputfiles')
               return
            elif self.outputsandbox != []:
               logger.error('ITransform.outputsandbox is set, you can\'t set ITransform.outputfiles')
               return      
                        
         #reduce duplicate values here, leave only duplicates for LCG, where we can have replicas    
         uniqueValuesDict = []
         uniqueValues = []
        
         for val in value:
            key = '%s%s' % (val.__class__.__name__, val.namePattern)               
            if key not in uniqueValuesDict:
               uniqueValuesDict.append(key)
               uniqueValues.append(val) 
            elif val.__class__.__name__ == 'LCGSEFile':   
               uniqueValues.append(val) 
        
         super(ITransform,self).__setattr__(attr, uniqueValues) 

      elif attr == 'inputfiles':

         if value != []:     
            if self.inputsandbox != []:
               logger.error('ITransform.inputsandbox is set, you can\'t set ITransform.inputfiles')
               return      
                                
         super(ITransform,self).__setattr__(attr, value) 

      elif attr == 'outputsandbox':

         if value != []:     
            
            if getConfig('Output')['ForbidLegacyOutput']:
               logger.error('Use of ITransform.outputsandbox is forbidden, please use ITransform.outputfiles')
               return

            if self.outputfiles != []:
               logger.error('ITransform.outputfiles is set, you can\'t set ITransform.outputsandbox')
               return

         super(ITransform,self).__setattr__(attr, value)

      elif attr == 'inputsandbox':

         if value != []:     

            if getConfig('Output')['ForbidLegacyInput']:
               logger.error('Use of ITransform.inputsandbox is forbidden, please use ITransform.inputfiles')
               return

            if self.inputfiles != []:
               logger.error('ITransform.inputfiles is set, you can\'t set ITransform.inputsandbox')
               return
                
         super(ITransform,self).__setattr__(attr, value)

      elif attr == 'outputdata':

         if value != None:   

            if getConfig('Output')['ForbidLegacyOutput']:
               logger.error('Use of ITransform.outputdata is forbidden, please use ITransform.outputfiles')
               return

            if self.outputfiles != []:
               logger.error('ITransform.outputfiles is set, you can\'t set ITransform.outputdata')
               return
         super(ITransform,self).__setattr__(attr, value)
                
      else:   
         super(ITransform,self).__setattr__(attr, value)


   def resetUnitsByStatus( self, status = 'bad' ):
      """Reset all units of a given status"""
      for unit in self.units:
         if unit.status == status:
            print "Resetting Unit %d, Transform %d..." % (unit.getID(), self.getID() )
            self.resetUnit( unit.getID() )

   def removeUnusedJobs( self ):
      """Remove all jobs that aren't being used, e.g. failed jobs"""
      for unit in self.units:
         for jid in unit.prev_job_ids:
            try:
               logger.warning("Removing job '%d'..." % jid)
               job = GPI.jobs(jid)
               job.remove()
            except:
               logger.error ("Problem removing job '%d'" % jid)
