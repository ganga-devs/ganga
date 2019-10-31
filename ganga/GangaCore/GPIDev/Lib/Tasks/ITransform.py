

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem, FileItem, GangaFileItem
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.ColourText import status_colours, overview_colours, ANSIMarkup
markup = ANSIMarkup()
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Lib.Job import MetadataDict
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Base.Proxy import stripProxy, isType, getName
from .IUnit import IUnit
import time
import os
from GangaCore.GPIDev.Lib.Tasks.ITask import addInfoString
from GangaCore.GPIDev.Lib.Tasks.common import getJobByID
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
from GangaCore.GPIDev.Lib.File.File import File

logger = getLogger()

class ITransform(GangaObject):
    _schema = Schema(Version(1, 0), {
        'status': SimpleItem(defvalue='new', protected=1, copyable=1, doc='Status - running, pause or completed', typelist=[str]),
        'name': SimpleItem(defvalue='Simple Transform', doc='Name of the transform (cosmetic)', typelist=[str]),
        'application': ComponentItem('applications', defvalue=None, optional=1, load_default=False, doc='Application of the Transform.'),
        'inputsandbox': FileItem(defvalue=[], sequence=1, doc="list of File objects shipped to the worker node "),
        'outputsandbox': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc="list of filenames or patterns shipped from the worker node"),
        'backend': ComponentItem('backends', defvalue=None, optional=1, load_default=False, doc='Backend of the Transform.'),
        'splitter': ComponentItem('splitters', defvalue=None, optional=1, load_default=False, doc='Splitter used on each unit of the Transform.'),
        'postprocessors': ComponentItem('postprocessor', defvalue=None, doc='list of postprocessors to run after job has finished'),
        'merger': ComponentItem('mergers', defvalue=None, hidden=1, copyable=0, load_default=0, optional=1, doc='Merger to be done over all units when complete.'),
        'unit_merger': ComponentItem('mergers', defvalue=None, load_default=0, optional=1, doc='Merger to be copied and run on each unit separately.'),
        'copy_output': ComponentItem('datasets', defvalue=None, load_default=0, optional=1, doc='The dataset to copy all units output to, e.g. Grid dataset -> Local Dataset'),
        'unit_copy_output': ComponentItem('datasets', defvalue=None, load_default=0, optional=1, doc='The dataset to copy each individual unit output to, e.g. Grid dataset -> Local Dataset'),
        'run_limit': SimpleItem(defvalue=8, doc='Number of times a partition is tried to be processed.', protected=1, typelist=[int]),
        'minor_run_limit': SimpleItem(defvalue=3, doc='Number of times a unit can be resubmitted', protected=1, typelist=[int]),
        'major_run_limit': SimpleItem(defvalue=3, doc='Number of times a junit can be rebrokered', protected=1, typelist=[int]),
        'units': ComponentItem('units', defvalue=[], sequence=1, copyable=1, doc='list of units'),
        'inputdata': ComponentItem('datasets', defvalue=[], sequence=1, protected=1, optional=1, load_default=False, doc='Input datasets to run over'),
        'outputdata': ComponentItem('datasets', defvalue=None, optional=1, load_default=False, doc='Output dataset template'),
        'inputfiles': GangaFileItem(defvalue=[], sequence=1, doc="list of file objects that will act as input files for a job"),
        'outputfiles' : GangaFileItem(defvalue=[], sequence=1, doc="list of OutputFile objects to be copied to all jobs"),
        'metadata': ComponentItem('metadata', defvalue=MetadataDict(), doc='the metadata', protected=1),
        'rebroker_on_job_fail': SimpleItem(defvalue=True, doc='Rebroker if too many minor resubs'),
        'abort_loop_on_submit': SimpleItem(defvalue=True, doc='Break out of the Task Loop after submissions'),
        'required_trfs': SimpleItem(defvalue=[], typelist=[int], sequence=1, doc="IDs of transforms that must complete before this unit will start. NOTE DOESN'T COPY OUTPUT DATA TO INPUT DATA. Use TaskChainInput Dataset for that."),
        'chain_delay': SimpleItem(defvalue=0, doc='Minutes delay between a required/chained unit completing and starting this one', protected=0, typelist=[int]),
        'submit_with_threads': SimpleItem(defvalue=False, doc='Use Ganga Threads for submission'),
        'max_active_threads': SimpleItem(defvalue=10, doc='Maximum number of Ganga Threads to use. Note that the number of simultaneous threads is controlled by the queue system (default is 5)'),
        'info' : SimpleItem(defvalue=[],typelist=[str],protected=1,sequence=1,doc="Info showing status transitions and unit info"),
        'id': SimpleItem(defvalue=-1, protected=1, doc='ID of the Transform', typelist=[int]),
        #'force_single_unit' : SimpleItem(defvalue=False, doc='Force all input data into one Unit'),
    })

    _category = 'transforms'
    _name = 'ITransform'
    _exportmethods = ['addInputData', 'resetUnit', 'setRunLimit', 'getJobs', 'setMinorRunLimit',
                      'setMajorRunLimit', 'getID', 'overview', 'resetUnitsByStatus', 'removeUnusedJobs',
                      'showInfo', 'showUnitInfo', 'pause', 'n_all', 'n_status' ]
    _hidden = 0

    def showInfo(self):
        """Print out the info in a nice way"""
        print("\n".join( self.info ))

    def showUnitInfo(self, uid):
        """Print out the given unit info in a nice way"""
        self.units[uid].showInfo()

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

    def overview(self, status=''):
        """Show the status of the units in this transform"""
        for unit in self.units:
            # display colour given state
            o = ""
            o += ("%d:  " % self.units.index(unit)) + unit.name

            # is unit active?
            if unit.active:
                o += " " * (40 - len(o) + 3) + "*"
            else:
                o += " " * (40 - len(o) + 3) + "-"

            # sub job status
            o += "\t %i" % unit.n_status("submitted")
            o += "\t %i" % unit.n_status("running")
            o += "\t %i" % unit.n_status("completed")
            o += "\t %i" % unit.n_status("failed")
            o += "\t %i" % unit.minor_resub_count
            o += "\t %i" % unit.major_resub_count

            # change colour on state
            if unit.status == 'completed':
                o = markup(o, overview_colours["completed"])
            elif not unit.active:
                o = markup(o, overview_colours["bad"])
            elif unit.status == "recreating":
                o = markup(o, overview_colours["attempted"])
            elif len(unit.active_job_ids) == 0:
                o = markup(o, overview_colours["hold"])
            else:
                o = markup(o, overview_colours["running"])

            print(o)


# Special methods:
    def __init__(self):
        super(ITransform, self).__init__()
        self.initialize()

    def _auto__init__(self):
        self.status = 'new'

    def _readonly(self):
        """A transform is read-only if the status is not new."""
        if self.status == "new":
            return 0
        return 1

    def initialize(self):
        from GangaCore.Lib.Localhost.Localhost import Localhost
        self.backend = Localhost()

    def check(self):
        """Check this transform has valid data, etc. and has the correct units"""

        # ignore anything but new transforms
        if self.status != "new":
            return

        # first, validate the transform
        if not self.validate():
            raise ApplicationConfigurationError(
                "Validate failed for Transform %s" % self.name)

        self.updateStatus("running")

    def startup(self):
        """This function is used to set the status after restarting Ganga"""
        pass

# Public methods
    def resetUnit(self, uid):
        """Reset the given unit"""
        addInfoString( self, "Reseting Unit %i" % ( uid ) )

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

        # if the id isn't already set, use the index from the parent Task
        if self.id < 0:
            task = self._getParent()
            if not task:
                raise ApplicationConfigurationError(
                    "This transform has not been associated with a task and so there is no ID available")
            self.id = task.transforms.index(self)
        
        return self.id

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
        if self.status == "pause" or self.status == "new":
            return 0

        # check for complete required units
        task = self._getParent()
        for trf_id in self.required_trfs:
            if task.transforms[trf_id].status != "completed":
                return 0

        # report the info for this transform
        unit_status = { "new":0, "hold":0, "running":0, "completed":0, "bad":0, "recreating":0 }
        for unit in self.units:
            unit_status[unit.status] += 1
         
        info_str = "Unit overview: %i units, %i new, %i hold, %i running, %i completed, %i bad. to_sub %i" % (len(self.units), unit_status["new"], unit_status["hold"],
                                                                                                              unit_status["running"], unit_status["completed"],
                                                                                                              unit_status["bad"], self._getParent().n_tosub())
      
        addInfoString(self, info_str)
                
        # ask the unit splitter if we should create any more units given the
        # current data
        self.createUnits()

        # set the start time if not already set
        if len(self.required_trfs) > 0 and self.units[0].start_time == 0:
            for unit in self.units:
                unit.start_time = time.time() + self.chain_delay * 60 - 1

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
                logger.info("Unit %d of transform %d, Task %d has aborted the loop" % (
                    unit.getID(), self.getID(), task.id))
                return 1

            unit_status_list.append(unit.status)

        # now check for download
        for unit in unit_update_list:
            if unit.update() and self.abort_loop_on_submit:
                logger.info("Unit %d of transform %d, Task %d has aborted the loop" % (
                    unit.getID(), self.getID(), task.id))
                return 1

            unit_status_list.append(unit.status)

        from GangaCore.GPIDev.Lib.Tasks.TaskChainInput import TaskChainInput
        # check for any TaskChainInput completions
        for ds in self.inputdata:
            if isType(ds, TaskChainInput) and ds.input_trf_id != -1:
                if task.transforms[ds.input_trf_id].status != "completed":
                    return 0

        # update status and check
        for state in ['running', 'hold', 'bad', 'completed']:
            if state in unit_status_list:
                if state == 'hold':
                    state = "running"
                if state != self.status:
                    self.updateStatus(state)
                break

    def createUnits(self):
        """Create new units if required given the inputdata"""

        from GangaCore.GPIDev.Lib.Tasks.TaskChainInput import TaskChainInput
        # check for chaining
        for ds in self.inputdata:
            if isType(ds, TaskChainInput) and ds.input_trf_id != -1:

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
                        new_unit = self.createChainUnit(
                            self._getParent().transforms[ds.input_trf_id].units, ds.use_copy_output)
                        if new_unit:
                            self.addChainUnitToTRF(
                                new_unit, ds, -1, prev_unit=rec_unit)

                else:

                    # loop over units in parent trf and create units as
                    # required
                    for in_unit in self._getParent().transforms[ds.input_trf_id].units:

                        # is there a unit already linked?
                        done = False
                        rec_unit = None
                        for out_unit in self.units:
                            if '%d:%d' % (ds.input_trf_id, in_unit.getID()) in out_unit.req_units:
                                done = True
                                # check if the unit is being recreated
                                if out_unit.status == "recreating":
                                    rec_unit = out_unit
                                break

                        if not done or rec_unit:
                            new_unit = self.createChainUnit(
                                [in_unit], ds.use_copy_output)
                            if new_unit:
                                self.addChainUnitToTRF(
                                    new_unit, ds, in_unit.getID(), prev_unit=rec_unit)

    def createChainUnit(self, parent_units, use_copy_output=True):
        """Create a chained unit given the parent outputdata"""
        return IUnit()

    def addChainUnitToTRF(self, unit, inDS, unit_id=-1, prev_unit=None):
        """Add a chained unit to this TRF. Override for more control"""
        if unit_id == -1:
            unit.req_units.append('%d:ALL' % (inDS.input_trf_id))
            unit.name = "Parent: TRF %d, All Units" % (inDS.input_trf_id)
        else:
            unit.req_units.append('%d:%d' % (inDS.input_trf_id, unit_id))
            unit.name = "Parent: TRF %d, Unit %d" % (
                inDS.input_trf_id, unit_id)

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

    def setRunlimit(self, newRL):
        """Set the number of times a job should be resubmitted before the transform is paused"""
        self.run_limit = newRL
        logger.debug("Runlimit set to %i", newRL)

# Methods that can/should be overridden by derived classes
    def validate(self):
        """Override this to validate that the transform is OK"""

        from GangaCore.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy
        # make sure a path has been selected for any local downloads
        if self.unit_copy_output is not None and isType(self.unit_copy_output, TaskLocalCopy):
            if self.unit_copy_output.local_location == '':
                logger.error("No path selected for Local Output Copy")
                return False

        if self.copy_output is not None and isType(self.copy_output, TaskLocalCopy):
            if self.copy_output.local_location == '':
                logger.error("No path selected for Local Output Copy")
                return False

        # this is a generic trf so assume the application and splitter will do
        # all the work
        return True

    def addUnitToTRF(self, unit, prev_unit=None):
        """Add a unit to this Transform given the input and output data"""
        if not unit:
            raise ApplicationConfigurationError("addUnitTOTRF failed for Transform %d (%s): No unit specified" % (self.getID(), self.name))

        addInfoString( self, "Adding Unit to TRF...")
        unit.updateStatus("hold")
        unit.active = True
        if prev_unit:
            unit.prev_job_ids += prev_unit.prev_job_ids
            self.units[prev_unit.getID()] = unit
        else:
            self.units.append(unit)
            stripProxy(unit).id = len(self.units) - 1

# Information methods
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

    def n_status(self, status):
        return sum([u.n_status(status) for u in self.units])

    def info(self):
        logger.info(markup("%s '%s'" % (getName(self), self.name), status_colours[self.status]))
        logger.info("* backend: %s" % getName(self.backend))
        logger.info("Application:")
        self.application.printTree()

    def updateStatus(self, status):
        """Update the transform status"""
        self.status = status

    def createUnitCopyOutputDS(self, unit_id):
        """Create a the Copy Output dataset to use with this unit. Overload to handle more than the basics"""

        from GangaCore.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy
        if isType(self.unit_copy_output, TaskLocalCopy):
            logger.warning("Default implementation of createUnitCopyOutputDS can't handle datasets of type '%s'" % getName(self.unit_copy_output))
            return

        # create copies of the Copy Output DS and add Unit name to path
        self.units[unit_id].copy_output = self.unit_copy_output.clone()
        self.units[unit_id].copy_output.local_location = os.path.join(
            self.unit_copy_output.local_location, self.units[unit_id].name.replace(":", "_").replace(" ", "").replace(",", "_"))

    def __setattr__(self, attr, value):

        if attr == 'outputfiles':

            if value != []:
                if self.outputdata is not None:
                    logger.error(
                        'ITransform.outputdata is set, you can\'t set ITransform.outputfiles')
                    return
                elif self.outputsandbox != []:
                    logger.error(
                        'ITransform.outputsandbox is set, you can\'t set ITransform.outputfiles')
                    return

            # reduce duplicate values here, leave only duplicates for LCG,
            # where we can have replicas
            uniqueValuesDict = []
            uniqueValues = []

            for val in value:
                key = '%s%s' % (getName(val), val.namePattern)
                if key not in uniqueValuesDict:
                    uniqueValuesDict.append(key)
                    uniqueValues.append(val)
                elif getName(val) == 'LCGSEFile':
                    uniqueValues.append(val)

            super(ITransform, self).__setattr__(attr, uniqueValues)

        elif attr == 'inputfiles':

            if value != []:
                if self.inputsandbox != []:
                    logger.error(
                        'ITransform.inputsandbox is set, you can\'t set ITransform.inputfiles')
                    return

            super(ITransform, self).__setattr__(attr, value)

        elif attr == 'outputsandbox':

            if value != []:

                if getConfig('Output')['ForbidLegacyOutput']:
                    logger.error(
                        'Use of ITransform.outputsandbox is forbidden, please use ITransform.outputfiles')
                    return

                if self.outputfiles != []:
                    logger.error(
                        'ITransform.outputfiles is set, you can\'t set ITransform.outputsandbox')
                    return

            super(ITransform, self).__setattr__(attr, value)

        elif attr == 'inputsandbox':

            if value != []:

                if getConfig('Output')['ForbidLegacyInput']:
                    logger.error(
                        'Use of ITransform.inputsandbox is forbidden, please use ITransform.inputfiles')
                    return

                if self.inputfiles != []:
                    logger.error(
                        'ITransform.inputfiles is set, you can\'t set ITransform.inputsandbox')
                    return

            super(ITransform, self).__setattr__(attr, value)

        elif attr == 'outputdata':

            if value is not None:

                if getConfig('Output')['ForbidLegacyOutput']:
                    logger.error(
                        'Use of ITransform.outputdata is forbidden, please use ITransform.outputfiles')
                    return

                if self.outputfiles != []:
                    logger.error(
                        'ITransform.outputfiles is set, you can\'t set ITransform.outputdata')
                    return
            super(ITransform, self).__setattr__(attr, value)

        else:
            super(ITransform, self).__setattr__(attr, value)

    def resetUnitsByStatus(self, status='bad'):
        """Reset all units of a given status"""
        for unit in self.units:
            if unit.status == status:
                logger.info("Resetting Unit %d, Transform %d..." %
                            (unit.getID(), self.getID()))
                self.resetUnit(unit.getID())

    def checkUnitsAreCompleted(self, parent_units):
        """Check the given parent units are complete"""
        for parent in parent_units:
            if len(parent.active_job_ids) == 0 or parent.status != "completed":
                return False

        return True

    def getChainInclExclMasks(self, parent_units):
        """return the include/exclude masks from the TaskChainInput"""
        incl_pat_list = []
        excl_pat_list = []
        from GangaCore.GPIDev.Lib.Tasks.TaskChainInput import TaskChainInput
        for parent in parent_units:
            for inds in self.inputdata:
                if isType(inds, TaskChainInput) and inds.input_trf_id == parent._getParent().getID():
                    incl_pat_list += inds.include_file_mask
                    excl_pat_list += inds.exclude_file_mask

        return incl_pat_list, excl_pat_list

    def getParentUnitJobs(self, parent_units, include_subjobs=True):
        """Return the list of parent jobs"""
        job_list = []
        for parent in parent_units:
            job = getJobByID(parent.active_job_ids[0])
            if job.subjobs:
                job_list += job.subjobs
            else:
                job_list += [job]

        return job_list

    def removeUnusedJobs(self):
        """Remove all jobs that aren't being used, e.g. failed jobs"""
        for unit in self.units:
            for jid in unit.prev_job_ids:
                try:
                    logger.warning("Removing job '%d'..." % jid)
                    job = getJobByID(jid)
                    job.remove()
                except Exception as err:
                    logger.debug("removeUnused: %s" % str(err))
                    logger.error("Problem removing job '%d'" % jid)
