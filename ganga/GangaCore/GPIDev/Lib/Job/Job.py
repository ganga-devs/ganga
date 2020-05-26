

import copy
import errno
import glob
import inspect
import os
import time
import uuid
import sys
import GangaCore.Core.FileWorkspace
from GangaCore.GPIDev.MonitoringServices import getMonitoringObject
from GangaCore.Core.exceptions import GangaException, IncompleteJobSubmissionError, JobManagerError, TypeMismatchError, SplitterError
from GangaCore.Core import Sandbox
from GangaCore.Core.GangaRepository import getRegistry
from GangaCore.Core.GangaRepository.SubJobXMLList import SubJobXMLList
from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaCore.GPIDev.Adapters.IApplication import PostprocessStatusUpdate
from GangaCore.GPIDev.Adapters.IPostProcessor import MultiPostProcessor
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Base.Objects import Node
from GangaCore.GPIDev.Base.Proxy import addProxy, getName, getRuntimeGPIObject, isType, runtimeEvalString, stripProxy
from GangaCore.GPIDev.Lib.File import MassStorageFile, getFileConfigKeys
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaListByRef
from GangaCore.GPIDev.Lib.Job.MetadataDict import MetadataDict
from GangaCore.GPIDev.Schema import ComponentItem, FileItem, GangaFileItem, Schema, SimpleItem, Version
from GangaCore.Utility.Config import ConfigError, getConfig
from GangaCore.Utility.logging import getLogger, log_user_exception

from .JobTime import JobTime
from GangaCore.Lib.Localhost import Localhost
from GangaCore.Lib.Executable import Executable

logger = getLogger()
config = GangaCore.Utility.Config.getConfig('Configuration')


def lazyLoadJobFQID(this_job):
    return lazyLoadJobObject(this_job, 'fqid')


def lazyLoadJobStatus(this_job):
    return lazyLoadJobObject(this_job, 'status', False)


def lazyLoadJobBackend(this_job):
    return lazyLoadJobObject(this_job, 'backend')


def lazyLoadJobApplication(this_job):
    return lazyLoadJobObject(this_job, 'application')


def lazyLoadJobObject(raw_job, this_attr, do_eval=True):
    ## Returns an object which corresponds to an attribute from a Job, or matches it's default equivalent without triggering a load from disk
    ## i.e. lazy loading a Dirac backend will return a raw Dirac() object and lazy loading the status will return the status string
    ## These are all evaluated from the strings in the index file for the job.
    ## dont_eval lets the method know a string is expected to be returned and not evaluated so nothing is evaluated against the GPI

    this_job = stripProxy(raw_job)

    if this_job._getRegistry() is not None:
        if this_job._getRegistry().has_loaded(this_job):
            return getattr(this_job, this_attr)

    lzy_loading_str = 'display:'+ this_attr
    job_index_cache = this_job._index_cache
    if isinstance(job_index_cache, dict) and lzy_loading_str in job_index_cache:
        obj_name = job_index_cache[lzy_loading_str]
        if obj_name is not None and do_eval:
            job_obj = stripProxy(getRuntimeGPIObject(obj_name, True))
            if job_obj is None:
                job_obj = getattr(this_job, this_attr)
        elif not do_eval:
            job_obj = obj_name
        else:
            job_obj = getattr(this_job, this_attr)

    else:
        job_obj = getattr(this_job, this_attr)

    return job_obj


class JobStatusError(GangaException):

    def __init__(self, *args):
        GangaException.__init__(self, *args)


class JobError(GangaException):

    def __init__(self, what=''):
        GangaException.__init__(self, what)
        self.what = what

    def __str__(self):
        return "JobError: %s" % self.what


class FakeError(GangaException):

    def __init__(self):
        super(FakeError, self).__init__()


class JobInfo(GangaObject):

    """ Additional job information.
        Partially implemented
    """
    _schema = Schema(Version(0, 1), {
        'submit_counter': SimpleItem(defvalue=0, protected=1, doc="job submission/resubmission counter"),
        'monitor': ComponentItem('monitor', defvalue=None, load_default=0, comparable=0, optional=1, doc="job monitor instance"),
        'uuid': SimpleItem(defvalue='', protected=1, comparable=0, doc='globally unique job identifier'),
        'monitoring_links': SimpleItem(defvalue=[], typelist=[tuple], sequence=1, protected=1, copyable=0, doc="list of tuples of monitoring links")
    })

    _category = 'jobinfos'
    _name = 'JobInfo'

    def __init__(self):
        super(JobInfo, self).__init__()

    def _auto__init__(self):
        self.uuid = str(uuid.uuid4())

    def increment(self):
        self.submit_counter += 1


def _outputfieldCopyable():
    if 'ForbidLegacyOutput' in getConfig('Output'):
        if getConfig('Output')['ForbidLegacyOutput']:
            outputfieldCopyable = 0
    else:
        outputfieldCopyable = 1


class Job(GangaObject):

    """Job is an interface for submision, killing and querying the jobs :-).

    Basic configuration:

    The    "application"    attribute    defines   what    should    be
    run. Applications  may be generic arbitrary  executable scripts or
    complex, predefined objects.

    The  "backend" attribute  defines  where and  how  to run.  Backend
    object  represents  a resource  or  a  batch  system with  various
    configuration parameters.

    Available applications, backends and other job components may be listed
    using the plugins() function. See help on plugins() function.

    The "status"  attribute represents the  state of Ganga  job object.
    It  is automatically  updated  by the  monitoring  loop. Note  that
    typically at the  backends the jobs have their  own, more detailed
    status.    This    information   is   typically    available   via
    "job.backend.status" attribute.

    Bookkeeping and persistency:

    Job objects contain basic book-keeping information: "id", "status"
    and  "name".  Job  objects   are  automatically  saved  in  a  job
    repository which may be a  special directory on a local filesystem
    or a remote database.

    Input/output and file workspace:

    There   is  an  input/output   directory  called   file  workspace
    associated with each  job ("inputdir" and "outputdir" properties).
    When a  job is submitted, all  input files are copied  to the file
    workspace  to keep  consistency  of  the input  while  the job  is
    running. Ganga then ships all  files in the input workspace to the
    backend systems in a sandbox.

    The   list  of  input   files  is   defined  by   the  application
    (implicitly).  Additional files may be explicitly specified in the
    "inputsandbox" attribute.

    Job splitting:

    The "splitter" attributes defines how a large job may be divided into
    smaller subjobs.  The subjobs are automatically created when the main
    (master) job is submitted. The "subjobs" attribute gives access to
    individual subjobs. The "master" attribute of a subjob points back to the
    master job.

    Postprocessors:

    The "postprocessors" attribute is a list of actions to perform once the job has completed.
    This includes how the output of the subjobs may be merged, user defined checks which may fail
    the job, and an email notification.

    Datasets: PENDING
    Datasets are highly application and virtual organisation specific.
    """

    _schema = Schema(Version(1, 6), {'inputsandbox': FileItem(defvalue=[], sequence=1, doc="list of File objects shipped to the worker node "),
                                     'outputsandbox': SimpleItem(defvalue=[], typelist=[str], sequence=1, copyable=_outputfieldCopyable(), doc="list of filenames or patterns shipped from the worker node"),
                                     'info': ComponentItem('jobinfos', defvalue=None, doc='JobInfo '),
                                     'comment': SimpleItem('', protected=0, changable_at_resubmit=1, doc='comment of the job'),
                                     'time': ComponentItem('jobtime', defvalue=JobTime(), protected=1, comparable=0, doc='provides timestamps for status transitions'),
                                     'application': ComponentItem('applications', defvalue=Executable(), doc='specification of the application to be executed'),
                                     'backend': ComponentItem('backends', defvalue=Localhost(), doc='specification of the resources to be used (e.g. batch system)'),
                                     'inputfiles': GangaFileItem(defvalue=[], sequence=1, doc="list of file objects that will act as input files for a job"),
                                     'outputfiles': GangaFileItem(defvalue=[], sequence=1, doc="list of file objects decorating what have to be done with the output files after job is completed "),
                                     'non_copyable_outputfiles': GangaFileItem(defvalue=[], hidden=1, sequence=1, doc="list of file objects that are not to be copied accessed via proxy through outputfiles", copyable=0),
                                     'id': SimpleItem('', protected=1, comparable=0, doc='unique Ganga job identifier generated automatically'),
                                     'status': SimpleItem('new', protected=1, checkset='_checkset_status', doc='current state of the job, one of "new", "submitted", "running", "completed", "killed", "unknown", "incomplete"', copyable=False),
                                     'name': SimpleItem('', doc='optional label which may be any combination of ASCII characters', typelist=[str]),
                                     'inputdir': SimpleItem(getter="getStringInputDir", defvalue=None, transient=1, protected=1, comparable=0, load_default=0, optional=1, copyable=0, typelist=[str], doc='location of input directory (file workspace)'),
                                     'outputdir': SimpleItem(getter="getStringOutputDir", defvalue=None, transient=1, protected=1, comparable=0, load_default=0, optional=1, copyable=0, typelist=[str], doc='location of output directory (file workspace)'),
                                     'inputdata': ComponentItem('datasets', defvalue=None, load_default=0, optional=1, doc='dataset definition (typically this is specific either to an application, a site or the virtual organization'),
                                     'outputdata': ComponentItem('datasets', defvalue=None, load_default=0, optional=1, copyable=_outputfieldCopyable(), doc='dataset definition (typically this is specific either to an application, a site or the virtual organization'),
                                     'splitter': ComponentItem('splitters', defvalue=None, load_default=0, optional=1, doc='optional splitter'),
                                     'subjobs': ComponentItem('jobs', defvalue=GangaList(), sequence=1, protected=1, load_default=0, copyable=0, comparable=0, optional=1, proxy_get="_subjobs_proxy", doc='list of subjobs (if splitting)', summary_print='_subjobs_summary_print'),
                                     'master': ComponentItem('jobs', getter="_getMasterJob", transient=1, protected=1, load_default=0, defvalue=None, optional=1, copyable=0, comparable=0, doc='master job', visitable=0),
                                     'postprocessors': ComponentItem('postprocessor', defvalue=MultiPostProcessor(), doc='list of postprocessors to run after job has finished'),
                                     'virtualization': ComponentItem('virtualization', defvalue=None, load_default=0, optional=1, doc='optional virtualization to be used'),
                                     'merger': ComponentItem('mergers', defvalue=None, hidden=1, copyable=0, load_default=0, optional=1, doc='optional output merger'),
                                     'do_auto_resubmit': SimpleItem(defvalue=False, doc='Automatically resubmit failed subjobs'),
                                     'metadata': ComponentItem('metadata', defvalue=MetadataDict(), doc='the metadata', protected=1, copyable=0),
                                     'fqid': SimpleItem(getter="getStringFQID", transient=1, protected=1, load_default=0, defvalue=None, optional=1, copyable=0, comparable=0, typelist=[str], doc='fully qualified job identifier', visitable=0),
                                     'been_queued': SimpleItem(transient=1, hidden=1, defvalue=False, optional=0, copyable=0, comparable=0, typelist=[bool], doc='flag to show job has been queued for postprocessing', visitable=0),
                                     'parallel_submit': SimpleItem(transient=1, defvalue=True, doc="Enable Submission of subjobs in parallel"),
                                     })

    _category = 'jobs'
    _name = 'Job'
    _exportmethods = ['prepare', 'unprepare', 'submit', 'remove', 'kill',
                      'resubmit', 'peek', 'force_status', 'runPostProcessors', 'returnSubjobStatuses']

    default_registry = 'jobs'

    _additional_slots = ['_storedRTHandler', '_storedJobSubConfig', '_storedAppSubConfig', '_storedJobMasterConfig', '_storedAppMasterConfig', '_stored_subjobs_proxy']

    # TODO: usage of **kwds may be envisaged at this level to optimize the
    # overriding of values, this must be reviewed
    def __init__(self, prev_job=None, **kwds):
        """
        This constructs a new job object
        Args:
            prev_job (Job, JobTemplate): This is assumed to be an old job or a job template which we're hoping to copy the configuration of
        """

        # WE WILL ONLY EVER ACCEPT Job or JobTemplate by design
        if prev_job:
            try:
                assert isinstance(prev_job, (Job, JobTemplate))
            except AssertionError:
                raise TypeMismatchError("Can only constuct a Job with 1 non-keyword argument which is another Job, or JobTemplate")

        # START INIT OF SELF

        # TODO add the kwds as a pass through so that they're handled in a sane/consistent way.
        super(Job, self).__init__()
        # Finished initializing 'special' objects which are used in getter methods and alike
        self.time.newjob()  # <-----------NEW: timestamp method

        #logger.debug("__init__")

        for i in Job._additional_slots:
            if not hasattr(self, i):
                setattr(self, i, None)

        # FINISH INIT OF SELF

        # If we're copying data from an existing Job/JobTemplate
        if prev_job:
            # We don't want to clone the time data of an existing job
            self.copyFrom(prev_job, ['time'])

            if prev_job.master is not None:
                if getConfig('Output')['ForbidLegacyInput']:
                    if not prev_job.inputfiles:
                        self.inputfiles = prev_job.master.inputfiles
                    else:
                        self.inputfiles = prev_job.inputfiles
                    self.inputsandbox = []
                else:
                    if not prev_job.inputsandbox:
                        self.inputsandbox = prev_job.master.inputsandbox
                    else:
                        self.inputsandbox = prev_job.inputsandbox
                    self.inputfiles = []

            if getConfig('Preparable')['unprepare_on_copy'] is True:
                self.unprepare()

    def __hash__(self):
        return hash(self.fqid)

    def __lt__(self, other):
        return self.id < other.id

    def __gt__(self, other):
        return self.id > other.id

    def _getMasterJob(self):
        parent = self._getParent()
        while parent is not None:
            if isType(parent, Job):
                break
            parent = self._getParent()
        return parent

    def _readonly(self):
        return self.status != 'new'

    # on the deepcopy reattach the outputfiles to call their
    # _on_attribute__set__
    def __deepcopy__(self, memo=None):

        # Due to problems on Hammercloud due to uncopyable object lets
        # explicitly stop these objects going anywhere near the __deepcopy__

        c = Job()

        c.time.newjob()
        c.backend = copy.deepcopy(self.backend)
        c.application = copy.deepcopy(self.application)
        c.inputdata = copy.deepcopy(self.inputdata)
        c.name = self.name
        c.comment = self.comment
        c.postprocessors = copy.deepcopy(self.postprocessors)
        c.splitter = copy.deepcopy(self.splitter)
        c.virtualization = copy.deepcopy(self.virtualization)
        c.parallel_submit = self.parallel_submit

        # Continue as before

        c.outputfiles = []
        for f in self.outputfiles:
            if hasattr(f, '_on_attribute__set__'):
                c.outputfiles.append(f._on_attribute__set__(self, 'outputfiles'))
                continue
            c.outputfiles.append(copy.deepcopy(f))

        if getConfig('Output')['ForbidLegacyInput']:
            # Want to move EVERYTHING into the inputfiles and leave the
            # inputsandbox empty
            c.inputfiles = []

            if self.inputfiles != []:
                for i in self.inputfiles:
                    c.inputfiles.append(copy.deepcopy(i))
            else:
                if self.master and self.master.inputfiles != []:
                    for i in self.master.inputfiles:
                        c.inputfiles.append(copy.deepcopy(i))

            # Apply needed transform to move Sandbox item to the
            if self.inputsandbox != []:
                from GangaCore.GPIDev.Lib.File.FileUtils import safeTransformFile
                for i in self.inputsandbox:
                    c.inputfiles.append(safeTransformFile(i))
            else:
                if self.master and self.master.inputsandbox != []:
                    from GangaCore.GPIDev.Lib.File.FileUtils import safeTransformFile
                    for i in self.master.inputsandbox:
                        c.inputfiles.append(safeTransformFile(i))

            c.inputsandbox = []

        else:
            if self.inputsandbox != []:
                c.inputsandbox = copy.deepcopy(self.inputsandbox)
            elif self.master and self.master.inputsandbox != []:
                c.inputsandbox = copy.deepcopy(self.master.inputsandbox)
            else:
                c.inputsandbox = []

            if self.inputfiles != []:
                c.inputfiles = copy.deepcopy(self.inputfiles)
            elif self.master and self.master.inputfiles != []:
                c.inputfiles = copy.deepcopy(self.inputfiles)
            else:
                c.inputfiles = []

        if self.master is not None:
            if getConfig('Output')['ForbidLegacyInput']:
                if self.inputfiles == []:
                    logger.debug("Copying Master inputfiles")
                    c.inputsandbox = []
                    c.inputfiles = copy.deepcopy(self.master.inputfiles)
                else:
                    logger.debug("Keeping own inputfiles")

            else:
                # elif (not getConfig('Output')['ForbidLegacyInput']):
                if self.inputsandbox == []:
                    logger.debug("Copying Master inputfiles")
                    c.inputsandbox = copy.deepcopy(self.master.inputsandbox)
                    c.inputfiles = []
                else:
                    logger.debug("Keeping own inputsandbox")

        logger.debug("Intercepted __deepcopy__")
        return c

    def _attribute_filter__get__(self, name):

        #logger.debug( "Intercepting _attribute_filter__get__" )

        # Attempt to spend too long loading un-needed objects into memory in
        # order to read job status
        if name == 'status':
            return object.__getattribute__(self, 'status')

        # FIXME Add some method of checking what objects are known in advance of calling the __getattribute__ method
        # Pref one that doesn't involve loading the job object if not needed

        special_list = []
        special_list.append('outputfiles')
        special_list.append('inputfiles')
        special_list.append('subjobs')

        if name not in special_list:
            return object.__getattribute__(self, name)

        if name == 'outputfiles':

            currentOutputFiles = object.__getattribute__(self, name)
            currentUnCopyableOutputFiles = object.__getattribute__(self, 'non_copyable_outputfiles')

            files = []
            files2 = []

            for f in currentOutputFiles:
                if f.containsWildcards() and hasattr(f, 'subfiles') and f.subfiles:
                    files.extend(f.subfiles)
                else:
                    files.append(f)

            for f in currentUnCopyableOutputFiles:
                if f.containsWildcards() and hasattr(f, 'subfiles') and f.subfiles:
                    files2.extend(f.subfiles)
                else:
                    files2.append(f)

            files3 = GangaList()
            files3.extend(files)
            files3.extend(files2)

            # FIXME THIS SHOULD NOT HAVE TO BE HERE! (It does else we end up with really bad errors and this is just wrong!)
            files3._setParent(self)

            return addProxy(files3)

        # If we ask for 'inputfiles', return the expanded list of subfiles
        if name == 'inputfiles':

            return object.__getattribute__(self, name)

        if name == 'subjobs':
            return self._subjobs_proxy()

        if name == "fqid":
            return self.getFQID('.')

        return object.__getattribute__(self, name)

    # status may only be set directly using updateStatus() method
    # only modules comming from GangaCore.GPIDev package may change it directly
    def _checkset_status(self, value):

        try:
            id = self.getFQID('.')
        except KeyError:
            try:
                id = self.id
            except KeyError:
                id = None

        if hasattr(self, 'status'):
            oldstat = self.status
        else:
            oldstat = None

        logger.debug('job %s "%s" setting raw status to "%s"', id, oldstat, value)

        ## This code appears to mimic the fact that we have a protected status within the Schema.
        ## This looks like it's supposed to prevent direct manipulation of the job.status property but this is done through stack manipulation, probably not the best way to achieve this.
        ## We may want to drop this code in future I'm leaving this in for historical purposes. rcurrie
        #import inspect
        #frame = inspect.stack()[2]
        # if not frame[0].f_code.co_name == 'updateStatus' and
        # if frame[0].f_code.co_filename.find('/Ganga/GPIDev/')==-1 and frame[0].f_code.co_filename.find('/Ganga/Core/')==-1:
        #    raise AttributeError('cannot modify job.status directly, use job.updateStatus() method instead...')
        #del frame

    class State(object):

        def __init__(self, state, transition_comment='', hook=None):
            self.state = state
            self.transition_comment = transition_comment
            self.hook = hook

    class Transitions(dict):

        def __init__(self, *states):
            self.states = {}
            for s in states:
                assert(s.state not in self)
                self[s.state] = s

    status_graph = {'new': Transitions(State('submitting', 'j.submit()', hook='monitorSubmitting_hook'),
                                       State('removed', 'j.remove()')),
        'submitting': Transitions(State('new', 'submission failed', hook='rollbackToNewState'),
                                  State('submitted', hook='monitorSubmitted_hook'),
                                  State('unknown', 'forced remove OR remote jobmgr error'),
                                  State('failed', 'manually forced or keep_on_failed=True', hook='monitorFailed_hook')),
        'submitted': Transitions(State('running'),
                                 State('killed', 'j.kill()', hook='monitorKilled_hook'),
                                 State('unknown', 'forced remove'),
                                 State('failed', 'j.fail(force=1)', hook='monitorFailed_hook'),
                                 State('completing'),
                                 State('completed', hook='postprocess_hook'),
                                 State('submitting', 'j.resubmit(force=1)')),
        'running': Transitions(State('completing'),
                               State('completed', 'job output already in outputdir', hook='postprocess_hook'),
                               State('failed', 'backend reported failure OR j.fail(force=1)', hook='monitorFailed_hook'),
                               State('killed', 'j.kill()', hook='monitorKilled_hook'),
                               State('unknown', 'forced remove'),
                               State('submitting', 'j.resubmit(force=1)'),
                               State('submitted', 'j.resubmit(force=1)')),
        'completing': Transitions(State('completed', hook='postprocess_hook'),
                                  State('failed', 'postprocessing error OR j.fail(force=1)', hook='postprocess_hook_failed'),
                                  State('unknown', 'forced remove'),
                                  State('submitting', 'j.resubmit(force=1)'),
                                  State('submitted', 'j.resubmit(force=1)')),
        'killed': Transitions(State('removed', 'j.remove()'),
                              State('failed', 'j.fail()'),
                              State('submitting', 'j.resubmit()'),
                              State('submitted', 'j.resubmit()')),
        'failed': Transitions(State('removed', 'j.remove()'),
                              State('submitting', 'j.resubmit()'),
                              State('completed', hook='postprocess_hook'),
                              State('submitted', 'j.resubmit()')),
        'completed': Transitions(State('removed', 'j.remove()'),
                                 State('failed', 'j.fail()'),
                                 State('submitting', 'j.resubmit()'),
                                 State('submitted', 'j.resubmit()')),
        'incomplete': Transitions(State('removed', 'j.remove()')),
        'unknown': Transitions(State('removed', 'forced remove')),
        'template': Transitions(State('removed'))
    }

    transient_states = ['incomplete', 'removed', 'unknown']
    initial_states = ['new', 'incomplete', 'template']

    def updateStatus(self, newstatus, transition_update=True, update_master=True, ignore_failures=False):
        """ Move job to the new status. according to the state transition graph (Job.status_graph).
        If transition is allowed:
          - call the specific transition hook (if exists)
          - call the general transition update hook (triggers the auto merger and application hooks)
          - commit the job
        If job cannot be commited, then revert to old status and raise JobStatusError.
        If transition is not allowed then raise JobStatusError.

        Transitions from to the current state are allowed by default (so you can updateStatus('running') if job is 'running').
        Such default transitions do not have hooks.
        """
        # For debugging to trace Failures and such

        fqid = self.getFQID('.')
        initial_status = self.status
        logger.debug('attempt to change job %s status from "%s" to "%s"', fqid, initial_status, newstatus)
        try:
            state = self.status_graph[initial_status][newstatus]
        except KeyError as err:
            # allow default transitions: s->s, no hook
            if newstatus == initial_status:
                state = Job.State(newstatus)
            else:
                raise JobStatusError('forbidden status transition of job %s from "%s" to "%s"' % (fqid, initial_status, newstatus))

        try:
            if state.hook:
                try:
                    getattr(self, state.hook)()
                except PostprocessStatusUpdate as x:
                    newstatus = x.status

            if transition_update:
                # we call this even if there was a hook
                newstatus = self.transition_update(newstatus)

                if (newstatus == 'completed') and (initial_status != 'completed') and (ignore_failures is not True):
                    if self.outputFilesFailures():
                        logger.info("Job %s outputfile Failure" % self.getFQID('.'))
                        self.updateStatus('failed')
                        return

            if initial_status != newstatus:
                self.time.timenow(newstatus)
            else:
                logger.debug("Status changed from '%s' to '%s'. No new timestamp was written", initial_status, newstatus)

            # move to the new state AFTER hooks are called
            self.status = newstatus
            logger.debug("Status changed from '%s' to '%s'" % (initial_status, self.status))

        except Exception as x:
            self.status = initial_status
            log_user_exception()
            raise JobStatusError(x)

        final_status = self.status

        if final_status != initial_status and self.master is None:
            logger.info('job %s status changed to "%s"', self.getFQID('.'), final_status)
        if update_master and self.master is not None:
            self.master.updateMasterJobStatus()

    def transition_update(self, new_status):
        """Propagate status transitions"""

        if new_status in ['completed', 'failed', 'killed']:
            if len(self.postprocessors) > 0:
                if self.master:
                    logger.debug("Running postprocessor for Job %s" % self.getFQID('.'))
                else:
                    logger.info("Running postprocessor for Job %s" % self.getFQID('.'))
                passed = self.postprocessors.execute(self, new_status)
                if passed is not True:
                    new_status = 'failed'

        # Propagate transition updates to applications
        if self.application:
            self.application.transition_update(new_status)
        return new_status

    def getBackendOutputPostprocessDict(self):
        """Return a 'live' version of the output post processing map.
        Can't be done at load/global namespace because then user modules are ignored."""

        backend_output_postprocess = {}

        keys = getFileConfigKeys()

        for key in keys:
            try:
                for configEntry in getConfig('Output')[key]['backendPostprocess']:
                    if configEntry not in backend_output_postprocess:
                        backend_output_postprocess[configEntry] = {}

                    backend_output_postprocess[configEntry][key] = getConfig('Output')[key]['backendPostprocess'][configEntry]
            except ConfigError as err:
                logger.debug("ConfigError: %s" % err)
                pass

        return backend_output_postprocess

    def postprocessoutput(self, outputfiles, outputdir):

        if len(outputfiles) == 0:
            return

        for outputfile in outputfiles:
            backendClass = getName(self.backend)
            outputfileClass = getName(outputfile)

            # on Batch backends these files can be compressed only on the
            # client
            if backendClass == 'LSF':
                if outputfile.compressed and (outputfile.namePattern == 'stdout' or outputfile.namePattern == 'stderr'):
                    for currentFile in glob.glob(os.path.join(outputdir, outputfile.namePattern)):
                        os.system("gzip %s" % currentFile)


            backend_output_postprocess = self.getBackendOutputPostprocessDict()
            if backendClass in backend_output_postprocess:
                if outputfileClass in backend_output_postprocess[backendClass]:

                    if not self.subjobs:
                        logger.debug("Job %s Setting Location of %s: %s" % (self.getFQID('.'), getName(outputfile), outputfile.namePattern))
                        try:
                            outputfile.setLocation()
                        except Exception as err:
                            logger.error("Error: %s" % err)

                    if backend_output_postprocess[backendClass][outputfileClass] == 'client':
                        try:
                            logger.debug("Job %s Putting File %s: %s" % (self.getFQID('.'), getName(outputfile), outputfile.namePattern))
                            outputfile.put()
                            logger.debug("Cleaning up after put")
                            outputfile.cleanUpClient()
                        except Exception as err:
                            logger.error("Error Putting or cleaning up file: %s, err::%s" % (outputfile.namePattern, err))

        # leave it for the moment for debugging
        #os.system('rm %s' % postprocessLocationsPath)

    def validateOutputfilesOnSubmit(self):

        for outputfile in self.outputfiles:
            if isType(outputfile, MassStorageFile):
                (validOutputFiles, errorMsg) = outputfile.validate()

                if validOutputFiles is False:
                    return (validOutputFiles, errorMsg)

        return True, ''

    def outputFilesFailures(self):

        postprocessFailure = False

        # check if any output was matched - be careful about master jobs
        if getConfig('Output')['FailJobIfNoOutputMatched'] and not self.subjobs:
            for outputfile in self.outputfiles:
                if not outputfile.hasMatchedFiles():
                    logger.warning("Job: %s OutputFile failed to match file type %s: %s" % (self.getFQID('.'), getName(outputfile), outputfile.namePattern))
                    postprocessFailure = True

        # check for failure reasons
        for outputfile in self.outputfiles:
            if hasattr(outputfile, 'failureReason') and outputfile.failureReason != '':
                logger.warning("Job %s OutputFile failed for file: %s" % (self.getFQID('.'), outputfile.namePattern))
                postprocessFailure = True
            else:
                for subfile in outputfile.subfiles:
                    if hasattr(subfile, 'failureReason') and subfile.failureReason != '':
                        logger.warning("Job%s OutputFile failed due to reason: %s" % (self.getFQID('.'), outputfile.namePattern))
                        postprocessFailure = True

        return postprocessFailure

    def getSubJobStatuses(self):
        """
        This returns a set of all of the different subjob statuses whilst respecting lazy loading
        """

        if isinstance(self.subjobs, SubJobXMLList):
            stats = set(self.subjobs.getAllSJStatus())
        else:
            stats = set(sj.status for sj in self.subjobs)

        return stats

    def returnSubjobStatuses(self):
        stats = []
        if isinstance(self.subjobs, SubJobXMLList):
            stats = self.subjobs.getAllSJStatus()
        else:
            stats = [sj.status for sj in self.subjobs]

        return "%s/%s/%s/%s" % (stats.count('running'), stats.count('failed') + stats.count('killed'), stats.count('completing'), stats.count('completed'))

    def updateMasterJobStatus(self):
        """
        Update master job status based on the status of subjobs.
        This is an auxiliary method for implementing bulk subjob monitoring.
        """
        stats = self.getSubJobStatuses()

        # ignore non-split jobs
        if not stats and self.master is not None:
            logger.warning('ignoring master job status updated for job %s (NOT MASTER)', self.getFQID('.'))
            return

        new_stat = None

        for s in ['submitting', 'submitted', 'running', 'completing', 'failed', 'killed', 'completed']:
            if s in stats:
                new_stat = s
                break

        if new_stat == self.status:
            return

        if not new_stat:
            logger.critical('undefined state for job %s, status=%s', self.id, stats)
        self.updateStatus(new_stat)

    def getMonitoringService(self):
        return getMonitoringObject(self)

    def monitorPrepare_hook(self, subjobconfig):
        """Let monitoring services work with the subjobconfig after it's prepared"""
        self.getMonitoringService().prepare(subjobconfig=subjobconfig)

    def monitorSubmitting_hook(self):
        """Let monitoring services perform actions at job submission"""
        self.getMonitoringService().submitting()

    def monitorSubmitted_hook(self):
        """Send monitoring information (e.g. Dashboard) at the time of job submission"""
        self.getMonitoringService().submit()

    def runPostProcessors(self):
        logger.info("Job %s Manually Running PostProcessors" % self.getFQID('.'))
        try:
            self.application.postprocess()
        except Exception as x:
            logger.error("Job %s Application postprocess failed" % self.getFQID('.'))
            logger.error("\n%s" % x)
        try:
            self.postprocessoutput(self.outputfiles, self.outputdir)
        except Exception as x:
            logger.error("Job %s postprocessoutput failed" % self.getFQID('.'))
            logger.error("\n%s" % x)

        logger.debug("Job %s Finished" % self.getFQID('.'))

    def postprocess_hook(self):
        if self.master:
            logger.debug("Job %s Running PostProcessor hook" % self.getFQID('.'))
        else:
            logger.info("Job %s Running PostProcessor hook" % self.getFQID('.'))
        self.application.postprocess()
        self.getMonitoringService().complete()
        self.postprocessoutput(self.outputfiles, self.outputdir)

    def postprocess_hook_failed(self):
        if self.master:
            logger.debug("Job %s PostProcessor Failed" % self.getFQID('.'))
        else:
            logger.info("Job %s PostProcessor Failed" % self.getFQID('.'))
        self.application.postprocess_failed()
        self.getMonitoringService().fail()

    def monitorFailed_hook(self):
        self.getMonitoringService().fail()

    def monitorKilled_hook(self):
        self.getMonitoringService().kill()

    def monitorRollbackToNew_hook(self):
        self.getMonitoringService().rollback()

    def _auto__init__(self, registry=None, unprepare=None):

        logger.debug("Intercepting the _auto__init__ function")

        if registry is None:
            registry = getRegistry(self.default_registry)

        if unprepare is True:
            logger.debug("Calling unprepare() from Job.py")
            self.application.unprepare()

        # increment the shareref counter if the job we're copying is prepared.
        # ALEX added try/if for loading of named job templates
        # temporary fix but looks like prep registry hasn't been loaded at the time
        # Dont think it should matter as templates tend not to be prepared
        # try:
        # if hasattr(getRegistry("prep"), 'getShareRef'):
        #shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        # except: pass


        cfg = GangaCore.Utility.Config.getConfig('Configuration')
        if cfg['autoGenerateJobWorkspace']:
            self._init_workspace()

        super(Job, self)._auto__init__()

        # register the job (it will also commit it)
        # job gets its id now
        registry._add(self)

    def _init_workspace(self):
        logger.debug("Job %s Calling _init_workspace", self.getFQID('.'))
        self.getDebugWorkspace(create=True)

    def getWorkspace(self, what, create=True):
        Workspace = getattr(GangaCore.Core.FileWorkspace, what)
        w = Workspace()
        w.jobid = self.getFQID(os.sep)
        if create and w.jobid is not None:
            w.create(w.jobid)
        return w

    def createPackedInputSandbox(self, files, master=False):
        """ Create a packed input sandbox which contains files (a list of File or FileBuffer objects).
        'master' flag is used to make a difference between the master and shared input sandbox which is important
        if the job is not split (subjob and masterjob are the same object)
        """

        name = '_input_sandbox_' + self.getFQID('_') + '%s.tgz'

        if master:
            if self.master is not None:
                name = '_input_sandbox_' + self.master.getFQID('_') + '%s.tgz'
            name = name % "_master"
        else:
            name = name % ""

        newFiles = []
        for _f in files:
            if hasattr(_f, 'name') and not _f.name.startswith('.nfs'):
                newFiles.append(_f)
        files = newFiles

        if not files:
            return []

        #logger.debug( "\n" )
        logger.debug("Creating Packed InputSandbox %s" % name)
        logger.debug("With:")
        for f in files:
            if hasattr(f, 'name'):
                logger.debug("\t" + f.name)
            else:
                logger.debug("\t" + f)
        #logger.debug( "\n" )

        # if the the master job of this subjob exists and the master sandbox is requested
        # the master sandbox has already been created so just look for it
        # else if it has not been prepared we need to construct it as usual

        if master:
            logger.debug("Returning Master InputSandbox")
            if self.application.is_prepared is True:
                logger.debug("Master Application is Prepared!")
                return [self.getInputWorkspace().getPath(name)]
            else:
                logger.debug("Master Application is NOT Prepared!")
                if self.master is None:
                    return Sandbox.createPackedInputSandbox(files, self.getInputWorkspace(), name)
                else:
                    return Sandbox.createPackedInputSandbox(files, self.master.getInputWorkspace(), name)

        logger.debug("Returning new InputSandbox")
        return Sandbox.createPackedInputSandbox(files, self.getInputWorkspace(), name)

    def createInputSandbox(self, files, master=False):
        """ Create an unpacked input sandbox which contains files (a list of File or FileBuffer objects).
        'master' flag is not used in this case, and it provided only for uniformity with createPackedInputSandbox() method
        """

        logger.debug("Creating InputSandbox")
        files = [f for f in files if hasattr(f, 'name') and not f.name.startswith('.nfs')]

        if self.master is not None and master:
            return Sandbox.createInputSandbox(files, self.master.getInputWorkspace())
        else:
            return Sandbox.createInputSandbox(files, self.getInputWorkspace())

    def getStringFQID(self):
        return self.getFQID('.')

    def getStringInputDir(self):
        # return self.getInputWorkspace(create=self.status !=
        # 'removed').getPath()
        cfg = GangaCore.Utility.Config.getConfig('Configuration')
        if cfg['autoGenerateJobWorkspace']:
            ## This needs to use the NodeAttribute to AVOID causing loading of a Job during initialization!
            return self.getInputWorkspace(create=self.status != 'removed').getPath()
        else:
            return self.getInputWorkspace(create=False).getPath()

    def getStringOutputDir(self):
        # return self.getOutputWorkspace(create=self.status !=
        # 'removed').getPath()
        cfg = GangaCore.Utility.Config.getConfig('Configuration')
        if cfg['autoGenerateJobWorkspace']:
            ## This needs to use the NodeAttribute to AVOID causing loading of a Job during initialization!
            return self.getOutputWorkspace(create=self.status != 'removed').getPath()
        else:
            return self.getOutputWorkspace(create=False).getPath()

    def getFQID(self, sep=None):
        """Return a fully qualified job id (within registry): a list of ids [masterjob_id,subjob_id,...]
        If optional sep is specified FQID string is returned, ids are separated by sep.
        For example: getFQID('.') will return 'masterjob_id.subjob_id....'
        """
        ## This prevents exceptions during initialization
        if hasattr(self, 'id'):
            fqid = [self.id]
        else:
            return None

        if self.master is not None:
            cur = self.master  # FIXME: or use master attribute?
        else:
            cur = None

        while cur is not None:
            fqid.append(cur.id)
            if cur.master is not None:
                cur = cur.master
            else:
                cur = None
        fqid.reverse()

        if sep:
            return sep.join([str(id) for id in fqid])
        return fqid

    def getInputWorkspace(self, create=True):
        return self.getWorkspace('InputWorkspace', create=create)

    def getOutputWorkspace(self, create=True):
        return self.getWorkspace('OutputWorkspace', create=create)

    def getDebugWorkspace(self, create=True):
        return self.getWorkspace('DebugWorkspace', create=create)

    def peek(self, filename="", command=""):
        """
        Allow viewing of job output (and input) files

        Arguments other than self:
        filename : name of file to be viewed
                   => For backends where this is enabled, the filename
                      for a job in the "running" state is relative to
                      the job's work directory unless the filename begins
                      with "../".  In all other cases, the filename is
                      relative to the job's output directory
        command  : command to be used for viewing the file
                   => If no command is given, then the command defined in
                      the [File_Associations] section of the Ganga
                      configuration file(s) is used

        Example usage:

           # examine contents of output/work directory
           peek()

           # examine contents of output directory,
           # even in case of job in "running" state
           peek( "../output" )

           # examine contents of input directory
           peek( "../input" )

           # View ROOT histograms, running root.exe in a new terminal window
           peek( "histograms.root", "root.exe &&" )

           # View contents of file in output/work directory, using
           # command defined in configuration file
           peek( "output.txt" )

           # View ROOT histograms in ouput/work directory,
           # running root.exe in a new terminal window
           peek( "histograms.root", "root.exe &&" )

        Return value: None
        """

        pathStart = filename.split(os.sep)[0]
        if(self.status in ['running', 'submitted']) and (pathStart != ".."):
            subjob_num = len(self.subjobs)
            if subjob_num == 0:
                self.backend.peek(filename=filename, command=command)
            elif subjob_num > 0:
                for sj in self.subjobs:
                    logger.info("\n  subjob ID: %s" % (sj.getFQID('.')))
                    sj.backend.peek(filename=filename, command=command)
        else:
            topdir = os.path.dirname(self.inputdir.rstrip(os.sep))
            path = os.path.join(topdir, "output", filename).rstrip(os.sep)
            self.viewFile(path=path, command=command)
        return None

    def viewFile(self, path="", command=""):
        """
        Allow file viewing

        Arguments other than self:
        path    : path to file to be viewed
        command  : command to be used for viewing the file
                   => If no command is given, then the command defined in
                      the [File_Associations] section of the Ganga
                      configuration file(s) is used

        This is intended as a helper function for the peek() method.

        Return value: None
        """

        config = getConfig("File_Associations")

        if not os.path.exists(path):
            if len(glob.glob(path)) == 1:
                path = glob.glob(path)[0]

        if os.path.exists(path):
            if os.path.islink(path):
                path = os.readlink(path)
            if not command:
                if os.path.isdir(path):
                    command = config["listing_command"]
                else:
                    suffix = os.path.splitext(path)[1].lstrip(".")
                    try:
                        command = config[suffix]
                    except ConfigError as err:
                        logger.debug("Config Err: %s" % err)
                        command = config["fallback_command"]

            mode = os.P_WAIT

            try:
                tmpList = command.split("&&")
                termCommand = tmpList[1]
                if not termCommand:
                    termCommand = config["newterm_command"]
                exeopt = config["newterm_exeopt"]
                exeCommand = " ".join([tmpList[0], path])
                argList = [termCommand, exeopt, exeCommand]
                mode = os.P_NOWAIT
            except IndexError as err:
                logger.debug("Index Err: %s" % err)
                tmpList = command.split("&")
                if (len(tmpList) > 1):
                    mode = os.P_NOWAIT
                    command = tmpList[0]
                argList = command.split()
                argList.append(path)

            cmd = argList[0]
            os.spawnvp(mode, cmd, argList)
        else:
            logger.warning("File/directory '%s' not found" % path)

        return None

    def prepare(self, force=False):
        """A method to put a job's application into a prepared state. Returns 
        True on success.

        The benefits of preparing an application are twofold:

        1) The application can be copied from a previously executed job and
           run again over a different input dataset.
        2) Sharing applications (and their associated files) between jobs will 
           optimise disk usage of the Ganga client.

        See help(j.application.prepare) for application-specific comments.

        Prepared applications are always associated with a Shared Directory object
        which contains their required files. Details for all Shared Directories in use
        can been seen by calling 'shareref'. See help(shareref) for further details.

        """
        if not hasattr(self.application, 'is_prepared'):
            logger.warning("Non-preparable application %s cannot be prepared" % getName(self.application))
            return

        if (self.application.is_prepared is not None) and (force is False):
            msg = "The application associated with job %s has already been prepared. To force the operation, call prepare(force=True)" % self.id
            raise JobError(msg)
        if (self.application.is_prepared is None):
            add_to_inputsandbox = addProxy(self.application).prepare()
            if isType(add_to_inputsandbox, (list, tuple, GangaList)):
                self.inputsandbox.extend(add_to_inputsandbox)
        elif (self.application.is_prepared is not None) and (force is True):
            self.application.unprepare(force=True)
            addProxy(self.application).prepare(force=True)


    def unprepare(self, force=False):
        """Revert the application associated with a job to the unprepared state
        Returns True on success.
        """
        if not hasattr(self.application, 'is_prepared'):
            logger.warning("Non-preparable application %s cannot be unprepared" % getName(self.application))
            return

        if not self._readonly():
            logger.debug("Running unprepare() within Job.py")
            self.application.unprepare()
        else:
            logger.error("Cannot unprepare a job in the %s state" % self.status)

    def _getMasterAppConfig(self):

        appmasterconfig = None
        if self.master is None:
            #   I am the master Job
            appmasterconfig = self._storedAppMasterConfig
            if appmasterconfig is None:
                # I am going to generate the appmasterconfig now
                logger.debug("Job %s Calling application.master_configure" % self.getFQID('.'))
                appmasterconfig = self.application.master_configure()[1]
                self._storedAppMasterConfig = appmasterconfig
        else:
            # I am a sub-job, lets ask the master job what to do
            appmasterconfig = self.master._getMasterAppConfig()

        return appmasterconfig

    def _getAppSubConfig(self, subjobs=None):

        if subjobs is None:
            subjobs = GangaList()

        appsubconfig = []
        if self.master is None:
            #   I am the master Job
            appsubconfig = self._storedAppSubConfig
            if appsubconfig is None or len(appsubconfig) == 0:
                appmasterconfig = self._getMasterAppConfig()
                logger.debug("Job %s Calling application.configure %s times" % (self.getFQID('.'), len(self.subjobs)))
                appsubconfig = [j.application.configure(appmasterconfig)[1] for j in subjobs]

        else:
            #   I am a sub-job, lets just generate our own config
            appsubconfig = self._storedAppSubConfig
            if appsubconfig is None or len(appsubconfig) == 0:
                appmasterconfig = self._getMasterAppConfig()
                logger.debug("Job %s Calling application.configure 1 times" % self.getFQID('.'))
                appsubconfig = [self.application.configure(appmasterconfig)[1]]

        self._storedAppSubConfig = appsubconfig

        return appsubconfig

    def _getJobMasterConfig(self):

        jobmasterconfig = None
        if self.master is None:
            #   I am the master Job
            #   I have saved the config previously as a transient
            jobmasterconfig = self._storedJobMasterConfig
            if jobmasterconfig is None:
                #   I am going to generate the config now
                appmasterconfig = self._getMasterAppConfig()
                logger.debug("appConf: %s" % appmasterconfig)
                rtHandler = self._getRuntimeHandler()
                logger.debug("Job %s Calling rtHandler.master_prepare for RTH: %s" % (self.getFQID('.'), getName(rtHandler)))
                jobmasterconfig = rtHandler.master_prepare(self.application, appmasterconfig)
                self._storedJobMasterConfig = jobmasterconfig
        else:
            #   I am a sub-job, lets ask the master job what to do
            jobmasterconfig = self.master._getJobMasterConfig()

        logger.debug("JobMasterConfig: %s" % jobmasterconfig)

        return jobmasterconfig

    @staticmethod
    def _prepare_sj( rtHandler, i, app, sub_c, app_master_c, job_master_c, finished):
        if app.is_prepared in [None, False]:
            app.prepare()
        finished[i] = rtHandler.prepare(app, sub_c, app_master_c, job_master_c)
        return

    def _getJobSubConfig(self, subjobs):

        jobsubconfig = None
        if self.master is None:
            #   I am the master Job
            jobsubconfig = self._storedJobSubConfig
            if jobsubconfig is None:
                rtHandler = self._getRuntimeHandler()
                appmasterconfig = self._getMasterAppConfig()
                jobmasterconfig = self._getJobMasterConfig()
                appsubconfig = self._getAppSubConfig(subjobs)
                logger.debug("Job %s Calling rtHandler.prepare %s times" % (self.getFQID('.'), len(self.subjobs)))
                logger.info("Preparing subjobs")

                #Make an empty list
                jobsubconfig = [None]*len(subjobs)

                if self.parallel_submit is False:
                    jobsubconfig = [rtHandler.prepare(sub_job.application, sub_conf, appmasterconfig, jobmasterconfig) for (sub_job, sub_conf) in zip(subjobs, appsubconfig)]
                else:

                    finished = {}

                    from GangaCore.Core.GangaThread.WorkerThreads import getQueues
                    index = 0
                    for sub_j, sub_conf in zip(subjobs, appsubconfig):
                        getQueues()._monitoring_threadpool.add_function(self._prepare_sj, (rtHandler, index, sub_j.application, sub_conf, appmasterconfig, jobmasterconfig, finished))
                        index += 1

                    while len(finished) != len(subjobs):
                        time.sleep(0.25)

                    for index in finished.keys():
                        jobsubconfig[index] = finished[index]

        else:
            #   I am a sub-job, lets calculate my config
            rtHandler = self._getRuntimeHandler()
            appmasterconfig = self._getMasterAppConfig()
            jobmasterconfig = self._getJobMasterConfig()
            appsubconfig = self._getAppSubConfig(self)
            logger.debug("Job %s Calling rtHandler.prepare once for self" % self.getFQID('.'))
            jobsubconfig = [rtHandler.prepare(self.application, appsubconfig[0], appmasterconfig, jobmasterconfig)]

        self._storedJobSubConfig = jobsubconfig

        logger.debug("jobsubconfig: %s" % jobsubconfig)

        return jobsubconfig

    def _getRuntimeHandler(self):

        rtHandler = None
        if self.master is None:
            #   I am the master Job
            rtHandler = self._storedRTHandler
            if rtHandler is None:
                # select the runtime handler
                try:
                    logger.debug("Job %s Calling allHandlers.get" % self.getFQID('.'))
                    rtHandler = allHandlers.get(getName(self.application), getName(self.backend))()
                except KeyError as x:
                    msg = 'runtime handler not found for application=%s and backend=%s' % (getName(self.application), getName(self.backend))
                    logger.error("Available: %s" % list(allHandlers.handlers.keys()))
                    logger.error("Wanted: %s" % getName(self.backend))
                    logger.error("Available: %s" % allHandlers.handlers[getName(self.backend)])
                    logger.error("Wanted: %s" % getName(self.application))
                    logger.error(msg)
                    raise JobError(msg)
                self._storedRTHandler = rtHandler

        else:
            rtHandler = self.master._getRuntimeHandler()

        return rtHandler

    def _selfAppPrepare(self, prepare):

        def delay_check(somepath):
            for i in range(100):
                if os.path.exists(somepath):
                    return True
                else:
                    time.sleep(0.1)
            return False

        if hasattr(self.application, 'is_prepared'):
            logger.debug("Calling Job.prepare()")
            if (self.application.is_prepared is None) or (prepare is True):
                logger.debug("Job %s Calling self.prepare(force=%s)" % (self.getFQID('.'), prepare))
                self.prepare(force=True)
            elif self.application.is_prepared is True:
                msg = "Job %s's application has is_prepared=True. This prevents any automatic (internal) call to the application's prepare() method." % self.getFQID('.')
                logger.info(msg)
            else:
                msg = "Job %s's application has already been prepared." % self.getFQID('.')
                logger.info(msg)

            if self.application.is_prepared is not True and self.application.is_prepared is not None:
                shared_path = GangaCore.GPIDev.Lib.File.getSharedPath()
                delay_result = delay_check(os.path.join(shared_path, self.application.is_prepared.name))
                if delay_result is not True:
                    logger.warning("prepared directory is :%s \t,\t but expected something else" % self.application.is_prepared)
                    logger.warning("tested: %s" % os.path.join(shared_path, self.application.is_prepared.name))
                    logger.warning("shared_path: %s" % shared_path)
                    logger.warning("result: %s" % delay_result)
                    msg = "Cannot find shared directory for prepared application; reverting job to new and unprepared"
                    self.unprepare()
                    raise JobError(msg)
        else:
            logger.debug("Not calling prepare")

        if hasattr(self.application, 'is_prepared'):
            logger.debug("App preparedness: %s" % self.application.is_prepared)

        return

    def _unsetSubmitTransients(self):
        self._storedRTHandler = None
        self._storedJobSubConfig = None
        self._storedAppSubConfig = None
        self._storedJobMasterConfig = None
        self._storedAppMasterConfig = None

    def _doSplitting(self):
        # Temporary polution of Atlas stuff to (almost) transparently switch
        # from Panda to Jedi
        rjobs = None

        if getName(self.backend) == "Jedi" and self.splitter:
            logger.error("You should not use a splitter with the Jedi backend. The splitter will be ignored.")
            self.splitter = None
            rjobs = [self]
        elif self.splitter and not self.master is not None:

            fqid = self.getFQID('.')

            # App Configuration
            logger.debug("App Configuration, Job %s:" % fqid)

            # The App is configured first as information in the App may be
            # needed by the Job Splitter
            appmasterconfig = self._getMasterAppConfig()

            logger.info("Splitting Job: %s" % fqid)

            subjobs = self.splitter.validatedSplit(self)
            if subjobs:
                if not isType(self.subjobs, (list, GangaList)):
                    self.subjobs = GangaList()
                # print "*"*80
                # subjobs[0].printTree(sys.stdout)

                # EBKE changes
                i = 0
                # bug fix for #53939 -> first set id of the subjob and then append to self.subjobs
                #self.subjobs = subjobs
                # for j in self.subjobs:
                for sj in subjobs:
                    sj.info.uuid = str(uuid.uuid4())
                    sj.status = 'new'
                    #j.splitter = None
                    sj.time.timenow('new')
                    sj.id = i
                    i += 1
                    self.subjobs.append(sj)

                cfg = GangaCore.Utility.Config.getConfig('Configuration')
                for j in self.subjobs:
                    if cfg['autoGenerateJobWorkspace']:
                        j._init_workspace()

                rjobs = self.subjobs
                logger.info('submitting %s subjobs', len(rjobs))
            else:
                rjobs = [self]
        else:
            rjobs = [self]

        return rjobs

    def submit(self, keep_going=None, keep_on_fail=None, prepare=False):
        """Submits a job. Return true on success.

        First  the  application   is  configured  which  may  generate
        additional  input files,  preprocess  executable scripts  etc.
        Then  backend handler is  used to  submit the  configured job.
        The job is automatically checkpointed to persistent storage.

        The default values of keep_going and keep_on_fail are controlled by [GPI_Semantics] configuration options.

        When the submission fails the job status is automatically
        reverted to new and all files in the input directory are
        deleted (keep_on_fail=False is the default behaviour unless modified in configuration).
        If keep_on_fail=True then the job status
        is moved to the failed status and input directory is left intact.
        This is helpful for debugging anf implements the request #43143.

        For split jobs: consult https://twiki.cern.ch/twiki/bin/view/ArdaGrid/GangaSplitters#Subjob_submission
        """
        self._getRegistry()._flush([self])
        logger.debug("Submitting Job %s" % self.getFQID('.'))

        gpiconfig = getConfig('GPI_Semantics')

        if keep_going is None:
            keep_going = gpiconfig['job_submit_keep_going']

        if keep_on_fail is None:
            keep_on_fail = gpiconfig['job_submit_keep_on_fail']

        # make sure nobody writes to the cache during this operation
        # job._registry.cache_writers_mutex.lock()

        supports_keep_going = 'keep_going' in inspect.getfullargspec(self.backend.master_submit)[0]

        if keep_going and not supports_keep_going:
            msg = 'job.submit(keep_going=True) is not supported by %s backend' % getName(self.backend)
            logger.error(msg)
            raise JobError(msg)

        # can only submit jobs in a 'new' state
        if self.status != 'new':
            msg = "cannot submit job %s which is in '%s' state" % (self.getFQID('.'), self.status)
            logger.error(msg)
            raise JobError(msg)

        from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySliceProxy

        try:
            assert(self.subjobs in [[], GangaList()] or ((isType(self.subjobs, JobRegistrySliceProxy) or isType(self.subjobs, SubJobXMLList)) and len(self.subjobs) == 0) )
        except AssertionError:
            raise JobManagerError("Number of subjobs in the job is inconsistent so not submitting the job")

        # no longer needed with prepared state
        # if self.master is not None:
        #    msg = "Cannot submit subjobs directly."
        #    logger.error(msg)
        #    raise JobError(msg)

        rtHandler = self._getRuntimeHandler()

        try:

            logger.info("submitting job %s", self.getFQID('.'))
            # prevent other sessions from submitting this job concurrently.
            self.updateStatus('submitting')

            self.getDebugWorkspace(create=False).remove(preserve_top=True)

            # Calls the self.prepare method ALWAY
            logger.debug("Preparing Application")
            #if hasattr(self.application, 'is_prepared'):
            #    logger.debug("Calling self.prepare()")
            #    self.prepare()
            self._selfAppPrepare(prepare)

            # Splitting
            logger.debug("Checking Job: %s for splitting" % self.getFQID('.'))
            # split into subjobs
            rjobs = self._doSplitting()

            # Some old jobs may still contain None assigned to the self.subjobs so be careful when checking length
            if self.splitter is not None and not self.subjobs:
                raise SplitterError("Splitter '%s' failed to produce any subjobs from splitting. Aborting submit" % (getName(self.splitter),))

            #
            logger.debug("Now have %s subjobs" % len(self.subjobs))
            logger.debug("Also have %s rjobs" % len(rjobs))

            # Output Files
            # validate the output files
            for this_job in rjobs:
                (validOutputFiles, errorMsg) = this_job.validateOutputfilesOnSubmit()
                if not validOutputFiles:
                    raise JobError(errorMsg)

            # configure the application of each subjob
            appsubconfig = self._getAppSubConfig(rjobs)
            if appsubconfig is not None:
                logger.debug("# appsubconfig: %s" % len(appsubconfig))

            # Job Configuration
            logger.debug("Job Configuration, Job %s:" % self.getFQID('.'))

            # prepare the master job with the correct runtime handler
            # Calls rtHandler.master_prepare if it hasn't already been called by the master job or by self
            # Only stored as transient if the master_prepare successfully
            # completes
            jobmasterconfig = self._getJobMasterConfig()
            logger.debug("Preparing with: %s" % getName(rtHandler))

            # prepare the subjobs with the runtime handler
            # Calls the rtHandler.prepare if it hasn't already been called by the master job or by self
            # Only stored as a transient if the prepare successfully completes
            jobsubconfig = self._getJobSubConfig(rjobs)
            logger.debug("# jobsubconfig: %s" % len(jobsubconfig))

            # Submission
            logger.debug("Submitting to a backend, Job %s:" % self.getFQID('.'))

            # notify monitoring-services
            self.monitorPrepare_hook(jobsubconfig)

            # submit the job
            # master_submit has been written as the interface which ganga
            # should call, not submit directly

            if supports_keep_going:
                if 'parallel_submit' in inspect.getfullargspec(self.backend.master_submit)[0]:
                    r = self.backend.master_submit( rjobs, jobsubconfig, jobmasterconfig, keep_going, self.parallel_submit)
                else:
                    r = self.backend.master_submit( rjobs, jobsubconfig, jobmasterconfig, keep_going)
            else:
                r = self.backend.master_submit( rjobs, jobsubconfig, jobmasterconfig)

            if not r:
                raise JobManagerError('error during submit')

            # This appears to be done by the backend now in a way that handles sub-jobs,
            # in the case of a master job however we need to still perform this
            if len(rjobs) != 1:
                self.info.increment()
            if self.master is None:
                self.updateStatus('submitted')
            # make sure that the status change goes to the repository, NOTE:
            # this commit is redundant if updateStatus() is used on the line
            # above
            self._getRegistry()._flush([self])

            return 1

        except IncompleteJobSubmissionError as x:
            logger.warning('Not all subjobs have been sucessfully submitted: %s', x)
            self.updateStatus('failed')
            raise x
        except Exception as err:
            if isType(err, GangaException):
                log_user_exception(logger, debug=True)
                logger.error("%s" % err)
            else:
                log_user_exception(logger, debug=False)

            if keep_on_fail:
                self.updateStatus('failed')
                
            else:
                # revert to the new status
                logger.error('%s ... reverting job %s to the new status', err, self.getFQID('.'))
                self.updateStatus('new')
            raise JobError("Error: %s" % err).with_traceback(sys.exc_info()[2])

    def rollbackToNewState(self):
        """
        Rollback the job to the "new" state if submitting of job failed:
            - cleanup the input and output workspace preserving the top dir(bug ##19434)
            - do not remove debug directory
            - cleanup subjobs
        This method is used as a hook for submitting->new transition
        @see updateJobStatus() 
        """

        # notify monitoring-services
        self.monitorRollbackToNew_hook()

        self.getInputWorkspace().remove(preserve_top=True)
        self.getOutputWorkspace().remove(preserve_top=True)
        # notify subjobs
        for sj in self.subjobs:
            sj.application.transition_update("removed")
        # delete subjobs
        self.subjobs = GangaList()

    def remove(self, force=False):
        """Remove the job.

        If job  has been submitted try  to kill it  first. Then remove
        the   file   workspace   associated   with   the   job.

        If force=True then remove job without killing it.
        """

        this_job_status = lazyLoadJobStatus(self)
        this_job_id = lazyLoadJobFQID(self)

        template = this_job_status == 'template'

        if template:
            logger.info('removing template %s', this_job_id)
        else:
            logger.info('removing job %s', this_job_id)

        if this_job_status == 'removed':
            msg = 'job %s already removed' % this_job_id
            logger.error(msg)
            raise JobError(msg)

        if this_job_status == 'completing':
            msg = 'job %s is completing (may be downloading output), do force_status("failed") and then remove() again' % self.getFQID('.')
            logger.error(msg)
            raise JobError(msg)

        if self.master is not None:
            msg = 'cannot remove subjob %s' % self.getFQID('.')
            logger.info(msg)
            raise JobError(msg)

        if getConfig('Output')['AutoRemoveFilesWithJob']:
            logger.info('Removing all output data of types %s' % getConfig('Output')['AutoRemoveFileTypes'])
            def removeFiles(this_file):
                if getName(this_file) in getConfig('Output')['AutoRemoveFileTypes'] and hasattr(this_file, '_auto_remove'):
                    this_file._auto_remove()

            def collectFiles(this_job):
                collectedFiles = []
                for _f in this_job.outputfiles:
                    if _f.containsWildcards() and hasattr(_f, 'subfiles') and _f.subfiles:
                        collectedFiles.extend(_f.subfiles)
                    else:
                        collectedFiles.append(_f)
                return collectedFiles

            _filesToRemove = []
            for sj in self.subjobs:
                _filesToRemove.extend(collectFiles(sj))
            _filesToRemove.extend(collectFiles(self))

            list(map(removeFiles, _filesToRemove))

        if this_job_status in ['submitted', 'running']:
            try:
                if not force:
                    self._kill(transition_update=False)
            except GangaException as x:
                log_user_exception(logger, debug=True)
            except Exception as x:
                log_user_exception(logger)
                logger.warning('unhandled exception in j.kill(), job id=%s', self.id)

        # incomplete or unknown jobs may not have valid application or backend
        # objects
        if this_job_status not in ['incomplete', 'unknown']:
            # tell the backend that the job was removed
            # this is used by Remote backend to remove the jobs remotely
            # bug #44256: Job in state "incomplete" is impossible to remove

            backend_obj = lazyLoadJobBackend(self)
            application_obj = lazyLoadJobApplication(self)

            if backend_obj is not None:
                if hasattr(backend_obj, 'remove'):
                    self.backend.remove()
            else:
                if hasattr(self.backend, 'remove'):
                    self.backend.remove()

            if application_obj is not None:
                if hasattr(application_obj, 'transition_update'):
                    self.application.transition_update('removed')
                    for sj in self.subjobs:
                        sj.application.transition_update('removed')
            else:
                self.application.transition_update('removed')
                for sj in self.subjobs:
                    sj.application.transition_update('removed')

        if not template:
            # remove the corresponding workspace files

            try:

                if len(self.subjobs) > 0:
                    for sj in self.subjobs:
                        def doit_sj(f):
                            try:
                                f()
                            except OSError as err:
                                logger.warning('cannot remove file workspace associated with the sub-job %s : %s', self.getFQID('.'), err)

                        wsp_input = sj.getInputWorkspace(create=False)
                        doit_sj(wsp_input.remove)
                        wsp_output = sj.getOutputWorkspace(create=False)
                        doit_sj(wsp_output.remove)
                        wsp_debug = sj.getDebugWorkspace(create=False)
                        doit_sj(wsp_debug.remove)
            except KeyError as err:
                logger.debug("KeyError, likely job hasn't been loaded.")
                logger.debug("In that case try and skip")
                pass

            def doit(f):
                try:
                    f()
                except OSError as err:
                    logger.warning('cannot remove file workspace associated with the job %s : %s', self.id, err)

            wsp_input = self.getInputWorkspace(create=False)
            wsp_input.jobid = this_job_id
            doit(wsp_input.remove)
            wsp_output = self.getOutputWorkspace(create=False)
            wsp_output.jobid = this_job_id
            doit(wsp_output.remove)
            wsp_debug = self.getDebugWorkspace(create=False)
            wsp_debug.remove(preserve_top=False)

            wsp = self.getInputWorkspace(create=False)
            wsp.subpath = ''
            wsp.jobid = this_job_id
            doit(wsp.remove)

            try:

                # If the job is associated with a shared directory resource (e.g. has a prepared() application)
                # decrement the reference counter.
                if hasattr(self.application, 'is_prepared') and self.application.__getattribute__('is_prepared'):
                    if self.application.is_prepared is not True:
                        self.application.decrementShareCounter(self.application.is_prepared)
            except KeyError as err:
                logger.debug("KeyError, likely job hasn't been loaded.")
                logger.debug("In that case try and skip")
                pass

        if self._registry:
            try:
                self._registry._remove(self, auto_removed=1)
            except GangaException as err:
                logger.warning("Error trying to fully remove Job #'%s':: %s" % (self.getFQID('.'), err))

        self.status = 'removed'

        try:
            self._releaseSessionLockAndFlush()
        except Exception as err:
            logger.debug("Remove Err: %s" % err)
            pass

    allowed_force_states = {'completed': ['completing', 'failed'],
                            'failed': ["submitting", "completing", "completed", "submitted", "running", "killed"]}

    def force_status(self, status, force=False):
        """ Force job to enter the "failed" or "completed" state. This may be
        used for marking jobs "bad" jobs or jobs which are stuck in one of the
        internal ganga states (e.g. completing).

        To see the list of allowed states do: job.force_status(None)
        """

        if status is None:
            logger.info("The following states may be forced")
            revstates = {}
            for s1 in Job.allowed_force_states:
                for s2 in Job.allowed_force_states[s1]:
                    revstates.setdefault(s2, {})
                    revstates[s2][s1] = 1

            for s in revstates:
                logger.info("%s => %s" % (s, list(revstates[s].keys())))
            return

        if self.status == status:
            return

        if status not in Job.allowed_force_states:
            raise JobError('force_status("%s") not allowed. Job may be forced to %s states only.' % (
                status, list(Job.allowed_force_states.keys())))

        if self.status not in Job.allowed_force_states[status]:
            raise JobError('Only a job in one of %s may be forced into "%s" (job %s)' % (Job.allowed_force_states[status], status, self.getFQID('.')))

        if not force:
            if self.status in ['submitted', 'running']:
                try:
                    self._kill(transition_update=False)
                except JobError as x:
                    x.what += "Use force_status('%s',force=True) to ignore kill errors." % status
                    raise x
        try:
            logger.info('Forcing job %s to status "%s"', self.getFQID('.'), status)
            self.updateStatus(status, ignore_failures=True)
        except JobStatusError as x:
            logger.error(x)
            raise x

    def kill(self):
        """Kill the job. Raise JobError exception on error.
        """
        self._kill(transition_update=True)

    def _kill(self, transition_update):
        """Private helper. Kill the job. Raise JobError exception on error.
        """
        try:
            # make sure nobody writes to the cache during this operation
            # job._registry.cache_writers_mutex.lock()

            fqid = self.getFQID('.')
            logger.info('killing job %s', fqid)
            if self.status not in ['submitted', 'running']:
                if self.status in ['completed', 'failed']:
                    logger.warning("Job %s has already reached it's final state: %s and cannot be killed" % (self.getFQID('.'), self.status))
                    return True
                else:
                    msg = "cannot kill job which is in '%s' state. " % self.status
                    logger.error(msg)
                    raise JobError(msg)
            try:
                if self.backend.master_kill():
                    ############
                    # added as part of typestamp prototype by Justin
                    if not self._getParent():
                        for jobs in self.getJobObject().subjobs:
                            # added this 10/8/2009 - now only kills subjobs
                            # which aren't finished.
                            if jobs.status not in ['failed', 'killed', 'completed']:
                                jobs.updateStatus('killed', transition_update=transition_update)
                    self.updateStatus('killed', transition_update=transition_update)
                    #
                    ############

                    return True
                else:
                    msg = "backend.master_kill() returned False"
                    raise JobError(msg)
            except GangaException as x:
                msg = "failed to kill job %s: %s" % (self.getFQID('.'), x)
                logger.error(msg)
                raise JobError(msg)
        finally:
            pass  # job._registry.cache_writers_mutex.release()

    def resubmit(self, backend=None):
        """Resubmit a failed or completed job.  A backend object may
        be specified to change some submission parameters (which
        parameters may be effectively changed depends on a
        particular backend implementation).

        Example:
         b = j.backend.copy()
         b.CE = 'some CE'
         j.resubmit(backend=b)

        Note: it is not possible to change the type of the backend in this way.

        """
        return self._resubmit(backend=backend)

    def auto_resubmit(self):
        """ Private method used for auto resubmit functionality by the monitoring loop.
        """
        return self._resubmit(auto_resubmit=True)

    def _resubmit(self, backend=None, auto_resubmit=False):
        """ Internal implementation of resubmit which handles the publically accessible resubmit() method proper as well
        as the auto_resubmit use case used in the monitoring loop.
        """
        # there are possible two race condition which must be handled somehow:
        #  - a simple job is monitorable and at the same time it is 'resubmitted' - potentially the monitoring may update the status back!
        #  - same for the master job...

        fqid = self.getFQID('.')
        logger.info('resubmitting job %s', fqid)

        if backend and auto_resubmit:
            msg = "job %s: cannot change backend when auto_resubmit=True. This is most likely an internal implementation problem." % self.getFQID('.')
            logger.error(msg)
            raise JobError(msg)

        if len(self.subjobs) != 0:
            these_jobs = self.subjobs
        else:
            these_jobs = [self]

        for this_job in these_jobs:
            (validOutputFiles, errorMsg) = this_job.validateOutputfilesOnSubmit()
            if not validOutputFiles:
                raise JobError(errorMsg)

        if self.status in ['new']:
            msg = "cannot resubmit a new job %s, please use submit()" % (self.getFQID('.'))
            logger.error(msg)
            raise JobError(msg)

        # the status check is disabled when auto_resubmit
        if self.status not in ['completed', 'failed', 'killed'] and not auto_resubmit:
            msg = "cannot resubmit job %s which is in '%s' state" % (self.getFQID('.'), self.status)
            logger.error(msg)
            raise JobError(msg)

        if backend is not None:
            backend = backend

        # do not allow to change the backend type
        if backend and not isType(self.backend, type(backend)):
            msg = "cannot resubmit job %s: change of the backend type is not allowed" % self.getFQID('.')
            logger.error(msg)
            raise JobError(msg)

        # if the backend argument is identical (no attributes changed) then it is equivalent to None
        # the good side effect is that in this case we don't require any backend resubmit method to support
        # the extra backend argument
        if backend == self.backend:
            backend = None

        # check if the backend supports extra 'backend' argument for
        # master_resubmit()
        supports_master_resubmit = len(inspect.getfullargspec(self.backend.master_resubmit)[0]) > 1

        if not supports_master_resubmit and backend:
            raise JobError('%s backend does not support changing of backend parameters at resubmission (optional backend argument is not supported)' % getName(self.backend))

        def check_changability(obj1, obj2):
            # check if the only allowed attributes have been modified
            for name, item in obj1._schema.allItems():
                v1 = getattr(obj1, name)
                v2 = getattr(obj2, name)
                if not item['changable_at_resubmit'] and item['copyable']:
                    if v1 != v2:
                        raise JobError('%s.%s attribute cannot be changed at resubmit' % (getName(obj1), name))
                if item.isA(ComponentItem):
                    check_changability(v1, v2)

        if backend:
            check_changability(self.backend, backend)

        oldstatus = self.status

        try:
            config_resubOFS = config['resubmitOnlyFailedSubjobs']
            if config_resubOFS is True:
                rjobs = [s for s in self.subjobs if s.status in ['failed']]
            else:
                rjobs = self.subjobs

            if not rjobs and not self.subjobs:
                rjobs = [self]
            elif auto_resubmit:  # get only the failed jobs for auto resubmit
                rjobs = [s for s in rjobs
                         if s.status == 'failed' and s.info.submit_counter <= getConfig("PollThread")['MaxNumResubmits']]

            if rjobs:
                for sjs in rjobs:
                    sjs.info.increment()
                    # bugfix: #31690: Empty the outputdir of the subjob just
                    # before resubmitting it
                    sjs.getOutputWorkspace().remove(preserve_top=True)
            else:
                logger.debug('There is nothing to do for resubmit of Job: %s' % self.getFQID('.'))
                logger.debug('It\'s assumed all subjobs here have been completed, continuing silently')
                self.updateStatus(oldstatus)
                return

            self.updateStatus('submitting')

            self.getDebugWorkspace().remove(preserve_top=True)

            try:
                if auto_resubmit:
                    result = self.backend.master_auto_resubmit(rjobs)
                else:
                    if backend is None:
                        result = self.backend.master_resubmit(rjobs)
                    else:
                        result = self.backend.master_resubmit(
                            rjobs, backend=backend)

                if not result:
                    raise JobManagerError('error during submit')
            except IncompleteJobSubmissionError as x:
                logger.warning('Not all subjobs of job %s have been sucessfully re-submitted: %s', self.getFQID('.'), x)

            # fix for bug 77962 plus for making auto_resubmit test work
            if auto_resubmit:
                self.info.increment()

            if self.subjobs:
                for sjs in self.subjobs:
                    sjs.time.timenow('resubmitted')
            else:
                self.time.timenow('resubmitted')

            # FIXME: if job is not split, then default implementation of
            # backend.master_submit already have set status to "submitted"
            self.updateStatus('submitted')

        except GangaException as x:
            logger.error("failed to resubmit job, %s" % x)
            logger.warning('reverting job %s to the %s status', fqid, oldstatus)
            self.status = oldstatus
            raise

    def auto_kill(self):
        self.kill()

    def _repr(self):
        if self.id is None:
            id = "None"
        else:
            id = self.getFQID('.')
        return "%s#%s" % (getName(self), id)

    def _subjobs_proxy(self):

        from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, _wrap

        subjob_slice = stripProxy(self._stored_subjobs_proxy)
        if not isType(self._stored_subjobs_proxy, JobRegistrySlice):
            subjob_slice = JobRegistrySlice('jobs(%s).subjobs' % self.id)
            self._stored_subjobs_proxy = _wrap(subjob_slice)

        if len(self._stored_subjobs_proxy) != len(self.subjobs):

            if isType(self.subjobs, SubJobXMLList):
                subjob_slice.objects = self.subjobs
                #self._stored_subjobs_proxy = _wrap(self._stored_subjobs_proxy)
            elif isType(self.subjobs, (list, GangaList)):
                subjob_slice = stripProxy(self._stored_subjobs_proxy)
                #First clear the dictionary
                if subjob_slice.objects:
                    del subjob_slice.objects[:]
                #Not put the objects back in
                for sj in self.subjobs:
                    subjob_slice.objects[sj.id] = sj
                #self._stored_subjobs_proxy = _wrap(self._stored_subjobs_proxy)
            else:
                raise GangaException("This should never arise, cannot understand subjob list")

        return self._stored_subjobs_proxy

    def _subjobs_summary_print(self, value, verbosity_level, interactive=False):
        rslice = self._subjobs_proxy()
        return rslice._display(1)

    def __setattr__(self, attr, value):

        if attr == 'outputfiles':

            if value != []:
                if self.outputdata is not None:
                    logger.error('job.outputdata is set, you can\'t set job.outputfiles')
                    return
                elif self.outputsandbox != []:
                    logger.error('job.outputsandbox is set, you can\'t set job.outputfiles')
                    return

            # reduce duplicate values here
            uniqueValues = GangaList()
            for val in value:
                if val not in uniqueValues:
                    uniqueValues.append(val)

            super(Job, self).__setattr__(attr, uniqueValues)

        elif attr == 'inputfiles':

            super(Job, self).__setattr__(attr, value)

        elif attr == 'outputsandbox':

            if value != []:

                if getConfig('Output')['ForbidLegacyOutput']:
                    logger.error('Use of job.outputsandbox is forbidden, please use job.outputfiles')
                    return

                if self.outputfiles:
                    logger.error('job.outputfiles is set, you can\'t set job.outputsandbox')
                    return

            super(Job, self).__setattr__(attr, value)

        elif attr == 'inputsandbox':

            # print "Setting inputsandbox to be: %s" %  value

            if value != []:

                if getConfig('Output')['ForbidLegacyInput']:
                    logger.error('Use of job.inputsandbox is forbidden, please use job.inputfiles')
                    raise GangaException('Use of job.inputsandbox is forbidden, please use job.inputfiles')

            super(Job, self).__setattr__(attr, value)

        elif attr == 'outputdata':

            if value is not None:

                if getConfig('Output')['ForbidLegacyOutput']:
                    logger.error('Use of job.outputdata is forbidden, please use job.outputfiles')
                    return

                if self.outputfiles:
                    logger.error('job.outputfiles is set, you can\'t set job.outputdata')
                    return
            super(Job, self).__setattr__(attr, value)

        elif attr == 'comment':

            super(Job, self).__setattr__(attr, value)

        elif attr == 'backend':

            # Temporary polution of Atlas stuff to (almost) transparently
            # switch from Panda to Jedi
            configPanda = None
            if value is not None and getName(value) == "Panda":
                configPanda = GangaCore.Utility.Config.getConfig('Panda')

            if configPanda and not configPanda['AllowDirectSubmission']:
                logger.error("Direct Panda submission now deprecated - Please switch to Jedi() backend and remove any splitter.")
                from GangaPanda.Lib.Jedi import Jedi

                new_value = Jedi()

                # copy over attributes where possible
                for attr in ['site', 'extOutFile', 'libds', 'accessmode', 'forcestaged', 'individualOutDS', 'bexec', 'nobuild']:
                    setattr(new_value, attr, copy.deepcopy(getattr(value, attr)))

                for attr in ['long', 'cloud', 'anyCloud', 'memory', 'cputime', 'corCheck', 'notSkipMissing', 'excluded_sites',
                             'excluded_clouds', 'express', 'enableJEM', 'configJEM', 'enableMerge', 'configMerge', 'usecommainputtxt',
                             'rootver', 'overwriteQueuedata', 'overwriteQueuedataConfig']:
                    setattr(new_value.requirements, attr, copy.deepcopy(getattr(value.requirements, attr)))

                super(Job, self).__setattr__('backend', new_value)
            else:
                new_value = stripProxy(runtimeEvalString(self, attr, value))
                super(Job, self).__setattr__('backend', new_value)
        elif attr.startswith('_'):
            # If it's an internal attribute then just pass it on
            super(Job, self).__setattr__(attr, value)
        else:
            new_value = stripProxy(runtimeEvalString(self, attr, value))
            super(Job, self).__setattr__(attr, new_value)

    def splitterCopy(self, other_job, _ignore_atts=None):
        """
        A method for copying the job object. This is a copy of the generic GangaObject method with 
        some checks removed for maximum speed. This should therefore be used with great care!
        """

        if _ignore_atts is None:
            _ignore_atts = []
        _srcobj = other_job

        for name, item in self._schema.allItems():
            if name in _ignore_atts:
                continue

            copy_obj = copy.deepcopy(getattr(_srcobj, name))
            setattr(self, name, copy_obj)

        ## Fix some objects losing parent knowledge
        src_dict = other_job.__dict__
        for key, val in src_dict.items():
            this_attr = getattr(other_job, key)


class JobTemplate(Job):

    """A placeholder for Job configuration parameters.

    JobTemplates are normal Job objects but they are never submitted. They have their own JobRegistry, so they do not get mixed up with
    normal jobs. They have always a "template" status.

    Create a job with an existing job template t:

         j = Job(t)

    Save a job j as a template t:

         t = JobTemplate(j)

    You may save commonly used job parameters in a template and create new jobs easier and faster.
    """
    _category = 'jobs'
    _name = 'JobTemplate'

    _schema = Job._schema.inherit_copy()
    _schema.datadict["status"] = SimpleItem('template', protected=1, checkset='_checkset_status',
                                            doc='current state of the job, one of "new", "submitted", "running", "completed", "killed", "unknown", "incomplete"')

    default_registry = 'templates'

    def __init__(self):
        super(JobTemplate, self).__init__()
        self.status = "template"

    def _readonly(self):
        return 0

    # FIXME: for the moment you have to explicitly define all methods if you
    # want to export them...
    def remove(self, force=False):
        """See Job for documentation. The force optional argument has no effect (it is provided for the compatibility with Job interface)"""
        return super(JobTemplate, self).remove()

    def submit(self):
        """ Templates may not be submitted, return false."""
        return super(JobTemplate, self).submit()

    def kill(self):
        """ Templates may not be killed, return false."""
        return super(JobTemplate, self).kill()


#
#
# $Log: Job.py,v $
# Revision 1.10  2009/02/24 14:59:34  moscicki
# when removing jobs which are in the "incomplete" or "unknown" status, do not trigger callbacks on application and backend -> they may be missing!
#
# Revision 1.12.2.5  2009/07/14 15:09:37  ebke
# Missed fix
#
# Revision 1.12.2.4  2009/07/13 22:10:52  ebke
# Update for the new GangaRepository:
# * Moved dict interface from Repository to Registry
# * Clearly specified Exceptions to be raised by Repository
# * proper exception handling in Registry
# * moved _writable to _getWriteAccess, introduce _getReadAccess
# * clarified locking, logic in Registry, less in Repository
# * index reading support in XML (no writing, though..)
# * general index reading on registry.keys()
#
# Revision 1.12.2.3  2009/07/10 13:30:06  ebke
# Fixed _commit not commiting the root object
#
# Revision 1.12.2.2  2009/07/10 11:30:34  ebke
# Remove reference to _data in status in preparation for lazy loading
#
# Revision 1.12.2.1  2009/07/08 11:18:21  ebke
# Initial commit of all - mostly small - modifications due to the new GangaRepository.
# No interface visible to the user is changed
#
# Revision 1.12  2009/05/20 12:35:40  moscicki
# debug directory (https://savannah.cern.ch/bugs/?50305)
#
# Revision 1.11  2009/05/20 09:23:46  moscicki
# debug directory (https://savannah.cern.ch/bugs/?50305)
#
# Revision 1.10  2009/02/24 14:59:34  moscicki
# when removing jobs which are in the "incomplete" or "unknown" status, do not trigger callbacks on application and backend -> they may be missing!
#
# Revision 1.9  2009/02/02 12:54:55  moscicki
# bugfix: bug #45679: j.application.transition_update("removed") is not called on j.remove()
#
# Revision 1.8  2008/11/21 13:45:58  moscicki
# #bug #44256: Job in state "incomplete" is impossible to remove
#
# Revision 1.7  2008/11/07 12:39:53  moscicki
# added j.submit(keep_on_fail=False) option (request #43143)
#
# Revision 1.6  2008/10/02 10:31:05  moscicki
# bugfix #41372: added backend.remove() method to support job removal on the Remote backend
#
# Revision 1.5  2008/09/09 14:51:14  moscicki
# bug #40696: Exception raised during resubmit() should be propagated to caller
#
# Revision 1.4  2008/09/09 14:37:16  moscicki
# bugfix #40220: Ensure that default values satisfy the declared types in the schema
#
# factored out type checking into schema module, fixed a number of wrongly declared schema items in the core
#
# Revision 1.3  2008/08/18 13:18:58  moscicki
# added force_status() method to replace job.fail(), force_job_failed() and
# force_job_completed()
#
# Revision 1.2  2008/08/04 14:28:20  moscicki
# bugfix: #39655: Problem with slice operations on templates
#
# REMOVED TABS
#
# Revision 1.1  2008/07/17 16:40:54  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.62.4.17  2008/04/21 08:46:51  wreece
# Imports missing symbol. test Ganga/test/Bugs/Savannah28511 now passes
#
# Revision 1.62.4.16  2008/04/18 13:42:02  moscicki
# remove obsolete printout
#
# Revision 1.62.4.15  2008/04/18 07:06:24  moscicki
# bugfix 13404  (reintroduced in 5)
#
# Revision 1.62.4.14  2008/04/02 11:29:35  moscicki
# inputdir and outputdir are not stored persistently anymore but are calculated wrt to the current workspace configuration
#
# this makes it easier to relocate local repository and, in the future, to implement local workspace cache for remote repository/workspace
#
# Revision 1.62.4.13  2008/03/17 19:38:40  roma
# bug fix #28511
#
# Revision 1.62.4.12  2008/03/06 14:10:35  moscicki
# added warning if fail() fails
#
# Revision 1.62.4.11  2008/03/04 10:23:43  amuraru
# fixed rollbackToNewStatu
#
# Revision 1.62.4.10  2008/03/03 14:57:06  amuraru
# fixed the state transition when merger fails
#
# Revision 1.62.4.9  2008/02/29 10:27:21  moscicki
# fixes in Job._kill() method
#
# added fail() and remove() method in GPI slices (not all keywords implemented yet)
#
# Revision 1.62.4.8  2008/02/28 15:48:11  moscicki
# cannot submit subjobs directly
#
# Revision 1.62.4.7  2008/02/28 13:07:37  moscicki
# added fail() and improved behaviour of kill()
# improved logging messages
# removed race condition in status update hook
#
# Revision 1.62.4.6  2007/12/19 16:27:17  moscicki
# remove(): ignore kill failures
# fixed messaging on kill
#
# Revision 1.62.4.5  2007/12/18 09:06:42  moscicki
# integrated typesystem from Alvin
#
# Revision 1.62.4.4  2007/12/10 17:29:01  amuraru
# merged changes from Ganga 4.4.4
#
# Revision 1.62.4.3  2007/11/13 18:37:26  wreece
# Merges head change in Job with branch. Fixes warnings in Mergers. Merges MergerTests with head. Adds new test to GangaList. Fixes config problems in Root.
#
# Revision 1.62.4.2  2007/11/08 11:56:44  moscicki
# pretty print for subjobs added
#
# Revision 1.62.4.1  2007/11/07 17:02:12  moscicki
# merged against Ganga-4-4-0-dev-branch-kuba-slices with a lot of manual merging
#
# Revision 1.65  2007/12/04 14:11:02  moscicki
# added submitted->submitting transition to fix problems with master job status in case of subjob resubmission
#
# Revision 1.64  2007/10/19 15:24:40  amuraru
# allow *->submitting transition in order to support resubmission (hurng-chun)
#
# Revision 1.63  2007/10/09 13:05:17  moscicki
# JobInfo object from Vladimir
# Savannah #30045 bugfix from Will (merger recursive updateStatus() call)
#
# Revision 1.62  2007/09/12 16:40:46  amuraru
# use the internal logger when in log_user_exception
#
# Revision 1.61  2007/07/27 15:13:38  moscicki
# merged the monitoring services branch from kuba
#
# Revision 1.60  2007/07/27 13:52:00  moscicki
# merger updates from Will (from GangaMergers-1-0 branch)
#
# Revision 1.59  2007/07/10 13:08:30  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.58.2.1  2007/05/14 13:32:11  wreece
# Adds the merger related code on a seperate branch. All tests currently
# run successfully.
#
# Revision 1.58  2007/05/11 13:26:05  moscicki
# fix in the state transition to support:
#
# temporary functions to help getting jobs out of completing and submitting states:
# force_job_completed(j): may be applied to completing jobs
# force_job_failed(j): may be applied to submitting or completing jobs
#
# Revision 1.57.4.2  2007/06/18 14:35:20  moscicki
# mergefrom_HEAD_18Jun07
#
# Revision 1.57.4.1  2007/06/18 10:16:34  moscicki
# slices prototype
#
# Revision 1.58  2007/05/11 13:26:05  moscicki
# fix in the state transition to support:
#
# temporary functions to help getting jobs out of completing and submitting states:
# force_job_completed(j): may be applied to completing jobs
# force_job_failed(j): may be applied to submitting or completing jobs
#
# Revision 1.57  2007/04/20 17:29:34  moscicki
# re-enabled the removal of subjobs table in the repository on job submission failure (revert to new status)
#
# Revision 1.56  2007/03/29 16:59:20  moscicki
# exception handling fixed
#
# Revision 1.55  2007/03/26 16:14:36  moscicki
#  - changed formatting of exception messages
#  - fix removing the subjobs and reseting the counter when the split job submission fails (Ganga/test/GPI/CrashMultipleSubmitSubjobs.gpi) - TO BE CHECKED!
#
# Revision 1.54  2007/02/28 18:16:56  moscicki
# support for generic: self.application.postprocess()
#
# removed JobManager from resubmit() and kill()
#
# Revision 1.53  2007/02/22 13:46:10  moscicki
# simplification of the internal code: removal of JobManager and ApplicationManager
#
# bugfix 23737
#
# Revision 1.52  2007/01/25 16:18:21  moscicki
# mergefrom_Ganga-4-2-2-bugfix-branch_25Jan07 (GangaBase-4-14)
#
# Revision 1.51  2006/10/26 16:27:24  moscicki
# explicit subjob support (Alexander Soroko)
#
# Revision 1.50.2.3  2006/12/14 18:21:16  kuba
# monitoring hook for job.submit()
#
# Revision 1.50.2.2  2006/11/24 14:31:39  amuraru
# implementation of peek() function
#
# Revision 1.50.2.1  2006/11/02 09:27:10  amuraru
# Fixed [bug #21225]
#
# Revision 1.50  2006/10/22 22:31:54  adim
# allow manual subjob submmission (relaxed the checks in Job.submit() changing the assertion with a warning)
#
# Revision 1.49  2006/10/19 12:34:18  adim
# *** empty log message ***
#
# Revision 1.48  2006/10/19 12:33:35  adim
# *** empty log message ***
#
# Revision 1.47  2006/10/19 12:05:32  adim
# allow manual subjob submmission (relaxed the checks in Job.submit() changing the assertion with a warning)
#
# Revision 1.46  2006/10/03 10:41:27  moscicki
# log user exceptions in job state transitions
#
# Revision 1.45  2006/10/02 14:48:49  moscicki
# make a difference between master and sub packed input sandbox in case there is not splitting (they were overriding each other)
#
# Revision 1.44  2006/09/19 09:40:33  adim
# Bug fix #17080
#
# Revision 1.43  2006/09/08 13:03:08  adim
# Fixed Bug#19434
# Added a rollback hook for the submitting->new transition (activated when
# submission fails) which cleans up the Input and Output workspaces.
#
# Revision 1.42  2006/08/29 12:54:02  moscicki
# trivial fix
#
# Revision 1.41  2006/08/29 12:03:26  moscicki
# - createInputSandbox()
# - fixes in resubmit()
#
# Revision 1.40  2006/08/24 16:58:26  moscicki
# added createPackedInputSandbox()
#
# Revision 1.39  2006/08/11 13:35:10  moscicki
# preliminary resubmit implementation
#
# Revision 1.38  2006/08/02 08:28:29  moscicki
# id of subjobs starts with 0 not 1
#
# Revision 1.37  2006/08/01 09:28:57  moscicki
# updated state list
#
# Revision 1.36  2006/07/31 12:15:43  moscicki
# updateMasterJobStatus() helper method for bulk subjob monitoring
# more transitions from submitted added
#
# Revision 1.35  2006/07/28 15:01:32  moscicki
# small bugfix
#
# Revision 1.34  2006/07/28 12:52:37  moscicki
# default self transitions enabled (s->s)
# improved FQID (removed broken caching)
# use FQID for logging messages
# comments
#
# Revision 1.33  2006/07/27 20:13:46  moscicki
#  - JobStatusError
#  - added simple job state machine
#  - status has "checkset" metaproperty
#  - updateStatus()
#  - postprocessing_hook()
#  - getInputWorkspace(), getOutputWorkspace()
#  - getFQID()
#  - changed subjob ids (short numbers)
#  - fixed commit (suboptimal)
#  - changes to exception handling
#
# Revision 1.32  2006/07/10 13:24:41  moscicki
# changes from Johannes: outputdata handling
#
# exception fixes...
#
# Revision 1.31  2006/06/13 12:34:29  moscicki
#   _category = "mergers" # plural -> following other category names
#
#  Job.merge(self, sum_outputdir = None , subjobs = None, **options)
#   -> see the docstring for more details (logfile is passed in options)
#
# Revision 1.30  2006/06/13 08:50:04  moscicki
# Added merger
#
# Revision 1.29  2006/06/09 14:31:51  moscicki
# exception fix
#
# Revision 1.28  2006/02/13 15:19:14  moscicki
# support for two-phase confguration (...,master config, splitting, sub config,...)
#
# Revision 1.27  2006/02/10 14:23:13  moscicki
# fixed: bug #14524 overview: jobs[ id ].remove() doesn't delete top level job directory
#
# Revision 1.26  2005/12/08 12:01:03  moscicki
# _init_workspace() method (this is a temporary name)
# inputdir/outputdir of subjobs now point to the real directories (TODO: which are still not in a correct place in the filesystem)
#
# Revision 1.25  2005/12/02 15:30:35  moscicki
# schema changes: master, subjobs, splitter properties
# splitting support
# customizable _repr() method
#
# Revision 1.24  2005/11/14 10:34:16  moscicki
# added running state, GUI prefs
#
#
# Revision 1.23  2005/10/21 13:17:56  moscicki
# bufix #12475 (killing job in a running state)
#
# Revision 1.22.2.1  2005/11/04 11:40:12  ctan
# *** empty log message ***
#
# Revision 1.22  2005/09/23 09:30:17  moscicki
# minor
#
# Revision 1.21  2005/09/19 10:57:31  asaroka
# Restriction on job name format is removed as redundant.
#
# Revision 1.20  2005/08/29 10:01:36  moscicki
# added docs
#
# Revision 1.19  2005/08/26 09:55:49  moscicki
# outputsandbox property, many comments added
#
# Revision 1.18  2005/08/24 15:33:50  moscicki
# added docstrings
#
# Revision 1.17  2005/08/23 17:09:27  moscicki
# minor changes
#
#
# nga.GPIDev.Lib.File.FileUtils
