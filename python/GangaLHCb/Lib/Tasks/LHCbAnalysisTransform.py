from Ganga import GPI
from Ganga.GPIDev.Lib.Tasks.common import status_colours, overview_colours, markup, fgcol, col
from Ganga.GPIDev.Lib.Job.Job import Job
from Ganga.GPIDev.Lib.Tasks.Transform import Transform
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Base.Proxy import getName
from Ganga.GPIDev.Base.Proxy import isType, stripProxy
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from LHCbTaskDummySplitter import LHCbTaskDummySplitter
from Ganga.Core import GangaException, GangaAttributeError
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
import Ganga.Utility.Config
from copy import deepcopy
import os
from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)
config = Ganga.Utility.Config.getConfig('Configuration')


partition_colours = {
    'ignored': "",
    'hold': fgcol("lgray"),
    'ready': fgcol("lgreen"),
    'running': fgcol("green"),
    'completed': fgcol("blue"),
    'attempted': fgcol("yellow"),
    'failed': fgcol("lred"),
    'bad': fgcol("red"),
    'unknown': fgcol("white"),
}

job_colours = {
    'new': col("black", "white"),
    'submitting': col("black", "orange"),
    'submitted': col("white", "orange"),
    'running': col("black", "green"),
    'completing': col("white", "green"),
    'completed': col("white", "blue"),
    'killed': col("white", "lred"),
    'failed': col("black", "lred"),
    'incomplete': col("red", "lcyan"),
    'unknown': col("white", "magenta")
}


class LHCbAnalysisTransform(Transform):

    """The LHCbAnalysisTransform class contains the setup needed for a standard job,
    such as application, backend etc. In addition one can define a BKQuery to attach
    to the transforms 'query' attribute. This enables the transform to be updated to
    account for new additions as well as removals of data files from the BKQuery dataset.

    Transforms are attached to an LHCbAnalysisTask object using the appendTransform()
    method of the task. All of the functionality of the Transform is designed to be used
    via a task so it is recomended that you attach it to one."""

    _schema = Transform._schema.inherit_copy()
    _schema.datadict['name'].defvalue = 'LHCbAnalysisTransform'
    _schema.datadict['query'] = ComponentItem(
        'query', defvalue=None, load_default=0, hidden=0, protected=0, optional=1, copyable=1, doc='Bookkeeping query object BKQuery()')
    _schema.datadict['splitter'] = ComponentItem(
        'splitters', defvalue=None, load_default=0, hidden=0, protected=0, optional=1, copyable=1, doc='optional splitter')
    _schema.datadict['merger'] = ComponentItem(
        'mergers', defvalue=None, load_default=0, hidden=0, protected=0, optional=1, copyable=1, doc='optional merger')
    _schema.datadict['removed_data'] = ComponentItem(
        'datasets', defvalue=None, optional=1, load_default=False, doc='removed dataset')
    _schema.datadict['toProcess_dataset'] = ComponentItem(
        'datasets', defvalue=None, hidden=1, optional=0, copyable=0, load_default=True, doc='dataset to process')
    _schema.datadict['task_id'] = SimpleItem(
        defvalue=-1, hidden=0, protected=1, optional=0, copyable=0, doc='index of the parent task', typelist=['int'])
    _schema.datadict['transform_id'] = SimpleItem(
        defvalue=-1, hidden=0, protected=1, optional=0, copyable=0, doc='index of this transform within parent task', typelist=['int'])
    _category = 'transforms'
    _name = 'LHCbAnalysisTransform'
    _exportmethods = Transform._exportmethods
    _exportmethods += ['update']

    # Special methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def __init__(self):
        super(LHCbAnalysisTransform, self).__init__()
        self.toProcess_dataset = LHCbDataset()
        self.removed_data = LHCbDataset()

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def __deepcopy__(self, memo=None):
        l = LHCbAnalysisTransform()
        l.application = deepcopy(self.application, memo)
        l.backend = deepcopy(self.backend, memo)
        l.splitter = deepcopy(self.splitter, memo)
        l.merger = deepcopy(self.merger, memo)
        l.query = deepcopy(self.query, memo)
        l.run_limit = deepcopy(self.run_limit, memo)
        l.inputsandbox = deepcopy(self.inputsandbox)
        l.outputsandbox = deepcopy(self.outputsandbox)
        if self.inputdata:
            l.inputdata = LHCbDataset()
            l.inputdata.files = self.inputdata.files[:]
        if self.outputdata:
            l.outputdata = OutputData()
            l.outputdata.files = self.outputdata.files[:]
        l.name = self.name
        return l

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _attribute_filter__set__(self, name, value):
        if name is 'inputdata':
            if self.query is not None:
                raise GangaAttributeError(
                    None, 'Cannot set the inputdata if a BKQuery object has already been given')
            else:
                logger.warning(
                    "User defined inputdata will be overwritten if one attaches a BKQuery object to the transform.query attribute")
                logger.warning(
                    "Running with only inputdata defined will not enable the task to be updated in line with the BK database giving essentially no more functionality than a Job")
        elif name is 'query':
            if not isType(value, BKQuery):
                raise GangaAttributeError(
                    None, 'LHCbTransform expects a BKQuery object for its query attribute!')
        return value

    # Public GPI methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def overview(self):
        """ Get an ascii art overview over task status. Can be overridden """
        o = markup("Transform %s: %s '%s'\n" % (self.transform_id, getName(self), self.name), status_colours[self.status])
        o += "------------------------------------------------------------\n"
        partitions = self._partition_status.keys()
        partitions.sort()
        for c in partitions:
            s = self._partition_status[c]
            if c in self.getPartitionApps():

                mj = self._getPartitionMasterJob(c)
                failure = 0
                if mj.status in ['submitting', 'submitted', 'running', 'completing', 'completed']:
                    failure = mj.info.submit_counter - 1
                else:
                    failure = mj.info.submit_counter

                o += markup("Partition %i (attached to job# %s, containing %i datafiles):%i" % (
                    c, mj.id, self._datafile_count(self.getPartitionJobs(c)), failure), partition_colours[s])
                o += '\n'
                p_jobs = [pj for pj in self.getPartitionJobs(c)]
                p_jobs.sort(key=lambda job: job.id)
                for j in p_jobs:
                    fails = 0
                    # if j.status in
                    # ['submitting','submitted','running','completing','completed']:
                    if j.status in ['submitted', 'running', 'completing', 'completed']:
                        fails = j.info.submit_counter - 1
                    else:
                        fails = j.info.submit_counter
                    o += markup("%i:%i" % (j.id, fails), job_colours[j.status])
                    o += " "
                    if (p_jobs.index(j) + 1) % 20 == 0:
                        o += "\n"
                o += "\n"
            else:
                o += markup("Partition %i" % c, overview_colours[s])
                o += "\n"
        logger.info(o)

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def run(self, check=True):
        """Start the transform running, thereby assigning all necessary jobs."""
        if self.task_id is -1 and self.transform_id is -1:
            logger.error(
                "Please attach this transform to a persistant LHCbAnalysisTask object before running using the appendTransform() method.")
            return
        if self.query is None and (self.inputdata is not None):
            logger.warning(
                "Running a transform without a BKQuery object attached negates the update feature which keeps is up to date with the bookkeeping database.")
            logger.warning(
                "This essentially equates a transform with a regular job.")
            self.toProcess_dataset.files += self.inputdata.files[:]
            self.setPartitionStatus(len(self._partition_status), 'ready')
        self._submitJobs(1)
        return super(LHCbAnalysisTransform, self).run(check)

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def update(self, resubmit=False):
        """Update the dataset information of the transforms. This will
        include any new data in the processing or re-run jobs that have data which
        has been removed."""
        if self.query is None:
            raise GangaException(
                None, 'Cannot call update() on an LHCbTransform without the query attribute set')
        if len(self.toProcess_dataset.files):
            raise GangaException(
                None, 'Cannot call update() on an LHCbTransform that has already been updated. There is outstanding data to process, try typing transform.run()')

        # Get the latest dataset
        logger.info('Retrieving latest bookkeeping information for transform %i:%i, please wait...' % (
            self.task_id, self.transform_id))
        latest_dataset = self.query.getDataset()
        self.toProcess_dataset.files = latest_dataset.files

        # Compare to previous inputdata, get new and removed
        logger.info('Checking for new and removed data, please wait...')
        dead_data = LHCbDataset()
        if self.inputdata is not None:
            # Get new files
            self.toProcess_dataset.files = latest_dataset.difference(
                self.inputdata).files
            # Get removed files
            dead_data.files += self.inputdata.difference(latest_dataset).files
            # If nothing to be updated then exit

        # Carry out actions as needed
        redo_jobs = self._getJobsWithRemovedData(dead_data)
        new_jobs = len(self.toProcess_dataset.files)
        if not new_jobs and not redo_jobs:
            logger.info('Transform %i:%i is already up to date' %
                        (self.task_id, self.transform_id))
            return

        if redo_jobs and not resubmit:
            logger.info('There are jobs with out-of-date datasets, some datafiles must '
                        'be removed. Updating will mean loss of existing output and mean that merged data '
                        'will change respectively. Due to the permenant nature of this request please recall '
                        'update with the True argument as update(True)')
            self.toProcess_dataset.files = []
            return

        if redo_jobs:
            self.removed_data.files += dead_data.files
            for j in redo_jobs:
                if j.status in ['submitting', 'submitted', 'running', 'completing']:
                    logger.warning(
                        'Job \'%s\' as it is still running but is marked for resubmission due to removed data. It will be killed first' % j.fqid)
                    j.kill()
                # for j in self._getJobsWithRemovedData(self.removed_data):
                logger.info(
                    'Resubmitting job \'%s\' as it\'s dataset is out of date.' % j.fqid)
                j.resubmit()

        if new_jobs:
            logger.info('Transform %i:%i updated, adding partition %i containing %i more file(s) for processing' % (
                self.task_id, self.transform_id, len(self._partition_status), len(self.toProcess_dataset.files)))
            self.setPartitionStatus(len(self._partition_status), 'ready')
            if self.status != 'new':
                # After the first time, when transform is running or complete,
                # calling update will submit the jobs thereby blocking the user
                # thread
                self._submitJobs(1)
        self.inputdata = LHCbDataset(latest_dataset.files)

    # Public methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Called as part of the tasks monitoring loop when task not in 'new'
    def checkStatus(self):
        """Update the partitions statuses and then process the status of the transform."""
        self.updatePartitions()
        status = set(self._partition_status.values())
        if 'running' in status:
            # Check here as well else have to wait until all partitions are
            # finished.
            self._resubmitAttemptedJobs()
            self.updateStatus('running')
        elif 'ready' in status:
            self.updateStatus('running')
        elif 'attempted' in status:
            self._resubmitAttemptedJobs()
            self.updateStatus('running')
        # elif 'failed' in status:
            # self.updateStatus('completed')
        else:
            if self.merger is not None:
                self._mergeTransformOutput()
            self.updateStatus('completed')

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def createNewJob(self, partition):
        """ Returns a new job initialized with the transforms application, backend and name """
        j = GPI.Job()
        stripProxy(j).backend = self.backend.clone()
        stripProxy(j).application = self.application.clone()
        stripProxy(j).application.tasks_id = "%i:%i" % (
            self.task_id, self.transform_id)
        stripProxy(j).application.id = self.getNewAppID(partition)
        if self.splitter is not None:
            stripProxy(j).splitter = LHCbTaskDummySplitter(self.splitter)
        # if self.merger is not None:
            # stripProxy(j).merger = self.merger
        j.inputdata = self.toProcess_dataset
        j.outputdata = self.outputdata
        j.inputsandbox = self.inputsandbox
        j.outputsandbox = self.outputsandbox
        j.name = "T%i Tr%i P%i" % (self.task_id, self.transform_id, partition)
        j.do_auto_resubmit = True
        self.toProcess_dataset.files = []
        return j

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # seems to only be called by the monitoring thread as part of submitJobs, leaving room for the user to
    # quit the session before everything is submitted. for large submits associated with
    # transform.run() and t.update() this is called now by the user thread via
    # self.submitJobs(1)
    def getJobsForPartitions(self, partitions):
        """Create Jobs for new partitions."""
        # need to fix this in future releases
        if len(partitions) > 1:
            logger.warning(
                'Dont know how to deal with multiple partition creation yet.')
            return []
        if not len(self.toProcess_dataset.files):
            # logger.warning('No dataset to attach to new job, this message could arise due to conflict with the monitoring thread in which case please ignore')
            return []

        j = self.createNewJob(partitions[0])
        return [j]

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Masking the submitJobs from the monitoring loop as it interferes with
    # long job submission/updating.
    def submitJobs(self, n):
        return 0

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def updatePartitions(self):
        """Convenient method for updating ALL partitions at once."""
        for p in self._partition_status:
            self.updatePartitionStatus(p)

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Sseems to be called at startup only!?, now calling it also in checkStatus to make
    # it part of the Tasks monitoring thread loop.
    def updatePartitionStatus(self, partition):
        """ Calculate the correct status of the given partition."""
        running_status = set(
            ['submitting', 'submitted', 'running', 'completing'])
        nonProcessing_states = set(['bad', 'ignored', 'unknown'])
        # If the partition has status, and is not in a fixed state...
        if partition in self._partition_status and (not self._partition_status[partition] in nonProcessing_states):

            # if we have no applications, we are in "ready" state
            if not partition in self.getPartitionApps():
                if self._partition_status[partition] != "hold":
                    self._partition_status[partition] = "ready"

            elif self._partition_status[partition] != "hold":
                status = set(
                    [pj.status for pj in self.getPartitionJobs(partition)])

                if status.intersection(running_status):
                    self._partition_status[partition] = "running"
                elif 'new' in status:
                    self._partition_status[partition] = 'ready'
                elif 'failed' in status:
                    mj = self._getPartitionMasterJob(partition)
                    if mj.status not in running_status:
                        failures = mj.info.submit_counter
                        if failures >= self.run_limit:
                            self._partition_status[partition] = "failed"
                        elif failures > 0:
                            if len(mj.subjobs):
                                # Catches the fact that master job submit
                                # counter doesnt increment when subjobs
                                # resubmitted.
                                stripProxy(mj).info.submit_counter += 1
                            self._partition_status[partition] = "attempted"
                else:
                    self._partition_status[partition] = "completed"

    # Private methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _datafile_count(self, job_reg_slice):
        """Count the number of datafiles for any given registry slice."""
        r = 0
        for j in job_reg_slice:
            r += len(j.inputdata.files)
        return r

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _getJobsWithRemovedData(self, lost_dataset):
        """Find the jobs associated with removed data files."""
        jobs = []
        if not len(lost_dataset.files):
            return jobs
        if self.task_id is -1:  # Needed for self.getPartitionJobs below
            logger.warning(
                'Transforms must be attached to tasks before checking for removed data, attach this transform using t=LHCbAnalysisTask();t.appendTransform(<thisTransform>)')
            return jobs

        running_status = set(
            ['submitting', 'submitted', 'running', 'completing'])
        rf = set([file.name for file in lost_dataset.files])
        for p in self._partition_status:
            for pj in self.getPartitionJobs(p):
                pjf = set([file.name for file in pj.inputdata.files])
                dead_datafiles = pjf.intersection(rf)
                for ddf in dead_datafiles:
                    if pj.status in running_status:
                        logger.info(
                            'running job %s from %s has an obsolete datafile(s), it will be killed and re-submitted' % (pj.fqid, pj.name))
                        # pj.kill()
                    del stripProxy(pj).inputdata.files[
                        pj.inputdata.getFileNames().index(ddf)]
                    jobs += [pj]
        return jobs

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _getPartitionMasterJob(self, partition):
        """Get the master job from any number of partition jobs."""
        partition_jobs = self.getPartitionJobs(
            partition)  # only call method once
        if not len(partition_jobs):
            raise GangaException(
                None, 'Cant get partition master job when NO jobs assigned to partition')
        elif len(partition_jobs) is 1:
            return partition_jobs[0]
        # Need registry access here might be better to get registry directly
        # as in prepared stuff, see Executable for example or even
        # tasksregistry.py!
        return GPI.jobs(partition_jobs[0].fqid.split('.')[0])

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def _mergeTransformOutput(self):
        """Merge the output from a transforms jobs."""
        outputdir = os.path.join(config['gangadir'], 'workspace', config['user'], config[
                                 'repositorytype'], 'Tasks', str(self.task_id), str(self.transform_id))
        try:
            if not os.path.exists(outputdir):
                os.makedirs(outputdir)
            self.merger.merge(self.getJobs(), outputdir)
        except Exception as x:
            logger.error(
                'There was a problem merging the output from all partitions.')

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Once partition finished, if in state 'partition_status' then resubmit
    # jobs in state 'job_status'. This works in conjunction with the auto_resubmit
    # which only resubmits subjobs while the master job is running IF they have not
    # been submitted more than n times and IF the ratio of failed to complete is betetr
    # than x. This will resubmit ALL failed jobs once the partition has
    # finished.
    def _resubmitAttemptedJobs(self):
        """Resubmit all 'failed' and 'killed' jobs within 'attempted' partitions."""
        partition_status = ['attempted']
        job_status = ['failed', 'killed']
        for p in (part for part, state in self._partition_status.iteritems() if state in partition_status):
            for j in (job for job in self.getPartitionJobs(p) if job.status in job_status):
                if j.info.submit_counter >= self.run_limit:
                    continue
                logger.warning('Resubmitting job %s from T%i Tr%i P%i' % (
                    j.fqid, self.task_id, self.transform_id, p))
                j.resubmit()

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Private version of submitJobs which can be called from this class
    # to achieve job submission.
    def _submitJobs(self, n):
        return super(LHCbAnalysisTransform, self).submitJobs(n)

# End of class LHCbAnalysisTransform
########################################################################
