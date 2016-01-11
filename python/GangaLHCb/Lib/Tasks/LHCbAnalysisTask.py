from copy import deepcopy

from LHCbAnalysisTransform import LHCbAnalysisTransform
from LHCbAnalysisTransform import job_colours
from LHCbAnalysisTransform import markup
from LHCbAnalysisTransform import partition_colours
from Ganga.GPIDev.Lib.Tasks.Task import Task
from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.Core.exceptions import GangaException
from Ganga.Core.exceptions import GangaAttributeError
from Ganga.Utility.logging import getLogger
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
logger = getLogger(modulename=True)

# to do...
# 1) Multi threading, even with multi threads, bottle neck at the server while updating.
# 2) Check prepared works(looks like does because of application.clone())
# 3) Dirac bulk submit
# 4) Sort out job resubmission on failure - done
# 5) Mergers - done
# 6) submit_counter for master job = 1 always unless whole job resubmitted. - done
# 7) Partition updating from completed by manual resubmit - done


class LHCbAnalysisTask(Task):

    """LHCbAnalysisTask - Top level class that looks after groups of LHCbAnalysisTransforms.

    The LHCbAnalysisTask class looks after the running of LHCb Analysis jobs, including helping to keep
    them up to date when new data is added or removed.

    Create a task using the following syntax

    In[1]: t = LHCbAnalysisTask()

    Append a transform to the task as below:

    In[2]: tr = LHCbAnalysisTransform()
    In[3]: t.appendTransform(tr)

    Or use an existing transform as a template to clone many new transforms, each with their
    own bookkeeping query (BKQuery) object.

    In[4]: t.addQuery(tr,[BKQuery('<dataset1>'), BKQuery(<dataset2>)])

    To set task running, do:

    In[5]: t.run()

    Can view the tasks registry by using either of the following

    In[6]: tasks          # less verbose
    In[7]: tasks.table()  # more verbose

    A Pretty, operational view of all the partition and subjobs within a task can be obtained with.

    In[8]: t.overview()

    Finally can update all the transforms within a task with.

    In[9]: t.update()"""

    _schema = Task._schema.inherit_copy()
    _schema.datadict['name'].defvalue = 'LHCbAnalysisTask'
    _category = 'tasks'
    _name = 'LHCbAnalysisTask'
    _exportmethods = Task._exportmethods
    _exportmethods += ['addQuery', 'update']

    default_registry = "tasks"

    # Public GPI methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def addQuery(self, transform, bkQuery, associate=True):
        """Allows the user to add multiple transforms corresponding to the list of
        BKQuery type objects given in the second argument. The first argument
        is a transform object to use as the basis for the creation of further
        transforms."""
        if not isType(transform, LHCbAnalysisTransform):
            raise GangaException(
                None, 'First argument must be an LHCbAnalysisTransform objects to use as the basis for establishing the new transforms')

        # Check if the template transform is associated with the Task
        try:
            self.transforms.index(transform)
        except:
            if associate:
                logger.info(
                    'The transform is not associated with this Task, doing so now.')
                self.appendTransform(transform)

        # Check if the BKQuery input is correct and append/update
        if type(bkQuery) is not list:
            bkQuery = [bkQuery]
        for bk in bkQuery:
            if not isType(bk, BKQuery):
                raise GangaAttributeError(
                    None, 'LHCbTransform expects a BKQuery object or list of BKQuery objects passed to the addQuery method')
            if transform.query is None:  # If template has no query itself
                logger.info('Attaching query to transform')
                transform.query = stripProxy(bk)
                transform.update()
            else:  # Duplicate from template
                logger.info('Duplicating transform to add new query.')
                tr = deepcopy(transform)
                tr.query = stripProxy(bk)
                self.appendTransform(tr)

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def appendTransform(self, transform):
        """Append a transform to this task. This method also performs an update() on
        the transform once successfully appended."""
        r = super(LHCbAnalysisTask, self).appendTransform(transform)
        if hasattr(transform, 'task_id'):
            stripProxy(transform).task_id = self.id
        else:
            raise GangaException(None, 'Couldnt set the task id')
        if hasattr(transform, 'transform_id'):
            try:
                stripProxy(transform).transform_id = self.transforms.index(transform)
            except:
                raise GangaException(
                    None, 'transform not added to task properly')
        else:
            raise GangaException(None, 'Coundnt set the transform id')

        # User may wish to append a transform and then update the whole task
        # after appending a query later.
        if transform.query is not None:
            transform.update()
        self.updateStatus()
        return r

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def help(self):
        """Brief description of the LHCbAnalysisTask object."""
        logger.info(
            "This is an LHCbTask, Which simplifies the query driven analysis of data")

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def overview(self):
        """ Get an ascii art overview over task status."""
        logger.info("Partition Colours: " + ", ".join([markup(key, partition_colours[key])
                                                       for key in ["hold", "ready", "running", "completed", "attempted", "failed", "bad", "unknown"]]))
        logger.info("Job Colours: " + ", ".join([markup(job, job_colours[job])
                                                 for job in ["new", "submitting", "submitted", "running", "completing", "completed", "failed", "killed", "incomplete", "unknown"]]))
        logger.info(
            "Lists the transforms, their partitions and partition subjobs, as well as the number of failures.")
        logger.info("Format: (partition/subjob number)[:(number of failures)]")
        logger.info('')
        for t in self.transforms:
            t.overview()

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    def update(self, resubmit=False):
        """Update the dataset information of all attached transforms. This will
        include any new data in the processing or re-run jobs that have data which
        has been removed."""
        # Tried to use multithreading, better to check the tasksregistry class
        # Also tried multiprocessing but bottleneck at server.
        for t in self.transforms:
            try:
                t.update(resubmit)
            except GangaException as e:
                logger.warning(e.__str__())
                continue

    # Public methods
    #####################################################################

    #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Called as part of the monitoring loop, which doesn't call this
    # while task in 'new'
    def updateStatus(self):
        """Check and update the status of each attached transform and then update
        own status."""
        if self.status is not 'new':  # only want to start this when user runs the task else jobs submitted when appending
            for t in self.transforms:  # could thread this
                t.checkStatus()

        # The overridden method will update the tasks status based on the
        # transforms status
        return super(LHCbAnalysisTask, self).updateStatus()

# End of class LHCbAnalysisTask
########################################################################
