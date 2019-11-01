from copy import deepcopy

from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.Core.exceptions import GangaException
from GangaCore.Core.exceptions import GangaAttributeError
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
import time
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
from GangaCore.GPIDev.Lib.Tasks import ITask
from GangaLHCb.Lib.Tasks.LHCbTransform import LHCbTransform
from GangaCore.Utility.logging import getLogger

logger = getLogger()

########################################################################


class LHCbTask(ITask):

    """LHCb add-ons for the Task framework"""
    _schema = Schema(Version(1, 0), dict(ITask._schema.datadict.items()))

    _category = 'tasks'
    _name = 'LHCbTask'
    _exportmethods = ITask._exportmethods + ['addQuery', 'updateQuery', 'removeUnusedData', 'cleanTask']

    _tasktype = "ITask"

    default_registry = "tasks"

    def cleanTask(self):
        """Delete unused data and then remove all unused jobs"""
        self.removeUnusedData()
        self.removeUnusedJobs()

    def removeUnusedData(self):
        """Remove any output data from orphaned jobs"""
        for trf in self.transforms:
            if hasattr(trf, "removeUnusedData"):
                trf.removeUnusedData()

    def addQuery(self, transform, bkQuery, associate=True):
        """Allows the user to add multiple transforms corresponding to the list of
        BKQuery type objects given in the second argument. The first argument
        is a transform object to use as the basis for the creation of further
        transforms."""
        if not isinstance(transform, LHCbTransform):
            raise GangaException(
                None, 'First argument must be an LHCbTransform object to use as the basis for establishing the new transforms')

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
            if len(transform.queries) != 0:  # If template has no query itself
                logger.info('Attaching query to transform')
                transform.addQuery(stripProxy(bk))
            else:  # Duplicate from template
                logger.info('Duplicating transform to add new query.')
                tr = deepcopy(transform)
                tr.addQuery(stripProxy(bk))
                self.appendTransform(tr)

    def appendTransform(self, transform):
        """Append a transform to this task. This method also performs an update() on
        the transform once successfully appended."""
        r = super(LHCbTask, self).appendTransform(transform)
        return r

    def updateQuery(self, resubmit=False):
        """Update the dataset information of all attached transforms. This will
        include any new data in the processing or re-run jobs that have data which
        has been removed."""
        # Tried to use multithreading, better to check the tasksregistry class
        # Also tried multiprocessing but bottleneck at server.
        for t in self.transforms:
            try:
                t.updateQuery(resubmit)
            except GangaException as e:
                logger.warning(e.__str__())
                continue

        # update the status of the Task in case we're started running again
        self.updateStatus()
