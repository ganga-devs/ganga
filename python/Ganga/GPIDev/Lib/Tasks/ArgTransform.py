from __future__ import absolute_import
from .Transform import Transform
from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Schema import Schema
from Ganga.GPIDev.Schema import SimpleItem
from Ganga.GPIDev.Schema import Version
from Ganga.Utility.util import containsGangaObjects
from .TaskApplication import ArgSplitterTask


class ArgTransform(Transform):
    _schema = Schema(Version(1, 0), dict(Transform._schema.datadict.items() + {
        'args': SimpleItem(defvalue=[], typelist=['list', 'Ganga.GPIDev.Lib.GangaList.GangaList.GangaList'], sequence=1, doc='A list of lists of arguments to pass to script'),
    }.items()))
    _category = 'transforms'
    _name = 'ArgTransform'
    _exportmethods = Transform._exportmethods

    def check(self):
        nargs = len(self.args)
        self.setPartitionsStatus(range(1, nargs + 1), "ready")
        if "_partition_status" in self.getNodeData():
            self.setPartitionsLimit(nargs + 1)

    def getJobsForPartitions(self, partitions):
        """Create Ganga Jobs for the next N partitions that are ready and submit them."""
        j = self.createNewJob(partitions[0])
        if len(partitions) > 1:
            j.splitter = ArgSplitterTask()
            j.splitter.args = [self.args[p - 1] for p in partitions]
            j.splitter.task_partitions = partitions
        else:
            p = partitions[0]
            if (p < 1 or p > len(self.args)):
                raise ApplicationConfigurationError(
                    "Partition %i did not find a corresponding argment!", p)
            j.application.args = self.args[p - 1]
        return [j]
