from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySliceProxy
from Ganga.GPIDev.Lib.Tasks import ITask

########################################################################                                                                                                                                                                     

class ND280Task(ITask):
    """T2K add-ons for the Task framework"""
    _schema = Schema(Version(1,0), dict(ITask._schema.datadict.items() + {
        }.items()))

    _category = 'tasks'
    _name = 'ND280Task'
    _exportmethods = ITask._exportmethods + [ ]

    _tasktype = "ITask"

    default_registry = "tasks"
