from Ganga.GPIDev.Lib.Tasks import ITask
from Ganga.GPIDev.Schema import Schema, Version

########################################################################


class CoreTask(ITask):

    """General non-experimentally specific Task"""
    _schema = Schema(Version(1, 0), dict(list(ITask._schema.datadict.items()) + list({
    }.items())))

    _category = 'tasks'
    _name = 'CoreTask'
    _exportmethods = ITask._exportmethods + []

    _tasktype = "ITask"

    default_registry = "tasks"
