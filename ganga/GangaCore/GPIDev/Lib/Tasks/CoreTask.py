from GangaCore.GPIDev.Lib.Tasks import ITask
from GangaCore.GPIDev.Schema import Schema, Version

########################################################################


class CoreTask(ITask):

    """General non-experimentally specific Task"""
    _schema = Schema(Version(1, 0), dict(ITask._schema.datadict.items()))

    _category = 'tasks'
    _name = 'CoreTask'
    _exportmethods = ITask._exportmethods + []

    _tasktype = "ITask"

    default_registry = "tasks"
