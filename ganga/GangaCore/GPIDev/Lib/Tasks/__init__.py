
from GangaCore.Core.GangaRepository import addRegistry
from GangaCore.GPIDev.Lib.Tasks.TaskRegistry import TaskRegistry

# Tasks
from GangaCore.GPIDev.Lib.Tasks.ITask import ITask
from GangaCore.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaCore.GPIDev.Lib.Tasks.TaskChainInput import TaskChainInput
from GangaCore.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy

from GangaCore.GPIDev.Lib.Tasks.CoreTask import CoreTask
from GangaCore.GPIDev.Lib.Tasks.CoreTransform import CoreTransform
from GangaCore.GPIDev.Lib.Tasks.CoreUnit import CoreUnit

# Create the registry
myTaskRegistry = TaskRegistry("tasks", "Tasks Registry")
addRegistry(myTaskRegistry)


def stopTasks():
    global myTaskRegistry
    myTaskRegistry.stop()


