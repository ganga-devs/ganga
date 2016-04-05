from __future__ import absolute_import
from Ganga.Core.GangaRepository import addRegistry
from Ganga.GPIDev.Lib.Tasks.TaskRegistry import TaskRegistry

# Tasks
from Ganga.GPIDev.Lib.Tasks.ITask import ITask
from Ganga.GPIDev.Lib.Tasks.ITransform import ITransform
from Ganga.GPIDev.Lib.Tasks.TaskChainInput import TaskChainInput
from Ganga.GPIDev.Lib.Tasks.TaskLocalCopy import TaskLocalCopy

from Ganga.GPIDev.Lib.Tasks.CoreTask import CoreTask
from Ganga.GPIDev.Lib.Tasks.CoreTransform import CoreTransform
from Ganga.GPIDev.Lib.Tasks.CoreUnit import CoreUnit

# Create the registry
myTaskRegistry = TaskRegistry("tasks", "Tasks Registry")
addRegistry(myTaskRegistry)


def stopTasks():
    global myTaskRegistry
    myTaskRegistry.stop()


