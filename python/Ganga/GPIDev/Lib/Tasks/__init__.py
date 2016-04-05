from __future__ import absolute_import
from Ganga.Core.GangaRepository import addRegistry
from .TaskRegistry import TaskRegistry

myTaskRegistry = TaskRegistry("tasks", "Tasks Registry")

addRegistry(myTaskRegistry)

def stopTasks():
    global myTaskRegistry
    myTaskRegistry.stop()

# Tasks
from .ITask import ITask
from .ITransform import ITransform
from .TaskChainInput import TaskChainInput
from .TaskLocalCopy import TaskLocalCopy

from .CoreTask import CoreTask
from .CoreTransform import CoreTransform
from .CoreUnit import CoreUnit

# Start Logger
from .common import logger

