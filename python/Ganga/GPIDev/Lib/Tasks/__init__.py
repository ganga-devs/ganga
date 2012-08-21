# Import classes that should be in the Tasks namespace
## Import the list of tasks and the task and abstract job definition
#from TaskList import TaskList

from Ganga.Core.GangaRepository import addRegistry
from TaskRegistry import TaskRegistry
addRegistry(TaskRegistry("tasks", "Tasks Registry"))

## Tasks
from Task import Task
from Transform import Transform

from ITask import ITask
from ITransform import ITransform
from TaskChainInput import TaskChainInput

# Start Logger
#import Ganga.Utility.logging
#logger = Ganga.Utility.logging.getLogger()
from common import logger

from TaskApplication import ExecutableTask, ArgSplitterTask 
