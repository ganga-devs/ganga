# Import classes that should be in the Tasks namespace
from tasklist import TaskList
from abstractjob import AbstractJob
from task import Task
from mcjob import MCJob
from mctask import MCTask
from anajob import AnaJob
from anatask import AnaTask

# Start Logger
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
#logger.level = -10 

