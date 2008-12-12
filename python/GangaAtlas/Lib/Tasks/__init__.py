# Import classes that should be in the Tasks namespace

## Import the list of tasks and the task and abstract job definition
from TaskList import TaskList

## Tasks
from Task import Task
from MCTask import MCTask
from AnaTask import AnaTask

## Transforms
from Transform import Transform
from MCTransforms import EvgenTransform, SimulTransform, ReconTransform
from AnaTransform import AnaTransform

# Applications
from TaskApplication import ExecutableTask, AthenaMCTask, AthenaMCTaskSplitterJob, AthenaTask

# Start Logger
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

# Make job removals report to the application
# They do not do this without this workaround
# this is probably a Ganga Bug
def _job_remove(self, force=False):
   if self.subjobs:
      for j in self.subjobs:
         j.application.transition_update("removed")
   else:
      self.application.transition_update("removed")
   self._old_remove(force)

from Ganga.GPIDev.Lib.Job.Job import Job
Job._old_remove =  Job.remove
Job.remove = _job_remove
