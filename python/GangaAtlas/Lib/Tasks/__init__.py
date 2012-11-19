# Import classes that should be in the Tasks namespace
## Import the list of tasks and the task and abstract job definition

## Tasks
from MCTask import MCTask
from AnaTask import AnaTask
from MultiTask import MultiTask

## Transforms
from MCTransforms import EvgenTransform, SimulTransform, ReconTransform
from AnaTransform import AnaTransform
from MultiTransform import MultiTransform
from AtlasTask import AtlasTask
from AtlasTransform import AtlasTransform
from AtlasUnit import AtlasUnit

# Applications
from TaskApplication import AthenaTask, AthenaMCTask, AthenaMCTaskSplitterJob, AnaTaskSplitterJob

# Start Logger
#import Ganga.Utility.logging
#logger = Ganga.Utility.logging.getLogger()
from Ganga.GPIDev.Lib.Tasks.common import logger
