from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 
import time
from Ganga.GPIDev.Lib.Tasks import ITask

########################################################################

class AtlasTask(ITask):
    """Atlas add-ons for the Task framework"""
    _schema = Schema(Version(1,0), dict(ITask._schema.datadict.items() + {
        }.items()))
    
    _category = 'tasks'
    _name = 'AtlasTask'
    _exportmethods = ITask._exportmethods + [ 'getContainerName' ]

    _tasktype = "ITask"
    
    default_registry = "tasks"
    
    def getContainerName(self):
        if self.name == "":
            name = "task"
        else:
            name = self.name
            
        name_base = ["user",getNickname(),self.creation_date, name, "id_%i" % self.id ]            
        return (".".join(name_base) + "/").replace(" ", "_")
