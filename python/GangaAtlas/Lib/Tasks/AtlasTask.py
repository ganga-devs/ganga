from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 
import time
from Ganga.GPIDev.Lib.Tasks import ITask
from GangaAtlas.Lib.Tasks.AtlasTransform import AtlasTransform

########################################################################

class AtlasTask(ITask):
    """Atlas add-ons for the Task framework"""
    _schema = Schema(Version(1,0), dict(ITask._schema.datadict.items() + {
        }.items()))
    
    _category = 'tasks'
    _name = 'AtlasTask'
    _exportmethods = ITask._exportmethods + [ 'getContainerName', 'initializeFromDictionary', 'checkOutputContainers' ]

    _tasktype = "ITask"
    
    default_registry = "tasks"
    
    def getContainerName(self):
        if self.name == "":
            name = "task"
        else:
            name = self.name
            
        name_base = ["user",getNickname(),self.creation_date, name, "id_%i" % self.id ]            
        return (".".join(name_base) + "/").replace(" ", "_")

    def checkOutputContainers(self):
        """Go through all transforms and check all datasets are registered"""
        logger.info("Checking output data has been registered. This can take a few minutes...")
        for trf in self.transforms:
            logger.info("Checking containers in Tranform %d..." % trf.getID() )
            trf.checkOutputContainers()

    def initializeFromDictionary(self, ds_dict, template = None, files_per_job = -1, MB_per_job = -1, subjobs_per_unit = -1,
                                 application = None, backend = None):
        """Initialize transforms from a dictionary of containers"""
        if self.status != "new":
            logger.error("Cannot add more data to a new task yet. Give me time :)")
            return
        
        for trf_name, physics_container in ds_dict.iteritems():
            trf = AtlasTransform()
            trf.name = trf_name
            self.appendTransform(trf)
            if files_per_job > 0:
                trf.files_per_job = files_per_job
            if MB_per_job > 0:
                trf.MB_per_job = MB_per_job
            if subjobs_per_unit > 0:
                trf.subjobs_per_unit = subjobs_per_unit
            trf.application = application._impl.clone()
            trf.backend = backend._impl.clone()
            trf.initializeFromContainer(physics_container, template)
