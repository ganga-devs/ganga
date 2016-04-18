from Ganga.GPIDev.Lib.Tasks.common import *
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 
import time
from Ganga.GPIDev.Lib.Tasks import ITask
from GangaAtlas.Lib.Tasks.AtlasTransform import AtlasTransform
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_lock, dq2
from dq2.common.DQException import DQException
from dq2.clientapi.DQ2 import DQ2, DQUnknownDatasetException, DQDatasetExistsException, DQFileExistsInDatasetException, DQInvalidRequestException
from dq2.container.exceptions import DQContainerAlreadyHasDataset, DQContainerDoesNotHaveDataset
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig
configDQ2 = getConfig('DQ2')

from Ganga.Utility.logging import getLogger
logger = getLogger()

########################################################################

class AtlasTask(ITask):
    """Atlas add-ons for the Task framework"""
    _schema = Schema(Version(1,0), dict(ITask._schema.datadict.items() + {
        }.items()))
    
    _category = 'tasks'
    _name = 'AtlasTask'
    _exportmethods = ITask._exportmethods + [ 'getContainerName', 'initializeFromDictionary', 'checkOutputContainers', 'checkInputDatasets' ]

    _tasktype = "ITask"
    
    default_registry = "tasks"
    
    def getContainerName(self, max_length = configDQ2['OUTPUTDATASET_NAMELENGTH'] - 2):
        if self.name == "":
            name = "task"
        else:
            name = self.name

        # check container name isn't too big            
        dsn = ["user",getNickname(),self.creation_date, name, "id_%i/" % self.id ]
        if len(".".join(dsn)) > max_length:
            dsn = ["user",getNickname(),self.creation_date, name[: - (len(".".join(dsn)) - max_length)], "id_%i/" % self.id ]

        return (".".join(dsn)).replace(":", "_").replace(" ", "").replace(",","_")

    def checkOutputContainers(self):
        """Go through all transforms and check all datasets are registered"""
        logger.info("Cleaning out overall Task container...")

        try:
            dslist = []
            dq2_lock.acquire()
            try:
                dslist = dq2.listDatasetsInContainer(self.getContainerName())
            except:
                dslist = []

            try:
                dq2.deleteDatasetsFromContainer(self.getContainerName(), dslist )

            except DQContainerDoesNotHaveDataset:
                  pass
            except Exception as x:
                logger.error("Problem cleaning out Task container: %s %s", x.__class__, x)
            except DQException as x:
                logger.error('DQ2 Problem cleaning out Task container: %s %s' %( x.__class__, x))
        finally:
            dq2_lock.release()

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

    def checkInputDatasets(self):
        """Check the distribution of the input datasets"""
        for trf in self.transforms:
            trf.checkInputDatasets()
