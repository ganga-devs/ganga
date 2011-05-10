from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.Lib.LCG import LCGJobConfig
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.Lib.Mergers.Merger import *

# ------------------------------------------------------
# Fluka RTHandler
class FlukaRTHandler(IRuntimeHandler):
    """Fluka RT Handler"""
    
    def master_prepare( self, app, appconfig ):
        """prepare the master job"""

        #print "Preparing LCG Fluka Master job..."
        import time

        # Set the output dataset name
        job = app._getParent()
        if job.outputdata.name == '':
            job.outputdata.name = 'users_fluka_seed' + str(app.seed) + '_trigs' + str(app.num_triggers) + '_' + time.strftime('%b_%a_%m_%d_%H_%M_%S')
        
    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        #print "Preparing LCG Fluka subjob"
        
        # set up the executable script
        exe = os.path.join(os.path.dirname(__file__),'run-fluka.sh')
        job = app._getParent()

        # sort out subjob outputdata
        if job._getRoot().subjobs:
            job.outputdata.data = job._getRoot().outputdata.data
            job.outputdata.name = job._getRoot().outputdata.name
        
        # set the environment
        environment = {}
        environment['NA48_CARD_FILE'] = os.path.basename( app.card_file.name )
        environment['NA48_SEED'] = app.seed
        environment['NA48_NUM_TRIGS'] = app.num_triggers
        
        environment['LFC_HOST'] = "prod-lfc-shared-central.cern.ch"
        environment['NA48_DATASET_NAME'] = job.outputdata.name
            
        # generate the config
        inputbox = [ app.card_file, File(os.path.join(os.path.dirname(__file__),'fluka_store_data.py')) ]
        outputbox = []
        lcg_config = LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], None)
        
        return lcg_config
        
allHandlers.add('Fluka','LCG',FlukaRTHandler)
