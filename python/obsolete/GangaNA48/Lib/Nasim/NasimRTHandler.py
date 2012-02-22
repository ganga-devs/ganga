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
# Nasim RTHandler
class NasimRTHandler(IRuntimeHandler):
    """Nasim RT Handler"""
    
    def master_prepare( self, app, appconfig ):
        """prepare the master job"""

        #print "Preparing LCG NAsim Master job..."
        import time

        # Set the output dataset name
        job = app._getParent()
        if job.outputdata.name == '':
            job.outputdata.name = 'users_run' + str(app.run_number) + '_beam' + str(app.beam) + '_trigs' + str(app.num_triggers) + '_' + time.strftime('%b_%a_%m_%d_%H_%M_%S')
        
    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        #print "Preparing LCG NAsim subjob"
        
        # set up the executable script
        exe = os.path.join(os.path.dirname(__file__),'run-nasim.sh')
        job = app._getParent()

        # sort out subjob outputdata
        if job._getRoot().subjobs:
            job.outputdata.data = job._getRoot().outputdata.data
            job.outputdata.name = job._getRoot().outputdata.name
        
        # set the environment
        environment = {}
        environment['NA48_JOB_FILE'] = os.path.basename( app.job_file.name )
        environment['NA48_TITLES_FILE'] = os.path.basename( app.titles_file.name )
        environment['NA48_BEAM_TYPE'] = app.beam
        environment['NA48_SEED'] = app.seed
        environment['NA48_NUM_TRIGS'] = app.num_triggers
        environment['NA48_RUN_NUM'] = app.run_number
        
        environment['LFC_HOST'] = "prod-lfc-shared-central.cern.ch"
        environment['NA48_DATASET_NAME'] = job.outputdata.name
        environment['NA48_OUTPUT_FILES'] = ':'.join(job.outputdata.data)
            
        # generate the config
        inputbox = [ app.job_file, app.titles_file, File(os.path.join(os.path.dirname(__file__),'store_data.py')) ]
        outputbox = []
        lcg_config = LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], None)
        
        return lcg_config
        
allHandlers.add('Nasim','LCG',NasimRTHandler)
