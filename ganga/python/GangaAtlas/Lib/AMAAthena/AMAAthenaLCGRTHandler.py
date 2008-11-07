###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthenaLCGRTHandler.py,v 1.1 2008-09-02 12:50:45 hclee Exp $
###############################################################################
# AMAAthena LCG Runtime Handler
#
# NIKHEF/ATLAS 

import os

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
from Ganga.Utility.logging import getLogger
from GangaAtlas.Lib.Athena.AthenaLCGRTHandler import *
  
class AMAAthenaLCGRTHandler(AthenaLCGRTHandler):
    """AMAAthena LCG Runtime Handler"""
    
    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object

        athena_jc = AthenaLCGRTHandler.prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig)

        exe = os.path.join(os.path.dirname(__file__), 'ama_athena-lcg.sh')
        inputbox  = athena_jc.inputbox
        outputbox = athena_jc.outputbox
        environment  = athena_jc.env
        requirements = athena_jc.requirements

        ## reset the ATHENA_MAX_EVENTS env. variable 
        max_events = -1
        if app.max_events:
            max_events = app.max_events
            if max_events.__class__.__name__ == 'int':
                max_events = str(max_events)
            environment['ATHENA_MAX_EVENTS'] = max_events

        ## AMAAthena specific setup 
        conf_name = os.path.basename(app.driver_config.config_file.name)
        environment['AMA_DRIVER_CONF'] = conf_name

        sample_name = 'mySample'
        if job.name:
            sample_name = job.name

        environment['AMA_SAMPLE_NAME']=sample_name
        outputbox += [ 'summary/summary_%s_confFile_%s_nEvts_%s.root' % (sample_name, conf_name, str(max_events) ) ]

        if job.inputdata._name == 'StagerDataset':

            ## needs a valid dataset name 
            if not job.inputdata.dataset:
                raise ApplicationConfigurationError(None,'dataset name not specified in job.inputdata')
            ## StagerDataset support on LCG is under construction
            if job.backend._name in ['LCG', 'NG']:
                raise ApplicationConfigurationError(None,'StagerDataset support on the grid is not yet implemented. Please use DQ2Dataset instead.')

            grid_sample_file = os.path.join(job.inputdir,'grid_sample.list')
            job.inputdata.make_sample_file(sampleName=sample_name, filepath=grid_sample_file)
            inputbox += [ job.inputdata.grid_sample_file ]
            environment['AMA_WITH_STAGER']='1'

        elif job.inputdata._name == 'DQ2Dataset':
            pass

        else:
            raise ApplicationConfigurationError(None,'AMAAthena works only with StagerDataset and DQ2Dataset as job\'s inputdata')

        return LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], requirements)

    def master_prepare( self, app, appconfig ):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object

        ## always refill the master job's inputdata with the up-to-date GUID list
        ## N.B. job.master returns the job's master job if it has one, if not, the
        ##      job itself is the master job.
        if (not job.master) and (job.inputdata._name == 'StagerDataset'):
            job.inputdata.fill_guids()

        athena_jc = AthenaLCGRTHandler.master_prepare(self, app, appconfig)

        exe = os.path.join(os.path.dirname(__file__), 'ama_athena-lcg.sh')
        inputbox  = athena_jc.inputbox
        outputbox = athena_jc.outputbox
        environment = athena_jc.env
        requirements = athena_jc.requirements

        ## add athena-utility.sh into inputbox
        inputbox += [ File( os.path.join(os.path.dirname(__file__), 'athena-utility.sh') ) ]

        ## add AMADriver configuration files into inputbox 
        inputbox += [ app.driver_config.config_file ]
        inputbox += app.driver_config.include_file

        return LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], requirements)

allHandlers.add('AMAAthena', 'LCG', AMAAthenaLCGRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configLCG = getConfig('LCG')
logger = getLogger('GangaAtlas')
