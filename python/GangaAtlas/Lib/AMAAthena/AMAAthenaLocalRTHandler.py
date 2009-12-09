###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthenaLocalRTHandler.py,v 1.8 2009-03-11 19:56:45 hclee Exp $
###############################################################################
# AMAAthena Local Runtime Handler
#
# NIKHEF/ATLAS 

import os

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from GangaAtlas.Lib.Athena.AthenaLocalRTHandler import *
  
class AMAAthenaLocalRTHandler(AthenaLocalRTHandler):
    """AMAAthena Local Runtime Handler"""
    
    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object

        athena_jc = AthenaLocalRTHandler.prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig)

        exe = os.path.join(os.path.dirname(__file__), 'ama_athena-local.sh')
        inputbox  = athena_jc.inputbox
        outputbox = athena_jc.outputbox
        environment = athena_jc.env

        ## reset the ATHENA_MAX_EVENTS env. variable 
        max_events = -1
        if app.max_events:
            max_events = app.max_events
            if max_events.__class__.__name__ == 'int':
                max_events = str(max_events)
            environment['ATHENA_MAX_EVENTS'] = max_events
        
        outputbox += [ 'summary/*.root' ]

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)

    def master_prepare( self, app, appconfig ):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object

        if job.inputdata._name != 'StagerDataset':
            raise ApplicationConfigurationError(None,'j.inputdata should be StagerDataset')

        ## update the guid list if using DQ2 mode
        if not job.master:
            if job.inputdata.type in ['', 'DQ2']:
                job.inputdata.fill_guids()

        athena_jc = AthenaLocalRTHandler.master_prepare(self, app, appconfig)

        exe = os.path.join(os.path.dirname(__file__), 'ama_athena-local.sh')
        inputbox  = athena_jc.inputbox
        outputbox = athena_jc.outputbox
        environment = athena_jc.env

        ## add ama_athena-utility.sh into inputbox
        inputbox += [ File( os.path.join(os.path.dirname(__file__), 'ama_athena-utility.sh') ) ]

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)

allHandlers.add('AMAAthena', 'Local', AMAAthenaLocalRTHandler)
allHandlers.add('AMAAthena', 'LSF'  , AMAAthenaLocalRTHandler)
allHandlers.add('AMAAthena', 'PBS'  , AMAAthenaLocalRTHandler)
allHandlers.add('AMAAthena', 'SGE'  , AMAAthenaLocalRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configLCG = getConfig('LCG')
logger = getLogger('GangaAtlas')
