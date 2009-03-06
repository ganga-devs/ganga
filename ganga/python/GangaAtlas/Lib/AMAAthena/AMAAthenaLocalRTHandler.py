###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthenaLocalRTHandler.py,v 1.7 2009-02-23 20:35:27 hclee Exp $
###############################################################################
# AMAAthena Local Runtime Handler
#
# NIKHEF/ATLAS 

import os

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
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

        ## LCG UI setup
        if os.path.exists(configLCG['GLITE_SETUP']):
            environment['LCG_SETUP'] = configLCG['GLITE_SETUP']
        elif os.path.exists(configLCG['EDG_SETUP']): 
            environment['LCG_SETUP'] = configLCG['EDG_SETUP']
        else:
            pass

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

        environment['AMA_FLAG_LIST'] = app.driver_flags 
        environment['AMA_LOG_LEVEL'] = app.log_level 

        environment['AMA_SAMPLE_NAME']=sample_name
        #outputbox += [ 'summary/summary_%s_confFile_%s_nEvts_%s.root' % ( sample_name, conf_name, str(max_events) ) ]
        outputbox += [ 'summary/*.root' ]

        if job.inputdata._name == 'StagerDataset':
            if not job.inputdata.dataset:
                raise ApplicationConfigurationError(None,'dataset name not specified in job.inputdata')

            grid_sample_file = os.path.join(job.inputdir,'grid_sample.list')
            job.inputdata.make_sample_file(sampleName=sample_name, filepath=grid_sample_file)
            inputbox += [ job.inputdata.grid_sample_file ]
            environment['AMA_WITH_STAGER']='1'

        elif job.inputdata._name == 'DQ2Dataset':

            ## setting default SE host
            if configDQ2['DQ2_LOCAL_SITE_ID']:
                import StagerDataset
                sename = StagerDataset.get_srm_host(configDQ2['DQ2_LOCAL_SITE_ID'])
                environment['VO_ATLAS_DEFAULT_SE'] = sename

        else:
            raise ApplicationConfigurationError(None,'AMAAthena works only with StagerDataset and DQ2Dataset as job\'s inputdata')

        # always enable remove_proxy
        try:
            environment['X509CERTDIR']=os.environ['X509_CERT_DIR']
        except KeyError:
            environment['X509CERTDIR']=''
        
        try:
            proxy = os.environ['X509_USER_PROXY']
        except KeyError:
            proxy = '/tmp/x509up_u%s' % os.getuid()
      
        ## no need to copy remote proxy if running right on the local machine
        if job.backend._name not in ['Local']: 
            REMOTE_PROXY = '%s:%s' % (socket.getfqdn(),proxy)
            environment['REMOTE_PROXY'] = REMOTE_PROXY

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)

    def master_prepare( self, app, appconfig ):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object

        ## always refill the master job's inputdata with the up-to-date GUID list
        ## N.B. job.master returns the job's master job if it has one, if not, the
        ##      job itself is the master job.
        if (not job.master) and (job.inputdata._name == 'StagerDataset'):
            if job.inputdata.type in ['LOCAL']:
                ## here we don't need DQ2 client on the WN
                pass
            else: 
                ## here we need DQ2 client on the WN
                job.inputdata.fill_guids()

        athena_jc = AthenaLocalRTHandler.master_prepare(self, app, appconfig)

        exe = os.path.join(os.path.dirname(__file__), 'ama_athena-local.sh')
        inputbox  = athena_jc.inputbox
        outputbox = athena_jc.outputbox
        environment = athena_jc.env

        ## add ama_athena-utility.sh into inputbox
        inputbox += [ File( os.path.join(os.path.dirname(__file__), 'ama_athena-utility.sh') ) ]

        ## add AMADriver configuration files into inputbox 
        inputbox += [ app.driver_config.config_file ]
        inputbox += app.driver_config.include_file

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)

allHandlers.add('AMAAthena', 'Local', AMAAthenaLocalRTHandler)
allHandlers.add('AMAAthena', 'LSF'  , AMAAthenaLocalRTHandler)
allHandlers.add('AMAAthena', 'PBS'  , AMAAthenaLocalRTHandler)
allHandlers.add('AMAAthena', 'SGE'  , AMAAthenaLocalRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configLCG = getConfig('LCG')
logger = getLogger('GangaAtlas')
