##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TagPrepareLocalRTHandler.py,v 1.51 2009-07-23 20:12:25 elmsheus Exp $
###############################################################################
# TagPrepare Local Runtime Handler
#
# ATLAS/ARDA

import os, pwd, commands, re, string, time, sys

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from GangaAtlas.Lib.ATLASDataset import ATLASLocalDataset

from GangaAtlas.Lib.ATLASDataset import ATLASDataset, isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE, getIncompleteLocations, whichCloud
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

__directory__ = os.path.dirname(__file__)
__athdirectory__ = sys.modules['GangaAtlas.Lib.Athena'].__path__[0]

def _append_file_buffer(inputbox,name,array):

    inputbox.append(FileBuffer(name,'\n'.join(array)+'\n'))

def _append_files(inputbox,*names):

    for name in names:
        inputbox.append(File(os.path.join(__directory__,name)))

class TagPrepareLocalRTHandler(IRuntimeHandler):
    """TagPrepare Local Runtime Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("TagPrepareLocalRTHandler prepare called, %s", job.id)

        # prepare inputdata
        input_files = []
        input_guids = []
       
        if job.inputdata:

            # check for subjobs
            if job._getRoot().subjobs:
                if job.inputdata._name == 'ATLASLocalDataset':
                    if not job.inputdata.names: raise ApplicationConfigurationError(None,'No inputdata has been specified.')
                    input_files = job.inputdata.names

                elif job.inputdata._name == 'DQ2Dataset':
                    raise ApplicationConfigurationError(None,'Cannot use DQ2Dataset with a local job'  )
            else:
                if job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)

                elif job.inputdata._name == 'DQ2Dataset':
                    raise ApplicationConfigurationError(None,'Cannot use DQ2Dataset with a local job'  )

        if job.outputdata:
            raise ApplicationConfigurationError(None,'No outputdata required for TagPrepare job.')

        if job._getRoot().subjobs:
            jid = "%d.%d" % (job._getRoot().id, job.id)
        else:
            jid = "%d" % job.id

        # prepare inputsandbox
        inputbox = [File(os.path.join(__athdirectory__,'athena-utility.sh')) ]
        if input_files:     _append_file_buffer(inputbox,'input_files',input_files)

        exe = os.path.join(__directory__,'run-tagprepare-local.sh')
        outputbox = jobmasterconfig.outputbox
        environment  = jobmasterconfig.env.copy()
        
        # If ArgSplitter is used
        try:
            if job.application.args:
                environment['ATHENA_OPTIONS'] = environment['ATHENA_OPTIONS'] + ' ' + ' '.join(job.application.args)
                if job.application.options:
                    job.application.options = job.application.options + ' ' + job.application.args
                else:
                    job.application.options = job.application.args
        except AttributeError:
            pass
        
        output_location = ''
        environment['OUTPUT_LOCATION'] = output_location
        environment['ATLASOutputDatasetLFC'] = config['ATLASOutputDatasetLFC']

        # Fix DATASETNAME env variable for DQ2_COPY mode
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.inputdata.dataset:
                from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import resolve_container
                datasets = resolve_container(job.inputdata.dataset)
                environment['DATASETNAME'] = datasets[0]
                try:
                    environment['DATASETLOCATION'] = ':'.join(job.inputdata.get_locations(overlap=False)[ datasets[0] ])
                except:
                    printout = 'Job submission failed ! Dataset %s could not be found in DQ2 ! Maybe retry ?' %(datasets[0])
                    raise ApplicationConfigurationError(None,printout )

        # Work around for glite WMS spaced environement variable problem
        inputbox.append(FileBuffer('athena_options',environment['ATHENA_OPTIONS']+'\n'))

        # append a property for monitoring to the jobconfig of subjobs
        lcg_config = StandardJobConfig(File(exe), inputbox, [], outputbox, environment)
        return lcg_config

    def master_prepare( self, app, appconfig):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object
        logger.debug('TagPrepareLCGRTHandler master_prepare called: %s', job.id )

        self.username = gridProxy.identity(safe=True)

        # prepare input sandbox
        if app.atlas_release == '':
            logger.warning('No Athena release specified - defaulting to 15.6.9')
            app.atlas_release = '15.6.9'
            
        logger.warning("Copying grid proxy to input sandbox for transfer to WN...")
        if (str(app.atlas_release[:3]) == '16.'):
            __tpdirectoryrel__ = os.path.join( __directory__, 'r16' )
        else:
            __tpdirectoryrel__ = os.path.join( __directory__, 'r15' )
                
        inputbox = [ ( File(os.path.join(__athdirectory__,'athena-utility.sh')) ),
                     ( File(os.path.join(__directory__,'get_tag_info.py')) ),
                     ( File(os.path.join(__directory__,'get_tag_info2.py')) ),
                     ( File(os.path.join(__directory__,'template.root')) ),
                     ( File(os.path.join(__tpdirectoryrel__,'libPOOLCollectionTools.so.cmtref'))),
                     ( File(os.path.join(__tpdirectoryrel__,'libPOOLCollectionTools.so'))),
                     ( File(os.path.join(__tpdirectoryrel__,'CollSplitByGUID.exe'))),
                     ( File(os.path.join(__tpdirectoryrel__,'CollCompressEventInfo.exe'))),
                     ( File(gridProxy.location())) ]
            
        ## insert more scripts to inputsandbox for FileStager
        if job.inputdata and job.inputdata._name == 'DQ2Dataset' and job.inputdata.type in ['FILE_STAGER']:
            _append_files(inputbox,'make_filestager_joption.py','dm_util.py','fs-copy.py')
            #_append_files(inputbox,'make_filestager_joption.py','dm_util.py')

        if job.inputsandbox: inputbox += job.inputsandbox
        
        #       prepare environment

        try:
            atlas_software = config['ATLAS_SOFTWARE']
        except ConfigError:
            raise ConfigError('No default location of ATLAS_SOFTWARE specified in the configuration.')

        # prepare environment
        environment={
            'MAXNUMREFS'     : str(app.max_num_refs),
            'STREAM_REF'     : app.stream_ref,
            'ATLAS_RELEASE'  : app.atlas_release,
            'ATHENA_OPTIONS' : '',
            'ATLAS_SOFTWARE' : atlas_software,
            'ATHENA_USERSETUPFILE' : '',
            'ATLAS_PROJECT' : '',
            'ATLAS_EXETYPE' : 'ATHENA',
            'GANGA_GLITE_UI': getConfig('LCG')['GLITE_SETUP'],
            'DQ2_SETUP'     : getConfig('defaults_DQ2SandboxCache')['setup'],
            'GANGA_VERSION' : configSystem['GANGA_VERSION'],
            'PROXY_NAME'    : os.path.basename(gridProxy.location()),
            'GANGA_OUTPUT_PATH' : job.outputdir
        }

        if app.lcg_prepare:
            environment['LCG_PREPARE'] = '1'
            
#       jobscript

        exe = os.path.join(__directory__,'run-tagprepare-local.sh')

#       output sandbox
        if app.lcg_prepare:        
            outputbox = [
                'taginfo.pkl', 'subcoll.tar.gz'
                ]
        else:
            outputbox = ['taginfo.pkl' ]


        if job.outputsandbox: outputbox += job.outputsandbox

        return StandardJobConfig(File(exe),inputbox,[],outputbox,environment) 

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

allHandlers.add('TagPrepare','Local',TagPrepareLocalRTHandler)
allHandlers.add('TagPrepare','PBS',TagPrepareLocalRTHandler)
allHandlers.add('TagPrepare','LSF',TagPrepareLocalRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configSystem = getConfig('System')
logger = getLogger()
