##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaLCGRTHandler.py,v 1.51 2009-07-23 20:12:25 elmsheus Exp $
###############################################################################
# TagPrepare LCG Runtime Handler
#
# ATLAS/ARDA

import os, pwd, commands, re, string, time, sys

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.Lib.LCG import LCGJobConfig
from GangaAtlas.Lib.AtlasLCGRequirements import AtlasLCGRequirements

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

class TagPrepareLCGRTHandler(IRuntimeHandler):
    """Athena LCG Runtime Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("TagPrepareLCGRTHandler prepare called, %s", job.id)

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
                    if not job.inputdata.names: raise ApplicationConfigurationError(None,'No inputdata has been specified. Failure in job %s.%s. Dataset %s' %(job._getRoot().id, job.id, job.inputdata.dataset)  )
                    input_guids = job.inputdata.guids
                    input_files = job.inputdata.names
                    job.inputdata.type = 'DQ2_COPY'

            else:
                if job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)

                elif job.inputdata._name == 'DQ2Dataset':
                    
                    job.inputdata.type = 'DQ2_COPY'
                    input_guids, input_files = _splitlist(job.inputdata.get_contents())
                    job.inputdata.names = input_files          
                    job.inputdata.guids = input_guids          

        if job.outputdata:
            raise ApplicationConfigurationError(None,'No outputdata required for TagPrepare job.')

        if job._getRoot().subjobs:
            jid = "%d.%d" % (job._getRoot().id, job.id)
        else:
            jid = "%d" % job.id

        if getConfig('LCG')['JobLogHandler'] == 'DQ2':
            raise ApplicationConfigurationError(None,'Staging of log files in DQ2 requested but not possible for TagPrepare.')
        
        # prepare inputsandbox
        inputbox = [File(os.path.join(__athdirectory__,'athena-utility.sh')),
                    File(os.path.join(__directory__,'get_tag_info.py'))]
        if input_guids:     _append_file_buffer(inputbox,'input_guids',input_guids)
        if input_files:     _append_file_buffer(inputbox,'input_files',input_files)

        exe = os.path.join(__directory__,'run-tagprepare-lcg.sh')
        outputbox = jobmasterconfig.outputbox
        requirements = jobmasterconfig.requirements.__copy__()
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
        lcg_config = LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], requirements)
        return lcg_config

    def master_prepare( self, app, appconfig):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object
        logger.debug('TagPrepareLCGRTHandler master_prepare called: %s', job.id )

        self.username = gridProxy.identity(safe=True)

        # Check if all sites are in the same cloud
        if job.backend.requirements.sites:
            firstCloud = whichCloud(job.backend.requirements.sites[0])
            for site in job.backend.requirements.sites:
                cloud = whichCloud(site)
                if cloud != firstCloud:
                    printout = 'Job submission failed ! Site specified with j.backend.requirements.sites=%s are not in the same cloud !' %(job.backend.requirements.sites)
                    raise ApplicationConfigurationError(None,printout )


        # prepare input sandbox
        inputbox = [ ( File(os.path.join(__athdirectory__,'athena-utility.sh')) ),
                     ( File(os.path.join(__directory__,'get_tag_info.py')))]
            
        # CN: added TNTJobSplitter clause  
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            _append_files(inputbox,os.path.join(__athdirectory__, 'ganga-stage-in-out-dq2.py'),
                          os.path.join(__athdirectory__, 'dq2_get'),
                          os.path.join(__athdirectory__, 'dq2info.tar.gz'))

        ## insert more scripts to inputsandbox for FileStager
        if job.inputdata and job.inputdata._name == 'DQ2Dataset' and job.inputdata.type in ['FILE_STAGER']:
            _append_files(inputbox,'make_filestager_joption.py','dm_util.py','fs-copy.py')
            #_append_files(inputbox,'make_filestager_joption.py','dm_util.py')

        #       add libDCache.so and libRFIO.so to fix broken access in athena 12.0.x
        if not 'ganga-stage-in-out-dq2.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, os.path.join(__athdirectory__, 'ganga-stage-in-out-dq2.py'))
        if not 'dq2tracerreport.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, os.path.join(__athdirectory__,'dq2tracerreport.py'))
        if not 'db_dq2localid.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, os.path.join(__athdirectory__, 'db_dq2localid.py'))
        if not 'getstats.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, os.path.join(__athdirectory__, 'getstats.py'))

        _append_files(inputbox,os.path.join(__athdirectory__, 'libdcap.so'))

        if job.inputsandbox: inputbox += job.inputsandbox
            
        # prepare environment
        environment={
            'MAXNUMREFS'     : str(app.max_num_refs),
            'STREAM_REF'     : app.stream_ref,
            'ATLAS_RELEASE'  : app.atlas_release,
            'ATHENA_OPTIONS' : '',
            'ATHENA_USERSETUPFILE' : '',
            'ATLAS_PROJECT' : '',
            'ATLAS_EXETYPE' : 'ATHENA',
            'GANGA_VERSION' : configSystem['GANGA_VERSION']
        }

        environment['DCACHE_RA_BUFFER'] = config['DCACHE_RA_BUFFER']
        requirements = AtlasLCGRequirements()
        
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.inputdata.dataset:
                datasetname = job.inputdata.dataset
                environment['DATASETNAME'] = ':'.join(datasetname)
                environment['DATASETLOCATION'] = ':'.join(job.inputdata.get_locations())
                environment['DQ2_URL_SERVER'] = configDQ2['DQ2_URL_SERVER']
                environment['DQ2_URL_SERVER_SSL'] = configDQ2['DQ2_URL_SERVER_SSL']
                environment['DATASETTYPE'] = job.inputdata.type
                if job.inputdata.failover:
                    environment['DATASETFAILOVER'] = 1
                environment['DATASETDATATYPE'] = job.inputdata.datatype
                if job.inputdata.accessprotocol:
                    environment['DQ2_LOCAL_PROTOCOL'] = job.inputdata.accessprotocol
                if job.inputdata.check_md5sum:
                    environment['GANGA_CHECKMD5SUM'] = 1
                    
            else:
                raise ApplicationConfigurationError(None,'j.inputdata.dataset is empty - DQ2 dataset name needs to be specified.')

            # Raise submission exception
            if (not job.backend.CE and 
                not (job.backend.requirements._name == 'AtlasLCGRequirements' and job.backend.requirements.sites) and
                not (job.splitter and job.splitter._name == 'DQ2JobSplitter') and
                not (job.splitter and job.splitter._name == 'TNTJobSplitter') and
                not (job.splitter and job.splitter._name == 'AnaTaskSplitterJob')):

                raise ApplicationConfigurationError(None,'Job submission failed ! Please use DQ2JobSplitter or specify j.backend.requirements.sites or j.backend.requirements.CE !')

            if job.inputdata.match_ce_all or job.inputdata.min_num_files>0:
                raise ApplicationConfigurationError(None,'Job submission failed ! Usage of j.inputdata.match_ce_all or min_num_files is obsolete ! Please use DQ2JobSplitter or specify j.backend.requirements.sites or j.backend.requirements.CE !')
            #if job.inputdata.number_of_files and (job.splitter and job.splitter._name == 'DQ2JobSplitter'):
            #    allLoc = job.inputdata.get_locations(complete=0)
            #    completeLoc = job.inputdata.get_locations(complete=1)
            #    incompleteLoc = []
            #    for loc in allLoc:
            #        if loc not in completeLoc:
            #            incompleteLoc.append(loc)
            #    if incompleteLoc:
            #        raise ApplicationConfigurationError(None,'Job submission failed ! Dataset is incomplete ! Usage of j.inputdata.number_of_files and DQ2JobSplitter is not allowed for incomplete datasets !')

        # prepare job requirements
        cmtconfig = app.atlas_cmtconfig
        if not cmtconfig in ['i686-slc4-gcc34-opt', 'i686-slc5-gcc43-opt']:
            cmtconfig = 'i686-slc4-gcc34-opt'

        requirements.software = ['VO-atlas-offline-%s-%s' %(app.atlas_release, cmtconfig )]

        #       add software requirement of dq2clients
        if job.inputdata and job.inputdata.type in [ 'DQ2_DOWNLOAD', 'TNT_DOWNLOAD', 'DQ2_COPY', 'FILE_STAGER']:
            try:
                # override the default one if the dq2client_version is presented 
                # in the job backend's requirements object
                dq2client_version = job.backend.requirements.dq2client_version
            except AttributeError:
                pass
            if dq2client_version:
                requirements.software += ['VO-atlas-dq2clients-%s' % dq2client_version]
                environment['DQ2_CLIENT_VERSION'] = dq2client_version

#       jobscript

        exe = os.path.join(__directory__,'run-tagprepare-lcg.sh')
        #exe = os.path.join(__directory__,'get_tag_info.py')

#       output sandbox
        outputbox = [
            'taginfo.pkl'
        ]


        if job.outputsandbox: outputbox += job.outputsandbox

        return LCGJobConfig(File(exe),inputbox,[],outputbox,environment,[],requirements) 

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

allHandlers.add('TagPrepare','LCG',TagPrepareLCGRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configSystem = getConfig('System')
logger = getLogger()
