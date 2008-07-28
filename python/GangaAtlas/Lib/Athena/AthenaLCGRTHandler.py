##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaLCGRTHandler.py,v 1.2 2008-07-28 14:27:34 elmsheus Exp $
###############################################################################
# Athena LCG Runtime Handler
#
# ATLAS/ARDA

import os, pwd, commands, re, string, time

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.Lib.LCG import LCGJobConfig
from GangaAtlas.Lib.AtlasLCGRequirements import AtlasLCGRequirements

from GangaAtlas.Lib.ATLASDataset import ATLASDataset, isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE, getIncompleteLocations
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.GPIDev.Credentials import GridProxy

# the config file may have a section
# aboout monitoring

mc = getConfig('MonitoringServices')

# None by default
mc.addOption('Athena/LCG', None, 'FIXME')
mc.addOption('Athena', None, 'FIXME')

__directory__ = os.path.dirname(__file__)

def _append_file_buffer(inputbox,name,array):

    inputbox.append(FileBuffer(name,'\n'.join(array)+'\n'))

def _append_files(inputbox,*names):

    for name in names:
        inputbox.append(File(os.path.join(__directory__,name)))

def _splitlist(list):

    a_list = []
    b_list = []
    for a, b in list:
        a_list.append(a)
        b_list.append(b)

    return a_list, b_list
 
class AthenaLCGRTHandler(IRuntimeHandler):
    """Athena LCG Runtime Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("AthenaLCGRTHandler prepare called, %s", job.id)

#       prepare inputdata

        input_files = []
        input_guids = []
        input_tag_files = []
        input_tag_guids = []
        input_esd_files = []
        input_esd_guids = []
       
        if job.inputdata:

            # DQ2Dataset, ATLASLocalDataset and ATLASCastorDataset job splitting is done in AthenaSplitterJob

            if job._getRoot().subjobs:
                if job.inputdata._name == 'ATLASLocalDataset' or job.inputdata._name == 'ATLASCastorDataset':
                    if not job.inputdata.names: raise ApplicationConfigurationError(None,'No inputdata has been specified.')
                    input_files = job.inputdata.names

                elif job.inputdata._name == 'ATLASDataset':
                    if not job.inputdata.lfn: raise ApplicationConfigurationError(None,'No inputdata has been specified.') 
                    input_files = job.inputdata.lfn

                elif job.inputdata._name == 'DQ2Dataset':
                    if not job.inputdata.names: raise ApplicationConfigurationError(None,'No inputdata has been specified.')
                    input_guids = job.inputdata.guids
                    input_files = job.inputdata.names
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD', 'DQ2_COPY' ]:
                        job.inputdata.type ='DQ2_LOCAL'
                    if not job.inputdata.datatype in ['DATA', 'MC', 'MuonCalibStream']:
                        job.inputdata.datatype ='MC'

            else:
                if job.inputdata._name == 'ATLASCastorDataset':
                    input_files = ATLASCastorDataset.get_filenames(app)

                elif job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)

                elif job.inputdata._name == 'ATLASDataset':
                    input_files = ATLASDataset.get_filenames(app)

                elif job.inputdata._name == 'DQ2Dataset':
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD', 'DQ2_COPY' ]:
                        job.inputdata.type ='DQ2_LOCAL'
                    if not job.inputdata.datatype in ['DATA', 'MC', 'MuonCalibStream']:
                        job.inputdata.datatype ='MC'

                    input_guids, input_files = _splitlist(job.inputdata.get_contents())

                    if job.inputdata.tagdataset:
                        input_tag_guids, input_tag_files = _splitlist(job.inputdata.get_tag_contents())
                    if job.inputdata.use_aodesd_backnav:
                        input_esd_guids, input_esd_files = _splitlist(job.inputdata.get_contents(backnav=True))

                    job.inputdata.names = input_files 	 
                    job.inputdata.guids = input_guids 	 

#       prepare outputdata
       
        output_location = ''
        if job.outputdata:

            if job.outputdata._name=='DQ2OutputDataset':

                if job.outputdata.location:
                    if isDQ2SRMSite(job.outputdata.location):
                        output_location = job.outputdata.location
                    else:
                        logger.warning('Unknown output location %s.',job.outputdata.location)
                elif job._getRoot().subjobs and job._getRoot().outputdata.location:
                    if isDQ2SRMSite(job._getRoot().outputdata.location):
                        output_location = job._getRoot().outputdata.location
                    else:
                        logger.warning('Unknown output location %s.',job.getRoot().outputdata.location)
                        
                logger.debug('Output: %s,%s',output_location, job.outputdata.location)
            else:
                if job.outputdata.location:
                    output_location = job.outputdata.location
                else:
                    try:
                        output_location = config['LCGOutputLocation']
                    except ConfigError:
                        logger.warning('No default output location specified in the configuration.')
            if job.outputdata.location:
                job.outputdata.location = output_location 
                logger.debug('Output: %s,%s',output_location, job.outputdata.location)

        if job._getRoot().subjobs:
            jid = "%d.%d" % (job._getRoot().id, job.id)
        else:
            jid = "%d" % job.id

        if output_location and job.outputdata and job.outputdata._name!='DQ2OutputDataset':
            output_location = os.path.join(output_location, jid)
            if job.outputdata:
                # Remove trailing number if job is copied

                pat = re.compile(r'\/[\d\.]+\/[\d\.]+$')
                if re.findall(pat,output_location):
                    output_location = re.sub(pat, '', output_location)
                    output_location = os.path.join(output_location, jid)

                job.outputdata.location = output_location

        if job.outputdata and job.outputdata._name=='DQ2OutputDataset':
            if job._getRoot().subjobs:
                jobid = "%d" % (job._getRoot().id)
            else:
                jobid = "%d" % job.id

            # Extract username from certificate
            proxy = GridProxy()
            username = proxy.identity()

            jobdate = time.strftime('%Y%m%d')

            if job.outputdata.datasetname:
                # new datasetname during job resubmission
                pat = re.compile(r'^users\.%s\.ganga' % username)
                if re.findall(pat,job.outputdata.datasetname):
                    if job.outputdata.dataset_exists():
                        output_datasetname = job.outputdata.datasetname
                    else:
                        output_datasetname = 'users.%s.ganga.%s.%s' % ( username, jobid, jobdate)
                        
                    #output_lfn = 'users/%s/ganga/%s/' % (username,jobid)
                    #output_lfn = 'users/%s/ganga/' % (username)
                    output_lfn = 'users/%s/ganga/%s/' % (username,output_datasetname)
                else:
                    # append user datasetname for new configuration
                    if job.outputdata.use_datasetname and job.outputdata.datasetname:
                        output_datasetname = job.outputdata.datasetname
                    else:
                        output_datasetname = 'users.%s.ganga.%s' % (username,job.outputdata.datasetname)

                    #output_lfn = 'users/%s/ganga/%s/' % (username,job.outputdata.datasetname)
                    #output_lfn = 'users/%s/ganga/' % (username)
                    output_lfn = 'users/%s/ganga/%s/' % (username,output_datasetname)
            else:
                # No datasetname is given
                output_datasetname = 'users.%s.ganga.%s.%s' % (username,jobid, jobdate)
                #output_lfn = 'users/%s/ganga/%s/' % (username,jobid)
                #output_lfn = 'users/%s/ganga/' % (username)
                output_lfn = 'users/%s/ganga/%s/' % (username,output_datasetname)
            output_jobid = jid
            job.outputdata.datasetname=output_datasetname
            if not job.outputdata.dataset_exists(output_datasetname):
                if job._getRoot().subjobs:
                    if job.id==0:
                        job.outputdata.create_dataset(output_datasetname)
                else:
                    job.outputdata.create_dataset(output_datasetname)
            else:
                if (job._getRoot().subjobs and job.id==0) or not job._getRoot().subjobs:
                    logger.warning("Dataset %s already exists - appending new files to this dataset", output_datasetname)
                    output_location = job.outputdata.get_locations(datasetname=output_datasetname, quiet=True)
                    logger.debug('Output3: %s,%s',output_location, job.outputdata.location)
                    if output_location:
                        output_location = output_location[0] 
                        if job._getRoot().subjobs:
                            job._getRoot().outputdata.location=output_location
                            job.outputdata.location=output_location
                        else:
                            job.outputdata.location=output_location
                            
                    logger.debug('Output4: %s,%s',output_location, job.outputdata.location)

#       prepare inputsandbox

        inputbox = [ ]
        if input_guids:     _append_file_buffer(inputbox,'input_guids',input_guids)
        if input_files:     _append_file_buffer(inputbox,'input_files',input_files)
        if input_tag_guids: _append_file_buffer(inputbox,'input_tag_guids',input_tag_guids)
        if input_tag_files: _append_file_buffer(inputbox,'input_tag_files',input_tag_files)
        if input_esd_guids: _append_file_buffer(inputbox,'input_esd_guids',input_esd_guids)
        if input_esd_files: _append_file_buffer(inputbox,'input_esd_files',input_esd_files)
        if job.outputdata and job.outputdata.outputdata:
            _append_file_buffer(inputbox,'output_files',job.outputdata.outputdata)
        elif job.outputdata and not job.outputdata.outputdata:
            raise ApplicationConfigurationError(None,'j.outputdata.outputdata is empty - Please specify output filename(s).')

        exe = os.path.join(__directory__,'athena-lcg.sh')
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
        
        if output_location and output_location.find('/castor/cern.ch/grid/atlas/t0')>=0:
            raise ApplicationConfigurationError(None,'You are try to save the output to TIER0DISK - please use another area !')
        if not output_location:
            output_location = ''
        environment['OUTPUT_LOCATION'] = output_location
        environment['ATLASOutputDatasetLFC'] = config['ATLASOutputDatasetLFC']
        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            environment['OUTPUT_DATASETNAME'] = output_datasetname
            environment['OUTPUT_LFN'] = output_lfn
            environment['OUTPUT_JOBID'] = output_jobid
            environment['DQ2_URL_SERVER']= configDQ2['DQ2_URL_SERVER']
            environment['DQ2_URL_SERVER_SSL'] = configDQ2['DQ2_URL_SERVER_SSL']
            if job.outputdata.use_shortfilename:
                environment['GANGA_SHORTFILENAME'] = 1
            else:
                environment['GANGA_SHORTFILENAME'] = ''
            
        # CN: extra condition for TNTSplitter
        if job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter':
            # set up dq2 environment
            datasetname = job.inputdata.dataset
            environment['DATASETNAME']= ':'.join(datasetname)
            environment['DATASETLOCATION'] = ':'.join(job.inputdata.get_locations())
            environment['DQ2_URL_SERVER'] = configDQ2['DQ2_URL_SERVER']
            environment['DQ2_URL_SERVER_SSL'] = configDQ2['DQ2_URL_SERVER_SSL']
            environment['DATASETTYPE'] = job.inputdata.type
            environment['DATASETDATATYPE'] = job.inputdata.datatype
            if job.inputdata.accessprotocol:
                 environment['DQ2_LOCAL_PROTOCOL'] = job.inputdata.accessprotocol
            if job.inputsandbox: inputbox += job.inputsandbox   

        # Work around for glite WMS spaced environement variable problem
        inputbox.append(FileBuffer('athena_options',environment['ATHENA_OPTIONS']+'\n'))

# append a property for monitoring to the jobconfig of subjobs
        lcg_config = LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], requirements)
	lcg_config.monitoring_svc = mc['Athena']
	return lcg_config

    def master_prepare( self, app, appconfig):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object
        logger.debug('AthenaLCGRTHandler master_prepare called: %s', job.id )

        # Expand Athena jobOptions
        athena_options = ' '.join([os.path.basename(opt_file.name) for opt_file in app.option_file])
        #if app.options: athena_options = ' -c ' + app.options + ' ' + athena_options
        if app.options:
            athena_options = app.options + ' ' + athena_options

        athena_usersetupfile = os.path.basename(app.user_setupfile.name)

#       prepare input sandbox

        inputbox = [ File(opt_file.name) for opt_file in app.option_file ]

        if job.inputdata and job.inputdata._name == 'ATLASDataset':
            if job.inputdata.lfc:
                _append_files(inputbox,'ganga-stagein-lfc.py')
            else:
                _append_files(inputbox,'ganga-stagein.py')
            
        if app.user_area.name: 
            inputbox.append(File(app.user_area.name))

        #if app.group_area.name: inputbox += [ File(app.group_area.name) ]
        if app.group_area.name and str(app.group_area.name).find('http')<0:
            inputbox.append(File(app.group_area.name))
    
        if app.user_setupfile.name: inputbox.append(File(app.user_setupfile.name))

        # CN: added TNTJobSplitter clause  
        if job.inputdata and job.inputdata._name == 'DQ2Dataset' or (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
            _append_files(inputbox,'ganga-stage-in-out-dq2.py','dq2_get','dq2info.tar.gz')
            if job.inputdata and job.inputdata.type == 'LFC' and not (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
                _append_files(inputbox,'dq2_get_old')


        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            if not 'ganga-stage-in-out-dq2.py' in [ os.path.basename(file.name) for file in inputbox ]:
                _append_files(inputbox,'ganga-stage-in-out-dq2.py')
            _append_files(inputbox,'ganga-joboption-parse.py')
            if not 'dq2info.tar.gz' in [os.path.basename(file.name) for file in inputbox ]:
                _append_files(inputbox,'dq2info.tar.gz') 

        #       add libDCache.so and libRFIO.so to fix broken access in athena 12.0.x
        if not 'ganga-stage-in-out-dq2.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, 'ganga-stage-in-out-dq2.py')
        if str(app.atlas_release).find('12.')>=0:
            _append_files(inputbox, 'libDCache.so','libRFIO.so','libdcap.so')
        elif str(app.atlas_release).find('13.')>=0:
            _append_files(inputbox,'libdcap.so')
        else:
            _append_files(inputbox,'libdcap.so')

        if job.inputsandbox: inputbox += job.inputsandbox
            
#       prepare environment

        if not app.atlas_release: 
            raise ApplicationConfigurationError(None,'j.application.atlas_release is empty - No ATLAS release version found by prepare() or specified.')

        environment={ 
            'ATLAS_RELEASE'  : app.atlas_release,
	    'ATHENA_OPTIONS' : athena_options,
	    'ATHENA_USERSETUPFILE' : athena_usersetupfile,
            'ATLAS_PROJECT' : app.atlas_project,
            'ATLAS_EXETYPE' : app.atlas_exetype
        }
        if app.atlas_environment:
            for var in app.atlas_environment:
                vars=var.split('=')
                if len(vars)==2:
                    environment[vars[0]]=vars[1]

        if app.atlas_production and app.atlas_release.find('12.')>=0 and app.atlas_project != 'AtlasPoint1':
            temp_atlas_production = re.sub('\.','_',app.atlas_production)
            prod_url = config['PRODUCTION_ARCHIVE_BASEURL']+'/AtlasProduction_'+ temp_atlas_production +'_noarch.tar.gz'
            logger.info('Using Production cache from: %s', prod_url)
            environment['ATLAS_PRODUCTION_ARCHIVE'] = prod_url

        if app.atlas_production and (app.atlas_project == 'AtlasPoint1' or app.atlas_release.find('12.')<=0):
            environment['ATLAS_PRODUCTION'] = app.atlas_production
        
        if app.user_area.name: environment['USER_AREA'] = os.path.basename(app.user_area.name)
        #if app.group_area.name: environment['GROUP_AREA']=os.path.basename(app.group_area.name)
        if app.group_area.name:
            if str(app.group_area.name).find('http')>=0:
                environment['GROUP_AREA_REMOTE'] = str(app.group_area.name)
            else:
                environment['GROUP_AREA'] = os.path.basename(app.group_area.name)

        if app.max_events:
            environment['ATHENA_MAX_EVENTS'] = str(app.max_events)

        requirements = AtlasLCGRequirements()
        
        if job.inputdata and job.inputdata._name == 'ATLASDataset':
            if job.inputdata.lfc:
                environment['GANGA_LFC_HOST'] = job.inputdata.lfc
        
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.inputdata.dataset:
                datasetname = job.inputdata.dataset
                environment['DATASETNAME'] = ':'.join(datasetname)
                environment['DATASETLOCATION'] = ':'.join(job.inputdata.get_locations())
                environment['DQ2_URL_SERVER'] = configDQ2['DQ2_URL_SERVER']
                environment['DQ2_URL_SERVER_SSL'] = configDQ2['DQ2_URL_SERVER_SSL']
                environment['DATASETTYPE'] = job.inputdata.type
                environment['DATASETDATATYPE'] = job.inputdata.datatype
                if job.inputdata.accessprotocol:
                    environment['DQ2_LOCAL_PROTOCOL'] = job.inputdata.accessprotocol
                if job.inputdata.check_md5sum:
                    environment['GANGA_CHECKMD5SUM'] = 1
                    
            else:
                raise ApplicationConfigurationError(None,'j.inputdata.dataset is empty - DQ2 dataset name needs to be specified.')

            # Restrict CE to list provided from DQ2 system with ToA info
            if (not job.backend.CE and 
                not (job.backend.requirements._name == 'AtlasLCGRequirements' and job.backend.requirements.sites) and
                not (job.splitter and job.splitter._name == 'DQ2JobSplitter')):

                cesall = []
                cesincomplete = []
                # Use only complete sources by default
                ces = job.inputdata.get_locations(complete=1)
                # Find sites with AOD and ESD dataset
                if job.inputdata.use_aodesd_backnav:
                    cesbacknav = job.inputdata.get_locations(complete=1,backnav=True)
                    cesnew = []
                    for ices in cesbacknav:
                        if ices in ces:
                            cesnew.append(ices)
                    ces = cesnew
                # Use complete and incomplete sources if match_ce_all
                if job.inputdata.match_ce_all:
                    cesall = job.inputdata.get_locations(complete=0)
                    # Find sites with AOD and ESD dataset
                    if job.inputdata.use_aodesd_backnav:
                        cesbacknav = job.inputdata.get_locations(complete=0,backnav=True)
                        cesnew = []
                        for ices in cesbacknav:
                            if ices in ces:
                                cesnew.append(ices)
                        cesall = cesnew
                    
                if job.inputdata.min_num_files>0:
                    logger.warning('Please be patient, scanning LFC catalogs for incomplete dataset locations')
                    cesincomplete = getIncompleteLocations(job.inputdata.list_locations_num_files(complete=0), job.inputdata.min_num_files)
                    # Find sites with AOD and ESD dataset
                    if job.inputdata.use_aodesd_backnav:
                        cesincompletebacknav = getIncompleteLocations(job.inputdata.list_locations_num_files(complete=0,backnav=True),job.inputdata.min_num_files)
                        cesnew = []
                        for ices in cesincompletebacknav:
                            if ices in cesincomplete:
                                cesnew.append(ices)
                        cesincomplete = cesnew

                if cesall and not cesincomplete:
                    ces = cesall
                elif cesincomplete and not cesall:
                    ces += cesincomplete
                elif cesincomplete and cesall:
                    ces += cesincomplete
                    
                    
                if not ces:
                    raise ApplicationConfigurationError(None,'DQ2 returned no complete dataset location - unable to submit the job to an appropriate CE. Use match_ce_all instead for incomplete sources ?')
                
                requirements.sites = ces

            # Add TAG datasetname
            if job.inputdata.tagdataset:
                environment['TAGDATASETNAME'] = ':'.join(job.inputdata.tagdataset)

#       prepare job requirements
        
        if app.atlas_release.find('11.')>=0 or app.atlas_release.find('10.')>=0:
            requirements.software = ['VO-atlas-release-%s' % app.atlas_release ]
        elif app.atlas_release.find('12.0.0')>=0 or app.atlas_release.find('12.0.1')>=0 or app.atlas_release.find('12.0.2')>=0:
            requirements.software = ['VO-atlas-offline-%s' % app.atlas_release ]
        elif app.atlas_release.find('13.')>=0 and app.atlas_project!="AtlasPoint1":
            requirements.software = ['VO-atlas-production-%s' % app.atlas_release ] 
        elif app.atlas_release.find('13.')>=0 and app.atlas_project!="AtlasPoint1" and app.atlas_production!='':
            requirements.software = ['VO-atlas-production-%s' % app.atlas_production]
        elif app.atlas_release.find('13.')>=0 and app.atlas_project=="AtlasPoint1":
            requirements.software = ['VO-atlas-point1-%s' % app.atlas_production ] 
        elif app.atlas_release.find('14.')>=0:
            if app.atlas_cmtconfig:
                cmtconfig = app.atlas_cmtconfig
            else:
                cmtconfig = 'i686-slc4-gcc34-opt'
            if cmtconfig != 'i686-slc4-gcc34-opt':
                cmtconfig = 'i686-slc4-gcc34-opt'
            if app.atlas_production=='':
                requirements.software = ['VO-atlas-offline-%s-%s' %(app.atlas_release, cmtconfig )]
            else:
                requirements.software = ['VO-atlas-production-%s-%s' %(app.atlas_production, cmtconfig )]
        else:
            requirements.software = ['VO-atlas-production-%s' % app.atlas_release ]

#       jobscript

        exe = os.path.join(__directory__,'athena-lcg.sh')

#       output sandbox
        outputbox = [
            'output_guids',
            'output_location',
            'output_data'
        ]
        if job.outputsandbox: outputbox += job.outputsandbox

        return LCGJobConfig(File(exe),inputbox,[],outputbox,environment,[],requirements) 


allHandlers.add('Athena','LCG',AthenaLCGRTHandler)
allHandlers.add('Athena','Condor',AthenaLCGRTHandler)
allHandlers.add('Athena','Cronus',AthenaLCGRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
logger = getLogger()
