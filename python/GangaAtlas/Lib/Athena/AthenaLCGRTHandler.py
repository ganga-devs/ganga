##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaLCGRTHandler.py,v 1.51 2009-07-23 20:12:25 elmsheus Exp $
###############################################################################
# Athena LCG Runtime Handler
#
# ATLAS/ARDA

import os, pwd, commands, re, string, time, sys

from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import *
from GangaCore.GPIDev.Lib.File import *

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaCore.Utility.Config import getConfig, makeConfig, ConfigError
from GangaCore.Utility.logging import getLogger

from GangaCore.Lib.LCG import LCGJobConfig
from GangaAtlas.Lib.AtlasLCGRequirements import AtlasLCGRequirements
from GangaAtlas.Lib.AtlasLCGRequirements import AtlasCREAMRequirements

from GangaAtlas.Lib.ATLASDataset import isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE, getIncompleteLocations, whichCloud
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname
from GangaCore.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

# the config file may have a section
# aboout monitoring

mc = getConfig('MonitoringServices')


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

def getLCGReleaseTag( app ):
    """Helper function to return the LCG release tag for CE detection"""
    requirementsSoftware = []

    # check the correct app is being used
    if not app or not hasattr(app, 'atlas_release') or not hasattr(app, 'atlas_production') or not hasattr(app, 'atlas_cmtconfig'):
        return requirementsSoftware

    # now construct the requirements release string
    if app.atlas_release.find('11.')>=0 or app.atlas_release.find('10.')>=0:
        requirementsSoftware = ['VO-atlas-release-%s' % app.atlas_release ]
    elif app.atlas_release.find('12.0.0')>=0 or app.atlas_release.find('12.0.1')>=0 or app.atlas_release.find('12.0.2')>=0:
        requirementsSoftware = ['VO-atlas-offline-%s' % app.atlas_release ]
    elif app.atlas_release.find('13.')>=0 and app.atlas_project!="AtlasPoint1":
        requirementsSoftware = ['VO-atlas-production-%s' % app.atlas_release ] 
    elif app.atlas_release.find('13.')>=0 and app.atlas_project!="AtlasPoint1" and app.atlas_production!='':
        requirementsSoftware = ['VO-atlas-production-%s' % app.atlas_production]
    elif app.atlas_release.find('13.')>=0 and app.atlas_project=="AtlasPoint1":
        requirementsSoftware = ['VO-atlas-point1-%s' % app.atlas_production ] 
    elif app.atlas_release.find('14.')>=0 or app.atlas_release.find('15.')>=0 or app.atlas_release.find('16.')>=0 or app.atlas_release.find('17.')>=0:
        if app.atlas_cmtconfig:
            cmtconfig = app.atlas_cmtconfig
        else:
            cmtconfig = config['CMTCONFIG']
        if not cmtconfig in config['CMTCONFIG_LIST']:
            cmtconfig = config['CMTCONFIG']
        if app.atlas_production=='':
            requirementsSoftware = ['VO-atlas-offline-%s-%s' %(app.atlas_release, cmtconfig )]
        else:
            if app.atlas_project=="AtlasPoint1":
                requirementsSoftware = ['VO-atlas-point1-%s' %(app.atlas_production)]
            elif app.atlas_project=="AtlasTier0":
                requirementsSoftware = ['VO-atlas-tier0-%s' %(app.atlas_production)]
            elif app.atlas_project=="AtlasProduction":
                requirementsSoftware = ['VO-atlas-production-%s-%s' %(app.atlas_production, cmtconfig )]
            else:
                requirementsSoftware = ['VO-atlas-%s-%s-%s' %(app.atlas_project.lower(), app.atlas_production, cmtconfig )]
    else:
        requirementsSoftware = ['VO-atlas-production-%s' % app.atlas_release ]

    return requirementsSoftware
    
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
        add_files = []
       
        if job.inputdata:

            # DQ2Dataset, ATLASLocalDataset and ATLASCastorDataset job splitting is done in AthenaSplitterJob

            if job._getRoot().subjobs:
                if job.inputdata._name == 'ATLASLocalDataset':
                    if not job.inputdata.names: raise ApplicationConfigurationError('No inputdata has been specified.')
                    input_files = job.inputdata.names

                elif job.inputdata._name == 'ATLASTier3Dataset':
                    if not job.inputdata.names:
                        raise ApplicationConfigurationError('No inputdata has been specified.')
                    if job.inputdata.names:
                        input_files = job.inputdata.names
                        input_guids = input_files

                elif job.inputdata._name in [ 'DQ2Dataset', 'EventPicking']:
                    if not job.inputdata.names: raise ApplicationConfigurationError('No inputdata has been specified. Failure in job %s.%s. Dataset %s' %(job._getRoot().id, job.id, job.inputdata.dataset)  )
                    input_guids = job.inputdata.guids
                    input_files = job.inputdata.names

                    if job.inputdata.tag_info:

                        # check for conflicts with TAG_LOCAL or TAG_COPY
                        if job.inputdata.type in ['TAG_LOCAL', 'TAG_COPY']:
                            raise ApplicationConfigurationError("Cannot provide both tag_info and run as '%s'. Please use one or the other!" % job.inputdata.type)
                        
                        # check if FILE_STAGER is used
                        if job.inputdata.type == 'FILE_STAGER':
                            logger.warning("TAG jobs currently can't use the FILE_STAGER. Switching to DQ2_COPY instead.")
                            job.inputdata.type = 'DQ2_COPY'
                        
                        # add additional file info for tags
                        for tag_file in job.inputdata.tag_info:
                            for ref in job.inputdata.tag_info[tag_file]['refs']:
                                add_files.append( ref[1] + ':' + ref[0] + ':' + ref[2] )
                    
                    if not job.inputdata.type in ['DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD', 'DQ2_COPY', 'FILE_STAGER', 'TAG_LOCAL', 'TAG_COPY' ]:
                        job.inputdata.type ='DQ2_LOCAL'
                    if not job.inputdata.datatype in ['DATA', 'MC', 'MuonCalibStream']:
                        job.inputdata.datatype ='MC'

            else:
                if job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)

                elif job.inputdata._name == 'ATLASTier3Dataset':
                    if job.inputdata.names:
                        input_files = job.inputdata.names
                        input_guids = input_files
                    elif job.inputdata.pfnListFile:
                        logger.info('Loading file names from %s'%job.inputdata.pfnListFile.name)
                        pfnListFile = open(job.inputdata.pfnListFile.name)
                        job.inputdata.names = [ line.strip() for line in pfnListFile]
                        pfnListFile.close()
                        input_files = job.inputdata.names 
                        input_guids = input_files
                    else:
                        raise ApplicationConfigurationError('No inputdata has been specified.')

                elif job.inputdata._name in [ 'DQ2Dataset', 'EventPicking']:
                    if not job.inputdata.type in ['DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD', 'DQ2_COPY', 'FILE_STAGER', 'TAG_LOCAL', 'TAG_COPY' ]:
                        job.inputdata.type ='DQ2_LOCAL'
                    if not job.inputdata.datatype in ['DATA', 'MC', 'MuonCalibStream']:
                        job.inputdata.datatype ='MC'

                    input_guids, input_files = _splitlist(job.inputdata.get_contents())

                    if job.inputdata.use_aodesd_backnav:
                        input_esd_guids, input_esd_files = _splitlist(job.inputdata.get_contents(backnav=True))

                    job.inputdata.names = input_files          
                    job.inputdata.guids = input_guids          

#       prepare outputdata
       
        output_location = ''
        if job.outputdata:

            if job.outputdata._name=='DQ2OutputDataset':

                if job.outputdata.location:
                    if type(job.outputdata.location) == str and isDQ2SRMSite(job.outputdata.location):
                        output_location = job.outputdata.location
                    else:
                        logger.warning('Unknown output location %s.',job.outputdata.location)

                    #if job.backend.requirements._name == 'AtlasLCGRequirements':
                    #    if job.backend.requirements.cloud:
                    #        if whichCloud(output_location) != job.backend.requirements.cloud:
                    #            printout = 'Job submission failed ! j.outputdata.location=%s is not in the same cloud as j.backend.requirements.cloud=%s' %(job.outputdata.location, job.backend.requirements.cloud )
                    #            raise ApplicationConfigurationError(None, printout)
                    #    if job.backend.requirements.sites:
                    #        if whichCloud(output_location) != whichCloud(job.backend.requirements.sites[0]):
                    #            printout = 'Job submission failed ! j.outputdata.location=%s is not in the same cloud as j.backend.requirements.sites=%s'%(job.outputdata.location, job.backend.requirements.sites)
                    #            raise ApplicationConfigurationError(None,printout )     
                    
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

            # output dataset name from master_prepare
            output_datasetname = self.output_datasetname
            output_lfn = self.output_lfn

            output_jobid = jid
            # Set subjob datasetname
            job.outputdata.datasetname=output_datasetname
            # Set master job datasetname
            if job._getRoot().subjobs:
                job._getRoot().outputdata.datasetname=output_datasetname
            # Create output dataset -> moved to the worker node code !
            if not job.outputdata.dataset_exists(output_datasetname):
                if job._getRoot().subjobs:
                    if job.id==0:
                        #job.outputdata.create_dataset(output_datasetname)
                        pass
                else:
                    #job.outputdata.create_dataset(output_datasetname)
                    pass
                if output_location and configDQ2['USE_STAGEOUT_SUBSCRIPTION']:
                    job.outputdata.create_subscription(output_datasetname, output_location)    
                
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

        if getConfig('LCG')['JobLogHandler'] == 'DQ2' and (not job.outputdata or (job.outputdata and job.outputdata._name != 'DQ2OutputDataset')):
            raise ApplicationConfigurationError('Staging of log files in DQ2 requested, but DQ2 output dataset not specified.')
        
#       prepare inputsandbox

        inputbox = [File(os.path.join(__directory__,'athena-utility.sh')) ]
        if input_guids:     _append_file_buffer(inputbox,'input_guids',input_guids)
        if input_files:     _append_file_buffer(inputbox,'input_files',input_files)
        if add_files:     _append_file_buffer(inputbox,'add_files',add_files)
        if input_tag_guids: _append_file_buffer(inputbox,'input_tag_guids',input_tag_guids)
        if input_tag_files: _append_file_buffer(inputbox,'input_tag_files',input_tag_files)
        if input_esd_guids: _append_file_buffer(inputbox,'input_esd_guids',input_esd_guids)
        if input_esd_files: _append_file_buffer(inputbox,'input_esd_files',input_esd_files)

        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            for tag_file in job.inputdata.tag_info:
                if job.inputdata.tag_info[tag_file]['path'] != '':
                    inputbox.append( File( os.path.join( job.inputdata.tag_info[tag_file]['path'], tag_file) ) )

        # check for output data given in prepare info
        if job.outputdata and job.application.atlas_exetype == "ATHENA":
            for of in job.application.atlas_run_config['output']['alloutputs']:
                if not of in job.outputdata.outputdata:
                    job.outputdata.outputdata.append(of)

                                        
        if job.outputdata and job.outputdata.outputdata:
            _append_file_buffer(inputbox,'output_files',job.outputdata.outputdata)
        elif job.outputdata and not job.outputdata.outputdata:
            raise ApplicationConfigurationError('j.outputdata.outputdata is empty - Please specify output filename(s).')

        exe = os.path.join(__directory__,'run-athena-lcg.sh')
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
            raise ApplicationConfigurationError('You are try to save the output to TIER0DISK - please use another area !')
        if not output_location:
            output_location = ''
        if configDQ2['USE_STAGEOUT_SUBSCRIPTION']:
            output_location = ''
        environment['OUTPUT_LOCATION'] = output_location
        environment['ATLASOutputDatasetLFC'] = config['ATLASOutputDatasetLFC']
        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            environment['OUTPUT_DATASETNAME'] = output_datasetname
            environment['OUTPUT_LFN'] = output_lfn
            environment['OUTPUT_JOBID'] = output_jobid
            environment['DQ2_URL_SERVER']= configDQ2['DQ2_URL_SERVER']
            environment['DQ2_URL_SERVER_SSL'] = configDQ2['DQ2_URL_SERVER_SSL']
            environment['DQ2_OUTPUTFILE_NAMELENGTH'] = configDQ2['OUTPUTFILE_NAMELENGTH']
            if job.outputdata.use_shortfilename:
                environment['GANGA_SHORTFILENAME'] = '1'
            else:
                environment['GANGA_SHORTFILENAME'] = ''
                
            environment['DQ2_OUTPUT_SPACE_TOKENS']= ':'.join(configDQ2['DQ2_OUTPUT_SPACE_TOKENS'])
            environment['DQ2_BACKUP_OUTPUT_LOCATIONS']= ':'.join(configDQ2['DQ2_BACKUP_OUTPUT_LOCATIONS'])
            
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

        if job.inputdata and job.inputdata._name == 'DQ2Dataset' and job.inputdata.tag_info:
            if job.inputdata.tag_info[job.inputdata.tag_info.keys()[0] ]['dataset'] != '' and job.inputdata.tag_info[tag_file]['path'] == '':
                environment['TAG_TYPE'] = 'DQ2'
            else:
                environment['TAG_TYPE'] = 'LOCAL'                

        # Fix DATASETNAME env variable for DQ2_COPY mode
        if job.inputdata and ( job.inputdata._name in [ 'DQ2Dataset', 'EventPicking']) and (job.inputdata.type in ['DQ2_LOCAL', 'DQ2_COPY', 'FILE_STAGER', 'TAG_LOCAL', 'TAG_COPY' ]):
            if job.inputdata.dataset:
                from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import resolve_container
                datasets = resolve_container(job.inputdata.dataset)
                environment['DATASETNAME'] = datasets[0]
                try:
                    environment['DATASETLOCATION'] = ':'.join(job.inputdata.get_locations(overlap=False)[ datasets[0] ])
                except:
                    printout = 'Job submission failed ! Dataset %s could not be found in DQ2 ! Maybe retry ?' %(datasets[0])
                    raise ApplicationConfigurationError(printout )

        if job.inputdata and job.inputdata._name == 'ATLASTier3Dataset':
            environment['DATASETTYPE'] = 'TIER3'

        # Work around for glite WMS spaced environement variable problem
        inputbox.append(FileBuffer('athena_options',environment['ATHENA_OPTIONS']+'\n'))

        # Write trf parameters
        trf_params = ' '
        for key, value in job.application.trf_parameter.iteritems():
            if key == 'dbrelease':
                environment['DBDATASETNAME'] = value.split(':')[0]
                environment['DBFILENAME'] = value.split(':')[1]
            else:
                trf_params = trf_params + key + '=' + str(value) + ' '
        if trf_params!=' ' and job.application.atlas_exetype=='TRF':
           _append_file_buffer(inputbox,'trf_params', [ trf_params ] ) 

        # set RecExCommon options
        environment['RECEXTYPE'] = job.application.recex_type

        # event based splitting:  set max_events and skip_events
        if job._getRoot().splitter and hasattr(job._getRoot().splitter, 'numevtsperjob') and job._getRoot().splitter.numevtsperjob > 0 :
                environment['ATHENA_MAX_EVENTS'] = str(job.application.max_events)
                environment['ATHENA_SKIP_EVENTS'] = str(job.application.skip_events)

        # pick event 
        if job._getRoot().splitter and job._getRoot().inputdata and job._getRoot().inputdata._name == 'EventPicking' :
                #Replace blank space 
                environment['ATHENA_RUN_EVENTS'] = str(job.application.run_event).replace(' ', '')
                environment['ATHENA_FILTER_POLICY'] = str(job.inputdata.pick_filter_policy)
        
# append a property for monitoring to the jobconfig of subjobs
        lcg_config = LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], requirements)
        lcg_config.monitoring_svc = mc['Athena']
        return lcg_config

    def master_prepare( self, app, appconfig):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object
        logger.debug('AthenaLCGRTHandler master_prepare called: %s', job.id )


        if job._getRoot().subjobs:
            jobid = "%d" % (job._getRoot().id)
        else:
            jobid = "%d" % job.id

        # Generate output dataset name
        if job.outputdata:
            if job.outputdata._name=='DQ2OutputDataset':
                dq2_datasetname = job.outputdata.datasetname
                dq2_isGroupDS = job.outputdata.isGroupDS
                dq2_groupname = job.outputdata.groupname
            else:
                dq2_datasetname = ''
                dq2_isGroupDS = False
                dq2_groupname = ''
            self.output_datasetname, self.output_lfn = dq2outputdatasetname(dq2_datasetname, jobid, dq2_isGroupDS, dq2_groupname)

        # Check if all sites are in the same cloud
        if job.backend.requirements.sites:
            firstCloud = whichCloud(job.backend.requirements.sites[0])
            for site in job.backend.requirements.sites:
                cloud = whichCloud(site)
                if cloud != firstCloud:
                    printout = 'Job submission failed ! Site specified with j.backend.requirements.sites=%s are not in the same cloud !' %(job.backend.requirements.sites)
                    raise ApplicationConfigurationError(printout )


        #this next for loop instructs ganga to use option_files that live in the appropriate shared directory (the job
        #will already have been prepared
        #(if is_prepared is True, then we've most likely submitted a job via GangaRobot. We know what we're doing.
        #if app.is_prepared is not True:
        #    for position in xrange(len(app.option_file)):
        #        app.option_file[position]=File(os.path.join(app.is_prepared.name,os.path.basename(app.option_file[position].name)))
        # Expand Athena jobOptions
        if not app.atlas_exetype in ['EXE']:
            athena_options = ' '.join([os.path.basename(opt_file.name) for opt_file in app.option_file])
            #if app.options: athena_options = ' -c ' + app.options + ' ' + athena_options
            if app.options:
                athena_options = app.options + ' ' + athena_options
                
            inputbox = [ File(opt_file.name) for opt_file in app.option_file ]
        else:
            athena_options = ' '.join([os.path.basename(opt_file.name) for opt_file in app.option_file])
            inputbox = []
            
        athena_usersetupfile = os.path.basename(app.user_setupfile.name)

#       prepare input sandbox

        
        inputbox.append( File(os.path.join(__directory__,'athena-utility.sh')) )

        if app.user_area.name: 
            #we will now use the user_area that's stored in the users shared directory
            if app.is_prepared is not True:
                tmp_user_name = os.path.join(os.path.join(shared_path,app.is_prepared.name),os.path.basename(app.user_area.name))
                inputbox.append(File(tmp_user_name))
            else:
                inputbox.append(File(app.user_area.name))

        #if app.group_area.name: inputbox += [ File(app.group_area.name) ]
        if app.group_area.name and str(app.group_area.name).find('http')<0:
            #we will now use the group_area that's stored in the users shared directory
            if app.is_prepared is not True:
                tmp_group_name = os.path.join(os.path.join(shared_path,app.is_prepared.name),os.path.basename(app.group_area.name))
                inputbox.append(File(tmp_group_name))
            else:
                inputbox.append(File(app.group_area.name))
    
        if app.user_setupfile.name: inputbox.append(File(app.user_setupfile.name))

        # CN: added TNTJobSplitter clause  

        if job.inputdata and (job.inputdata._name in [ 'DQ2Dataset', 'EventPicking' ] ) or (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
            _append_files(inputbox,'ganga-stage-in-out-dq2.py','dq2_get','dq2info.tar.gz')
            if job.inputdata and job.inputdata.type == 'LFC' and not (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
                _append_files(inputbox,'dq2_get_old')

        if job.inputdata and job.inputdata._name ==  'ATLASTier3Dataset':
            _append_files(inputbox,'ganga-stage-in-out-dq2.py','dq2info.tar.gz')

        ## insert more scripts to inputsandbox for FileStager
        if job.inputdata and (job.inputdata._name in [ 'DQ2Dataset', 'EventPicking']) and job.inputdata.type in ['FILE_STAGER']:
            _append_files(inputbox,'make_filestager_joption.py','dm_util.py','fs-copy.py')
            #_append_files(inputbox,'make_filestager_joption.py','dm_util.py')

        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            #if not job.outputdata.location:
            #    raise ApplicationConfigurationError('j.outputdata.location is empty - Please specify a DQ2 output location - job not submitted !')
            if not 'ganga-stage-in-out-dq2.py' in [ os.path.basename(file.name) for file in inputbox ]:
                _append_files(inputbox,'ganga-stage-in-out-dq2.py')
            _append_files(inputbox,'ganga-joboption-parse.py')
            if not 'dq2info.tar.gz' in [os.path.basename(file.name) for file in inputbox ]:
                _append_files(inputbox,'dq2info.tar.gz') 

        #       add libDCache.so and libRFIO.so to fix broken access in athena 12.0.x
        if not 'ganga-stage-in-out-dq2.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, 'ganga-stage-in-out-dq2.py')
        if not 'dq2tracerreport.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, 'dq2tracerreport.py')
        if not 'db_dq2localid.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, 'db_dq2localid.py')
        if not 'getstats.py' in [ os.path.basename(file.name) for file in inputbox ]:
            _append_files(inputbox, 'getstats.py')


        if str(app.atlas_release).find('12.')>=0:
            _append_files(inputbox, 'libDCache.so','libRFIO.so','libdcap.so')
        elif str(app.atlas_release).find('13.')>=0:
            _append_files(inputbox,'libdcap.so')
        else:
            _append_files(inputbox,'libdcap.so')

        if job.inputsandbox: inputbox += job.inputsandbox
            
#       prepare environment

        if not app.atlas_release: 
            raise ApplicationConfigurationError('j.application.atlas_release is empty - No ATLAS release version found. Run prepare() or specify a version explictly.')

        environment={ 
            'ATLAS_RELEASE'  : app.atlas_release,
            'ATHENA_OPTIONS' : athena_options,
            'ATHENA_USERSETUPFILE' : athena_usersetupfile,
            'ATLAS_PROJECT' : app.atlas_project,
            'ATLAS_EXETYPE' : app.atlas_exetype,
            'GANGA_VERSION' : configSystem['GANGA_VERSION']
        }

        environment['DCACHE_RA_BUFFER'] = config['DCACHE_RA_BUFFER']

        if app.atlas_environment:
            for var in app.atlas_environment:
                try:
                    vars = re.match("^(\w+)=(.*)",var).group(1)
                    value = re.match("^(\w+)=(.*)",var).group(2)
                    environment[vars]=value
                except:
                    logger.warning('Athena.atlas_environment variable not correctly configured: %s', var)
                    pass

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
            if (app.max_events != -999) and (app.max_events > -2):
                environment['ATHENA_MAX_EVENTS'] = str(app.max_events)
        
        if job.backend.requirements._name == 'AtlasLCGRequirements':
            requirements = AtlasLCGRequirements()
        elif job.backend.requirements._name == 'AtlasCREAMRequirements':
            requirements = AtlasCREAMRequirements()
        else:
            requirements = AtlasLCGRequirements()
        
        if 'ganga-stage-in-out-dq2.py' in [ os.path.basename(file.name) for file in inputbox ]:
            environment['DQ2_URL_SERVER'] = configDQ2['DQ2_URL_SERVER']
            environment['DQ2_URL_SERVER_SSL'] = configDQ2['DQ2_URL_SERVER_SSL']
        
        if job.inputdata and (job.inputdata._name in [ 'DQ2Dataset', 'EventPicking']):
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
                raise ApplicationConfigurationError('j.inputdata.dataset is empty - DQ2 dataset name needs to be specified.')

            # Raise submission exception
            if (not job.backend.CE and 
                not (job.backend.requirements._name in [ 'AtlasLCGRequirements', 'AtlasCREAMRequirements' ] and job.backend.requirements.sites) and
                not (job.splitter and job.splitter._name == 'DQ2JobSplitter') and
                not (job.splitter and job.splitter._name == 'TNTJobSplitter') and
                not (job.splitter and job.splitter._name == 'AnaTaskSplitterJob') and
                not (job.splitter and job.splitter._name == 'ATLASTier3Splitter')):

                raise ApplicationConfigurationError('Job submission failed ! Please use DQ2JobSplitter or specify j.backend.requirements.sites or j.backend.requirements.CE !')

            if job.inputdata.match_ce_all or job.inputdata.min_num_files>0:
                raise ApplicationConfigurationError('Job submission failed ! Usage of j.inputdata.match_ce_all or min_num_files is obsolete ! Please use DQ2JobSplitter or specify j.backend.requirements.sites or j.backend.requirements.CE !')
            #if job.inputdata.number_of_files and (job.splitter and job.splitter._name == 'DQ2JobSplitter'):
            #    allLoc = job.inputdata.get_locations(complete=0)
            #    completeLoc = job.inputdata.get_locations(complete=1)
            #    incompleteLoc = []
            #    for loc in allLoc:
            #        if loc not in completeLoc:
            #            incompleteLoc.append(loc)
            #    if incompleteLoc:
            #        raise ApplicationConfigurationError(None,'Job submission failed ! Dataset is incomplete ! Usage of j.inputdata.number_of_files and DQ2JobSplitter is not allowed for incomplete datasets !')

#       prepare job requirements
        requirementsSoftware = getLCGReleaseTag( app )

        releaseBlacklist = job.backend.requirements.list_release_blacklist()     
        if requirementsSoftware and  requirementsSoftware[0] in releaseBlacklist:
            logger.error('The athena release %s you are using is not recommended for distributed analysis !', requirementsSoftware[0])
            logger.error('For details, please have a look at https://twiki.cern.ch/twiki/bin/view/Atlas/DAGangaFAQ#Athena_Versions_Issues or ask for help and advice on the distributed analysis help list !')
            requirements.software = requirementsSoftware
        else:
            requirements.software = requirementsSoftware

        # Set athena architecture: 32 or 64 bit    
        environment['ATLAS_ARCH'] = '32'
        if requirementsSoftware and requirementsSoftware[0].find('x86_64')>=0:
            environment['ATLAS_ARCH'] = '64'
            
        #       add software requirement of dq2clients
        if job.inputdata and job.inputdata._name in [ 'DQ2Dataset', 'EventPicking' ]  and job.inputdata.type in [ 'TNT_DOWNLOAD', 'DQ2_COPY', 'FILE_STAGER'] or app.atlas_dbrelease or configDQ2['USE_ACCESS_INFO']:
            try:
                # override the default one if the dq2client_version is presented 
                # in the job backend's requirements object
                dq2client_version = job.backend.requirements.dq2client_version
            except AttributeError:
                pass
            if dq2client_version:
                #requirements.software += ['VO-atlas-dq2clients-%s' % dq2client_version]
                environment['DQ2_CLIENT_VERSION'] = dq2client_version

        if app.atlas_dbrelease:
            if not app._name == "AthenaTask" and not (job.splitter and (job.splitter._name == 'DQ2JobSplitter' or job.splitter._name == 'ATLASTier3Splitter')):
                raise ApplicationConfigurationError('Job submission failed ! Please use DQ2JobSplitter if you are using j.application.atlas_dbrelease !')
            try:
                environment['ATLAS_DBRELEASE'] = app.atlas_dbrelease.split(':')[0]
                environment['ATLAS_DBFILE'] = app.atlas_dbrelease.split(':')[1]
            except:
                logger.warning('Problems with the atlas_dbrelease configuration')


        # Fill AtlasLCGRequirements access mode 
        if configDQ2['USE_ACCESS_INFO']:
            logger.warning("config['DQ2']['USE_ACCESS_INFO']=True - You are using the improved worker node input access method - make sure you are using at least athena version 15.0.0 or the latest FileStager tag !" )
            import pickle, StringIO
            #if job.backend.requirements.sites:
            info = job.backend.requirements.list_access_info()
            fileHandle = StringIO.StringIO()
            pickle.dump(info,fileHandle)
            fileHandle.seek(-1)
            lines = fileHandle.read()
            inputbox.append(FileBuffer( 'access_info.pickle', lines))
            _append_files(inputbox, 'access_info.py')
            if not 'make_filestager_joption.py' in [ os.path.basename(file.name) for file in inputbox ]:
                _append_files(inputbox,'make_filestager_joption.py','dm_util.py','fs-copy.py')

#       jobscript

        exe = os.path.join(__directory__,'run-athena-lcg.sh')

#       output sandbox
        outputbox = [
            'output_guids',
            'output_location',
            'output_data',
            'stats.pickle'
        ]

        ## retrieve the FileStager log
        if configDQ2['USE_ACCESS_INFO'] or (job.inputdata and (job.inputdata._name in [ 'DQ2Dataset', 'EventPicking']) and job.inputdata.type in ['FILE_STAGER']):
            outputbox += ['FileStager.out', 'FileStager.err']
            
        if job.outputsandbox: outputbox += job.outputsandbox

        # Switch for DEBUG print-out in logfiles
        if app.useNoDebugLogs:
            environment['GANGA_LOG_DEBUG'] = '0'
        else:
            environment['GANGA_LOG_DEBUG'] = '1'
            
        return LCGJobConfig(File(exe),inputbox,[],outputbox,environment,[],requirements) 

allHandlers.add('Athena','LCG',AthenaLCGRTHandler)
allHandlers.add('Athena','CREAM',AthenaLCGRTHandler)
#allHandlers.add('Athena','Condor',AthenaLCGRTHandler)
allHandlers.add('Athena','Cronus',AthenaLCGRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configSystem = getConfig('System')
logger = getLogger()
