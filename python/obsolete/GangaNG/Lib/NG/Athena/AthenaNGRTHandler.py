###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaNGRTHandler.py,v 1.15 2009-06-25 13:04:37 bsamset Exp $
###############################################################################
# Athena NG Runtime Handler
#
# Maintained by the Oslo group (B. Samset, K. Pajchel)
#
# Date:   January 2007


import os, pwd, commands, re 
import time

try:
    import lfc
except:
    pass

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

from GangaNG.Lib.NG import NGRequirements, NGJobConfig
from GangaNG.Lib.NG.NGRequirements import NGRequirements

from GangaAtlas.Lib.ATLASDataset import ATLASDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from GangaAtlas.Lib.ATLASDataset import ATLASLocalDataset
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.GPIDev.Credentials import GridProxy

from dq2.common.DQException import DQInvalidRequestException
from dq2.clientapi.DQ2 import DQ2
from dq2.common.DQException import *

# the config file may have a section
# aboout monitoring

mc = getConfig('MonitoringServices')

# commented out 
# None by default
#mc['Athena'] = None

# None by default
mc.addOption('Athena/NG', None, 'FIXME')
#mc.addOption('Athena', None, 'FIXME')

def _append_file_buffer(inputbox,name,array):

    inputbox.append(FileBuffer(name,'\n'.join(array)+'\n'))

def _append_files(inputbox,*names):

    for name in names:
        inputbox.append(File(os.path.join(__directory__,name)))
        

class AthenaNGRTHandler(IRuntimeHandler):
    """Athena NG Runtime Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        #print '%%%%%%%%%%%%%%%%%%%%%%   in NGRTHandler prepare prepare subjob' 
        job = app._getParent() # Returns job or subjob object
        logger.debug("AthenaNGRTHandler prepare called, %s", job.id)

        input_files = []
        input_guids = []
        input_tag_files = []
        input_tag_guids = []
        arg = []
        # If job has inputdata
        if job.inputdata:

            if job.inputdata._name == 'ATLASCastorDataset':
                raise ApplicationConfigurationError(None,'ATLASCastorDataset not supported by NorduGrid.')
                #raise Exception('ATLASCastorDataset not supported by NorduGrid.')

            if job.inputdata._name == 'ATLASDataset':
                raise ApplicationConfigurationError(None,'ATLASDataser not supported by NorduGrid.')
                raise Exception('ATLASDataser not supported by NorduGrid.')
            
            # ATLASLocalDataset and ATLASCastorDataset job splitting is done in AthenaSplitterJob
            
            if job._getRoot().subjobs:
                if job.inputdata._name == 'ATLASLocalDataset':
                    if not job.inputdata.names: raise Exception('No inputdata has been specified.')
                    input_files = job.inputdata.names
                    input_guids = get_guids( input_files )

                elif job.inputdata._name == 'NGInputData':
                    input_guids = []
                    input_files = []
                    for f in job.inputdata.names:
                        fs = f.split('/')
                        input_files.append(fs[-1])
                        input_guids.append("00000000-0000-0000-0000-000000000000") #No guids needed, just for input file parsing

                elif job.inputdata._name == 'DQ2Dataset' and job.inputdata.accessprotocol =='GSIDCAP':
                    if not job.inputdata.names: raise ApplicationConfigurationError(None,'No inputdata has been specified.')
                    #if not job.inputdata.names: raise Exception('No inputdata has been specified.')
                    input_guids = job.inputdata.guids
                    #input_files = job.inputdata.names

                    # check and set env. variables for default LFC setup 
                    if not os.environ.has_key('LFC_HOST'):
                        try:
                            os.environ['LFC_HOST'] = config['DefaultLFC']
                        except Ganga.Utility.Config.ConfigError:
                            os.environ['LFC_HOST'] = 'lfc1.ndgf.org'
                            
                    for guid in input_guids:

                        #site = "srm.swegrid.se"
                        site = job.backend.requirements.gsidcap
                        sfn = get_dcap_path(guid,site)

                        if sfn!="":
                            input_files.append(sfn)
                        else:
                            print "Found no replica for guid "+guid+" at "+site+". Removing from inputs."
                            input_guids.remove(guid)

                    # Were all inputs removed?
                    if len(input_guids)==0:
                        raise ApplicationConfigurationError(None,'No inputs found for job %s at site %s.' % (job.getFQID('.'),site))

                    # Update job names to the gsidcap values
                    job.inputdata.names = input_files
                        
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD']:
                        job.inputdata.type ='DQ2_LOCAL'
                    
                elif job.inputdata._name == 'DQ2Dataset': 
                    if not job.inputdata.names: raise ApplicationConfigurationError(None,'No inputdata has been specified.')
                    #if not job.inputdata.names: raise Exception('No inputdata has been specified.')
                    input_guids = job.inputdata.guids
                    input_files = job.inputdata.names
                
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD']:
                        job.inputdata.type ='DQ2_LOCAL'
       
                elif job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter':
                    input_files = job.inputdata.names
                    input_guids = job.inputdata.guids

            else:
                if job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)
                    input_guids = get_guids( input_files )

                elif job.inputdata._name == 'NGInputData':
                    input_files = []
                    input_guids = []
                    for f in job.inputdata.names:
                        fs = f.split('/')
                        input_files.append(fs[-1])
                        input_guids.append("00000000-0000-0000-0000-000000000000") #No guids needed, just for input file parsing

                elif job.inputdata._name == 'DQ2Dataset' and job.inputdata.accessprotocol =='GSIDCAP':
                                    
                    contents = job.inputdata.get_contents()
                    input_guids = [ guid for guid, lfn in contents ]

                    # HACK! Mail Johannes about this...
                    if job.splitter==None:
                        #job.inputdata.names=[ lfn  for guid, lfn in contents ]
                        job.inputdata.guids=input_guids
                                                                        
                    if job.inputdata.tagdataset:
                        tag_contents = job.inputdata.get_tag_contents()
                        input_tag_files = [ lfn  for guid, lfn in tag_contents ]
                        input_tag_guids = [ guid for guid, lfn in tag_contents ] 

                    # check and set env. variables for default LFC setup 
                    if not os.environ.has_key('LFC_HOST'):
                        try:
                            os.environ['LFC_HOST'] = config['DefaultLFC']
                        except Ganga.Utility.Config.ConfigError:
                            os.environ['LFC_HOST'] = 'lfc1.ndgf.org'
                            
                    for guid in input_guids:

                        #site = "srm.swegrid.se"
                        site = job.backend.requirements.gsidcap
                        sfn = get_dcap_path(guid,site)

                        if sfn!="":
                            input_files.append(sfn)
                        else:
                            print "Found no replica for guid "+guid+" at "+site+". Removing from inputs."
                            input_guids.remove(guid)

                    # Were all inputs removed?
                    if len(input_guids)==0:
                        raise ApplicationConfigurationError(None,'No inputs found for job %s at site %s.' % (job.getFQID('.'),site))

                    job.inputdata.names = input_files
                        
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD']:
                        job.inputdata.type ='DQ2_LOCAL'

                    
                elif job.inputdata._name == 'DQ2Dataset':
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD']:
                        job.inputdata.type ='DQ2_LOCAL'
                        
                    contents = job.inputdata.get_contents()
                    input_files = [ lfn  for guid, lfn in contents ]
                    input_guids = [ guid for guid, lfn in contents ]

                    # HACK! Mail Johannes about this...
                    if job.splitter==None:
                        job.inputdata.names=input_files
                        job.inputdata.guids=input_guids
                                                                        
                    if job.inputdata.tagdataset:
                        tag_contents = job.inputdata.get_tag_contents()
                        input_tag_files = [ lfn  for guid, lfn in tag_contents ]
                        input_tag_guids = [ guid for guid, lfn in tag_contents ] 


            arg += get_arg( input_files, input_guids )
        else:
            arg += ['0']

        # Outputdataset
        output_location = ''

        if job.outputdata and job.outputdata.location:
            output_location=job.outputdata.location

        elif job.outputdata and job.outputdata.local_location:
            output_location=job.outputdata.local_location

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

        # can be set in config ? output_location=config['NGOutputLocation']
        if job.outputdata and job.outputdata._name=='DQ2OutputDataset':
            # Set a default output if none is provided
            if job.outputdata.location=='':
                job.outputdata.location = 'NDGF-T1_SCRATCHDISK'

        if job.outputdata and job.outputdata._name=='DQ2OutputDataset':
            if job._getRoot().subjobs:
                jobid = "%d" % (job._getRoot().id)
            else:
                jobid = "%d" % job.id

            #username = job.backend.getidentity(True)
            username = self.identity

            # Extract username from certificate 
            #username=""
            # ARC not working 
            # proxy = GridProxy(job.backend.middleware.upper())
            #proxyNG = GridProxy('ARC')
            #print 'AthenaNGRTHandler calling proxy.info '
            #useridARC = proxyNG.info(opt="-identity")
            #print 'AthenaNGRTHandler usridARC ', useridARC
            #proxy = GridProxy()
            #username = proxyNG.identity()
            #print 'AthenaNGRTHandler username ', username

            """
            for line in userid.split('/'):
                if line.startswith('CN='):
                    username = re.sub('^CN=','',line)
                    username = re.sub('\d+$','',username)
                    username = re.sub('[\d() ]','',username)
                    username = username.replace(' ','')
                    username = username.strip()
                    print 'Username from DN ', username
                    break
            """
    
            if username == '':    
                logger.warning('could not get DN from %s', userid)
                username = pwd.getpwuid(os.getuid())[0]
                username = username.strip()

            jobdate = time.strftime('%Y%m%d')

            usertag = configDQ2['usertag']

            if job.outputdata.datasetname:
                # new datasetname during job resubmission
                pat = re.compile(r'^%s\.%s\.ganga' % (usertag,username))
                if re.findall(pat,job.outputdata.datasetname):
                    if job.outputdata.dataset_exists():
                        output_datasetname = job.outputdata.datasetname
                    else:
                        output_datasetname = '%s.%s.ganga.%s.%s' % (usertag, username, jobid, jobdate)
                        
                else:
                    
                    # append user datasetname for new configuration
                    output_datasetname = '%s.%s.ganga.%s' % (usertag, username,job.outputdata.datasetname)

                output_lfn = '%s/%s/ganga/%s/' % (usertag,username,output_datasetname)

            else:
                # No datasetname is given
                output_datasetname = '%s.%s.ganga.%s.%s' % (usertag,username,jobid, jobdate)
                output_lfn = '%s/%s/ganga/%s/' % (usertag,username,output_datasetname)


            #print 'setting output_datasetbname ', output_datasetname
            #print 'output_lfn ',output_lfn  
            
            output_jobid = jid
            job.outputdata.datasetname=output_datasetname

            if not job.outputdata.dataset_exists(output_datasetname):
                if job._getRoot().subjobs:
                    if job.id==0:
                        job.outputdata.create_dataset(output_datasetname)
                        register_dataset(output_datasetname,job.outputdata.location)
                else:
                    job.outputdata.create_dataset(output_datasetname)
                    register_dataset(output_datasetname,job.outputdata.location)
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


        inputbox = [ ]
    
        if input_guids: _append_file_buffer(inputbox,'input_guids',input_guids)
        #inputbox += [ FileBuffer('input_guids','\n'.join(input_guids)+'\n') ]
            
        if input_files: _append_file_buffer(inputbox,'input_files',input_files)
        #inputbox += [ FileBuffer('input_files','\n'.join(input_files)+'\n') ]

        if input_tag_guids: _append_file_buffer(inputbox,'input_tag_guids',input_tag_guids)
        #inputbox += [ FileBuffer('input_tag_guids','\n'.join(input_tag_guids)+'\n') ]

        if input_tag_files: _append_file_buffer(inputbox,'input_tag_files',input_tag_files)
        #if input_esd_guids: _append_file_buffer(inputbox,'input_esd_guids',input_esd_guids)
        #if input_esd_files: _append_file_buffer(inputbox,'input_esd_files',input_esd_files)
        
        if job.outputdata and job.outputdata._name=='DQ2OutputDataset':
            job.outputdata.outputdata = dq2_outputname(job.outputdata.outputdata,output_datasetname,output_jobid,job.outputdata.use_shortfilename)

            if job.outputdata and job.outputdata.outputdata:
                _append_file_buffer(inputbox,'output_files',job.outputdata.outputdata)
            elif job.outputdata and not job.outputdata.outputdata:
                raise ApplicationConfigurationError(None,'j.outputdata.outputdata is empty - Please specify output filename(s).')
            
        exe = os.path.join(os.path.dirname(__file__),'wrapper-athena-ng.sh')
        inputbox.append(File(os.path.join(os.path.dirname(__file__),'athena-ng.sh')))
        #_append_file_buffer(inputbox,'athena-ng.sh',[os.path.join(os.path.dirname(__file__),'athena-ng.sh')])
        outputbox = jobmasterconfig.outputbox
        #outputbox.append("stdout.txt.gz")
        #outputbox.append("stderr.txt.gz")
        requirements = jobmasterconfig.requirements
        #print '%%% prepare requiremants RT', requirements.runtimeenvironment
        environment = jobmasterconfig.env.copy()
        # If ArgSplitter is used
        
        try:
            if job.application.args:
                environment['ATHENA_OPTIONS'] = environment['ATHENA_OPTIONS'] + ' ' + ' '.join(job.application.args)
                if job.application.options:
                    job.application.options = job.application.options + ' ' + job.application.args
                else:
                    job.application.options=job.application.args
        except AttributeError:
            pass
        
        #if output_location:
        #    environment['OUTPUT_LOCATION'] = output_location
        
        # We dont need is at the worker node but we need the output_lfn to costruct the storage path. 
        # Ther is no space in the DQ2OutputDataset schema so lets ise the environment to passe the
        # output_lfn to the backend prepare.
        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            environment['OUTPUT_DATASETNAME'] = output_datasetname
            environment['OUTPUT_LFN'] = output_lfn
            environment['OUTPUT_JOBID'] = output_jobid
            environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
            environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']    

        #print 'environment ', environment

        # append a property for monitoring to the jobconfig of subjobs
        #print 'Creating NGJobConfig in prepare RTHandler, exe ', exe
        #print 'inputbox ', inputbox
        #print 'arg ', arg
        #print 'outputbox ', outputbox
        #print 'environment ', environment
        #print 'requirements rt env ', requirements.runtimeenvironment
        ng_config = NGJobConfig(File(exe), inputbox, arg, outputbox, environment, [], requirements)
        ng_config.monitoring_svc = mc['Athena']
        return ng_config

    def master_prepare( self, app, appconfig):
        """Prepare the master job"""
        
        job = app._getParent() # Returns job or subjob object
        logger.debug("AthenaNGRTHandler master_prepare called, %s", job.id )

        self.identity = job.backend.getidentity(True)

        # Get DQ2Dataset content 
        #if job.inputdata and job.inputdata._name == 'DQ2Dataset':
        #    DQ2Dataset.content = DQ2Dataset.get_dataset(app)
        #    if job.inputdata.tagdataset:
        #        DQ2Dataset.content_tag = DQ2Dataset.get_dataset(app, True)

        # Expand Athena jobOptions
        athena_options = ""
        i = 0
        for ao in app.option_file:
            if i == 0:
                athena_options += os.path.basename(ao.name)
                i += 1
            else:
                athena_options += ' '+os.path.basename(ao.name)
                
        if app.options: athena_options = app.options + ' ' + athena_options

        # LCG:
        # Expand Athena jobOptions
        #athena_options = ' '.join([os.path.basename(opt_file.name) for opt_file in app.option_file])
        #if app.options: athena_options = ' -c ' + app.options + ' ' + athena_options
        #if app.options:
        #    athena_options = app.options + ' ' + athena_options

        athena_usersetupfile = os.path.basename(app.user_setupfile.name)

#       prepare input sandbox

        inputbox = []

        for ao in app.option_file:
            inputbox.append(ao)

        #inputbox = [ 
        #    File(app.option_file.name)
        #]
        #if job.inputdata and job.inputdata._name == 'ATLASDataset':
        #    inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-stagein.py')) ]
                    
        if app.user_area.name: inputbox += [ File(app.user_area.name) ]
            
        if app.user_setupfile.name: inputbox += [ File(app.user_setupfile.name) ]
        
        # NO STAGEINIG IN JOB
        #if job.inputdata and job.inputdata._name == 'DQ2Dataset':
        #    inputbox += [
        #        File(os.path.join(os.path.dirname(__file__),'ganga-stagein-dq2.py')),
        #        File(os.path.join(os.path.dirname(__file__),'dq2_get'))
        #        ]

        #if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
        #    inputbox += [
        #        File(os.path.join(os.path.dirname(__file__),'ganga-stageout-dq2.py')),
        #        File(os.path.join(os.path.dirname(__file__),'ganga-joboption-parse.py')),
        #        ]

        # NO STAGEIN
        #if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
        #    if not File(os.path.join(os.path.dirname(__file__),'ganga-stage-in-out-dq2.py')) in inputbox:
        #        inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-stage-in-out-dq2.py'))]
        #        inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-joboption-parse.py')) ]

        # Add move-linked-files-here.py to input sandbox
        inputbox += [
            File(os.path.join(os.path.dirname(__file__),'move-linked-files-here.py'))
            ]
                                                    
        if (job.inputsandbox):
            for file in job.inputsandbox:
                inputbox += [ file ]
                 
#       prepare environment

        environment={   
            'ATHENA_OPTIONS' : athena_options,
            'ATHENA_USERSETUPFILE' : athena_usersetupfile,
            'ATLAS_PROJECT' : app.atlas_project
        }

        # AtlasPoint1 update - not tested
        if app.atlas_production and app.atlas_project != 'AtlasPoint1':
            temp_atlas_production = re.sub('\.','_',app.atlas_production)
            prod_url = config['PRODUCTION_ARCHIVE_BASEURL']+'/AtlasProduction_'+ temp_atlas_production +'_noarch.tar.gz'
            logger.info('Using Production cache from: %s', prod_url)
            environment['ATLAS_PRODUCTION_ARCHIVE'] = prod_url

        if app.atlas_production and app.atlas_project == 'AtlasPoint1':
            environment['ATLAS_PRODUCTION'] = app.atlas_production

        # GROUP_AREA update - not tested
        if app.group_area.name:
            if str(app.group_area.name).find('http')>=0:
                environment['GROUP_AREA_REMOTE'] = str(app.group_area.name)
            else:
                environment['GROUP_AREA'] = os.path.basename(app.group_area.name)
        
        if app.user_area.name: environment['USER_AREA']=os.path.basename(app.user_area.name)
        if app.max_events: environment['ATHENA_MAX_EVENTS']=app.max_events

        # Get any special database release info
        if app.atlas_dbrelease:
            dbrl = app.atlas_dbrelease.split(':')
            if len(dbrl)>1:
                environment['DBDATASETNAME'] = dbrl[0]
                environment['DBFILENAME'] = dbrl[1]
            elif len(dbrl)==1:
                environment['DBFILENAME'] = dbrl[0]

        # Set application exe type in environment variable
        environment['ATHENA_EXE_TYPE']=app.atlas_exetype

        # Add special log files to be able to compress athena mega-logs
        environment['ATHENA_STDOUT'] = 'stdout.txt'
        environment['ATHENA_STDERR'] = 'stderr.txt'
        
        requirements = NGRequirements()
        
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.inputdata.dataset:
                datasetname = job.inputdata.dataset
                environment['DATASETNAME']= datasetname
                #environment['DATASETLOCATION'] = ':'.join(job.inputdata.list_locations(datasetname,quiet=True))
                #environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
                #environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']
                #environment['DATASETTYPE']=job.inputdata.type
#       Restrict CE to list provided from DQ2 system with ToA info
            if job.inputdata.match_ce_all:
                ces = job.inputdata.list_locations_ce(datasetname,quiet=True)
                #print '%%%  inpytdata match True ces: ', ces
                #job.backend.requirements.other += ['( %s )' % ' || '.join([ 'RegExp("%s",other.GlueCEInfoHostName)' % ce for ce in ces])]
                #requirements.other += ['( %s )' % ' || '.join([ 'RegExp("%s",other.GlueCEInfoHostName)' % ce for ce in ces])]
            if job.inputdata.tagdataset:
                environment['TAGDATASETNAME']= job.inputdata.tagdataset
                
#       prepare job requirements

        # Find the athena release, either from athena_production or athena_release if the first is not set
        if app._name == 'Athena':
            if app.atlas_release == '':
                raise ConfigError("j.application.atlas_release='' - No ATLAS release version found by prepare() or specified.")
            else:
                environment['ATLAS_RELEASE'] = app.atlas_release
                current_release = app.atlas_release
                if app.atlas_production != '':
                    current_release = app.atlas_production
                    environment['ATLAS_PRODUCTION'] = app.atlas_production                
                requirements.runtimeenvironment += [ 'APPS/HEP/ATLAS-%s' %  current_release ]

#       inputdata
        inputdata = []
        #if job.inputdata and job.inputdata._name == 'ATLASDataset':
        #    inputdata = [ 'lfn:%s' % lfn for lfn in input_files ]
        
#       jobscript
        exe = os.path.join(os.path.dirname(__file__),'wrapper-athena-ng.sh')
        #print '%%% This is exe ', exe

#       output sandbox
        outputbox = []
        #outputGUIDs='output_guids'
        #outputLOCATION='output_location'
        try:
            outputData = job.outputdata.outputdata
            outputbox += outputData 
        except:
            pass
        
        if (job.outputsandbox):
            for file in job.outputsandbox:
                outputbox += [ file ]

        #print '%%% Caling NGJobConfig'         
        return NGJobConfig(File(exe),inputbox,[],outputbox,environment,inputdata,requirements) 

def dq2_outputname(output_files,datasetname,output_jobid,use_short_filename):

    new_output_files = []
    for file in output_files:
        temptime = time.gmtime()
        output_datasetname = re.sub('\.[\d]+$','',datasetname)
        pattern=output_datasetname+".%04d%02d%02d%02d%02d%02d._%05d."+file

        i=output_jobid.split('.')
        if len(i)>1:
            new_output_file = pattern % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],int(i[1])+1)
            short_pattern = ".%04d%02d%02d%02d%02d%02d._%05d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],int(i[1])+1)
        else:
            new_output_file = pattern % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],1)
            short_pattern = ".%04d%02d%02d%02d%02d%02d._%05d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],1)
                    
        new_short_output_file = re.sub(".root", short_pattern+".root" , file )
        #print 'new_short_output_file 1',new_short_output_file
        if new_short_output_file == file:
            new_short_output_file =  short_pattern[1:] + "." + file
        #print 'new_short_output_file 2',new_short_output_file

        #filenew = file+"."+output_jobid
        if use_short_filename:
            new_output_files += [new_short_output_file]
        else:
            new_output_files += [new_output_file]

    return new_output_files

def get_arg( input_files, input_guids ):
    arg = []
    arg += [str(len(input_files))]
    n = len( input_files )
    i = 0
    while i < n:
        arg += [input_files[i].split('/')[-1]]
        arg += [input_guids[i]]
        i = i+1

    #print '%%% arg prepared ', arg     
    return arg

def register_dataset(datasetname,siteID):

    dq2=DQ2()
    if dq2.listDatasetReplicas(datasetname)=={}:
        dq2.registerDatasetLocation(datasetname, siteID)
    else:
        print 'Dataset already registered ', datasetname 
    return

def get_dcap_path(guid, requiredhost=""):

    # Assume that LFC_HOST has been set
    
    # Get the replicas 
    l = lfc.lfc_getreplica("",guid,"")
    replicas = l[1]
    replica = -1

    #for r in replicas:
    #    print r.host

    # Did we get anything?
    if len(replicas)==0:
        return ""

    # Check if file exists at required host
    if requiredhost!="":
        for i in range(len(replicas)):
            #print replicas[i].host
            if replicas[i].host==requiredhost:
                replica = i
                break
    if replica<0:
        return ""

    # Pick out the srm path and host
    sfn = replicas[replica].sfn

    # Get the host
    host = replicas[replica].host

    # Turn the srm path into a gsidcap one
    sfn = sfn.replace("srm://","gsidcap://")
    if requiredhost=='srm.swegrid.se':
        sfn = sfn.replace(host,"%s:22128/pnfs/swegrid.se/data" % host)
    else:
        sfn = sfn.replace(host,"%s:22128" % host)

    #print sfn
    
    return sfn


def get_guids(input_files):

    input_guids = []
    for f in input_files:
        rc, out = commands.getstatusoutput('pool_extractFileIdentifier '+f)
        if rc == 0:
            for line in out.split():
                match = re.search('^([\w]+-[\w]+-[\w]+-[\w]+-[\w]+)',line)
                if match:
                    poolguid = match.group(1)
                    print 'poolguid: %s' %poolguid
                    input_guids.append( match.group(1) )
                    
    #print '%%% input_guids', input_guids            
    return input_guids

allHandlers.add('Athena','NG',AthenaNGRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
logger = getLogger('Athena')

# $Log: not supported by cvs2svn $
# Revision 1.14  2009/06/25 09:05:36  bsamset
# Changed to using wrapper-athena-ng.sh
#
# Revision 1.13  2009/06/24 09:09:53  bsamset
# Added direct gsidcap access functionality
#
# Revision 1.12  2009/06/12 09:39:40  bsamset
# Added functionality to use a user-speficied database release, as set in j.application.atlas_dbrelease. Same syntax as on lcg.
#
# Revision 1.11  2009/06/02 10:40:18  bsamset
# Re-fixed a bug for treating ATLAS_PRODUCTION in rel. 14-series
#
# Revision 1.10  2009/05/30 08:49:06  bsamset
# Removed a remaining reference to USERDISK
#
# Revision 1.9  2009/05/28 09:41:22  bsamset
# Added gziping of athena log files, settable log file names through environment variables etc. Also fixed the propagation of atlas_production (again). Note: This update looses us live log file peeking of athena jobs. Must look into this later.
#
# Revision 1.8  2009/02/19 13:36:36  bsamset
# Added capability to move files to local disk from symlinks
#
# Revision 1.7  2009/02/17 13:17:17  bsamset
# Fixed guid handling
#
# Revision 1.6  2009/02/11 13:05:40  bsamset
# Removed reference to outputdata.use_dataset
#
# Revision 1.5  2008/12/05 11:26:24  bsamset
# Take lfc, srm info from ToA, allow for writing to remote storage, add timing info for HammerCloud
#
# Revision 1.4  2008/11/27 10:21:22  bsamset
# Removed superflous useridARC check
#
# Revision 1.3  2008/11/11 15:19:30  pajchel
# uertag in dataset name and outpu_lfn
#
# Revision 1.2  2008/10/21 09:28:32  bsamset
# Added ARA support, setup of local databases
#
# Revision 1.1  2008/07/17 16:41:29  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.14  2008/05/20 18:59:18  pajchel
# Minitoring, userid updates
#
# Revision 1.12  2008/05/05 12:10:31  pajchel
# ATLAS_REALEASE env
#
# Revision 1.10  2008/04/29 15:37:21  pajchel
# dq2 support
#
# Revision 1.1  2008/03/31 20:22:54  bsamset
# Added some DQ2 code, made for 4.4.0 release, needs updates!
#
# Revision 1.7  2007/04/13 11:32:30  bsamset
# Added hack to get dq2 list of names and files without splitter job.
#
# Revision 1.6  2007/04/10 08:28:42  bsamset
# Removed some debug statements
#
# Revision 1.5  2007/04/10 06:17:49  pajchel
# can run without local_lcation or location
#
# Revision 1.4  2007/03/21 15:36:03  bsamset
# Fixed more DQ2Dataset bugs, now job splitting also works (again). AtlasLocalDataset is fixed. arc package is now nordugrid-arc-standalone
#
# Revision 1.3  2007/03/20 15:15:09  bsamset
# Updated to work with 4.3.0 version of DQ2Dataset.
#
# Revision 1.2  2007/03/19 18:09:36  bsamset
# Several bugfixes, added arc middleware as external package
#
# Revision 1.1  2007/02/28 13:45:11  bsamset
# Initial relase of GangaNG
#

