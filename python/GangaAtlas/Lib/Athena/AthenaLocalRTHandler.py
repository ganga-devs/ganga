###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaLocalRTHandler.py,v 1.7 2008-08-19 14:05:03 elmsheus Exp $
###############################################################################
# Athena Local Runtime Handler
#
# ATLAS/ARDA

import os, socket, pwd, commands, re, string

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaAtlas.Lib.ATLASDataset import ATLASDataset, isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE, getIncompleteLocations
from GangaAtlas.Lib.ATLASDataset import ATLASCastorDataset
from GangaAtlas.Lib.ATLASDataset import ATLASLocalDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset

from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Credentials import GridProxy
  
class AthenaLocalRTHandler(IRuntimeHandler):
    """Athena Local Runtime Handler"""
    
    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("AthenaLocalRTHandler prepare called, %s", job.id )

        input_files = []
        input_guids = []
        input_tag_files = []
        input_tag_guids = []
        input_esd_files = []
        input_esd_guids = []

        # If job has inputdata
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
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD' ]:
                        job.inputdata.type ='DQ2_LOCAL'
       
            else:
                if job.inputdata._name == 'ATLASCastorDataset':
                    input_files = ATLASCastorDataset.get_filenames(app)

                elif job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)

                elif job.inputdata._name == 'ATLASDataset':
                    input_files = ATLASDataset.get_filenames(app)

                elif job.inputdata._name == 'DQ2Dataset':
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TNT_LOCAL', 'TNT_DOWNLOAD' ]:
                        job.inputdata.type ='DQ2_LOCAL'

                    contents = job.inputdata.get_contents()
                    input_files = [ lfn  for guid, lfn in contents ]
                    input_guids = [ guid for guid, lfn in contents ]

                    if job.inputdata.tagdataset:
                        tag_contents = job.inputdata.get_tag_contents()
                        input_tag_files = [ lfn  for guid, lfn in tag_contents ]
                        input_tag_guids = [ guid for guid, lfn in tag_contents ] 
                    if job.inputdata.use_aodesd_backnav:
                        esd_contents = job.inputdata.get_contents(backnav=True)
                        input_esd_files = [ lfn  for guid, lfn in esd_contents ]
                        input_esd_guids = [ guid for guid, lfn in esd_contents ]                        

                    job.inputdata.names = input_files          
                    job.inputdata.guids = input_guids          
 
        # Outputdataset
        output_location=''
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

            elif job.outputdata.location=='' and job.outputdata._name=='DQ2OutputDataset':
                output_location = ''
            elif job.outputdata.location:
                output_location = expandfilename(job.outputdata.location)
            else:
                try:
                    output_location=config['LocalOutputLocation']
                    if job.outputdata:
                        job.outputdata.location = expandfilename(output_location)
                except ConfigError:
                    logger.warning('No default output location specified in the configuration.')
        else:
            try:
                output_location=config['LocalOutputLocation']
            except ConfigError:
                logger.warning('No default output location specified in the configuration.')

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

            import time
            tempdate = time.localtime()
            jobdate = "%04d%02d%02d" %(tempdate[0],tempdate[1],tempdate[2])

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

        inputbox = [ ]
                
        if input_guids:
            inputbox += [ FileBuffer('input_guids','\n'.join(input_guids)+'\n') ]

        if input_files: 
            inputbox += [ FileBuffer('input_files','\n'.join(input_files)+'\n') ]

        if input_tag_guids:
            inputbox += [ FileBuffer('input_tag_guids','\n'.join(input_tag_guids)+'\n') ]

        if input_tag_files: 
            inputbox += [ FileBuffer('input_tag_files','\n'.join(input_tag_files)+'\n') ]

        if input_esd_guids:
            inputbox += [ FileBuffer('input_esd_guids','\n'.join(input_esd_guids)+'\n') ]

        if input_esd_files: 
            inputbox += [ FileBuffer('input_esd_files','\n'.join(input_esd_files)+'\n') ]

        if job.outputdata and job.outputdata.outputdata:
            inputbox += [ FileBuffer('output_files','\n'.join(job.outputdata.outputdata)+'\n') ]
        elif job.outputdata and not job.outputdata.outputdata:
            raise ApplicationConfigurationError(None,'j.outputdata.outputdata is empty - Please specify output filename(s).')
   
        exe = os.path.join(os.path.dirname(__file__),'athena-local.sh')
        outputbox = jobmasterconfig.outputbox
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
        environment['OUTPUT_LOCATION'] = output_location
        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            environment['OUTPUT_DATASETNAME'] = output_datasetname
            environment['OUTPUT_LFN'] = output_lfn
            environment['OUTPUT_JOBID'] = output_jobid
            environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
            environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']
            if job.outputdata.use_shortfilename:
                environment['GANGA_SHORTFILENAME'] = 1
            else:
                environment['GANGA_SHORTFILENAME'] = ''
            try:
                environment['GANGA_GLITE_UI']=configLCG['GLITE_SETUP']
            except:
                pass
            environment['DQ2_OUTPUT_SPACE_TOKENS']= ':'.join(configDQ2['DQ2_OUTPUT_SPACE_TOKENS'])
            environment['DQ2_BACKUP_OUTPUT_LOCATIONS']= ':'.join(configDQ2['DQ2_BACKUP_OUTPUT_LOCATIONS'])
            
        # CN: extra condition for TNTSplitter
        if job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter':
            # set up dq2 environment
            datasetname = j.inputdata.dataset
            environment['DATASETNAME']= ':'.join(datasetname) 
            environment['DATASETLOCATION'] = ':'.join(j.inputdata.get_locations())
            environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
            environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']
            environment['DATASETTYPE']=j.inputdata.type
            if job.inputdata.accessprotocol:
                 environment['DQ2_LOCAL_PROTOCOL'] = job.inputdata.accessprotocol
            if job.inputsandbox: inputbox += job.inputsandbox   

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)

    def master_prepare( self, app, appconfig ):
        """Prepare the master job"""
        
        job = app._getParent() # Returns job or subjob object

        logger.debug("AthenaLocalRTHandler master_prepare called, %s", job.id)

        # Expand Athena jobOptions
        if not app.option_file:
            raise ConfigError("j.application.option_file='' - No Athena jobOptions files specified.")

        athena_options = ''
        inputbox = []
        for option_file in app.option_file:
            athena_option = os.path.basename(option_file.name)
            athena_options += ' ' + athena_option
            if app.options:
                athena_options =  app.options + ' ' + athena_options
            inputbox += [ File(option_file.name) ]

        athena_usersetupfile = os.path.basename(app.user_setupfile.name)

#       prepare input sandbox

        if app.user_setupfile.name: inputbox += [ File(app.user_setupfile.name) ]
        #CN: added extra test for TNTJobSplitter
        if job.inputdata and job.inputdata._name == 'DQ2Dataset' or (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
            inputbox += [
                File(os.path.join(os.path.dirname(__file__),'ganga-stage-in-out-dq2.py')),
                File(os.path.join(os.path.dirname(__file__),'dq2_get')),
                File(os.path.join(os.path.dirname(__file__),'dq2info.tar.gz'))
                ]

        if job.inputdata and job.inputdata._name == 'ATLASDataset':
            if job.inputdata.lfc:
                inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-stagein-lfc.py')) ]
            else:
                inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-stagein.py')) ]

        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            if not job.outputdata.location:
                raise ApplicationConfigurationError(None,'j.outputdata.location is empty - Please specify a DQ2 output location - job not submitted !')
            if not File(os.path.join(os.path.dirname(__file__),'ganga-stage-in-out-dq2.py')) in inputbox:
                inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-stage-in-out-dq2.py')),
                                File(os.path.join(os.path.dirname(__file__),'dq2info.tar.gz')) ]

            inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-joboption-parse.py')) ]

        if job.inputsandbox:
            for file in job.inputsandbox:
                inputbox += [ file ]
        if app.user_area.name:
            inputbox += [ File(app.user_area.name) ]
        if app.group_area.name and string.find(app.group_area.name,"http")<0:
            inputbox += [ File(app.group_area.name) ]
   
#       prepare environment

        try:
            atlas_software = config['ATLAS_SOFTWARE']
        except ConfigError:
            raise ConfigError('No default location of ATLAS_SOFTWARE specified in the configuration.')

        if app.atlas_release=='':
            raise ConfigError("j.application.atlas_release='' - No ATLAS release version found by prepare() or specified.")
      
        environment={ 
            'ATLAS_RELEASE' : app.atlas_release,
            'ATHENA_OPTIONS' : athena_options,
            'ATLAS_SOFTWARE' : atlas_software,
            'ATHENA_USERSETUPFILE' : athena_usersetupfile,
            'ATLAS_PROJECT' : app.atlas_project,
            'ATLAS_EXETYPE' : app.atlas_exetype
        }
        if app.atlas_environment:
            for var in app.atlas_environment:
                vars=var.split('=')
                if len(vars)==2:
                    environment[vars[0]]=vars[1]

        if app.atlas_production and (app.atlas_project == 'AtlasPoint1' or app.atlas_release.find('12.')<=0):
            environment['ATLAS_PRODUCTION'] = app.atlas_production 

        if app.user_area.name: 
            environment['USER_AREA'] = os.path.basename(app.user_area.name)
        if app.group_area.name:
            if string.find(app.group_area.name,"http")>=0:
                environment['GROUP_AREA_REMOTE'] = "%s" % (app.group_area.name)
            else:
                environment['GROUP_AREA']=os.path.basename(app.group_area.name)

        if app.max_events: 
            max_events = app.max_events
            if max_events.__class__.__name__ == 'int':
                max_events = str(max_events)
            environment['ATHENA_MAX_EVENTS'] = max_events

        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.inputdata.dataset:
                datasetname = job.inputdata.dataset
                environment['DATASETNAME']=':'.join(datasetname)
                environment['DATASETLOCATION'] = ':'.join(job.inputdata.get_locations())
                environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
                environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']
                environment['DATASETTYPE']=job.inputdata.type
                if job.inputdata.accessprotocol:
                    environment['DQ2_LOCAL_PROTOCOL'] = job.inputdata.accessprotocol                

                try:
                    environment['X509CERTDIR']=os.environ['X509_CERT_DIR']
                except KeyError:
                    environment['X509CERTDIR']=''

                try:
                    proxy = os.environ['X509_USER_PROXY']
                except KeyError:
                    proxy = '/tmp/x509up_u%s' % os.getuid()

                REMOTE_PROXY = '%s:%s' % (socket.getfqdn(),proxy)
                environment['REMOTE_PROXY'] = REMOTE_PROXY
                try:
                    environment['GANGA_GLITE_UI']=configLCG['GLITE_SETUP']
                except:
                    pass

            else:
                raise ConfigError("j.inputdata.dataset='' - DQ2 dataset name needs to be specified.")
            
            if job.inputdata.tagdataset:
                environment['TAGDATASETNAME']= job.inputdata.tagdataset

        exe = os.path.join(os.path.dirname(__file__), 'athena-local.sh')

#       output sandbox
        outputbox = [ ]
        outputGUIDs='output_guids'
        outputLOCATION='output_location'
        outputDATA='output_data'
        outputbox.append( outputGUIDs )
        outputbox.append( outputLOCATION )
        outputbox.append( outputDATA )
        if (job.outputsandbox):
            for file in job.outputsandbox:
                outputbox += [ file ]

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)

class AthenaRemoteRTHandler(IRuntimeHandler):
    """Athena Remote Runtime Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""
        
        be_name = app._getParent().backend.remote_backend._name

        if be_name == "LCG" or be_name == "Condor" or be_name == "Cronus":
            rt_handler = AthenaLCGRTHandler()
            return rt_handler.prepare(app,appsubconfig,appmasterconfig,jobmasterconfig)
        else:
            rt_handler = AthenaLocalRTHandler()
            return rt_handler.prepare(app,appsubconfig,appmasterconfig,jobmasterconfig)
        

    def master_prepare(self,app,appmasterconfig):
        be_name = app._getParent().backend.remote_backend._name

        if be_name == "LCG" or be_name == "Condor" or be_name == "Cronus":
            rt_handler = AthenaLCGRTHandler()
            return rt_handler.master_prepare(app,appmasterconfig)
        else:
            rt_handler = AthenaLocalRTHandler()
            return rt_handler.master_prepare(app,appmasterconfig)


allHandlers.add('Athena', 'Local', AthenaLocalRTHandler)
allHandlers.add('Athena', 'LSF'  , AthenaLocalRTHandler)
allHandlers.add('Athena', 'PBS'  , AthenaLocalRTHandler)
allHandlers.add('Athena', 'SGE'  , AthenaLocalRTHandler)
allHandlers.add('Athena', 'Remote'  , AthenaRemoteRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configLCG = getConfig('LCG')
logger = getLogger()


#$Log: not supported by cvs2svn $
#Revision 1.6  2008/07/29 13:21:42  elmsheus
#Add  AthenaRemoteRTHandler class for the Remote backend
#
#Revision 1.5  2008/07/29 10:08:32  elmsheus
#Remove DQ2_OUTPUT_LOCATIONS again
#
#Revision 1.4  2008/07/28 16:56:31  elmsheus
#* ganga-stage-in-out-dq2.py:
#  - Add fix for DPM setup for NIKHEF and SARA
#  - remove special SE setup for RAL
#  - add MPPMU defaultSE=lcg-lrz-se.lrz-muenchen.de
#  - add DQ2_BACKUP_OUTPUT_LOCATIONS reading
#  - add DQ2_OUTPUT_SPACE_TOKENS reading
#  - change stage-out order for DQ2OutputDataset:
#  (1) j.outputdata.location, (2) DQ2_OUTUT_LOCATIONS,
#  (3) DQ2_BACKUP_OUTPUT_LOCATIONS (4) [ siteID ]
#
#* Add config options config['DQ2']:
#  DQ2_OUTPUT_SPACE_TOKENS: Allowed space tokens names of
#                           DQ2OutputDataset output
#  DQ2_OUTPUT_LOCATIONS: Default locations of
#                        DQ2OutputDataset output
#  DQ2_BACKUP_OUTPUT_LOCATIONS: Default backup locations of
#                               DQ2OutputDataset output
#
#* AthenaLCGRTHandler/AthenaLocalRTHandler
#  Enforce setting of j.outputdata.location for DQ2OutputDataset
#
#Revision 1.3  2008/07/28 14:44:13  elmsheus
#Add Remote backend submission to AthenaLocalRTHandler
#
#Revision 1.2  2008/07/28 14:27:34  elmsheus
#* Upgrade to DQ2Clients 0.1.17 and DQ2 API
#* Add full support for DQ2 container datasets in DQ2Dataset
#* Change in DQ2OutputDataset.retrieve(): use dq2-get
#* Fix bug #39286: Athena().atlas_environment omits type_list
#
#Revision 1.1  2008/07/17 16:41:18  moscicki
#migration of 5.0.2 to HEAD
#
#the doc and release/tools have been taken from HEAD
#
#Revision 1.53.2.9  2008/07/12 09:15:25  elmsheus
#Fix bug #38202
#
#Revision 1.53.2.8  2008/07/12 08:58:12  elmsheus
#* DQ2JobSplitter.py: Add numsubjobs option - now jobs can also be
#  splitted by number of subjobs
#* Athena.py: Introduce Athena.atlas_exetype, choices: ATHENA, PYARA, ROOT
#  Execute the following executable on worker node:
#  ATHENA: athena.py jobsOptions input.py
#  PYARA: python jobOptions
#  ROOT: root -q -b jobOptions
#* ganga-stage-in-out-dq2.py: produce now in parallel to input.py also a
#  flat file input.txt containing the inputfiles list. This files can be
#  read in but PYARA or ROOT application flow
#* Change --split and --splitfiles to use DQ2JobSplitter if LCG backend is used
#* Add --athena_exe ATHENA or PYARA or ROOT (see above)
#
#Revision 1.53.2.7  2008/06/27 14:24:22  elmsheus
#* DQ2JobSplitter: Change from AMGA siteindex to location file catalog
#* Expand and fix DQ2Dataset.list_location_siteindex()
#* Correct Local() backend dataset list problem, bug #38202
#* Change pybin behaviour in athena-local.sh and athena-lcg.sh
#
#Revision 1.53.2.6  2008/06/02 10:23:33  elmsheus
#Add GANGA_GLITE_UI environement is DQ2Dataset/DQ2OutputDataset is used in athena-local.sh
#
#Revision 1.53.2.5  2008/05/26 19:55:28  elmsheus
#Update AtlasProduction handling, add Athena.atlas_production
#
#Revision 1.53.2.4  2008/04/07 16:28:30  elmsheus
#ATLAS_PROJECT and install.sh fix
#
#Revision 1.53.2.3  2008/03/07 20:26:22  elmsheus
#* Apply Ganga-5-0-restructure-config-branch patch
#* Move GangaAtlas-4-15 tag to GangaAtlas-5-0-branch
#
#Revision 1.53.2.2  2008/02/18 11:03:23  elmsheus
#Copy GangaAtlas-4-13 to GangaAtlas-5-0-branch and config updates
#
#Revision 1.56  2008/02/12 12:30:58  nicholc
#improve operation of GangaTnt
#
#Revision 1.55  2008/02/04 16:34:33  elmsheus
#* Rewrite of ganga-stage-in-out-dq2.py:
#  - uses now dq2.info.TiersOfATLAS library instead of direct
#    usage of TiersOfAtlasCache.py
#  - new host indentification algorithm
#  - new improved stage-out procedure
#* Fix for storage element access problems at UAM, RAL
#
#Revision 1.54  2008/01/21 15:56:14  elmsheus
#Allow AtlasPoint1 setup
#
#Revision 1.56  2008/02/12 12:30:58  nicholc
#improve operation of GangaTnt
#
#Revision 1.55  2008/02/04 16:34:33  elmsheus
#* Rewrite of ganga-stage-in-out-dq2.py:
#  - uses now dq2.info.TiersOfATLAS library instead of direct
#    usage of TiersOfAtlasCache.py
#  - new host indentification algorithm
#  - new improved stage-out procedure
#* Fix for storage element access problems at UAM, RAL
#
#Revision 1.54  2008/01/21 15:56:14  elmsheus
#Allow AtlasPoint1 setup
#
#Revision 1.53  2007/11/04 14:24:38  elmsheus
#Fix missing user area
#
#Revision 1.52  2007/10/03 17:58:56  elmsheus
#* Add remote group area downloading support
#* Add seconds to filename time stamp
#
#Revision 1.51  2007/09/25 21:40:29  liko
#Improve error messages
#
#Revision 1.50  2007/07/16 11:40:13  elmsheus
#* Fix groupArea unpacking for Local()
#* Fix 13.0.10 for Local()
#* Change GUIPrefs of Athena.option_file
#
#Revision 1.49  2007/05/30 12:05:02  elmsheus
#Add empty outputdata.outputdata exception
#
#Revision 1.48  2007/05/28 15:11:30  elmsheus
#* Introduce AtlasProduction cache setup with Athena.atlas_production
#* Enable 1 file per job splitting with AthenaSplitterJob.match_subjobs_files=True
#* Catch non-LFC bulk exception
#* Change wrong logging to 'GangaAtlas'
#
#Revision 1.47  2007/04/13 12:12:11  elmsheus
#Add guid/lfn for unsplitted jobs
#
#Revision 1.46  2007/04/02 09:55:45  elmsheus
#* Add number_of_files option in DQ2Dataset
#* Update splitting etc to new get_contents method
#
#Revision 1.45  2007/03/13 15:39:59  elmsheus
#Fix date of dataset
#
#Revision 1.44  2007/03/13 13:45:21  elmsheus
#* Change default values of Athena.options and max_events and
#  convert max_events to str
#* Change logic of DQ2Dataset submission:
#  - Remove DQ2Dataset.match_ce
#  - by default jobs are sent to complete dataset locations
#  - with DQ2Dataset.match_ce_all=True jobs are sent to complete and
#    incomplete sources
#* Clean code in ganga-stage-in-out-dq2.py,
#  - use lcg-info for storage type identification
#  - VO_ATLAS_DEFAULT_SE as third option for host identification
#
#Revision 1.43  2007/03/07 15:01:45  elmsheus
#Use GridProxy.identity for username
#
#Revision 1.42  2007/03/07 12:54:08  elmsheus
#Fix missing -c for athena.options
#
#Revision 1.41  2007/03/06 11:13:37  elmsheus
#Add SGE and Cronus
#
#Revision 1.40  2007/03/05 15:40:48  elmsheus
#Small fixes
#
#Revision 1.39  2007/03/05 09:55:00  liko
#DQ2Dataset leanup
#
#Revision 1.38  2007/02/28 08:52:38  elmsheus
#Add multiple jobOptions files - schema change
#
#Revision 1.37  2007/02/23 09:34:38  elmsheus
#Change DQ2OutputDataset pfn,lfn,surl
#
#Revision 1.36  2007/02/21 16:44:02  elmsheus
#Change DQ2OutputDataset.dataset format, port changes to AthenaLocalRTHandler
#
#Revision 1.35  2007/02/13 09:12:04  elmsheus
#Remove duplication of dq2_get in inputsandbox
#
#Revision 1.34  2007/02/12 15:31:42  elmsheus
#Port 4.2.8 changes to head
#Fix job.splitter in Athena*RTHandler
#
#Revision 1.33  2007/01/30 11:19:41  elmsheus
#Port last changes from 4.2.7
#
#Revision 1.32  2007/01/22 09:50:25  elmsheus
#* Initial import of Tnt plugin of Caitriana Nicholson and Mike Kenyon
#
#Revision 1.31  2007/01/20 20:44:05  liko
#Fix small bug in config['ATLAS_SOFTWARE']
#
#Revision 1.30  2006/12/21 17:21:42  elmsheus
#* Remove DQ2 curl functionality
#* Introduce dq2_client library and port all calls
#* Remove curl calls and use urllib instead
#* Remove ganga-stagein-dq2.py and ganga-stageout-dq2.py and merge into
#  new ganga-stage-in-out-dq2.py
#* Move DQ2 splitting from Athena*RTHandler.py into AthenaSplitterJob
#  therefore introduce new field DQ2Dataset.guids
#* Use AthenaMC mechanism to register files in DQ2 also for Athena plugin
#  ie. all DQ2 communication is done in the Ganga UI
#
#Revision 1.29  2006/11/27 12:18:03  elmsheus
#Fix CVS merging errors
#
#Revision 1.28  2006/11/24 15:39:13  elmsheus
#Small fixes
#
#Revision 1.27  2006/11/24 13:32:37  elmsheus
#Merge changes from Ganga-4-2-2-bugfix-branch to the trunk
#Add Frederics changes and improvement for AthenaMC
#
#Revision 1.26.2.6  2006/11/22 16:46:50  elmsheus
#More fixes for DQ2Output renaming
#
#Revision 1.26.2.5  2006/11/22 14:20:53  elmsheus
#* introduce prefix_hack to lcg-cp/lr calls in
#  ATLASOutputDataset.retrieve()
#* fixed double downloading feature in
#  ATLASOutputDataset.retrieve()
#* move download location for ATLASOutputDataset.retrieve()
#  to job.outputdir from temp directory if local_location is not given
#* Print out clear error message if cmt parsing fails in Athena.py
#* Migrate to GridProxy library in Athena*RTHandler.py
#* Changes in output renaming schema for DQ2OutputDataset files
#
#* Fix proxy name bug in AthenaMCLCGRTHandler.py
#* Fix path problem in wrapper.sh
#
#Revision 1.26.2.4  2006/11/14 12:29:22  elmsheus
#Add args to options
#
#Revision 1.26.2.3  2006/11/13 13:38:04  elmsheus
#Add ArgSplitter.args to ATLAS_OPTIONS
#
#Revision 1.26.2.2  2006/11/10 15:55:36  elmsheus
#Fix missing DQ2_URL_SERVER
#
#Revision 1.26.2.1  2006/11/10 11:22:57  elmsheus
#Fix missing commands
#
#Revision 1.26  2006/10/23 07:21:00  elmsheus
#User certifcate name and users for output storage
#
#Revision 1.25  2006/10/12 15:17:21  elmsheus
#Fix for actually one DQ2 call
#
#Revision 1.24  2006/10/12 09:04:54  elmsheus
#DQ2 code clean-up
#
#Revision 1.23  2006/10/09 09:18:15  elmsheus
#Introduce shared inbox for job submission
#
#Revision 1.22  2006/09/29 12:23:03  elmsheus
#Small fixes
#
#Revision 1.21  2006/09/09 09:35:15  elmsheus
#Fix missing outputsandbox
#
#Revision 1.20  2006/09/09 09:13:53  elmsheus
#Fix missing DQ2Outputdataset name
#
#Revision 1.19  2006/09/09 08:16:33  elmsheus
#Change cert location for local jobs
#
#Revision 1.18  2006/09/08 16:11:45  elmsheus
#Expand SimpleItem directory variables with expandfilenames
#
#Revision 1.17  2006/09/08 12:13:44  elmsheus
#Fix local cert problem
#
#Revision 1.16  2006/09/07 12:41:45  elmsheus
#Fix bug ATLAS_RELEASE, Add cert to inbox for local job
#
#Revision 1.15  2006/08/14 12:40:29  elmsheus
#Fix dataset handling during job submission, add match_ce flag for DQ2Dataset, enable ATLASDataset also for Local backend
#
#Revision 1.14  2006/08/10 15:56:10  elmsheus
#Introduction of TAG analysis, dq2_get updates, minor bugfixes
#
#Revision 1.13  2006/07/31 16:06:19  elmsheus
#Fix output_location bug
#
#Revision 1.12  2006/07/31 13:44:16  elmsheus
#DQ2 updates, adapt to framework changes, migrate Ganga-2-7-2 fixes, enable 12.0.x, minor bugfixes
#
#Revision 1.11  2006/07/09 08:41:05  elmsheus
#ATLASOutputDataset introduction, DQ2 updates, Splitter and Merger code clean-up, and more
#
#Revision 1.9  2006/05/16 08:52:07  elmsheus
#DQ2Dataset.py:introduce type variable to choose between DQ2 and LFC catalog
#introduce type variable in other files,
#ganga-stagein-dq2.py: fix DQ2 handling, choose DQ2 or LFC catalog
#
#Revision 1.8  2006/05/15 20:30:50  elmsheus
#* DQ2Dataset.py:
#  return contents correctly if inputdata.names are given
#  introduce: type variable, choose LFC or DQ2
#* AthenaLCGRTHandler.py, AthenaLocalRTHandler.py:
#  remove code for trailing number removal in pool.root files and insert
#  code in ganga-stagein-dq2.py and input.py (choose LFC or DQ2)
#  save filenames in inputdata.names for subjobs
#  typo ATLASDataset
#* ganga-stagein-dq2.py:
#  code for trailing number removal in pool.root files
#  choose LFC or DQ2 dataset type
#* Athena.py:
#  code for trailing number removal in pool.root files in generated
#  input.py
#
#Revision 1.7  2006/05/09 13:45:30  elmsheus
#Introduction of
# Athena job splitting based on number of subjobs
# DQ2Dataset and DQ2 file download
# AthenaLocalDataset
#
#Revision 1.6  2006/03/20 01:42:45  liko
#Another KeyError ....
#
#Revision 1.5  2006/03/20 01:29:57  liko
#Add Conder handler
#
#Revision 1.4  2005/09/21 23:37:30  liko
#Bugfixes and support for Simulation
#
#Revision 1.3  2005/09/06 11:52:56  liko
#Small fixes
#
#Revision 1.2  2005/09/06 11:37:14  liko
#Mainly the Athena handler
#
