################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SFrameAppLCGRTHandler.py,v 1.4 2009/01/19 10:10:18 mbarison Exp $
################################################################################

import os, socket, pwd, commands, re, xml.dom.minidom
from xml.dom.minidom import Node

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaAtlas.Lib.ATLASDataset import ATLASDataset, isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE, getIncompleteLocations, whichCloud
from GangaAtlas.Lib.ATLASDataset import ATLASCastorDataset
from GangaAtlas.Lib.ATLASDataset import ATLASLocalDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset

from GangaAtlas.Lib.AtlasLCGRequirements import AtlasLCGRequirements

from SFrameApp import *

from Ganga.Lib.LCG import LCGRequirements, LCGJobConfig

from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Lib.File import *

from Ganga.GPIDev.Credentials import GridProxy

# get location of GangaAtlas files

from Ganga.Core.Sandbox import getGangaModulesAsSandboxFiles
import GangaAtlas.Lib.Athena


__athena_dir__ = getGangaModulesAsSandboxFiles([GangaAtlas.Lib.Athena])[0].name
__athena_dir__ = __athena_dir__.strip(os.path.basename(__athena_dir__))

__directory__ = os.path.dirname(__file__)

def _append_file_buffer(inputbox,name,array):

    inputbox.append(FileBuffer(name,'\n'.join(array)+'\n'))
    return

def _append_files(inputbox,*names):
    for name in names:
        inputbox.append(File(os.path.join(__athena_dir__,name)))
    return
        
class SFrameAppLCGRTHandler(IRuntimeHandler):
    """SFrame Athena-derived LCG Runtime Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("SFrameAppLCGRTHandler prepare called, %s", job.id)

        input_files = []
        input_guids = []
        input_tag_files = []
        input_tag_guids = []
        input_esd_files = []
        input_esd_guids = []

        inputbox = [File(os.path.join(__directory__,'sframe-utility.sh'))]
       
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
                    
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG']:
                        job.inputdata.type ='DQ2_LOCAL'
       
                elif job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter':
                    input_files = job.inputdata.names
                    input_guids = job.inputdata.guids

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

                    contents = [(guid, lfn) for guid, lfn in \
                                job.inputdata.get_contents() \
                                if '.root' in lfn]

                    input_files = [ lfn  for guid, lfn in contents]
                    input_guids = [ guid for guid, lfn in contents] 
                    
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
                        output_location=config['LCGOutputLocation']
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

        # stupid timestamping
        import time
        inputbox.append(FileBuffer('timestamps.txt',`time.gmtime()`+'\n'))


        exe = os.path.join(os.path.dirname(__file__),'sframe-lcg.sh')
        outputbox = jobmasterconfig.outputbox
        requirements = jobmasterconfig.requirements
        environment = jobmasterconfig.env.copy()
        # If ArgSplitter is used
#         try:
#             if job.application.args:
#                 #environment['ATHENA_OPTIONS'] = environment['ATHENA_OPTIONS'] + ' ' + ' '.join(job.application.args)
#                 if job.application.options:
#                     job.application.options = job.application.options + ' ' + job.application.args
#                 else:
#                     job.application.options = job.application.args
#         except AttributeError:
#             pass

        if output_location and output_location.find('/castor/cern.ch/grid/atlas/t0')>=0:
            raise ApplicationConfigurationError(None,'You are try to save the output to TIER0DISK - please use another area !')        
        
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

        # stupid timestamping
        outputbox += ['timestamps.txt']

        environment['JOBID'] = job.fqid

        # append a property for monitoring to the jobconfig of subjobs
        lcg_config = LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], requirements)
        lcg_config.monitoring_svc = mc['Athena']
        return lcg_config

    def master_prepare( self, app, appconfig):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("SFrameAppLCGRTHandler master_prepare called, %s", job.id )

        # Check if all sites are in the same cloud
        if job.backend.requirements.sites:
            firstCloud = whichCloud(job.backend.requirements.sites[0])
            for site in job.backend.requirements.sites:
                cloud = whichCloud(site)
                if cloud != firstCloud:
                    printout = 'Job submission failed ! Site specified with j.backend.requirements.sites=%s are not in the same cloud !' %(job.backend.requirements.sites)
                    raise ApplicationConfigurationError(None,printout )

        # prepare input sandbox



        inputbox = []


        # athena utility file
        _append_files(inputbox,'athena-utility.sh')

        if job.inputdata and job.inputdata._name == 'ATLASDataset':
            if job.inputdata.lfc:
                _append_files(inputbox,'ganga-stagein-lfc.py')
            else:
                _append_files(inputbox,'ganga-stagein.py')
            

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




        #inputbox += [ File(os.path.join(loc,'ganga_setype.py')) ]
        
        #inputbox += [ File(os.path.join(loc,'libDCache.so')) ]
        #inputbox += [ File(os.path.join(loc,'libRFIO.so')) ]
#         elif app.atlas_release.find('13.')>=0:
#             inputbox += [ File(os.path.join(loc,'ganga_setype.py')) ]
#             inputbox += [ File(os.path.join(loc,'libRFIO.so')) ]

        if job.inputsandbox: inputbox += job.inputsandbox


        # sframe archive?
        if app.sframe_archive.name:
            inputbox.append(File(app.sframe_archive.name))


        inputbox.append(File(os.path.join(os.path.dirname(__file__),'pool2sframe.py')))
        inputbox.append(File(os.path.join(os.path.dirname(__file__),'compile_archive.py')))
        

        inputbox.append(app.xml_options)
            
#       prepare environment

        if not app.atlas_release: raise ApplicationConfigurationError(None,'j.application.atlas_release is empty - No ATLAS release version found by prepare() or specified.')


        environment = app.env
        environment['ATLAS_RELEASE'] = app.atlas_release
        environment['SFRAME_XML'] = app.xml_options.name.split('/')[-1]

        if app.user_email != '':
            environment['USER_EMAIL'] = app.user_email


        if app.sframe_archive.name:
            environment['SFRAME_ARCHIVE'] = app.sframe_archive.name.split('/')[-1]

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
                if job.inputdata.failover:
                    environment['DATASETFAILOVER'] = 1
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

                raise ApplicationConfigurationError(None,'Job submission failed ! Please use DQ2JobSplitter or specify j.backend.requirements.sites or j.backend.requirements.CE !')

            if job.inputdata.match_ce_all or job.inputdata.min_num_files>0:
                raise ApplicationConfigurationError(None,'Job submission failed ! Usage of j.inputdata.match_ce_all or min_num_files is obsolete ! Please use DQ2JobSplitter or specify j.backend.requirements.sites or j.backend.requirements.CE !')


            # Add TAG datasetname
            if job.inputdata.tagdataset:
                environment['TAGDATASETNAME']= job.inputdata.tagdataset

        # prepare job requirements

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
        elif app.atlas_release.find('14.')>=0 or app.atlas_release.find('15.')>=0:
            if app.atlas_cmtconfig:
                cmtconfig = app.atlas_cmtconfig
            else:
                cmtconfig = 'i686-slc4-gcc34-opt'
            if cmtconfig != 'i686-slc4-gcc34-opt':
                cmtconfig = 'i686-slc4-gcc34-opt'
            if app.atlas_production=='':
                requirements.software = ['VO-atlas-offline-%s-%s' %(app.atlas_release, cmtconfig )]
            else:
                if app.atlas_project=="AtlasPoint1":
                    requirements.software = ['VO-atlas-point1-%s' %(app.atlas_production)]
                elif app.atlas_project=="AtlasTier0":
                    requirements.software = ['VO-atlas-tier0-%s' %(app.atlas_production)]
                else:
                    requirements.software = ['VO-atlas-production-%s-%s' %(app.atlas_production, cmtconfig )]
        else:
            requirements.software = ['VO-atlas-production-%s' % app.atlas_release ]


        #logger.warning("Software release: %s" % `requirements.software`)

            
        # put back to see if it works
        job.backend.requirements.software = requirements.software

#       add software requirement of dq2clients
        if job.inputdata and job.inputdata.type in ['DQ2_DOWNLOAD', 'TNT_DOWNLOAD']:
            dq2client_version = requirements.dq2client_version
            try:
                # override the default one if the dq2client_version is presented 
                # in the job backend's requirements object
                dq2client_version = job.backend.requirements.dq2client_version
            except AttributeError:
                pass
            requirements.software += ['VO-atlas-dq2clients-%s' % dq2client_version]
            environment['DQ2_CLIENT_VERSION'] = dq2client_version


        
        # jobscript

        exe = os.path.join(os.path.dirname(__file__),'sframe-lcg.sh')

        # output sandbox

        # parse XML file to get OutputData
        
        try:
            s_doc = xml.dom.minidom.parse(app.xml_options.name)

            for node in s_doc.getElementsByTagName("Cycle"):
                name = node.getAttribute("Name").encode('ascii')
                pfx  = node.getAttribute("PostFix").encode('ascii')
                
                for node2 in node.getElementsByTagName("InputData"):
                    typ = node2.getAttribute("Type").encode('ascii')

                    fname = "%s.%s%s.root" % (name.replace('::','.'), typ, pfx)

                    # don't duplicate entries!
                    if fname not in job.outputsandbox:
                        job.outputsandbox += [ fname ]
        except:
            raise ApplicationConfigurationError(None,"Cannot read XML file %s" % app.xml_options.name)

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

        return LCGJobConfig(File(exe),inputbox,[],outputbox,environment,[],requirements) 



allHandlers.add('SFrameApp', 'LCG'    , SFrameAppLCGRTHandler)
allHandlers.add('SFrameApp', 'Condor' , SFrameAppLCGRTHandler)
allHandlers.add('SFrameApp', 'Cronus' , SFrameAppLCGRTHandler)

config = getConfig('SFrameApp')
configDQ2 = getConfig('DQ2')
logger = getLogger('SFrameApp')

# $Log: SFrameAppLCGRTHandler.py,v $
# Revision 1.4  2009/01/19 10:10:18  mbarison
# using athena utility scripts
#
# Revision 1.3  2009/01/09 13:53:35  mbarison
# added mailspam
#
# Revision 1.2  2008/11/24 16:12:49  mbarison
# *** empty log message ***
#
# Revision 1.1  2008/11/19 15:42:59  mbarison
# first version
#
# Revision 1.3  2008/04/16 15:35:59  mbarison
# adding CVS log
#
