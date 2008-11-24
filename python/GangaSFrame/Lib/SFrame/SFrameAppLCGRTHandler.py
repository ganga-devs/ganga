################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SFrameAppLCGRTHandler.py,v 1.2 2008-11-24 16:12:49 mbarison Exp $
################################################################################

import os, socket, pwd, commands, re, xml.dom.minidom
from xml.dom.minidom import Node

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaAtlas.Lib.ATLASDataset import ATLASDataset, isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE
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

        inputbox = [File(os.path.join(os.path.dirname(__file__),'sframe-utility.sh'))]
       
        if job.inputdata:

            # DQ2Dataset, ATLASLocalDataset and ATLASCastorDataset job splitting is done in AthenaSplitterJob

            if job._getRoot().subjobs:
                if job.inputdata._name == 'ATLASLocalDataset' or job.inputdata._name == 'ATLASCastorDataset':
                    if not job.inputdata.names: raise Exception('No inputdata has been specified.')
                    input_files = job.inputdata.names
                 
                        
                elif job.inputdata._name == 'ATLASDataset':
                    if not job.inputdata.lfn: raise Exception('No inputdata has been specified.') 
                    input_files = job.inputdata.lfn

                elif job.inputdata._name == 'DQ2Dataset':
                    if not job.inputdata.names: raise Exception('No inputdata has been specified.')
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
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG']:
                        job.inputdata.type ='DQ2_LOCAL'

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
            raise Exception('j.outputdata.outputdata is empty - Please specify output filename(s).')


        # stupid timestamping
        import time
        inputbox += [FileBuffer('timestamps.txt',`time.gmtime()`+'\n')]


        exe = os.path.join(os.path.dirname(__file__),'sframe-lcg.sh')
        outputbox = jobmasterconfig.outputbox
        requirements = jobmasterconfig.requirements
        environment = jobmasterconfig.env.copy()
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
        #if output_location:
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
            if job._getRoot().subjobs:
                for j in job._getRoot().subjobs:
                    datasetname = j.inputdata.dataset
                    environment['DATASETNAME']= datasetname
                    environment['DATASETLOCATION'] = ':'.join(j.inputdata.get_locations())
                    environment['DQ2_URL_SERVER'] = configDQ2['DQ2_URL_SERVER']
                    environment['DQ2_URL_SERVER_SSL'] = configDQ2['DQ2_URL_SERVER_SSL']
                    environment['DATASETTYPE'] = j.inputdata.type
                    if j.inputdata.accessprotocol:
                        environment['DQ2_LOCAL_PROTOCOL'] = j.inputdata.accessprotocol

        # stupid timestamping
        outputbox += ['timestamps.txt']

        # append a property for monitoring to the jobconfig of subjobs
        lcg_config = LCGJobConfig(File(exe), inputbox, [], outputbox, environment, [], requirements)
        lcg_config.monitoring_svc = mc['Athena']
        return lcg_config

    def master_prepare( self, app, appconfig):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("SFrameAppLCGRTHandler master_prepare called, %s", job.id )

        # prepare input sandbox

        # get location of GangaAtlas files

        from Ganga.Core.Sandbox import getGangaModulesAsSandboxFiles
        import GangaAtlas.Lib.Athena

        loc = getGangaModulesAsSandboxFiles([GangaAtlas.Lib.Athena])[0].name

        loc = loc.strip(os.path.basename(loc))

        inputbox = []

        if job.inputdata and job.inputdata._name == 'ATLASDataset':
            if job.inputdata.lfc:
                inputbox += [ File(os.path.join(loc,'ganga-stagein-lfc.py')) ]
            else:
                inputbox += [ File(os.path.join(loc,'ganga-stagein.py')) ]
            

        # CN: added TNTJobSplitter clause  
        if job.inputdata and job.inputdata._name == 'DQ2Dataset' or (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
            inputbox += [
                File(os.path.join(loc,'ganga-stage-in-out-dq2.py')),
                File(os.path.join(loc,'dq2_get')),
                File(os.path.join(loc,'dq2info.tar.gz'))
            ]
            if job.inputdata and job.inputdata.type == 'LFC' and not (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
                inputbox += [
                    File(os.path.join(loc,'dq2_get_old'))
                    ]


        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            if not File(os.path.join(loc,'ganga-stage-in-out-dq2.py')) in [ os.path.basename(file.name) for file in inputbox ]:
                inputbox += [ File(os.path.join(loc,'ganga-stage-in-out-dq2.py'))]
            if not 'dq2info.tar.gz' in [os.path.basename(file.name) for file in inputbox ]:
                inputbox += [ File(os.path.join(loc,'dq2info.tar.gz'))]
                
            inputbox += [ File(os.path.join(loc,'ganga-joboption-parse.py')) ]

#       add libDCache.so and libRFIO.so to fix broken access in athena 12.0.x
#         if app.atlas_release.find('12.')>=0:


        inputbox += [ File(os.path.join(loc,'ganga_setype.py')) ]
        #inputbox += [ File(os.path.join(loc,'libDCache.so')) ]
        #inputbox += [ File(os.path.join(loc,'libRFIO.so')) ]
#         elif app.atlas_release.find('13.')>=0:
#             inputbox += [ File(os.path.join(loc,'ganga_setype.py')) ]
#             inputbox += [ File(os.path.join(loc,'libRFIO.so')) ]

        if job.inputsandbox: inputbox += job.inputsandbox


        # sframe archive?
        if app.sframe_archive.name:
            inputbox += [ File(app.sframe_archive.name)]


        inputbox += [File(os.path.join(os.path.dirname(__file__),'pool2sframe.py'))]
        inputbox += [File(os.path.join(os.path.dirname(__file__),'compile_archive.py'))]
        

        inputbox += [app.xml_options]
            
#       prepare environment

        if not app.atlas_release: raise Exception('j.application.atlas_release is empty - No ATLAS release version found by prepare() or specified.')


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
                environment['TAGDATASETNAME']= job.inputdata.tagdataset

        # prepare job requirements
        
        if app.atlas_release.find('11.')>=0 or app.atlas_release.find('10.')>=0:
            requirements.software=['VO-atlas-release-%s' % app.atlas_release ]
        elif app.atlas_release.find('12.0.0')>=0 or app.atlas_release.find('12.0.1')>=0 or app.atlas_release.find('12.0.2')>=0:
            requirements.software=['VO-atlas-offline-%s' % app.atlas_release ]
        else:
            requirements.software=['VO-atlas-production-%s' % app.atlas_release ]

        # inputdata
        inputdata = []
        #if job.inputdata and job.inputdata._name == 'ATLASDataset':
        #    inputdata = [ 'lfn:%s' % lfn for lfn in input_files ]
        
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
            raise Exception("Cannot read XML file %s" % app.xml_options.name)

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

        return LCGJobConfig(File(exe),inputbox,[],outputbox,environment,inputdata,requirements) 



allHandlers.add('SFrameApp', 'LCG'    , SFrameAppLCGRTHandler)
allHandlers.add('SFrameApp', 'Condor' , SFrameAppLCGRTHandler)
allHandlers.add('SFrameApp', 'Cronus' , SFrameAppLCGRTHandler)

config = getConfig('SFrameApp')
configDQ2 = getConfig('DQ2')
logger = getLogger('SFrameApp')

# $Log: not supported by cvs2svn $
# Revision 1.1  2008/11/19 15:42:59  mbarison
# first version
#
# Revision 1.3  2008/04/16 15:35:59  mbarison
# adding CVS log
#
