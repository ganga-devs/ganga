################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SFrameAppLocalRTHandler.py,v 1.1 2008-11-19 15:43:00 mbarison Exp $
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

from SFrameApp import *

from Ganga.Lib.LCG import LCGRequirements, LCGJobConfig

from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Lib.File import *

from Ganga.GPIDev.Credentials import GridProxy

class SFrameAppLocalRTHandler(IRuntimeHandler):
    """SFrame Athena-derived Local Runtime Handler"""

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("SFrameAppLocalRTHandler prepare called, %s", job.id)

        input_files = []
        input_guids = []
        input_tag_files = []
        input_tag_guids = []
        
        # If job has inputdata
        if job.inputdata:
            # ATLASLocalDataset and ATLASCastorDataset job splitting is done
            # in AthenaSplitterJob
            if (job.inputdata._name == 'ATLASLocalDataset' or job.inputdata._name == 'ATLASCastorDataset') and job.inputdata.names and job._getRoot().subjobs:
                input_files = job.inputdata.names
            # ATLASDataset job splitting is done in AthenaSplitterJob
            elif job.inputdata._name == 'ATLASDataset' and job.inputdata.lfn and job._getRoot().subjobs:
                input_files = job.inputdata.lfn
            # CN: splitting has been done in TNTJobSplitter
            elif job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter':
                input_files = job.inputdata.names    
                input_guids = job.inputdata.guids
            # DQ2Dataset job splitting is done in AthenaSplitterJob
            elif job.inputdata._name == 'DQ2Dataset' and job.inputdata.names and job._getRoot().subjobs:
                for guid in job.inputdata.guids:
                    input_guids.append(guid)
                for name in job.inputdata.names:
                    input_files.append(name)
                if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG']:
                    job.inputdata.type ='DQ2_LOCAL'
            else:
                # ATLASCastorDataset no job splitting
                if job.inputdata._name == 'ATLASCastorDataset':
                    input_files = ATLASCastorDataset.get_filenames(app)
                # ATLASLocalDataset no job splitting
                elif job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)
                # ATLASDataset no job splitting
                elif job.inputdata._name == 'ATLASDataset':
                    input_files = ATLASDataset.get_filenames(app)
                # DQ2Dataset no job splitting
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
                    job.inputdata.names = input_files
                    job.inputdata.guids = input_guids
 
        # Outputdataset
        output_location=''
        if job.outputdata:
            if job.outputdata.location and job.outputdata._name=='DQ2OutputDataset':
                from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import getTiersOfATLASCache
                toa = getTiersOfATLASCache()
                try:
                    temp_location = job.outputdata.location
                    if temp_location.__class__.__name__=='list':
                        temp_location = temp_location[0]
                    temp_srm=toa.sites[temp_location]['srm']
                    if temp_srm!='':
                        output_location = temp_location
                except KeyError:
                    output_location = ''
                    pass
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
                    output_datasetname = 'users.%s.ganga.%s.%s' % ( username, jobid, jobdate)
                    #output_lfn = 'users/%s/ganga/%s/' % (username,jobid)
                    output_lfn = 'users/%s/ganga/' % (username)
                else:
                    # append user datasetname for new configuration
                    output_datasetname = 'users.%s.ganga.%s' % (username,job.outputdata.datasetname)
                    #output_lfn = 'users/%s/ganga/%s/' % (username,job.outputdata.datasetname)
                    output_lfn = 'users/%s/ganga/' % (username)
            else:
                # No datasetname is given
                output_datasetname = 'users.%s.ganga.%s.%s' % (username,jobid, jobdate)
                #output_lfn = 'users/%s/ganga/%s/' % (username,jobid)
                output_lfn = 'users/%s/ganga/' % (username)
 
            output_jobid = jid
            job.outputdata.datasetname=output_datasetname
            if job._getRoot().subjobs:
                if job.id==0:
                    job.outputdata.create_dataset(output_datasetname)
            else:
                job.outputdata.create_dataset(output_datasetname)

        inputbox = [File(os.path.join(os.path.dirname(__file__),'sframe-utility.sh'))]
                
        if input_guids:
            inputbox += [ FileBuffer('input_guids','\n'.join(input_guids)+'\n') ]

        if input_files: 
            inputbox += [ FileBuffer('input_files','\n'.join(input_files)+'\n') ]

        if input_tag_guids:
            inputbox += [ FileBuffer('input_tag_guids','\n'.join(input_tag_guids)+'\n') ]

        if input_tag_files: 
            inputbox += [ FileBuffer('input_tag_files','\n'.join(input_tag_files)+'\n') ]

        if job.outputdata and job.outputdata.outputdata:
            inputbox += [ FileBuffer('output_files','\n'.join(job.outputdata.outputdata)+'\n') ]
        elif job.outputdata and not job.outputdata.outputdata:
            raise Exception('j.outputdata.outputdata is empty - Please specify output filename(s).')
   
        exe = os.path.join(os.path.dirname(__file__),'sframe-local.sh')
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

        # CN: extra condition for TNTSplitter
        if job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter':
            # set up dq2 environment
            if job._getRoot().subjobs:
                for j in job._getRoot().subjobs:
                    datasetname = j.inputdata.dataset
                    environment['DATASETNAME']= datasetname
                    environment['DATASETLOCATION'] = ':'.join(j.inputdata.get_locations())
                    environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
                    environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']
                    environment['DATASETTYPE']=j.inputdata.type

        # stupid timestamping
        import time
        inputbox += [FileBuffer('timestamps.txt',`time.gmtime()`+'\n')]
        outputbox += ['timestamps.txt']

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)


    def master_prepare( self, app, appconfig):
        """Prepare the master job"""

        job = app._getParent() # Returns job or subjob object
        logger.debug("SFrameAppLocalRTHandler master_prepare called, %s", job.id )

        # prepare input sandbox

        # get location of GangaAtlas files

        from Ganga.Core.Sandbox import getGangaModulesAsSandboxFiles
        import GangaAtlas.Lib.Athena

        loc = getGangaModulesAsSandboxFiles([GangaAtlas.Lib.Athena])[0].name

        loc = loc.strip(os.path.basename(loc))


        inputbox = []
        #CN: added extra test for TNTJobSplitter
        if job.inputdata and job.inputdata._name == 'DQ2Dataset' or (job._getRoot().splitter and job._getRoot().splitter._name == 'TNTJobSplitter'):
            inputbox += [
                File(os.path.join(loc, 'ganga-stage-in-out-dq2.py')),
                File(os.path.join(loc, 'dq2_get'))
                ]

        if job.inputdata and job.inputdata._name == 'ATLASDataset':
            if job.inputdata.lfc:
                inputbox += [ File(os.path.join(loc, 'ganga-stagein-lfc.py')) ]
            else:
                inputbox += [ File(os.path.join(loc, 'ganga-stagein.py')) ]

        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            if not File(os.path.join(loc, 'ganga-stage-in-out-dq2.py')) in inputbox:
                inputbox += [ File(os.path.join(loc, 'ganga-stage-in-out-dq2.py'))]
            inputbox += [ File(os.path.join(loc, 'ganga-joboption-parse.py')) ]

        if job.inputsandbox:
            for file in job.inputsandbox:
                inputbox += [ file ]

        # sframe archive?
        if app.sframe_archive.name:
            inputbox += [ File(app.sframe_archive.name)]

        inputbox += [File(os.path.join(os.path.dirname(__file__),'pool2sframe.py'))]
        inputbox += [File(os.path.join(os.path.dirname(__file__),'input2sframe.py'))]
        inputbox += [File(os.path.join(os.path.dirname(__file__),'compile_archive.py'))]
        inputbox += [app.xml_options]
            
#       prepare environment

        if not app.atlas_release: raise Exception('j.application.atlas_release is empty - No ATLAS release version found by prepare() or specified.')


        environment = app.env
        environment['ATLAS_SOFTWARE'] = getConfig('Athena')['ATLAS_SOFTWARE']
        environment['ATLAS_RELEASE'] = app.atlas_release
        environment['SFRAME_XML'] = app.xml_options.name.split('/')[-1]

        if app.sframe_archive.name:
            environment['SFRAME_ARCHIVE'] = app.sframe_archive.name.split('/')[-1]

        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.inputdata.dataset:
                datasetname = job.inputdata.dataset
                environment['DATASETNAME']=datasetname
                environment['DATASETLOCATION'] = ':'.join(job.inputdata.get_locations())
                environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
                environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']
                environment['DATASETTYPE']=job.inputdata.type

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
            else:
                raise ConfigError("j.inputdata.dataset='' - DQ2 dataset name needs to be specified.")

            # Add TAG datasetname
            if job.inputdata.tagdataset:
                environment['TAGDATASETNAME']= job.inputdata.tagdataset


        # inputdata
        inputdata = []
        #if job.inputdata and job.inputdata._name == 'ATLASDataset':
        #    inputdata = [ 'lfn:%s' % lfn for lfn in input_files ]
        
        # jobscript

        exe = os.path.join(os.path.dirname(__file__),'sframe-local.sh')

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

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)
        

allHandlers.add('SFrameApp', 'Local'    , SFrameAppLocalRTHandler)

config = getConfig('SFrameApp')
configDQ2 = getConfig('DQ2')
logger = getLogger('SFrameApp')

# $Log: not supported by cvs2svn $
# Revision 1.3  2008/04/16 15:34:23  mbarison
# see if cvs tag works
#
