###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaNGRTHandler_DQ2.py,v 1.1 2008-07-17 16:41:29 moscicki Exp $
###############################################################################
# Athena NG Runtime Handler
#
# Maintained by the Oslo group (B. Samset, K. Pajchel)
#
# Date:   January 2007


import os, pwd, commands, re 

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

from GangaNG.Lib.NG import NGRequirements, NGJobConfig

from GangaAtlas.Lib.ATLASDataset import ATLASDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset import DQ2OutputDataset
from GangaAtlas.Lib.ATLASDataset import ATLASLocalDataset
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.GPIDev.Credentials import GridProxy

# the config file may have a section
# aboout monitoring

mc = getConfig('MonitoringServices')

# None by default

mc['Athena'] = None

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
                raise Exception('ATLASCastorDataset not supported by NorduGrid.')

            if job.inputdata._name == 'ATLASDataset':
                raise Exception('ATLASDataser not supported by NorduGrid.')
            
            # ATLASLocalDataset and ATLASCastorDataset job splitting is done in AthenaSplitterJob
            
            if job._getRoot().subjobs:
                if job.inputdata._name == 'ATLASLocalDataset':
                    if not job.inputdata.names: raise Exception('No inputdata has been specified.')
                    input_files = job.inputdata.names
                    input_guids = get_guids( input_files )

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
                if job.inputdata._name == 'ATLASLocalDataset':
                    input_files = ATLASLocalDataset.get_filenames(app)
                    input_guids = get_guids( input_files )
                    
                elif job.inputdata._name == 'DQ2Dataset':
                    if not job.inputdata.type in ['DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG']:
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
        output_location = None
        if job.outputdata and job.outputdata.location:
            output_location=job.outputdata.location
            #print '%%% Output location ', output_location
        elif job.outputdata and job.outputdata.local_location:
            output_location=job.outputdata.local_location
            #print '%%% Output local_location ', output_location
        # Wait until ng registration works.    
        #else:
        #    try:
        #        output_location=config['NGOutputLocation']
        #        if job.outputdata:
        #            job.outputdata.location = output_location
        #            #print '%%% Output location NGOutputLocation ', output_location
        #    except KeyError:
        #        logger.warning('No default output location specified in the configuration.')

        if job._getRoot().subjobs:
            jid = "%d.%d" % (job._getRoot().id, job.id)
        else:
            jid = "%d" % job.id
        if output_location:
            #output_location = os.path.join(output_location, jid)
            #print '%%% output_location subjobs ', output_location
            if job.outputdata:
                # Remove trailing number if job is copied
                import re
                pat = re.compile(r'\/[\d\.]+\/[\d\.]+$')
                if re.findall(pat,output_location):
                    output_location = re.sub(pat, '', output_location)
                    output_location = os.path.join(output_location, jid)

                # print '%%% final output_location ', output_location
                job.outputdata.location = output_location

        if job.outputdata and job.outputdata._name=='DQ2OutputDataset':
            if job._getRoot().subjobs:
                jobid = "%d" % (job._getRoot().id)
            else:
                jobid = "%d" % job.id

            # Extract username from certificate
            username=""
            # proxy = GridProxy(job.backend.middleware.upper())
            proxy = GridProxy('ARC')
            
            userid = proxy.info(opt="-identity")

            for line in userid.split('/'):
                if line.startswith('CN='):
                    username = re.sub('^CN=','',line)
                    username = re.sub('\d+$','',username)
                    username = re.sub('[\d() ]','',username)
                    username = username.replace(' ','')
                    username = username.strip()
                    #print 'Username from DN ', username
                    break
            if username == '':    
                logger.warning('could not get DN from %s', userid)
                username = pwd.getpwuid(os.getuid())[0]
                username = username.strip()

            
            #if job.outputdata.datasetname:
            #    # new datasetname during job resubmission
            #    pat = re.compile(r'^user\.%s\.ganga' % username)
            #    if re.findall(pat,job.outputdata.datasetname):
            #        output_datasetname = 'user.%s.ganga.%s' % ( username, jobid)
            #        output_lfn = 'users/%s/ganga/%s/' % (username,jobid)
            #    else:
            #        # append user datasetname for new configuration
            #        output_datasetname = 'user.%s.ganga.%s' % (username,job.outputdata.datasetname)
            #        output_lfn = 'users/%s/ganga/%s/' % (username,job.outputdata.datasetname)
            #else:
            #    # No datasetname is given
            #    output_datasetname = 'user.%s.ganga.%s' % (username,jobid)
            #    output_lfn = 'users/%s/ganga/%s/' % (username,jobid)
            #output_jobid = jid
            #job.outputdata.datasetname=output_datasetname
            #if job._getRoot().subjobs:
            #    if job.id==0:
            #        job.outputdata.create_dataset(output_datasetname)
            #else:
            #    job.outputdata.create_dataset(output_datasetname)

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



        inputbox = [ ]
    
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

        exe = os.path.join(os.path.dirname(__file__),'athena-ng.sh')
        outputbox = jobmasterconfig.outputbox
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
        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            environment['OUTPUT_DATASETNAME'] = output_datasetname
            environment['OUTPUT_LFN'] = output_lfn
        #    environment['OUTPUT_JOBID'] = output_jobid
        #    environment['DQ2_URL_SERVER']=configDQ2['DQ2_URL_SERVER']
        #    environment['DQ2_URL_SERVER_SSL']=configDQ2['DQ2_URL_SERVER_SSL']    

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

        #print '%%%%%%%%%%%%%%%%%%%%%%   in NGRTHandler master_prepare'
        
        job = app._getParent() # Returns job or subjob object
        logger.debug("AthenaNGRTHandler master_prepare called, %s", job.id )

        # Get DQ2Dataset content 
        #if job.inputdata and job.inputdata._name == 'DQ2Dataset':
        #    DQ2Dataset.content = DQ2Dataset.get_dataset(app)
        #    if job.inputdata.tagdataset:
        #        DQ2Dataset.content_tag = DQ2Dataset.get_dataset(app, True)

        # Expand Athena jobOptions
        athena_options = ""
        for ao in app.option_file:            
            athena_options += os.path.basename(ao.name)+ ' '
        if app.options: athena_options = app.options + ' ' + athena_options

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

        if job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
            if not File(os.path.join(os.path.dirname(__file__),'ganga-stage-in-out-dq2.py')) in inputbox:
                inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-stage-in-out-dq2.py'))]
                inputbox += [ File(os.path.join(os.path.dirname(__file__),'ganga-joboption-parse.py')) ]
                                            
        if (job.inputsandbox):
            for file in job.inputsandbox:
                inputbox += [ file ]
                 
#       prepare environment

        environment={ 
            'ATLAS_RELEASE'  : app.atlas_release,
	    'ATHENA_OPTIONS' : athena_options,
	    'ATHENA_USERSETUPFILE' : athena_usersetupfile
        }
        if app.user_area.name: environment['USER_AREA']=os.path.basename(app.user_area.name)
        if app.max_events: environment['ATHENA_MAX_EVENTS']=app.max_events

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

        # fix a test is more something is already chosen
        if app.atlas_release:
            requirements.runtimeenvironment += [ 'APPS/HEP/ATLAS-%s' % app.atlas_release ]

#       inputdata
        inputdata = []
        #if job.inputdata and job.inputdata._name == 'ATLASDataset':
        #    inputdata = [ 'lfn:%s' % lfn for lfn in input_files ]
        
#       jobscript
        exe = os.path.join(os.path.dirname(__file__),'athena-ng.sh')
        #print '%%% This is exe ', exe

#       output sandbox
        outputbox = [ ]
        #outputGUIDs='output_guids'
        #outputLOCATION='output_location'
        outputData = job.outputdata.outputdata
        #print '%%% outputData ', outputData
        #outputbox.append( outputGUIDs )
        #outputbox.append( outputLOCATION )
        outputbox += outputData 
        if (job.outputsandbox):
            for file in job.outputsandbox:
                outputbox += [ file ]

        #print '%%% Caling NGJobConfig'        
        return NGJobConfig(File(exe),inputbox,[],outputbox,environment,inputdata,requirements) 

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

