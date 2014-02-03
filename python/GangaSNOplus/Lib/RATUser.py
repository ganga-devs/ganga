######################################################
# RATUser.py
# ---------
# Author: Matt Mottram
#         <m.mottram@sussex.ac.uk>
#
# Description:
#    Ganga application for SNO+ user analysis/simulation.
#
# Runs RAT snapshots on the given backend via ratRunner.py
# Ships the code in the input sandbox, can either download
# the snapshot from a given rat fork, or checkout and tar
# up the snapshot from a local repository.
#
# Classes:
#  - RATUser: user analysis/simulation applicaiton
#  - RATUserSplitter: splitter to create subjobs for subruns
#  - UserRTHandler: handles submission to local/batch backends
#  - UserLCGRTHandler: handles submission to LCG backend
#
# Revision History:
#  - 03/11/12: M. Mottram: first revision with proper documentation!
#  - 06/12/13: M. Mottram: Removed use of tokens, updated config and schema.
# 
######################################################

import os, re, string, commands
import socket
import random

import RATUtil

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Core import FileWorkspace
from Ganga.Core.exceptions import ApplicationConfigurationError

from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *

from Ganga.GPIDev.Credentials import GridProxy

from Ganga.GPIDev.Lib.File import *

from Ganga.Lib.Executable import Executable


###################################################################

config = Ganga.Utility.Config.makeConfig('defaults_RATUser','Defaults for the RATUser application')

config.addOption('local_softwareDir', None, 'Local snoing-install directory (or directory with env_rat-x.y.sh files)')
config.addOption('local_environment', [], 'Environment options required to run on local or batch system')
config.addOption('local_outputDir', None, '*Default* output directory if running on a batch or local system (can override)')
config.addOption('grid_outputDir', None, '*Defult* output directory if running on system with grid storage (can override)')
config.addOption('cacheDir', '~/gaspCache', 'Directory to store RAT snaphots (if required)')


# Assume that the applications should come from the same GangaSNOplus directory        
_app_directory = os.path.dirname(__file__)

###################################################################

class RATUser(IApplication):
    """The RAT job handler for data production and processing"""

    #_schema is required for any Ganga plugin
    #Add any options that are required, but try to set sensible default values to minimise effort required of user
    _schema = Schema(Version(1,1), {
            'discardOutput'     : SimpleItem(defvalue=False,doc='Do not store the output: default False',typelist=['bool']),
            'environment'       : SimpleItem(defvalue=[],doc='list of strings with the commands to setup the correct backend environment, or single string location of a file with the appropriate commands (if necessary)',typelist=['str','list']),
            'inputFile'         : SimpleItem(defvalue=None,doc='Input file name, macro cannot have the inroot process defined within!',
                                             typelist=['str','type(None)']),
            'nEvents'           : SimpleItem(defvalue=None,doc='Number of events to run, MUST not define number of events in the macro (/rat/run/start)',
                                             typelist=['int','type(None)']),
            'outputDir'         : SimpleItem(defvalue=None,doc='Which output directory should we use (default Grid: RATUser/general/, should be modified to RATUser/<your-name> if you dont want admins to mess with it!)',
                                             typelist=['str','type(None)']),
            'outputFile'        : SimpleItem(defvalue=None,doc='Output file name, macro must have outroot processor, but no output file defined!',
                                             typelist=['str','type(None)']),
            'rat_db_name'       : SimpleItem(defvalue=None, doc='RAT db name', typelist=['str', 'type(None)']),
            'rat_db_pswd'       : SimpleItem(defvalue=None, doc='RAT db password', typelist=['str', 'type(None)']),
            'rat_db_protocol'   : SimpleItem(defvalue=None, doc='RAT db protocol', typelist=['str', 'type(None)']),
            'rat_db_url'        : SimpleItem(defvalue=None, doc='RAT db password', typelist=['str', 'type(None)']),
            'rat_db_user'       : SimpleItem(defvalue=None, doc='RAT db password', typelist=['str', 'type(None)']),
            'ratBaseVersion'    : SimpleItem(defvalue='dev',doc='RAT version that ratVersion derives from, necessary to get the correct libraries (ROOT, Geant4 etc)',
                                             typelist=['str',"int"]),
            'ratFork'           : SimpleItem(defvalue='snoplus', doc='Fork of RAT [snoplus]', typelist=['str']),
            'ratMacro'          : SimpleItem(defvalue=None,doc='String pointing to the macro file to run',
                                             typelist=['str','type(None)']),
            'ratVersion'        : SimpleItem(defvalue=None,doc='RAT version tag for the version to download and install (can also be a branch name, not recommended)',
                                             typelist=['str','type(None)']),
            'softwareDir'       : SimpleItem(defvalue=None,doc='Software (snoing install) directory, required if running on a non-LCG backend',
                                             typelist=['str','type(None)']),
            'tRun'              : SimpleItem(defvalue=None,doc='Duration of run (cannot use with nEvents)',
                                             typelist=['int','type(None)']),
            'useDB'             : SimpleItem(defvalue=False,doc='Use the RAT database (snopl.us)?',typelist=['bool']),
            'versionUpdate'     : SimpleItem(defvalue=False, doc="Update the rat version tag?", typelist=['bool']),
            })
    
    _category = 'applications'
    _name = 'RATUser'

    config = Ganga.Utility.Config.getConfig('defaults_RATUser')
    def configure(self,masterappconfig):
        '''Configure method, called once per job.
        '''
        logger.debug('RAT::RATUser configure ...')

        job = self._getParent()
        masterjob = job._getParent()

        #Critical options:
        # - ratMacro 
        # - outputDirectory
        # - ratBaseVersion
        #If these aren't defined, don't let the user submit the job
        #Note, the ratMacro can be defined in the subjob...
        if self.ratMacro!=None:
            job.inputsandbox.append(File(self.ratMacro))
        else:
            logger.error('Rat macro not defined')
            raise Exception
        if self.ratBaseVersion=='':
            logger.error('Error: must give a rat base (fixed release) version number')
            raise Exception
        if self.outputFile:
            if RATUtil.checkCommand(self.ratMacro,['/rat/procset','file']):
                logger.error('Cannot specify outputfile in Ganga job if "/rat/procset file" line is present in macro')
                raise Exception
            if not RATUtil.checkCommand(self.ratMacro,['/rat/proclast','outroot']):
                logger.error('Have specified an output file, but no outroot processor present in macro')
                raise Exception
        if self.nEvents or self.tRun:
            if RATUtil.checkOption(self.ratMacro,'/rat/run/start'):
                logger.error('Cannot specify number of events in both macro and in the ganga Job - either/or')
                raise Exception
        if self.nEvents and self.tRun:
            logger.error('Cannot specify number of events and the duration of run!')
            raise Exception
        if self.inputFile:
            if RATUtil.checkText(self.ratMacro,['inroot/read']):
                logger.error('Cannot specify inputFile in Ganga job if "/rat/inroot/read" line is present in macro')
                raise Exception
        if self.useDB:
            if not config['rat_db_pswd']:
                logger.error('Need a password in order to contact the ratdb database')
                raise Exception

        #Always run rat with a log called rat.log
        job.outputsandbox.append('rat.log')
        job.outputsandbox.append('return_card.js')

        if self.ratVersion!=None:
            #download the code locally
            #only uses the main SNO+ rat branch for now
            #need to add pkl object to inform which branch we have and add others when required
            self.zipFileName = RATUtil.MakeRatSnapshot(self.ratFork, self.ratVersion, self.versionUpdate, 'rat/', os.path.expanduser(config['cacheDir']))
            job.inputsandbox.append(File(self.zipFileName))

        #all args have to be str/file - force rat base version to be a string
        self.ratBaseVersion=str(self.ratBaseVersion)

        return(None,None)
                                
###################################################################

class RATUserSplitter(ISplitter):
    '''Splitter for RAT User jobs.
    '''
    _name = "RATUserSplitter"
    _schema = Schema(Version(1,0), {
            'ratMacro' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc='A list of lists for specifying rat macros files'),
            'outputFile' : SimpleItem(defvalue=[],typelist=['str','type(None)'],sequence=1,doc='A list of lists for specifying rat output files'),
            'inputFile' : SimpleItem(defvalue=[],typelist=['str','type(None)'],sequence=1,doc='A list of lists for specifying rat input files'),
            'nEvents' : SimpleItem(defvalue=[],typelist=['int','type(None)'],sequence=1,doc='A list of the number of events for each sub job')
        } )

    def split(self,job):

        if self.outputFile!=[]:
            if len(self.outputFile)!=len(self.ratMacro):
                logger.error('Must have same number of macros, outputs and inputs for the splitter')
                raise Exception
        if self.inputFile!=[]:
            if len(self.inputFile)!=len(self.ratMacro):
                logger.error('Must have same number of macros, outputs and inputs for the splitter')
                raise Exception
        if self.nEvents!=[]:
            if len(self.nEvents)!=len(self.ratMacro):
                logger.error('Must have same number of nEvents as macros for the splitter')
                raise Exception
        
        subjobs = []

        for i,rm in enumerate(self.ratMacro):
            j = self.createSubjob(job)
            j.application.ratMacro = rm
            if self.outputFile!=[]:
                j.application.outputFile = self.outputFile[i]
            if self.inputFile!=[]:
                j.application.inputFile = self.inputFile[i]
            if self.nEvents!=[]:
                j.application.nEvents = self.nEvents[i]
            subjobs.append(j)
        return subjobs
    
###################################################################

class UserLCGRTHandler(IRuntimeHandler):
    '''RTHandler for Grid submission.
    Could include CE options and tags here.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Prepare method: called to configure the job for the specified backend.
        '''
        logger.debug('RAT::LCGRTHandler prepare ...')
        from Ganga.Lib.LCG import LCGJobConfig

        job=app.getJobObject()

        #remove the leading directory path of the macro (on the grid node we'll just
        #have the macro file)
        decimated = app.ratMacro.split('/')
        ratMacro  = decimated[len(decimated)-1]

        #Set the output directory
        if app.outputDir==None:
            if app.config['grid_outputDir']==None:
                logger.error('Output directory not defined')
                raise Exception
            else:
                app.outputDir = app.config['grid_outputDir']
        outputDir = app.outputDir
        lfcDir = os.path.join('lfn:/grid/snoplus.snolab.ca',outputDir)

        #add requirements for the snoing installer! We use this for every job!
        job.backend.requirements.software+=['VO-snoplus.snolab.ca-snoing',
                                            'VO-snoplus.snolab.ca-rat-%s'%(app.ratBaseVersion)]
        if job.backend.requirements.memory==0:
            job.backend.requirements.memory=1500 #default value if nothing set

        #on the grid, we need to use our own version of python
        #so have to send a python script to setup the correct environment
        #AND then run the correct python script!

        rrArgs = '' #args to send to ratRunner
        spArgs = [] #args to send to sillyPythonWrapper

        #ensure the rrArgs are space separated
        rrArgs += '-g lcg ' #always at lcg
        rrArgs += '-b %s '%app.ratBaseVersion
        rrArgs += '-m %s '%ratMacro
        rrArgs += '-d %s '%outputDir
        rrArgs += '-s $VO_SNOPLUS_SNOLAB_CA_SW_DIR/snoing-install ' #always same sw dir at LCG
        spArgs += ['-s','ratRunner.py','-l','lcg']
    
        if app.ratVersion!=None:
            rrArgs += '-v %s '%app.ratVersion
            #ship code to backend
            zipFileName = app.zipFileName
            decimated = zipFileName.split('/')
            zipFileName = decimated[len(decimated)-1]
            rrArgs += '-f %s '%zipFileName
        if app.outputFile:
            rrArgs += '-o %s '%app.outputFile
        if app.inputFile:
            rrArgs += '-i %s '%app.inputFile
        if app.nEvents:
            rrArgs += '-N %s '%app.nEvents
        elif app.tRun:
            rrArgs += '-T %s '%app.tRun
        if app.useDB:
            rrArgs += '--dbuser %s '%(app.config['rat_db_user'])
            rrArgs += '--dbpassword %s '%(app.config['rat_db_pswd'])
            rrArgs += '--dbname %s '%(app.config['rat_db_name'])
            rrArgs += '--dbprotocol %s '%(app.config['rat_db_protocol'])
            rrArgs += '--dburl %s '%(app.config['rat_db_url'])
        if app.discardOutput:
            rrArgs += '--nostore '

        spArgs += ['-a','"%s"'%rrArgs]#appends ratRunner args

        app._getParent().inputsandbox.append('%s/ratRunner.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)

        return LCGJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
                            inputbox = app._getParent().inputsandbox,
                            outputbox = app._getParent().outputsandbox,
                            args = spArgs)
    
###################################################################

class UserRTHandler(IRuntimeHandler):
    '''RTHandler for Batch and Local submission.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Prepare method: called to configure the job for the specified backend.
        '''
        logger.debug('RAT::RTHandler prepare ...')
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        job=app.getJobObject()

        #remove the leading directory path of the macro (on the grid node we'll just
        #have the macro file)
        decimated = app.ratMacro.split('/')
        ratMacro  = decimated[len(decimated)-1]

        if app.outputDir==None:
            if app.config['local_outputDir']==None:
                logger.error('Output directory not defined')
                raise Exception
            else:
                app.outputDir = app.config['local_outputDir']
        outputDir = app.outputDir
        if app.softwareDir==None:
            if app.config['local_softwareDir']==None:
                logger.error('RATUser requires softwareDir to be defined if running on any backend other than LCG')
                raise Exception
            else:
                app.softwareDir = app.config['local_softwareDir']
        if app.environment==[]:
            if app.config['local_environment']!=[]:
                app.environment = app.config['local_environment']

        if app.environment==[]:
            args = ['-b',app.ratBaseVersion,'-m',ratMacro,'-d',outputDir,'-s',app.softwareDir]
            if app.ratVersion!=None:
                args += ['-v',app.ratVersion]
                #ship code to backend
                zipFileName = app.zipFileName
                decimated = zipFileName.split('/')
                zipFileName = decimated[len(decimated)-1]
                args += ['-f',zipFileName]
            if app.outputFile:
                args += ['-o',app.outputFile]
            if app.inputFile:
                args += ['-i',app.inputFile]
            if app.nEvents:
                args += '-N %s '%app.nEvents
            elif app.tRun:
                args += '-T %s '%app.tRun
            if app.useDB:
                args += '--dbuser %s '%(app.config['rat_db_user'])
                args += '--dbpassword %s '%(app.config['rat_db_pswd'])
                args += '--dbname %s '%(app.config['rat_db_name'])
                args += '--dbprotocol %s '%(app.config['rat_db_protocol'])
                args += '--dburl %s '%(app.config['rat_db_url'])
            if app.discardOutput:
                args += '--nostore '

            app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)

            return StandardJobConfig(File('%s/ratRunner.py' % _app_directory),
                                     inputbox = app._getParent().inputsandbox,
                                     outputbox = app._getParent().outputsandbox,
                                     args = args)

        else:#running somewhere a specific environment needs to be setup first
            #can either use a specific file or a list of strings.  the latter needs to be converted to a temp file and shipped.
            envFile=None
            rrArgs = ''
            spArgs = []

            rrArgs += '-b %s '%app.ratBaseVersion
            rrArgs += '-m %s '%ratMacro
            rrArgs += '-d %s '%outputDir
            rrArgs += '-s %s '%app.softwareDir
            spArgs += ['-s','ratRunner.py','-l','misc']
            
            if type(app.environment)==list:
                #need to get the username
                tempname = 'tempRATUserEnv_%s'%os.getlogin()
                tempf = file('/tmp/%s'%(tempname),'w')
                for line in app.environment:
                    tempf.write('%s \n' % line)
                tempf.close()
                app._getParent().inputsandbox.append('/tmp/%s'%(tempname))
                envFile=tempname
            else:
                app._getParent().inputsandbox.append(app.environment)
                envFile=os.path.basename(app.environment)
            if app.ratVersion!=None:
                rrArgs += '-v %s '%app.ratVersion
                #ship code to backend
                zipFileName = app.zipFileName
                decimated = zipFileName.split('/')
                zipFileName = decimated[len(decimated)-1]
                rrArgs += '-f %s '%zipFileName
            if app.outputFile:
                rrArgs += '-o %s '%app.outputFile
            if app.inputFile:
                rrArgs += '-i %s '%app.inputFile
            if app.nEvents:
                rrArgs += '-N %s '%app.nEvents
            elif app.tRun:
                rrArgs += '-T %s '%app.tRun
            if app.useDB:
                rrArgs += '--dbuser %s '%(app.config['rat_db_user'])
                rrArgs += '--dbpassword %s '%(app.config['rat_db_pswd'])
                rrArgs += '--dbname %s '%(app.config['rat_db_name'])
                rrArgs += '--dbprotocol %s '%(app.config['rat_db_protocol'])
                rrArgs += '--dburl %s '%(app.config['rat_db_url'])
            if app.discardOutput:
                rrArgs += '--nostore '

            spArgs += ['-f',envFile]
            spArgs += ['-a','%s'%rrArgs]
                    
            app._getParent().inputsandbox.append('%s/ratRunner.py' % _app_directory)
            app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)
            
            return StandardJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
                                     inputbox = app._getParent().inputsandbox,
                                     outputbox = app._getParent().outputsandbox,
                                     args = spArgs)

###################################################################

class UserWGRTHandler(IRuntimeHandler):
    '''RTHandler for WestGrid submission.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Prepare method: called to configure the job for the specified backend.
        '''
        logger.debug('RAT::WGRTHandler prepare ...')
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        job=app.getJobObject()

        #remove the leading directory path of the macro (on the grid node we'll just
        #have the macro file)
        decimated = app.ratMacro.split('/')
        ratMacro  = decimated[len(decimated)-1]

        #Set the output directory
        if app.outputDir==None:
            if app.config['grid_outputDir']==None:
                logger.error('Output directory not defined')
                raise Exception
            else:
                app.outputDir = app.config['grid_outputDir']
        outputDir = app.outputDir

        if app.softwareDir==None:
            if app.config['local_softwareDir']==None:
                logger.error('RATUser requires softwareDir to be defined if running on any backend other than LCG')
                raise Exception
            else:
                app.softwareDir = app.config['local_softwareDir']

        voproxy = job.backend.voproxy
        if voproxy==None:
            #use the proxy from the environment (default behaviour)            
            try:
                voproxy = os.environ["X509_USER_PROXY"]
            except:
                logger.error('Cannot run without voproxy either in environment (X509_USER_PROXY) or specified for WG backend')
                raise Exception
        if not os.path.exists(voproxy):            
            logger.error('Valid WestGrid backend voproxy location MUST be specified: %s'%(voproxy))
            raise Exception

        rrArgs = ''
        spArgs = []

        rrArgs += '-g srm '#always use the srm copy mode
        rrArgs += '-b %s '%app.ratBaseVersion
        rrArgs += '-m %s '%ratMacro
        rrArgs += '-d %s '%outputDir
        rrArgs += '-s %s '%app.softwareDir
        rrArgs += '--voproxy %s '%voproxy
        spArgs += ['-s','ratRunner.py','-l','wg']

        job.backend.extraopts+="-l pmem=2gb,walltime=28:00:00"
        if app.ratVersion!=None:
            #add a memory requirement (compilation requires 2GB ram)
            #job.backend.extraopts+="-l pmem=2gb,walltime=28:00:00"
            rrArgs += '-v %s '%app.ratVersion
            #ship code to backend
            zipFileName = app.zipFileName
            decimated = zipFileName.split('/')
            zipFileName = decimated[len(decimated)-1]
            rrArgs += '-f %s '%zipFileName
        if app.outputFile:
            rrArgs += '-o %s '%app.outputFile
        if app.inputFile:
            rrArgs += '-i %s '%app.inputFile
        if app.nEvents:
            rrArgs += '-N %s '%app.nEvents
        elif app.tRun:
            rrArgs += '-T %s '%app.tRun
        if app.useDB:
            rrArgs += '--dbuser %s '%(app.config['rat_db_user'])
            rrArgs += '--dbpassword %s '%(app.config['rat_db_pswd'])
            rrArgs += '--dbname %s '%(app.config['rat_db_name'])
            rrArgs += '--dbprotocol %s '%(app.config['rat_db_protocol'])
            rrArgs += '--dburl %s '%(app.config['rat_db_url'])
        if app.discardOutput:
            rrArgs += '--nostore '

        spArgs += ['-a','%s'%rrArgs]
                    
        app._getParent().inputsandbox.append('%s/ratRunner.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)
        
        return StandardJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
                                 inputbox = app._getParent().inputsandbox,
                                 outputbox = app._getParent().outputsandbox,
                                 args = spArgs)

###################################################################

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('RATUser','Local', UserRTHandler)
allHandlers.add('RATUser','PBS', UserRTHandler)
allHandlers.add('RATUser','SGE', UserRTHandler)
allHandlers.add('RATUser','Condor', UserRTHandler)
allHandlers.add('RATUser','LCG', UserLCGRTHandler)
allHandlers.add('RATUser','TestSubmitter', UserRTHandler)
allHandlers.add('RATUser','Interactive', UserRTHandler)
allHandlers.add('RATUser','Batch', UserRTHandler)
allHandlers.add('RATUser','WestGrid', UserWGRTHandler)

logger = Ganga.Utility.logging.getLogger()
