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
# Can either use a token to download the snapshot (be sure
# to delete token afterwards) or ships the code in the input
# sandbox.
#
# Classes:
#  - RATUser: user analysis/simulation applicaiton
#  - RATUserSplitter: splitter to create subjobs for subruns
#  - UserRTHandler: handles submission to local/batch backends
#  - UserLCGRTHandler: handles submission to LCG backend
#
# Revision History:
#  - 03/11/12: M. Mottram: first revision with proper documentation!
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

class RATUser(IApplication):
    """The RAT job handler for data production and processing"""

    #_schema is required for any Ganga plugin
    #Add any options that are required, but try to set sensible default values to minimise effort required of user
    _schema = Schema(Version(1,1), {
            'ratMacro'          : SimpleItem(defvalue='',doc='String pointing to the macro file to run',
                                             typelist=['str']),
            'outputFile'        : SimpleItem(defvalue=None,doc='Output file name, macro must have outroot processor, but no output file defined!',
                                             typelist=['str','type(None)']),
            'inputFile'         : SimpleItem(defvalue=None,doc='Input file name, macro cannot have the inroot process defined within!',
                                             typelist=['str','type(None)']),
            'ratVersion'        : SimpleItem(defvalue='',doc='RAT version tag for the version to download and install',
                                             typelist=['str']),
            'ratBaseVersion'    : SimpleItem(defvalue='dev',doc='RAT version that ratVersion derives from, necessary to get the correct libraries (ROOT, Geant4 etc)',
                                             typelist=['str',"int"]),
            'token'             : SimpleItem(defvalue='',doc='OAuth token required to download code snapshot',
                                             typelist=['str']),#It might be better to ship an entire copy of the code with the job...
            'outputDir'         : SimpleItem(defvalue='',doc='Which output directory should we use (default Grid: RATUser/general/, should be modified to RATUser/<your-name> if you dont want admins to mess with it!)',
                                             typelist=['str']),
            'cacheDir'          : SimpleItem(defvalue='$HOME',doc='Cache path to download zips of RAT into (to be packaged and shipped with jobs).  Zips will not be removed.',
                                             typelist=['str']),
            'softwareDir'       : SimpleItem(defvalue='',doc='Software (snoing install) directory, required if running on a non-LCG backend',
                                             typelist=['str']),
            'environment'       : SimpleItem(defvalue=None,doc='list of strings with the commands to setup the correct backend environment, or single string location of a file with the appropriate commands (if necessary)',typelist=['list','str','type(None)']),
            'nEvents'           : SimpleItem(defvalue=None,doc='Number of events to run, MUST not define number of events in the macro (/rat/run/start)',
                                             typelist=['int','type(None)']),
            })
    
    _category = 'applications'
    _name = 'RATUser'

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
        if self.ratMacro!='':
            job.inputsandbox.append(File(self.ratMacro))
        else:
            logger.error('Rat macro not defined')
            raise Exception
        if self.outputDir=='':
            logger.error('Output directory not defined')
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
        if self.nEvents:
            if RATUtil.checkOption(self.ratMacro,'/rat/run/start'):
                logger.error('Cannot specify number of events in both macro and in the ganga Job - either/or')
                raise Exception
        if self.inputFile:
            if RATUtil.checkText(self.ratMacro,['inroot/read']):
                logger.error('Cannot specify inputFile in Ganga job if "/rat/inroot/read" line is present in macro')
                raise Exception
            
        #Always run rat with a log called rat.log
        job.outputsandbox.append('rat.log')
        job.outputsandbox.append('return_card.js')

        if self.token=='' and self.ratVersion!='':
            #download the code locally
            #only uses the main SNO+ rat branch for now
            #need to add pkl object to inform which branch we have and add others when required
            self.zipFileName = RATUtil.MakeRatSnapshot(self.ratVersion,'rat/',os.path.expanduser('~/gaspCache'))
            job.inputsandbox.append(File(self.zipFileName))
        else:
            #use a token to download the code on the backend
            pass

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
        
        subjobs = []

        for i,rm in enumerate(self.ratMacro):
            j = self.createSubjob(job)
            j.application.ratMacro = rm
            if self.outputFile!=[]:
                j.application.outputFile = self.outputFile[i]
            if self.inputFile!=[]:
                j.application.inputFile = self.inputFile[i]
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
        outputDir = app.outputDir
        #have removed this option - should remove this too (but add some sanity check on the dir)
        if outputDir == '':
            outputDir = 'RATUser/general'
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
    
        if app.ratVersion!='':
            rrArgs += '-v %s '%app.ratVersion
            if app.token!='':
                #download code at backend
                rrArgs += '-t %s '%app.token
            else:
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
            rrArgs += '-n %s '%app.nEvents

        spArgs += ['-a','"%s"'%rrArgs]#appends ratRunner args

        gaspDir = os.environ["GASP_DIR"]

        app._getParent().inputsandbox.append('%s/GangaSNOplus/Lib/ratRunner.py' % gaspDir)

        return LCGJobConfig(File('%s/GangaSNOplus/Lib/sillyPythonWrapper.py' % gaspDir),
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

        #Set the output directory
        outputDir = app.outputDir

        if app.softwareDir=='':
            logger.error('RATUser requires softwareDir to be defined if running on any backend other than LCG')
            raise Exception

        if app.environment==None:
            args = ['-b',app.ratBaseVersion,'-m',ratMacro,'-d',outputDir,'-s',app.softwareDir]
            if app.ratVersion!='':
                args += ['-v',app.ratVersion]
                if app.token!='':
                    #download code at backend
                    args += ['-t',app.token]
                else:
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
                args += '-n %s '%app.nEvents

            gaspDir = os.environ["GASP_DIR"]
            return StandardJobConfig(File('%s/GangaSNOplus/Lib/ratRunner.py' % gaspDir),
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
            if app.ratVersion!='':
                rrArgs += '-v %s '%app.ratVersion
                if app.token!='':
                    #download code at backend
                    rrArgs += '-t %s '%app.token
                else:
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
                rrArgs += '-n %s '%app.nEvents

            spArgs += ['-f',envFile]
            spArgs += ['-a','%s'%rrArgs]
                    
            gaspDir = os.environ["GASP_DIR"]
            
            app._getParent().inputsandbox.append('%s/GangaSNOplus/Lib/ratRunner.py' % gaspDir)
            
            return StandardJobConfig(File('%s/GangaSNOplus/Lib/sillyPythonWrapper.py' % gaspDir),
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
        outputDir = app.outputDir

        if app.softwareDir=='':
            logger.error('RATUser requires softwareDir to be defined if running on any backend other than LCG')
            raise Exception
        if job.backend.voproxy==None or not os.path.exists(job.backend.voproxy):
            logger.error('Valid WestGrid backend voproxy location MUST be specified.')
            raise Exception
        if job.backend.myproxy==None or not os.path.exists(job.backend.myproxy):
            logger.error('Valid WestGrid backend myproxy location MUST be specified.')
            raise Exception

        rrArgs = ''
        spArgs = []

        rrArgs += '-g srm '#always use the srm copy mode
        rrArgs += '-b %s '%app.ratBaseVersion
        rrArgs += '-m %s '%ratMacro
        rrArgs += '-d %s '%outputDir
        rrArgs += '-s %s '%app.softwareDir
        rrArgs += '--voproxy %s '%job.backend.voproxy
        rrArgs += '--myproxy %s '%job.backend.myproxy
        spArgs += ['-s','ratRunner.py','-l','wg']

        if app.ratVersion!='':
            #add a memory requirement (compilation requires 2GB ram)
            job.backend.extraopts+="-l pmem=2gb,walltime=28:00:00"
            rrArgs += '-v %s '%app.ratVersion
            if app.token!='':
                #download code at backend
                rrArgs += '-t %s '%app.token
            else:
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
            rrArgs += '-n %s '%app.nEvents

        spArgs += ['-a','%s'%rrArgs]
                    
        gaspDir = os.environ["GASP_DIR"]
        
        app._getParent().inputsandbox.append('%s/GangaSNOplus/Lib/ratRunner.py' % gaspDir)
        
        return StandardJobConfig(File('%s/GangaSNOplus/Lib/sillyPythonWrapper.py' % gaspDir),
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
