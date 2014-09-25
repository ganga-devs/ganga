######################################################
# RATUser.py
# ---------
# Author: Matt Mottram
#         <m.mottram@qmul.ac.uk>
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
#  - 26/08/14: M. Mottram: moved RT handlers to separate file (RTRATUser.py)
# 
######################################################

import os

from GangaSNOplus.Lib.Utilities import RATUtil

from Ganga.Core.exceptions import ApplicationConfigurationError

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *

from Ganga.GPIDev.Lib.File import *

###################################################################

config = Ganga.Utility.Config.makeConfig('defaults_RATUser','Defaults for the RATUser application')

config.addOption('local_softwareEnvironment', None, 'Local snoing-install directory (or directory with env_rat-x.y.sh files)')
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
            'softwareEnvironment': SimpleItem(defvalue=None,doc='Software environment file, required if running on a non-LCG backend',
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
        if not self.outputFile:
            logger.error('No output file defined!')   #checks if output file set in command line
            raise Exception 
        if RATUtil.check_command(self.ratMacro,['/rat/procset','file']):
            logger.error('Output file should only be defined via the command line and not in the macro!')  #checks if output file si set in the macro
            raise Exception
        if self.outputFile:
            if not RATUtil.check_command(self.ratMacro,['/rat/proclast','outroot']) and not RATUtil.check_command(self.ratMacro,['/rat/proclast','outntuple']) and not RATUtil.check_command(self.ratMacro,['/rat/proclast','outsoc']):
                logger.error('Have specified an output file, but no root, ntuple or soc processor present in macro')  #checks for putroot processor (needs to be there regardless where the output file is defined)
                raise Exception
        if not self.nEvents and not self.tRun:
            logger.error('Need to specifiy either the number of events or the duration of run! No number of events or run duration defined!') #checks if number of events or run duration is set in command line
            raise Exception
        if self.nEvents and self.tRun:
            logger.error('Cannot specify number of events and the duration of run!')
            raise Exception
        if not RATUtil.check_command(self.ratMacro,['/rat/run/start','']):
            logger.error('/rat/run/start must be set in the macro but no number of events should be specified! Number of events should only be defined via the command line!')  #check if the /rat/run/start command is set
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
            self.zipFileName = RATUtil.make_rat_snapshot(self.ratFork, self.ratVersion, self.versionUpdate, os.path.expanduser(config['cacheDir']))
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

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaSNOplus.Lib.RTHandlers.RTRATUser import UserRTHandler, UserLCGRTHandler, UserWGRTHandler, UserDiracRTHandler

allHandlers.add('RATUser','Local', UserRTHandler)
allHandlers.add('RATUser','PBS', UserRTHandler)
allHandlers.add('RATUser','SGE', UserRTHandler)
allHandlers.add('RATUser','Condor', UserRTHandler)
allHandlers.add('RATUser','LCG', UserLCGRTHandler)
allHandlers.add('RATUser','Dirac', UserDiracRTHandler)
allHandlers.add('RATUser','TestSubmitter', UserRTHandler)
allHandlers.add('RATUser','Interactive', UserRTHandler)
allHandlers.add('RATUser','Batch', UserRTHandler)
allHandlers.add('RATUser','WestGrid', UserWGRTHandler)

logger = Ganga.Utility.logging.getLogger()
