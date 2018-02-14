######################################################
# RATProd.py
# ---------
# Author: Matt Mottram
#         <m.mottram@qmul.ac.uk>
#
# Description:
#    Ganga application for SNO+ production.
#
# Runs tagged RAT releases on a given backend via ratProdRunner.py
#
# Classes:
#  - RATProd: production applicaiton
#  - RATSplitter: splitter to create subjobs for subruns
#
# Revision History:
#  - 03/11/12: M. Mottram: first revision with proper documentation!
#  - 26/08/14: M. Mottram: moved RTHandlers to RTRATProd.py
# 
######################################################

import os

from GangaCore.Core.exceptions import ApplicationConfigurationError

from GangaCore.GPIDev.Adapters.IApplication import IApplication
from GangaCore.GPIDev.Adapters.ISplitter import ISplitter
from GangaCore.GPIDev.Schema import *

from GangaCore.GPIDev.Lib.File import *

###################################################################

config = GangaCore.Utility.Config.makeConfig('defaults_RATProd','Defaults for the RATProd application')

config.addOption('local_softwareEnvironment', None, 'Local snoing-install directory (or directory with env_rat-x.y.sh files)')
config.addOption('local_environment', [], 'Environment options required to run on local or batch system')
config.addOption('local_outputDir', None, '*Default* output directory if running on a batch or local system (can override)')
config.addOption('grid_outputDir', None, '*Defult* output directory if running on system with grid storage (can override)')

# Assume that the applications should come from the same GangaSNOplus directory        
_app_directory = os.path.dirname(__file__)

###################################################################

class RATProd(IApplication):
    """The RAT job handler for data production and processing"""

    #_schema is required for any Ganga plugin
    _schema = Schema(Version(1,1), {            
            'environment'       : SimpleItem(defvalue=None,doc='list of strings with the commands to setup the correct backend environment, or single string location of a file with the appropriate commands (if necessary)',typelist=['list','str','type(None)']),
            'discardOutput'     : SimpleItem(defvalue=False,doc='Do not store the output: default False',typelist=['bool']),
            'inputDir'          : SimpleItem(defvalue='',doc='Provide a relative path (to the base dir - dependent on the backend) for any inputs',
                                             typelist=['str']),
            'inputFiles'        : SimpleItem(defvalue=[],doc='Input file names (must be a list)',
                                             typelist=['list','str']),#don't use if splitting
            'outputLog'         : SimpleItem(defvalue='rat_output.log',doc='Log file name (only used if RAT is run)',
                                             typelist=['str']),
            'outputDir'        : SimpleItem(defvalue='',doc='Provide a relative path (to the base dir - dependent on the backend) for the file to be archived',
                                             typelist=['str']),
            'outputFiles'       : SimpleItem(defvalue=[],doc='Output file names (can be a list)',
                                             typelist=['list','str']),#don't use if splitting
            'prodScript'        : SimpleItem(defvalue='',doc='String pointing to the script file to run.  Can be set by splitter.',
                                             typelist=['str']),
            'rat_db_name'       : SimpleItem(defvalue=None, doc='RAT db name', typelist=['str', 'type(None)']),
            'rat_db_pswd'       : SimpleItem(defvalue=None, doc='RAT db password', typelist=['str', 'type(None)']),
            'rat_db_protocol'   : SimpleItem(defvalue=None, doc='RAT db protocol', typelist=['str', 'type(None)']),
            'rat_db_url'        : SimpleItem(defvalue=None, doc='RAT db password', typelist=['str', 'type(None)']),
            'rat_db_user'       : SimpleItem(defvalue=None, doc='RAT db password', typelist=['str', 'type(None)']),
            'softwareEnvironment': SimpleItem(defvalue=None,doc='Software environment file, required if running on a non-LCG backend',
                                             typelist=['str', 'type(None)']),
            'ratMacro'          : SimpleItem(defvalue='',doc='String pointing to the macro file to run.  Can be set by splitter.',
                                             typelist=['str']),#shouldn't this be in the input sandbox?
            'ratVersion'        : SimpleItem(defvalue='4',doc='RAT version tag, necessary to setup environment (even if not running RAT)',
                                             typelist=['str']),
            'useDB'             : SimpleItem(defvalue=False,doc='Use the RAT database (snopl.us)?',typelist=['bool']),
            })
    
    _category = 'applications'
    _name = 'RATProd'

    def configure(self,masterappconfig):
        '''Configure method, called once per job.
        '''
        logger.debug('RAT::RATProd configure ...')

        job = self._getParent()
        masterjob = job._getParent()

        if self.prodScript=='' and self.ratMacro=='':
            logger.error('prodScript or ratMacro not defined')
            raise Exception
        elif self.prodScript!='' and self.ratMacro!='':
            logger.error('both prodScript and ratMacro are defined')
            raise Exception
        if self.useDB:
            if not config['rat_db_pswd']:
                logger.error('Need a password in order to contact the ratdb database')
                raise Exception

        #The production script is added in, line by line, into the submission script
        #job.inputsandbox.append(File(self.prodScript))

        if self.ratMacro!='':
            #decimated=self.ratMacro.split('/')
            #macFile=decimated[len(decimated)-1]#for when I thought we needed args to go with the script
            job.inputsandbox.append(File(self.ratMacro))
            #Always run rat with a log called rat.log
            job.outputsandbox.append('rat.log')
        else:
            job.inputsandbox.append(File(self.prodScript))
        job.outputsandbox.append('return_card.js')

        #we want a list of files - if we only have one (i.e. a string) then just force into a list
        if type(self.inputFiles) is str:
            self.inputFiles = [self.inputFiles]
        if type(self.outputFiles) is str:
            self.outputFiles = [self.outputFiles]

        return(None,None)
                     
###################################################################

class RATSplitter(ISplitter):
    '''Splitter for RAT jobs.  Essentially an ArgSplitter, but specifically for the output files.
    '''
    _name = "RATSplitter"
    _schema = Schema(Version(1,0), {
            'outputFiles' : SimpleItem(defvalue=[],typelist=['list','str'],sequence=1,doc='A list of lists for specifying output files.  Only works with the RATProd application.'),
            'inputFiles'  : SimpleItem(defvalue=[],typelist=['list','str'],sequence=1,doc='A list of lists for specifying intput files.  Only works with the RATProd application.'),
            'prodScript'  : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc='A list specifying the scripts (if used).'),
            'ratMacro'    : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc='A list specifying the macros (if used)'),
            } )
    
    def split(self,job):
        
        subjobs = []

        fin=True
        fout=True
        nFiles=0

        #check if there are input or output files
        if len(self.inputFiles)==0:
            fin=False
        else:
            nFiles = len(self.inputFiles)
        if len(self.outputFiles)==0:
            fout=False
        else:
            nFiles = len(self.outputFiles)
        nScript = len(self.prodScript)
        nMacro = len(self.ratMacro)
        if nScript!=0 and nMacro!=0:
            logger.error('cannot provide both scripts and macros, must be either or')
            raise Exception
        if nScript==0 and nMacro==0:
            logger.error('must provide EITHER macros or scripts')
            raise Exception

        #if neither, what the hell are we splitting for!?
        if fin==False and fout==False:
            logger.error('Why are you splitting?')
            raise Exception

        #if we have both input and output, they should have the same # entries
        if fin==True and fout==True:
            if len(self.inputFiles)!=len(self.outputFiles):
                logger.error('input/output numbers dont match')
                raise Exception
        if nScript!=0:
            if nScript!=nFiles:
                logger.error('script/file numbers dont match')
                raise Exception
        if nMacro!=0:
            if nMacro!=nFiles:
                logger.error('macro/file numbers dont match')
                raise Exception

        for i in range(nFiles):
            #nFiles should have the same number as either (or both) #input or #output
            j = self.createSubjob(job)
            # Add new arguments to subjob
            if fout is True:
                #if type(self.outputFiles[i]) is not list:
                #    logger.error('need a list of files! %s' % type(self.outputFiles[i]))
                j.application.outputFiles=self.outputFiles[i]
            if fin is True:
                #if type(self.inputFiles[i]) is not list:
                #    logger.error('need a list of files! %s' % type(self.inputFiles[i]))
                j.application.inputFiles=self.inputFiles[i]
            if nMacro!=0:
                j.application.ratMacro=self.ratMacro[i]
            else:
                j.application.prodScript=self.prodScript[i]
            subjobs.append(j)
        return subjobs
    
###################################################################

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaSNOplus.Lib.RTHandlers.RTRATProd import RTHandler, WGRTHandler, LCGRTHandler

allHandlers.add('RATProd','Local', RTHandler)
allHandlers.add('RATProd','PBS', RTHandler)
allHandlers.add('RATProd','SGE', RTHandler)
allHandlers.add('RATProd','Condor', RTHandler)
allHandlers.add('RATProd','LCG', LCGRTHandler)
allHandlers.add('RATProd','TestSubmitter', RTHandler)
allHandlers.add('RATProd','Interactive', RTHandler)
allHandlers.add('RATProd','Batch', RTHandler)
allHandlers.add('RATProd','WestGrid', WGRTHandler)

logger = GangaCore.Utility.logging.getLogger()
