######################################################
# RATProd.py
# ---------
# Author: Matt Mottram
#         <m.mottram@sussex.ac.uk>
#
# Description:
#    Ganga application for SNO+ production.
#
# Runs tagged RAT releases on a given backend via ratProdRunner.py
#
# Classes:
#  - RATProd: production applicaiton
#  - RATSplitter: splitter to create subjobs for subruns
#  - RTHandler: handles submission to local/batch backends
#  - LCGRTHandler: handles submission to LCG backend
#
# Revision History:
#  - 03/11/12: M. Mottram: first revision with proper documentation!
# 
######################################################

import os, re, string, commands
import socket
import random

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

class RATProd(IApplication):
    """The RAT job handler for data production and processing"""

    #_schema is required for any Ganga plugin
    _schema = Schema(Version(1,1), {
            'prodScript'        : SimpleItem(defvalue='',doc='String pointing to the script file to run.  Can be set by splitter.',
                                             typelist=['str']),
            'ratMacro'          : SimpleItem(defvalue='',doc='String pointing to the macro file to run.  Can be set by splitter.',
                                             typelist=['str']),#shouldn't this be in the input sandbox?
            'outputLog'         : SimpleItem(defvalue='rat_output.log',doc='Log file name (only used if RAT is run)',
                                             typelist=['str']),
            'outputFiles'       : SimpleItem(defvalue=[],doc='Output file names (can be a list)',
                                             typelist=['list','str']),#don't use if splitting
            'inputFiles'        : SimpleItem(defvalue=[],doc='Input file names (must be a list)',
                                             typelist=['list','str']),#don't use if splitting
            'ratVersion'        : SimpleItem(defvalue='4',doc='RAT version tag, necessary to setup environment (even if not running RAT)',
                                             typelist=['str']),
            'ratDirectory'      : SimpleItem(defvalue='',doc='RAT directory information: should be the snoing install directory. If different from the default VO_SNOPLUS_SNOLAB_CA_SW_DIR/snoing-install (grid), or SNOPLUS_SW_DIR (batch/local) then this value must be set',
                                             typelist=['str']),
            'outputDir'        : SimpleItem(defvalue='',doc='Provide a relative path (to the base dir - dependent on the backend) for the file to be archived',
                                             typelist=['str']),
            'inputDir'          : SimpleItem(defvalue='',doc='Provide a relative path (to the base dir - dependent on the backend) for any inputs',
                                             typelist=['str']),
            'environment'       : SimpleItem(defvalue=None,doc='list of strings with the commands to setup the correct backend environment, or single string location of a file with the appropriate commands (if necessary)',typelist=['list','str','type(None)']),
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

class RTHandler(IRuntimeHandler):
    '''Standard RTHandler (for all batch/local submission).
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):

        logger.debug('RAT::RTHandler prepare ...')
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        #create the backend wrapper script
        job=app.getJobObject()

        #Check whether we're looking for a non-default sw dir
        if app.ratDirectory=='':
            logger.error('Must specify a RAT directory')
            raise Exception
        else:
            swDir = app.ratDirectory

        #we need to know the name of the file to run
        macroFile = None
        prodFile = None
        if app.ratMacro!='':
            decimated=app.ratMacro.split('/')
            macroFile=decimated[len(decimated)-1]
        else:
            decimated=app.prodScript.split('/')
            prodFile=decimated[len(decimated)-1]

        foutList='['
        finList='['
        for i,var in enumerate(app.outputFiles):
            foutList+='%s,'%var
        for i,var in enumerate(app.inputFiles):
            finList+='%s,'%var
        if len(foutList)!=1:
            foutList='%s]'%foutList[:-1]#remove final comma, add close bracket
        if len(finList)!=1:
            finList='%s]'%finList[:-1]#remove final comma, add close bracket

        if app.environment==None:
            if app.ratMacro!='':
                args = ['-v',app.ratVersion,'-s',swDir,'-m',macroFile,'-d',app.outputDir,'-o',foutList,'-x',app.inputDir,'-i',finList]
            else:
                args = ['-v',app.ratVersion,'-s',swDir,'-k','-m',prodFile,'-d',app.outputDir,'-o',foutList,'-x',app.inputDir,'-i',finList]

            gaspDir = os.environ["GASP_DIR"]
            return StandardJobConfig(File('%s/GangaSNOplus/Lib/ratProdRunner.py' % gaspDir),
                                     inputbox = app._getParent().inputsandbox,
                                     outputbox = app._getParent().outputsandbox,
                                     args = args)
        else:#need a specific environment setup 
            #can either use a specific file or a list of strings.  the latter needs to be converted to a temp file and shipped.
            envFile=None
            if type(app.environment)==list:
                tempname = 'tempRATProdEnv_%s'%os.getlogin()
                tempf = file('/tmp/%s'%(tempname),'w')
                for line in app.environment:
                    tempf.write('%s \n' % line)
                tempf.close()
                app._getParent().inputsandbox.append('/tmp/%s'%(tempname))
                envFile=tempname
            else:
                app._getParent().inputsandbox.append(app.environment)
                envFile=os.path.basename(app.environment)
            if app.ratMacro!='':
                args = '-v %s -s %s -m %s -d %s -o %s -x %s -i %s' % (app.ratVersion,swDir,macroFile,app.outputDir,foutList,app.inputDir,finList)
            else:
                args = '-v %s -s %s -k -m %s -d %s -o %s -x %s -i %s' % (app.ratVersion,swDir,prodFile,app.outputDir,foutList,app.inputDir,finList)

            wrapperArgs = ['-s','ratProdRunner.py','-l','misc','-f',envFile,'-a',args]

            gaspDir = os.environ["GASP_DIR"]
            
            app._getParent().inputsandbox.append('%s/GangaSNOplus/Lib/ratProdRunner.py' % gaspDir)
            
            return StandardJobConfig(File('%s/GangaSNOplus/Lib/sillyPythonWrapper.py' % gaspDir),
                                     inputbox = app._getParent().inputsandbox,
                                     outputbox = app._getParent().outputsandbox,
                                     args = wrapperArgs)

###################################################################

class WGRTHandler(IRuntimeHandler):
    '''WGRTHandler for WestGrid submission.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):

        logger.debug('RAT::RTHandler prepare ...')
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        #create the backend wrapper script
        job=app.getJobObject()

        #Check whether we're looking for a non-default sw dir
        if app.ratDirectory=='':
            logger.error('Must specify a RAT directory')
            raise Exception
        else:
            swDir = app.ratDirectory
        if job.backend.voproxy==None or not os.path.exists(job.backend.voproxy):
            logger.error('Valid WestGrid backend voproxy location MUST be specified.')
            raise Exception
        if job.backend.myproxy==None or not os.path.exists(job.backend.myproxy):
            logger.error('Valid WestGrid backend myproxy location MUST be specified.')
            raise Exception

        #we need to know the name of the file to run
        macroFile = None
        prodFile = None
        if app.ratMacro!='':
            decimated=app.ratMacro.split('/')
            macroFile=decimated[len(decimated)-1]
        else:
            decimated=app.prodScript.split('/')
            prodFile=decimated[len(decimated)-1]

        foutList='['
        finList='['
        for i,var in enumerate(app.outputFiles):
            foutList+='%s,'%var
        for i,var in enumerate(app.inputFiles):
            finList+='%s,'%var
        if len(foutList)!=1:
            foutList='%s]'%foutList[:-1]#remove final comma, add close bracket
        if len(finList)!=1:
            finList='%s]'%finList[:-1]#remove final comma, add close bracket

        if app.ratMacro!='':
            args = '-g srm -v %s -s %s -m %s -d %s -o %s -x %s -i %s --voproxy %s --myproxy %s' % (app.ratVersion,swDir,macroFile,app.outputDir,foutList,app.inputDir,finList,job.backend.voproxy,job.backend.myproxy)
        else:
            args = '-g srm -v %s -s %s -k -m %s -d %s -o %s -x %s -i %s --voproxy %s --myproxy %s' % (app.ratVersion,swDir,prodFile,app.outputDir,foutList,app.inputDir,finList,job.backend.voproxy,job.backend.myproxy)

        wrapperArgs = ['-s','ratProdRunner.py','-l','wg','-a',args]

        gaspDir = os.environ["GASP_DIR"]
        
        app._getParent().inputsandbox.append('%s/GangaSNOplus/Lib/ratProdRunner.py' % gaspDir)
            
        return StandardJobConfig(File('%s/GangaSNOplus/Lib/sillyPythonWrapper.py' % gaspDir),
                                 inputbox = app._getParent().inputsandbox,
                                 outputbox = app._getParent().outputsandbox,
                                 args = wrapperArgs)

###################################################################

class LCGRTHandler(IRuntimeHandler):
    '''RTHandler for Grid submission.
    Could include CE options and tags here.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):

        logger.debug('RAT::LCGRTHandler prepare ...')
        from Ganga.Lib.LCG import LCGJobConfig

        #create the backend wrapper script
        job=app.getJobObject()

        job.backend.requirements.software='VO-snoplus.snolab.ca-rat-%s' % app.ratVersion

        #Check whether we're looking for a non-default sw dir
        if app.ratDirectory=='':
            swDir = '$VO_SNOPLUS_SNOLAB_CA_SW_DIR/snoing-install'
        else:
            swDir = app.ratDirectory

        #we need to know the name of the file to run
        macroFile = None
        prodFile = None
        if app.ratMacro!='':
            decimated=app.ratMacro.split('/')
            macroFile=decimated[len(decimated)-1]
        else:
            decimated=app.prodScript.split('/')
            prodFile=decimated[len(decimated)-1]

        foutList='['
        finList='['
        for i,var in enumerate(app.outputFiles):
            foutList+='%s,'%var
        for i,var in enumerate(app.inputFiles):
            finList+='%s,'%var
        if len(foutList)!=1:
            foutList='%s]'%foutList[:-1]#remove final comma, add close bracket
        if len(finList)!=1:
            finList='%s]'%finList[:-1]#remove final comma, add close bracket

        if app.ratMacro!='':
            args = '"-g lcg -v %s -s %s -m %s -d %s -o %s -x %s -i %s"' % (app.ratVersion,swDir,macroFile,app.outputDir,foutList,app.inputDir,finList)
        else:
            args = '"-g lcg -v %s -s %s -k -m %s -d %s -o %s -x %s -i %s"' % (app.ratVersion,swDir,prodFile,app.outputDir,foutList,app.inputDir,finList)

        wrapperArgs = ['-s','ratProdRunner.py','-l','lcg','-a',args]

        gaspDir = os.environ["GASP_DIR"]

        app._getParent().inputsandbox.append('%s/GangaSNOplus/Lib/ratProdRunner.py' % gaspDir)

        return LCGJobConfig(File('%s/GangaSNOplus/Lib/sillyPythonWrapper.py' % gaspDir),
                            inputbox = app._getParent().inputsandbox,
                            outputbox = app._getParent().outputsandbox,
                            args = wrapperArgs)

###################################################################

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('RATProd','Local', RTHandler)
allHandlers.add('RATProd','PBS', RTHandler)
allHandlers.add('RATProd','SGE', RTHandler)
allHandlers.add('RATProd','Condor', RTHandler)
allHandlers.add('RATProd','LCG', LCGRTHandler)
allHandlers.add('RATProd','TestSubmitter', RTHandler)
allHandlers.add('RATProd','Interactive', RTHandler)
allHandlers.add('RATProd','Batch', RTHandler)
allHandlers.add('RATProd','WestGrid', WGRTHandler)

logger = Ganga.Utility.logging.getLogger()
