######################################################
# RTRATUser.py
# ---------
# Author: Matt Mottram
#         <m.mottram@qmul.ac.uk>
#
# Description:
#    Ganga runtime handlers for SNO+ RATUser application.
#
# Prepares RATUser application for given backend.
#
#  - UserRTHandler: handles submission to local/batch backends
#  - UserWGRTHandler: handles submission to WestGrid backend
#  - UserLCGRTHandler: handles submission to LCG backend
#  - UserDiracRTHandler: handles submission to Dirac backend
#
# Revision History:
#  - 26/08/14: M. Mottram - moved from RATUser to allow testing
#              functions in testRatUser.py to work.
#
######################################################

import os
import string

from GangaSNOplus.Lib.Utilities import RATUtil

from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from GangaCore.GPIDev.Lib.File import *

from GangaDirac.Lib.RTHandlers.DiracRTHUtils import mangle_job_name, diracAPI_script_settings, API_nullifier


# Assume that the applications should come from the same GangaSNOplus/Lib/Applications directory
_app_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "../Applications"))

class UserLCGRTHandler(IRuntimeHandler):
    '''RTHandler for Grid submission.
    Could include CE options and tags here.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Prepare method: called to configure the job for the specified backend.
        '''
        logger.debug('RAT::LCGRTHandler prepare ...')
        from GangaCore.Lib.LCG import LCGJobConfig

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

        # Check the current worker node permissions
        grid_config = RATUtil.GridConfig.get_instance()
        for ce in grid_config.get_excluded_worker_nodes():
            ganga_job.backend.requirements.excludedCEs += '%s ' % ce

        # By default require 1500 MB of RAM
        memory_set = False
        if len(job.backend.requirements.other) != 0:
            for r in job.backend.requirements.other:
                if "GlueHostMainMemoryRAMSize" in r:
                    memory_set = True
        if not memory_set:
            job.backend.requirements.other += ['other.GlueHostMainMemoryRAMSize >= 1500']

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
        if app.softwareEnvironment is None:
            # The relative path for CVMFS, SNOPLUS_CVMFS_DIR will be set at the backend
            # Note the extra \ to escape the dollar in the initial python wrapper
            rrArgs += '-e \$SNOPLUS_CVMFS_DIR/sw/%s/env_rat-%s.sh ' % (app.ratBaseVersion, app.ratBaseVersion)
        else:
            rrArgs += '-e %s ' % app.softwareEnvironment
    
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
    
        spArgs += ['-a','"%s"'%rrArgs] #appends ratRunner args
        spArgs += ['ratRunner.py', 'lcg']

        app._getParent().inputsandbox.append('%s/ratRunner.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/check_root_output.py' % _app_directory)

        return LCGJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
                            inputbox = app._getParent().inputsandbox,
                            outputbox = app._getParent().outputsandbox,
                            args = spArgs)
    

def get_sandbox(app, appsubconfig, appmasterconfig, jobmasterconfig):
    job=app.getJobObject()

    inputsandbox = []
    outputsandbox = []
    ## Here add any sandbox files coming from the appsubconfig
    ## currently none. masterjobconfig inputsandbox added automatically
    if appsubconfig   : inputsandbox  += appsubconfig.getSandboxFiles()
    
    ## Strangly NEITHER the master outputsandbox OR job.outputsandbox
    ## are added automatically.
    if jobmasterconfig: 
        outputsandbox += jobmasterconfig.getOutputSandboxFiles()
    if appsubconfig: 
        outputsandbox += appsubconfig.getOutputSandboxFiles()
    inputset = set(inputsandbox)
    inputsandbox = list(inputset)
    outputset = set(outputsandbox)
    outputsandbox = list(outputset)
    
    print "Sandbox: ", inputsandbox, outputsandbox

    return inputsandbox, outputsandbox



###################################################################
class UserDiracRTHandler(IRuntimeHandler):
    '''RTHandler for Diract Grid submission.
    Could include CE options and tags here.
    '''

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Prepare method: called to configure the job for the specified backend.
        '''
        logger.debug('RAT::DiracRTHandler prepare ...')
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

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

        # Check the current worker node permissions
        grid_config = RATUtil.GridConfig.get_instance()

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
        if app.softwareEnvironment is None:
            # The relative path for CVMFS, SNOPLUS_CVMFS_DIR will be set at the backend
            # Note the extra \ to escape the dollar in the initial python wrapper
            rrArgs += '-e \$SNOPLUS_CVMFS_DIR/sw/%s/env_rat-%s.sh ' % (app.ratBaseVersion, app.ratBaseVersion)
        else:
            rrArgs += '-e %s ' % app.softwareEnvironment
    
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
    
        spArgs += ['-a','"%s"'%rrArgs] #appends ratRunner args
        spArgs += ['ratRunner.py', 'lcg']

        app._getParent().inputsandbox.append('%s/ratRunner.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/check_root_output.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/sillyPythonWrapper.py' % _app_directory)
        app._getParent().outputsandbox.append('Ganga_Executable.log')

        # Use the base script from GangaDirac.Lib.RTHandlers.DiracRTHUtils.diracAPI_script_template
        # (But explicitely place here in case of updates to the GangaDirac base script and updated for template strings)
        # 
        template_script = string.Template('''
# dirac job created by ganga
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
${DIRAC_IMPORT}
${DIRAC_JOB_IMPORT}
dirac = ${DIRAC_OBJECT}
j = ${JOB_OBJECT}
    
# default commands added by ganga
j.setName('${NAME}')
j.setExecutable('${EXE}','${EXE_ARG_STR}','${EXE_LOG_FILE}')
j.setExecutionEnv(${ENVIRONMENT})
j.setInputSandbox(${INPUT_SANDBOX})
j.setOutputSandbox(${OUTPUT_SANDBOX})
#j.setInputData(${INPUTDATA}) # SNO+ don't use this right now
#j.setParametricInputData(${PARAMETRIC_INPUTDATA}) # SNO+ don't use this right now

#j.setOutputData(${OUTPUTDATA},outputPath='${OUTPUT_PATH}',outputSE=${OUTPUT_SE}) # uploads handled by the ratRunner script
    
# <-- user settings
${SETTINGS}
# user settings -->

# diracOpts added by user
${DIRAC_OPTS}

# submit the job to dirac
j.setPlatform( 'ANY' )
result = dirac.submitJob(j)
output(result)
''')

        inputsandbox = []
        outputsandbox = []
        for i in app._getParent().inputsandbox:
            inputsandbox.append(i.name) # not sure why these are File objects...
        for i in app._getParent().outputsandbox:
            outputsandbox.append(i) # ...while these are strings

        dirac_script = template_script.substitute(
            DIRAC_IMPORT         = 'from DIRAC.Interfaces.API.Dirac import Dirac',
            DIRAC_JOB_IMPORT     = 'from DIRAC.Interfaces.API.Job import Job',
            DIRAC_OBJECT         = 'Dirac()',
            JOB_OBJECT           = 'Job()',
            NAME                 = mangle_job_name(app),
            EXE                  = 'sillyPythonWrapper.py',
            EXE_ARG_STR          = ' '.join([str(arg) for arg in spArgs]),
            EXE_LOG_FILE         = 'Ganga_Executable.log',
            ENVIRONMENT          = {'LFC_HOST': 'lfc.gridpp.rl.ac.uk'},
            INPUTDATA            = '##input_data##', # not used by sno+
            PARAMETRIC_INPUTDATA = '##parametricinput_data##', # not used by sno+
            OUTPUT_SANDBOX       = API_nullifier(outputsandbox),
            OUTPUTDATA           = '##API_nullifier(list(outputfiles))##',
            OUTPUT_PATH          = '##""##', # job.fqid,
            SETTINGS             = diracAPI_script_settings(app),
            DIRAC_OPTS           = job.backend.diracOpts,
            #REPLICATE            = getConfig('DIRAC')['ReplicateOutputData'], # This option isn't even in the script!
            # leave the sandbox for altering later as needs
            # to be done in backend.submit to combine master.
            # Note only using 2 #s as auto-remove 3
            INPUT_SANDBOX        = API_nullifier(inputsandbox)
        )

        print dirac_script

        return StandardJobConfig(dirac_script,
                                 inputbox =  app._getParent().inputsandbox,
                                 outputbox = app._getParent().outputsandbox)

###################################################################

class UserRTHandler(IRuntimeHandler):
    '''RTHandler for Batch and Local submission.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Prepare method: called to configure the job for the specified backend.
        '''
        logger.debug('RAT::RTHandler prepare ...')
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

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
        if app.softwareEnvironment is None:
            if app.config['local_softwareEnvironment'] is None:
                logger.error('RATUser requires softwareEnvironment to be defined if running on any backend other than LCG')
                raise Exception
            else:
                app.softwareEnvironment = app.config['local_softwareEnvironment']
        if app.environment==[]:
            if app.config['local_environment']!=[]:
                app.environment = app.config['local_environment']

        if app.environment==[]:
            args = ['-b',app.ratBaseVersion,'-m',ratMacro,'-d',outputDir,'-e',app.softwareEnvironment]
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
                args += ['-N',str(app.nEvents)]
            elif app.tRun:
                args += ['-T',app.tRun]
            if app.useDB:
                args += ['--dbuser',app.config['rat_db_user']]
                args += ['--dbpassword',app.config['rat_db_pswd']]
                args += ['--dbname',app.config['rat_db_name']]
                args += ['--dbprotocol',app.config['rat_db_protocol']]
                args += ['--dburl',app.config['rat_db_url']]
            if app.discardOutput:
                args += ['--nostore']

            app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)
            app._getParent().inputsandbox.append('%s/check_root_output.py' % _app_directory)

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
            rrArgs += '-e %s '%app.softwareEnvironment
            
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
            spArgs += ['ratRunner.py', 'misc']
  
            app._getParent().inputsandbox.append('%s/ratRunner.py' % _app_directory)
            app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)
            app._getParent().inputsandbox.append('%s/check_root_output.py' % _app_directory)
            
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
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

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

        if app.softwareEnvironment is None:
            if app.config['local_softwareEnvironment'] is None:
                logger.error('RATUser requires softwareEnvironment to be defined if running on any backend other than LCG')
                raise Exception
            else:
                app.softwareEnvironment = app.config['local_softwareEnvironment']

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
        rrArgs += '-e %s '%app.softwareEnvironment
        rrArgs += '--voproxy %s '%voproxy

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
        spArgs += ['ratRunner.py','wg']
                    
        app._getParent().inputsandbox.append('%s/ratRunner.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/job_tools.py' % _app_directory)
        app._getParent().inputsandbox.append('%s/check_root_output.py' % _app_directory)

        return StandardJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
                                 inputbox = app._getParent().inputsandbox,
                                 outputbox = app._getParent().outputsandbox,
                                 args = spArgs)

###################################################################
