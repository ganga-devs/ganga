######################################################
# RTRATProd.py
# ---------
# Author: Matt Mottram
#         <m.mottram@sussex.ac.uk>
#
# Description:
#    Ganga runtime handlers for SNO+ RATProd application.
#
# Prepares RATUser application for given backend.
#
#  - RTHandler: handles submission to local/batch backends
#  - WGRTHandler: handles submission to WestGrid backend
#  - LCGRTHandler: handles submission to LCG backend
#
# Revision History:
#  - 26/08/14: M. Mottram - moved from RATProd (as with RTRATUser)
#
######################################################

import os

from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from GangaCore.GPIDev.Lib.File import *


# Assume that the applications should come from the GangaSNOplus/Lib/Applications directory
_app_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "../Applications"))

class RTHandler(IRuntimeHandler):
    '''Standard RTHandler (for all batch/local submission).
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):

        logger.debug('RAT::RTHandler prepare ...')
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        #create the backend wrapper script
        job=app.getJobObject()

        #Check whether we're looking for a non-default sw dir
        if app.softwareEnvironment is None:
            logger.error('Must specify a RAT environment')
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

        if app.environment==None:
            args = []
            args += ['-v',app.ratVersion]
            args += ['-e',app.softwareEnvironment]
            args += ['-d',app.outputDir]
            args += ['-o',foutList]
            args += ['-x',app.inputDir]
            args += ['-i',finList]
            if app.ratMacro!='':
                args += ['-m',macroFile]
            else:
                args += ['-k','-m',prodFile]
            if app.useDB:
                args += ['--dbuser',app.config['rat_db_user']]
                args += ['--dbpassword',app.config['rat_db_pswd']]
                args += ['--dbname',app.config['rat_db_name']]
                args += ['--dbprotocol',app.config['rat_db_protocol']]
                args += ['--dburl',app.config['rat_db_url']]
            if app.discardOutput:
                args += ['--nostore']
                
            return StandardJobConfig(File('%s/ratProdRunner.py' % _app_directory),
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
            args = ''
            args += '-v %s '%(app.ratVersion)
            args += '-e %s '%(app.softwareEnvironment)
            args += '-d %s '%(app.outputDir)
            args += '-o %s '%(foutList)
            args += '-x %s '%(app.inputDir)
            args += '-i %s '%(finList)
            if app.ratMacro!='':
                args += '-m %s '%(macroFile)
            else:
                args += '-k -m %s '%(prodFile)
            if app.useDB:
                args += '--dbuser %s '%(app.config['rat_db_user'])
                args += '--dbpassword %s '%(app.config['rat_db_pswd'])
                args += '--dbname %s '%(app.config['rat_db_name'])
                args += '--dbprotocol %s '%(app.config['rat_db_protocol'])
                args += '--dburl %s '%(app.config['rat_db_url'])
            if app.discardOutput:
                args += '--nostore '

            wrapperArgs = ['-f', envFile, '-a', '"%s"' % args]
            wrapperArgs += ['ratProdRunner.py', 'misc']

            app._getParent().inputsandbox.append('%s/ratProdRunner.py' % _app_directory)
            
            return StandardJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
                                     inputbox = app._getParent().inputsandbox,
                                     outputbox = app._getParent().outputsandbox,
                                     args = wrapperArgs)

###################################################################

class WGRTHandler(IRuntimeHandler):
    '''WGRTHandler for WestGrid submission.
    '''
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):

        logger.debug('RAT::RTHandler prepare ...')
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        #create the backend wrapper script
        job=app.getJobObject()

        #Check whether we're looking for a non-default sw dir
        if app.softwareEnvironment is None:
            logger.error('Must specify a RAT directory')
            raise Exception

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

        args = ''
        args += '-g srm '
        args += '-v %s '%(app.ratVersion)
        args += '-s %s '%(swDir)
        args += '-d %s '%(app.outputDir)
        args += '-o %s '%(foutList)
        args += '-x %s '%(app.inputDir)
        args += '-i %s '%(finList)
        if app.ratMacro!='':
            args += '-m %s '%(macroFile)
        else:
            args += '-k -m %s '%(prodFile)
        args += '--voproxy %s '%(voproxy)
        if app.useDB:
            args += '--dbuser %s '%(app.config['rat_db_user'])
            args += '--dbpassword %s '%(app.config['rat_db_pswd'])
            args += '--dbname %s '%(app.config['rat_db_name'])
            args += '--dbprotocol %s '%(app.config['rat_db_protocol'])
            args += '--dburl %s '%(app.config['rat_db_url'])
        if app.discardOutput:
            args += '--nostore '

        wrapperArgs = ['-a', '"%s"' % args]
        wrapperArgs += ['ratProdRunner.py', 'wg']

        app._getParent().inputsandbox.append('%s/ratProdRunner.py' % _app_directory)
            
        return StandardJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
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
        from GangaCore.Lib.LCG import LCGJobConfig

        #create the backend wrapper script
        job=app.getJobObject()

        # Check the current worker node permissions
        grid_config = RATUtil.GridConfig.get_instance()
        for ce in grid_config.get_excluded_worker_nodes():
            ganga_job.backend.requirements.excludedCEs += '%s ' % ce

        #Check whether we're looking for a non-default sw dir
        if app.softwareEnvironment is None:
            # The relative path for CVMFS, SNOPLUS_CVMFS_DIR will be set at the backend
            # Note the extra \ to escape the dollar in the initial python wrapper
            app.softwareEnvironment = '\$SNOPLUS_CVMFS_DIR/sw/%s/env_rat-%s.sh' % (app.ratVersion, app.ratVersion)

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

        args = ''
        args += '-g lcg '
        args += '-v %s '%(app.ratVersion)
        args += '-e %s '%(app.softwareEnvironment)
        args += '-d %s '%(app.outputDir)
        args += '-o %s '%(foutList)
        args += '-x %s '%(app.inputDir)
        args += '-i %s '%(finList)
        if app.ratMacro!='':
            args += '-m %s '%(macroFile)
        else:
            args += '-k -m %s '%(prodFile)
        if app.useDB:
            args += '--dbuser %s '%(app.config['rat_db_user'])
            args += '--dbpassword %s '%(app.config['rat_db_pswd'])
            args += '--dbname %s '%(app.config['rat_db_name'])
            args += '--dbprotocol %s '%(app.config['rat_db_protocol'])
            args += '--dburl %s '%(app.config['rat_db_url'])
        if app.discardOutput:
            args += '--nostore '

        wrapperArgs = ['-a', '"%s"' % (args)]
        wrapperArgs += ['ratProdRunner.py', 'lcg']

        app._getParent().inputsandbox.append('%s/ratProdRunner.py' % _app_directory)

        return LCGJobConfig(File('%s/sillyPythonWrapper.py' % _app_directory),
                            inputbox = app._getParent().inputsandbox,
                            outputbox = app._getParent().outputsandbox,
                            args = wrapperArgs)

###################################################################
