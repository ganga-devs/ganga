#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
from GangaLHCb.Lib.RTHandlers.RTHUtils             import lhcbdiracAPI_script_template
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare,sandbox_prepare,script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils       import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_settings, API_nullifier
from GangaDirac.Lib.Backends.DiracUtils            import result_ok
from Ganga.GPIDev.Lib.File.OutputFileManager       import getOutputSandboxPatterns
from Ganga.GPIDev.Adapters.IRuntimeHandler         import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
from Ganga.Core.exceptions                         import ApplicationConfigurationError
from Ganga.Utility.Config                          import getConfig
from Ganga.Utility.logging                         import getLogger
from Ganga.Utility.util                            import unique
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbRootDiracRunTimeHandler(IRuntimeHandler):
    """The runtime handler to run ROOT jobs on the Dirac backend"""
    rootSoftwareVersionsCache = None
    
    def __check_versions_against_dirac(self, app):
        ## cache versions dict
        if not LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache:
            from GangaLHCb.Lib.Backends.Dirac import Dirac
            result = Dirac.execAPI('result = DiracLHCbCommands.getRootVersions()')
            if not result_ok(result):
                logger.error('Could not obtain available ROOT versions: %s' \
                             % str(result))
                logger.error('ROOT version will not be validated.')
            else:
                LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache = result['Value']

        ## check version
        if LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache and (not getConfig('LHCb')['ignore_version_check']):
            print 'hello'
            if not app.version in LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache:
                print 'hello2'
                msg = 'Invalid version: %s.  Valid versions: %s' \
                      % (app.version, str(LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache.keys()))
                raise ApplicationConfigurationError(None,msg)


    def master_prepare(self,app,appmasterconfig):
        self.__check_versions_against_dirac(app)
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)
        # check file is set OK
        if not app.script.name:
            msg = 'Root.script.name must be set.'
            raise ApplicationConfigurationError(None,msg)

        sharedir_scriptpath = os.path.join(get_share_path(app),
                                           os.path.basename(app.script.name))

        if not os.path.exists(sharedir_scriptpath):
            msg = 'Script must exist!'
            raise ApplicationConfigurationError(None,msg)

      
        return StandardJobConfig( inputbox  = unique(inputsandbox),
                                  outputbox = unique(outputsandbox) )
    

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        inputsandbox, outputsandbox        = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)
        input_data,   parametricinput_data = dirac_inputdata(app)
#        outputdata,   outputdata_path      = dirac_ouputdata(app)
        job=app.getJobObject()
        outputfiles=set([file.namePattern for file in job.outputfiles]).difference(set(getOutputSandboxPatterns(job)))


        params = { 'DIRAC_IMPORT'         : 'from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb',
                   'DIRAC_JOB_IMPORT'     : 'from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob',
                   'DIRAC_OBJECT'         : 'DiracLHCb()',
                   'JOB_OBJECT'           : 'LHCbJob()',
                   'NAME'                 : mangle_job_name(app),
                   'INPUTDATA'            : input_data,
                   'PARAMETRIC_INPUTDATA' : parametricinput_data,
                   'OUTPUT_SANDBOX'       : API_nullifier(outputsandbox),
##                    'OUTPUTDATA'           : API_nullifier(list(outputfiles)),
##                    'OUTPUT_PATH'          : job.fqid,
##                    'OUTPUT_SE'            : getConfig('DIRAC')['DiracOutputDataSE'],
                   'SETTINGS'             : diracAPI_script_settings(app),
                   'DIRAC_OPTS'           : job.backend.diracOpts,
                   'PLATFORM'             : getConfig('ROOT')['arch'],
                   # leave the sandbox for altering later as needs
                   # to be done in backend.submit to combine master.
                   # Note only using 2 #s as auto-remove 3
                   'INPUT_SANDBOX'        : '##INPUT_SANDBOX##'
                   }

        scriptpath = os.path.join(get_share_path(app),
                                  os.path.basename(app.script.name))
        if app.usepython:
            params.update({'ROOTPY_SCRIPT'   : scriptpath,
                           'ROOTPY_VERSION'  : app.version,
                           'ROOTPY_LOG_FILE' : 'Ganga_Root.log',
                           'ROOTPY_ARGS'     : app.args, })
        else:
            params.update({'ROOT_MACRO'    : scriptpath,
                           'ROOT_VERSION'  : app.version,
                           'ROOT_LOG_FILE' : 'Ganga_Root.log',
                           'ROOT_ARGS'     : app.args, })

        dirac_script = script_generator(lhcbdiracAPI_script_template(),
                                        **params)
        return StandardJobConfig( dirac_script,
                                  inputbox  = unique(inputsandbox ),
                                  outputbox = unique(outputsandbox) )


from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Root', 'Dirac',LHCbRootDiracRunTimeHandler )
