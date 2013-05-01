#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
from GangaLHCb.Lib.RTHandlers.RTHUtils             import lhcbdiracAPI_script_template
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare,sandbox_prepare,script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils       import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_settings, API_nullifier
from GangaDirac.Lib.Backends.DiracUtils            import result_ok
from GangaDirac.Lib.Files.DiracFile                import DiracFile
from Ganga.GPIDev.Lib.File.OutputFileManager       import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
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
            from GangaDirac.BOOT import dirac_ganga_server
            result = dirac_ganga_server.execute('getRootVersions()')
            if not result_ok(result):
                logger.error('Could not obtain available ROOT versions: %s' \
                             % str(result))
                logger.error('ROOT version will not be validated.')
            else:
                LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache = result['Value']

        ## check version
        if LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache and (not getConfig('LHCb')['ignore_version_check']):
            if not app.version in LHCbRootDiracRunTimeHandler.rootSoftwareVersionsCache:
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
        #outputfiles=set([file.namePattern for file in job.outputfiles]).difference(set(getOutputSandboxPatterns(job)))
        outputfiles=[file.namePattern for file in job.outputfiles if isinstance(file, DiracFile)]


        params = { 'DIRAC_IMPORT'         : 'from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb',
                   'DIRAC_JOB_IMPORT'     : 'from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob',
                   'DIRAC_OBJECT'         : 'DiracLHCb()',
                   'JOB_OBJECT'           : 'LHCbJob()',
                   'NAME'                 : mangle_job_name(app),
                   'INPUTDATA'            : input_data,
                   'PARAMETRIC_INPUTDATA' : parametricinput_data,
                   'OUTPUT_SANDBOX'       : API_nullifier(outputsandbox),
                   'OUTPUTDATA'           : API_nullifier(list(outputfiles)),
                   'OUTPUT_PATH'          : job.fqid,
                   'OUTPUT_SE'            : getConfig('DIRAC')['DiracOutputDataSE'],
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


        wrapper_path = os.path.join(job.getInputWorkspace().getPath(),
                                    'script_wrapper.py')
        python_wrapper =\
"""#!/usr/bin/env python
import os, sys
def formatVar(var):
    try:
        float(var)
        return str(var)
    except ValueError, v:
        return '\\\\\"%s\\\\\"'% var
    
del sys.argv[sys.argv.index('script_wrapper.py')]
###FIXARGS###
os.system('###COMMAND###' % str('###JOINER###'.join(sys.argv)))
###INJECTEDCODE###
"""
        params.update({'ROOTPY_SCRIPT'   : wrapper_path,
                       'ROOTPY_VERSION'  : app.version,
                       'ROOTPY_LOG_FILE' : 'Ganga_Root.log',
                       'ROOTPY_ARGS'     : [str(a) for a in app.args] })

        f=open(wrapper_path,'w')
        if app.usepython:
            python_wrapper = script_generator( python_wrapper,
                                               remove_unreplaced = False,
                                               FIXARGS = '',
                                               COMMAND = '/usr/bin/env python %s %s' % (os.path.basename(app.script.name),'%s'),
                                               JOINER = ' ',
                                               #INJECTEDCODE = getWNCodeForOutputPostprocessing(job,'')
                                               )
            
           
##             params.update({'ROOTPY_SCRIPT'   : wrapper_path,
##                            'ROOTPY_VERSION'  : app.version,
##                            'ROOTPY_LOG_FILE' : 'Ganga_Root.log',
##                            'ROOTPY_ARGS'     : app.args, })
        else:
            python_wrapper = script_generator( python_wrapper,
                                               remove_unreplaced = False,
                                               FIXARGS = 'sys.argv=[formatVar(v) for v in sys.argv]',
                                               COMMAND = 'export DISPLAY=\"localhoast:0.0\" && root -l -q \"%s(%s)\"' % (os.path.basename(app.script.name),'%s'),
                                               JOINER = ',',
                                               #INJECTEDCODE = getWNCodeForOutputPostprocessing(job,'')
                                               )
                      
##            params.update({'ROOT_MACRO'    : wrapper_path,
##                            'ROOT_VERSION'  : app.version,
##                            'ROOT_LOG_FILE' : 'Ganga_Root.log',
##                            'ROOT_ARGS'     : app.args, })

        
        f.write(python_wrapper)
        f.close()
        
        dirac_script = script_generator(lhcbdiracAPI_script_template(),
                                        **params)
        return StandardJobConfig( dirac_script,
                                  inputbox  = unique(inputsandbox ),
                                  outputbox = unique(outputsandbox) )


from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Root', 'Dirac',LHCbRootDiracRunTimeHandler )
