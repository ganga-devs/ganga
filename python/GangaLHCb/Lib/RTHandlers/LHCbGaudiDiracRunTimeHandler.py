import copy, os, pickle
from GangaLHCb.Lib.LHCbDataset.OutputData               import OutputData
from GangaLHCb.Lib.RTHandlers.RTHUtils                  import getXMLSummaryScript,is_gaudi_child,lhcbdiracAPI_script_template
from GangaGaudi.Lib.RTHandlers.GaudiDiracRunTimeHandler import GaudiDiracRunTimeHandler
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils      import get_share_path, master_sandbox_prepare, sandbox_prepare, script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils            import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_settings, API_nullifier
from GangaDirac.Lib.Backends.DiracUtils                 import result_ok
from GangaDirac.Lib.Files.DiracFile                     import DiracFile
from GangaDirac.Lib.Utilities.DiracUtilities            import execute
from Ganga.GPIDev.Lib.File.OutputFileManager            import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from Ganga.GPIDev.Adapters.StandardJobConfig            import StandardJobConfig
from Ganga.GPIDev.Lib.File                              import FileBuffer, LocalFile
from Ganga.GPIDev.Base.Proxy                            import addProxy
from Ganga.Utility.Config                               import getConfig
from Ganga.Utility.logging                              import getLogger
from Ganga.Utility.util                                 import unique
from Ganga.Core.exceptions                              import ApplicationConfigurationError
logger = getLogger()

class LHCbGaudiDiracRunTimeHandler(GaudiDiracRunTimeHandler):
## we dont do this anymore
#    gaudiSoftwareVersionsCache = None
#
#    def __check_versions_against_dirac(self, app):
#        ## cache versions dict
#        if not LHCbGaudiDiracRunTimeHandler.gaudiSoftwareVersionsCache:
#            result = execute('getSoftwareVersions()')
#            if not result_ok(result):
#                logger.error('Could not obtain available versions: %s' \
#                             % str(result))
#                logger.error('Version/platform will not be validated.')
#            else:
#                LHCbGaudiDiracRunTimeHandler.gaudiSoftwareVersionsCache = result['Value']
#
#        ## check version
#        if LHCbGaudiDiracRunTimeHandler.gaudiSoftwareVersionsCache and (not getConfig('LHCb')['ignore_version_check']):
#            if app.appname in LHCbGaudiDiracRunTimeHandler.gaudiSoftwareVersionsCache:
#                soft_info = LHCbGaudiDiracRunTimeHandler.gaudiSoftwareVersionsCache[app.appname]
#                if not app.version in soft_info:
#                    msg = 'Invalid version: %s.  Valid versions: %s' \
#                          % (app.version, str(soft_info.keys()))
#                    raise ApplicationConfigurationError(None,msg)
#                platforms = soft_info[app.version]
#                if not app.platform in platforms:
#                    msg = 'Invalid platform: %s. Valid platforms: %s' % \
#                          (app.platform,str(platforms))
#                    raise ApplicationConfigurationError(None,msg)


    def master_prepare(self,app,appmasterconfig):
        #self.__check_versions_against_dirac(app)
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig,['inputsandbox'])

        # add summary.xml
        outputsandbox += ['summary.xml','__parsedxmlsummary__']

        logger.debug( "Master Prepare LHCbGaudiDiracRunTimeHandler" )

        thisenv = None
        #if appmasterconfig:
        #    if hasattr( appmasterconfig, 'env' ):
        #        thisenv = appmasterconfig.env

        return StandardJobConfig( inputbox  = unique(inputsandbox),
                                  outputbox = unique(outputsandbox),
                                  env = thisenv )


    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):

        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)

        job=app.getJobObject()
        #outputfiles=set([file.namePattern for file in job.outputfiles]).difference(set(getOutputSandboxPatterns(job)))
        outputfiles=[file.namePattern for file in job.outputfiles if isinstance(file,DiracFile)]

        data_str  = 'import os\n'
        data_str += 'execfile(\'data.py\')\n'

        if hasattr(job,'_splitter_data'):
            data_str += job._splitter_data
        inputsandbox.append(FileBuffer('data-wrapper.py',data_str))


      
 
        ## Cant wait to get rid of this when people no-longer specify
        ## inputdata in options file
        #######################################################################
        new_job = copy.deepcopy(job)
        ## splitters ensure that subjobs pick up inputdata from job over that in
        ## optsfiles but need to take sare of unsplit jobs
        if not job.master:
            share_path = os.path.join(get_share_path(app),
                                      'inputdata',
                                      'options_data.pkl')

            if not job.inputdata:
                if os.path.exists(share_path):
                    f=open(share_path,'r+b')
                    new_job.inputdata = pickle.load(f)
                    f.close()
        
        ########################################################################

        ## Cant wait to get rid of this when people no-longer specify
        ## outputsandbox or outputdata in options file
        #######################################################################
        share_path = os.path.join(get_share_path(app),
                                  'output',
                                  'options_parser.pkl')

        if os.path.exists(share_path):
#        if not os.path.exists(share_path):
           # raise GangaException('could not find the parser')
           f=open(share_path,'r+b')
           parser = pickle.load(f)
           f.close()

           outbox, outdata = parser.get_output(job)

           #outputfiles.update(set(outdata[:]))
           #job.outputfiles.extend([addProxy(DiracFile(namePattern=f)) for f in outdata if f not in [j.namePattern for j in job.outputfiles]])
           job.non_copyable_outputfiles.extend([addProxy(DiracFile(namePattern=f))  for f in outdata if f not in [j.namePattern for j in job.outputfiles]])
           job.non_copyable_outputfiles.extend([addProxy(LocalFile(namePattern=f)) for f in outbox if f not in [j.namePattern for j in job.outputfiles]])
           outputfiles = unique(outputfiles + [f.namePattern for f in job.non_copyable_outputfiles if isinstance(f, DiracFile)])
           outputsandbox  = unique(outputsandbox  + outbox[:]) 
        #######################################################################

        input_data,   parametricinput_data = dirac_inputdata(new_job.application)
#        outputdata,   outputdata_path      = dirac_ouputdata(new_job.application)



        commandline = "python ./gaudipython-wrapper.py"
        if is_gaudi_child(app):
            commandline  = 'gaudirun.py '
            commandline += ' '.join([str(arg) for arg in app.args])
            commandline += ' options.pkl data-wrapper.py'
        logger.debug('Command line: %s: ', commandline)

        gaudi_script_path = os.path.join(job.getInputWorkspace().getPath(), "gaudi-script.py")
        script_generator(gaudi_script_template(),
                         #remove_unreplaced = False,
                         outputfile_path   = gaudi_script_path,
                         PLATFORM          = app.platform,
                         COMMAND           = commandline,
                         XMLSUMMARYPARSING = getXMLSummaryScript()#,
                         #OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')
                         )

        logger.debug( "input_data %s" % str( input_data ) )

        # not necessary to use lhcbdiracAPI_script_template any more as doing our own uploads to Dirac
        # remove after Ganga6 release
        # NOTE special case for replicas: replicate string must be empty for no replication
        dirac_script = script_generator(lhcbdiracAPI_script_template(),
                                        DIRAC_IMPORT         = 'from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb',
                                        DIRAC_JOB_IMPORT     = 'from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob',
                                        DIRAC_OBJECT         = 'DiracLHCb()',
                                        JOB_OBJECT           = 'LHCbJob()',
                                        NAME                 = mangle_job_name(app),
                                        APP_NAME             = app.appname,
                                        APP_VERSION          = app.version,
                                        APP_SCRIPT           = gaudi_script_path, 
                                        APP_LOG_FILE         = 'Ganga_%s_%s.log' % (app.appname, app.version),
                                        INPUTDATA            = input_data,
                                        PARAMETRIC_INPUTDATA = parametricinput_data,
                                        OUTPUT_SANDBOX       = API_nullifier(outputsandbox),
                                        OUTPUTDATA           = API_nullifier(list(outputfiles)),
                                        OUTPUT_PATH          = "", # job.fqid,#outputdata_path,
                                        OUTPUT_SE            = getConfig('DIRAC')['DiracOutputDataSE'],
                                        SETTINGS             = diracAPI_script_settings(new_job.application),
                                        DIRAC_OPTS           = job.backend.diracOpts,
                                        PLATFORM             = app.platform,
                                        REPLICATE            = 'True' if getConfig('DIRAC')['ReplicateOutputData'] else '',
                                        # leave the sandbox for altering later as needs
                                        # to be done in backend.submit to combine master.
                                        # Note only using 2 #s as auto-remove 3
                                        INPUT_SANDBOX        = '##INPUT_SANDBOX##'
                                        )
        logger.debug( "prepare: LHCbGaudiDiracRunTimeHandler" )

        #print(inputsandbox)

        thisenv = None
        #if appmasterconfig:
        #    if hasattr( appmasterconfig, 'env' ):
        #        thisenv = appmasterconfig.env

        return StandardJobConfig( dirac_script,
                                  inputbox  = unique(inputsandbox ),
                                  outputbox = unique(outputsandbox),
                                  env = thisenv )



#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def gaudi_script_template():
    '''Creates the script that will be executed by DIRAC job. '''
    script_template = """#!/usr/bin/env python
'''Script to run Gaudi application'''

from os import curdir, system, environ, pathsep, sep, getcwd
from os.path import join
import sys

def prependEnv(key, value):
    if environ.has_key(key): value += (pathsep + environ[key])
    environ[key] = value

# Main
if __name__ == '__main__':

    prependEnv('LD_LIBRARY_PATH', getcwd() + '/lib')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/python')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/###PLATFORM###/python')

    rc = system('''###COMMAND###''')/256

    ###XMLSUMMARYPARSING###

    ###OUTPUTFILESINJECTEDCODE###
    sys.exit(rc)
"""

    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
