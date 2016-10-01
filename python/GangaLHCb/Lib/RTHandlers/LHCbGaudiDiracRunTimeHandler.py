import copy
import os
import pickle
from Ganga.Core import BackendError
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from GangaGaudi.Lib.RTHandlers.GaudiDiracRunTimeHandler import GaudiDiracRunTimeHandler
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare, sandbox_prepare, script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_settings, API_nullifier
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.Utilities.DiracUtilities import execute
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.GPIDev.Lib.File import FileBuffer, LocalFile
from Ganga.GPIDev.Base.Proxy import addProxy, isType, stripProxy
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import unique
from Ganga.Core.exceptions import ApplicationConfigurationError
from GangaLHCb.Lib.RTHandlers.RTHUtils import getXMLSummaryScript, is_gaudi_child, lhcbdiracAPI_script_template, lhcbdirac_outputfile_jdl
logger = getLogger()


class LHCbGaudiDiracRunTimeHandler(GaudiDiracRunTimeHandler):

    def master_prepare(self, app, appmasterconfig):

        logger.debug("Master Prepare")
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig, ['inputsandbox'])

        # add summary.xml
        outputsandbox += ['summary.xml', '__parsedxmlsummary__']

        logger.debug("Master Prepare LHCbGaudiDiracRunTimeHandler")

        return StandardJobConfig(inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):

        logger.debug("Prepare")

        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)

        job = stripProxy(app).getJobObject()

        if job.inputdata:
            if not job.splitter:
                if len(job.inputdata) > 100:
                    raise BackendError("You're submitting a job to Dirac with no splitter and more than 100 files, please add a splitter and try again!")

        outputfiles = [this_file for this_file in job.outputfiles if isType(this_file, DiracFile)]

        data_str = 'import os\n'
        data_str += 'execfile(\'data.py\')\n'

        if hasattr(job, '_splitter_data'):
            data_str += job._splitter_data
        inputsandbox.append(FileBuffer('data-wrapper.py', data_str))

        input_data = []

        # Cant wait to get rid of this when people no-longer specify
        # inputdata in options file
        #######################################################################
        # splitters ensure that subjobs pick up inputdata from job over that in
        # optsfiles but need to take care of unsplit jobs
        if not job.master:
            share_path = os.path.join(get_share_path(app),
                                      'inputdata',
                                      'options_data.pkl')

            if not job.inputdata:
                if os.path.exists(share_path):
                    f = open(share_path, 'r+b')
                    job.inputdata = pickle.load(f)
                    f.close()

        #######################################################################

        # Cant wait to get rid of this when people no-longer specify
        # outputsandbox or outputdata in options file
        #######################################################################
        share_path = os.path.join(get_share_path(app),
                                  'output',
                                  'options_parser.pkl')

        if os.path.exists(share_path):
            #        if not os.path.exists(share_path):
            # raise GangaException('could not find the parser')
            f = open(share_path, 'r+b')
            parser = pickle.load(f)
            f.close()

            outbox, outdata = parser.get_output(job)

            from Ganga.GPIDev.Lib.File import FileUtils
            from Ganga.GPIDev.Base.Filters import allComponentFilters

            fileTransform = allComponentFilters['gangafiles']
            outdata_files = [fileTransform(this_file, None) for this_file in outdata if not FileUtils.doesFileExist(this_file, job.outputfiles)]
            job.non_copyable_outputfiles.extend([output_file for output_file in outdata_files if not isType(output_file, DiracFile)])
            outbox_files = [fileTransform(this_file, None) for this_file in outbox if not FileUtils.doesFileExist(this_file, job.outputfiles)]
            job.non_copyable_outputfiles.extend([outbox_file for outbox_file in outbox_files if not isType(outbox_file, DiracFile)])

            outputsandbox.extend([f.namePattern for f in job.non_copyable_outputfiles])

            outputsandbox.extend([f.namePattern for f in job.outputfiles if not isType(f, DiracFile)])
            outputsandbox = unique(outputsandbox)  # + outbox[:])
        #######################################################################

        input_data_dirac, parametricinput_data = dirac_inputdata(job.application)

        if input_data_dirac is not None:
            for f in input_data_dirac:
                if isType(f, DiracFile):
                    input_data.append(f.lfn)
                elif isType(f, str):
                    input_data.append(f)
                else:
                    raise ApplicationConfigurationError("Don't know How to handle anythig other than DiracFiles or strings to LFNs!")

        commandline = "python ./gaudipython-wrapper.py"
        if is_gaudi_child(app):
            commandline = 'gaudirun.py '
            commandline += ' '.join([str(arg) for arg in app.args])
            commandline += ' options.pkl data-wrapper.py'
        logger.debug('Command line: %s: ', commandline)

        gaudi_script_path = os.path.join(job.getInputWorkspace().getPath(), "gaudi-script.py")

        script_generator(gaudi_script_template(),
                         #remove_unreplaced = False,
                         outputfile_path=gaudi_script_path,
                         PLATFORM=app.platform,
                         COMMAND=commandline,
                         XMLSUMMARYPARSING=getXMLSummaryScript()  # ,
                         #OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')
                         )

        #logger.debug( "input_data %s" % str( input_data ) )

        # We want to propogate the ancestor depth to DIRAC when we have
        # inputdata set
        if job.inputdata is not None and isType(job.inputdata, LHCbDataset):

            # As the RT Handler we already know we have a Dirac backend
            if type(job.backend.settings) is not dict:
                raise ApplicationConfigurationError(None, 'backend.settings should be a dict')

            if 'AncestorDepth' in job.backend.settings:
                ancestor_depth = job.backend.settings['AncestorDepth']
            else:
                ancestor_depth = job.inputdata.depth
        else:
            ancestor_depth = 0

        lhcbdirac_script_template = lhcbdiracAPI_script_template()

        lhcb_dirac_outputfiles = lhcbdirac_outputfile_jdl(outputfiles)

        # not necessary to use lhcbdiracAPI_script_template any more as doing our own uploads to Dirac
        # remove after Ganga6 release
        # NOTE special case for replicas: replicate string must be empty for no
        # replication
        dirac_script = script_generator(lhcbdirac_script_template,
                                        DIRAC_IMPORT='from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb',
                                        DIRAC_JOB_IMPORT='from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob',
                                        DIRAC_OBJECT='DiracLHCb()',
                                        JOB_OBJECT='LHCbJob()',
                                        NAME=mangle_job_name(app),
                                        APP_NAME=stripProxy(app).appname,
                                        APP_VERSION=app.version,
                                        APP_SCRIPT=gaudi_script_path,
                                        APP_LOG_FILE='Ganga_%s_%s.log' % (stripProxy(app).appname, app.version),
                                        INPUTDATA=input_data,
                                        PARAMETRIC_INPUTDATA=parametricinput_data,
                                        OUTPUT_SANDBOX=API_nullifier(outputsandbox),
                                        OUTPUTFILESSCRIPT=lhcb_dirac_outputfiles,
                                        # job.fqid,#outputdata_path,
                                        OUTPUT_PATH="",
                                        SETTINGS=diracAPI_script_settings(job.application),
                                        DIRAC_OPTS=job.backend.diracOpts,
                                        PLATFORM=app.platform,
                                        REPLICATE='True' if getConfig('DIRAC')['ReplicateOutputData'] else '',
                                        ANCESTOR_DEPTH=ancestor_depth,
                                        ## This is to be modified in the final 'submit' function in the backend
                                        ## The backend also handles the inputfiles DiracFiles ass appropriate
                                        INPUT_SANDBOX='##INPUT_SANDBOX##'
                                        )
        logger.debug("prepare: LHCbGaudiDiracRunTimeHandler")

        return StandardJobConfig(dirac_script,
                                 inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def gaudi_script_template():
    '''Creates the script that will be executed by DIRAC job. '''

    import inspect
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                   'GaudiTemplate.py')

    from Ganga.GPIDev.Lib.File import FileUtils
    script_template = FileUtils.loadScript(script_location, '')

    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
