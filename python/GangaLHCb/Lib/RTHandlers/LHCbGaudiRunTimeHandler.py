#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import copy
import os
import pickle
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from RTHUtils import *
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from Ganga.GPIDev.Lib.File import FileBuffer, LocalFile, MassStorageFile
from Ganga.GPIDev.Base.Proxy import addProxy
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import unique
from GangaGaudi.Lib.RTHandlers.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import script_generator, get_share_path, master_sandbox_prepare, sandbox_prepare
logger = getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class LHCbGaudiRunTimeHandler(GaudiRunTimeHandler):

    """This is the application runtime handler class for Gaudi applications 
    using the local, interactive and LSF backends."""

    def master_prepare(self, app, appmasterconfig):
        inputsandbox, outputsandbox = master_sandbox_prepare(
            app, appmasterconfig, ['inputsandbox'])

        # add summary.xml
        outputsandbox += ['summary.xml', '__parsedxmlsummary__']

        thisenv = None
        # if appmasterconfig:
        #    if hasattr( appmasterconfig, 'env' ):
        #        thisenv = appmasterconfig.env

        return StandardJobConfig(inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox),
                                 env=thisenv)

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):

        logger.debug("Prepare")

        inputsandbox, outputsandbox = sandbox_prepare(
            app, appsubconfig, appmasterconfig, jobmasterconfig)

        job = app.getJobObject()

        logger.debug("Loading pickle files")

        #outputfiles=set([file.namePattern for file in job.outputfiles]).difference(set(getOutputSandboxPatterns(job)))
        # Cant wait to get rid of this when people no-longer specify
        # inputdata in options file
        #######################################################################
        #new_job = copy.deepcopy(job)
        # splitters ensure that subjobs pick up inputdata from job over that in
        # optsfiles but need to take sare of unsplit jobs
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

        logger.debug("Adding info from pickle files")

        if os.path.exists(share_path):
            #        if not os.path.exists(share_path):
            # raise GangaException('could not find the parser')
            f = open(share_path, 'r+b')
            parser = pickle.load(f)
            f.close()

            outbox, outdata = parser.get_output(job)

            # outputfiles.update(set(outdata[:]))
            #from Ganga.GPI import DiracFile
            #job.outputfiles.extend([addProxy(DiracFile(namePattern=f)) for f in outdata if f not in [j.namePattern for j in job.outputfiles]])
            job.non_copyable_outputfiles.extend([addProxy(MassStorageFile(
                namePattern=f)) for f in outdata if f not in [j.namePattern for j in job.outputfiles]])
            job.non_copyable_outputfiles.extend([addProxy(LocalFile(
                namePattern=f)) for f in outbox if f not in [j.namePattern for j in job.outputfiles]])
            outputsandbox = unique(outputsandbox + outbox[:])
        #######################################################################

        logger.debug("Doing XML Catalog stuff")

        data = job.inputdata
        data_str = ''
        if data:
            logger.debug("Returning options String")
            data_str = data.optionsString()
            if data.hasLFNs():
                logger.debug("Returning Catalogue")
                inputsandbox.append(
                    FileBuffer('catalog.xml', data.getCatalog()))
                cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ["xmlcatalog_file:catalog.xml"]\n'
                data_str += cat_opts

        logger.debug("Doing splitter_data stuff")
        if hasattr(job, '_splitter_data'):
            data_str += job._splitter_data
        inputsandbox.append(FileBuffer('data.py', data_str))

        logger.debug("Doing GaudiPython stuff")

        cmd = 'python ./gaudipython-wrapper.py'
        opts = ''
        if is_gaudi_child(job.application):
            opts = 'options.pkl'
            cmd = 'gaudirun.py ' + \
                ' '.join(job.application.args) + ' %s data.py' % opts

        logger.debug("Setting up script")

        script = script_generator(create_runscript(),
                                  remove_unreplaced=False,
                                  OPTS=opts,
                                  PROJECT_OPTS=job.application.setupProjectOptions,
                                  APP_NAME=job.application.appname,
                                  APP_VERSION=job.application.version,
                                  APP_PACKAGE=job.application.package,
                                  PLATFORM=job.application.platform,
                                  CMDLINE=cmd,
                                  XMLSUMMARYPARSING=getXMLSummaryScript())  # ,
# OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, ''))

        thisenv = None
        logger.debug("Returning StandardJobConfig")
        # if appmasterconfig:
        #    if hasattr( appmasterconfig, 'env' ):
        #        thisenv = appmasterconfig.env
        return StandardJobConfig(FileBuffer('gaudi-script.py', script, executable=1),
                                 inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox),
                                 env=thisenv)


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
