##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Executable.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File.File import File, ShareDir
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError

from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Base.Proxy import getName

import os
import shutil
from datetime import datetime
import tempfile
import tarfile
import random
import threading
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare, sandbox_prepare, script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_template, diracAPI_script_settings, API_nullifier, dirac_outputfile_jdl
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.util import unique
from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from GangaDirac.Lib.Files.DiracFile import DiracFile

logger = getLogger()

data_file = 'data.py'

prep_lock = threading.Lock()

def add_timeStampFile(given_path):
    """
    This creates a file in this directory given called __timestamp__ which contains the time so that the final file is unique
    Args:
        given_path (str): Path which we want to create the timestamp within

    """
    fmt = '%Y-%m-%d-%H-%M-%S'
    time_filename = os.path.join(given_path, '__timestamp__')
    logger.info("Constructing: %s" % time_filename)
    with open(time_filename, 'a+') as time_file:
        time_file.write(datetime.now().strftime(fmt))

def genDataFiles(job):
    """
    Generating a data.py file which contains the data we want gaudirun to use
    Args:
        job (Job): This is the job object which contains everything useful for generating the code
    """
    logger.debug("Doing XML Catalog stuff")

    inputsandbox = []

    data = job.inputdata
    data_str = ''
    if data:
        logger.debug("Returning options String")
        data_str = data.optionsString()
        if data.hasLFNs():
            logger.info("Generating Data catalog for job: %s" % job.fqid)
            logger.debug("Returning Catalogue")
            inputsandbox.append(FileBuffer('catalog.xml', data.getCatalog()))
            cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ["xmlcatalog_file:catalog.xml"]\n'
            data_str += cat_opts

        inputsandbox.append(FileBuffer(data_file, data_str))

    return inputsandbox

def generateWNScript(commandline, job):
    """
    Generate the script as a file buffer and return it
    Args:
        commandline (str): This is the command-line argument the script is wrapping
        job (Job): This is the job object which contains everything useful for generating the code
    """
    exe_script_name = 'gaudiRun-script.py'

    return FileBuffer(name=exe_script_name, contents=script_generator(gaudiRun_script_template(), COMMAND=commandline,
                                                                    OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')),
                      executable=True)

def collectPreparedFiles(app):
    """
    Collect the files from the Application in the prepared state
    Args:
        app (GaudiRun): This expects only the GaudiRun app
    """
    if not isinstance(app.is_prepared, ShareDir):
        raise ApplicationConfigurationError('Failed to prepare Application Correctly')
    shared_dir = app.getSharedPath()
    input_files, input_folders = [], []
    for root, dirs, files in os.walk(shared_dir, topdown=True):
        for name in files:
            input_files.append(os.path.join(root, name))
        for name in dirs:
            input_folders.append(os.path.join(root, name))
    return input_files, input_folders

def prepareCommand(app):
    """
    Returns the command which is to be run on the worker node
    Args:
        app (GaudiRun): This expects only the GaudiRun app
    """
    opts_file = app.getOptsFile()
    if isinstance(opts_file, (LocalFile, DiracFile)):
        # Ideally this would NOT need the basename, however LocalFile is special in this regard.
        # TODO Fix this after fixing LocalFile
        opts_name = os.path.basename(opts_file.namePattern)
    else:
        raise ApplicationConfigurationError("The filetype: %s is not yet supported for use as an opts file.\nPlease contact the Ganga devs is you wish this implemented." %
                                            getName(opts_file))
    full_cmd = "./run gaudirun.py %s %s" % (opts_name, data_file)
    return full_cmd

class GaudiRunRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Local backend"""

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        """
        Prepare the job in order to submit to the Local backend
        Args:
            app (GaudiRun): This application is only expected to handle GaudiRun Applications here
            appconfig (unknown): Output passed from the application configuration call
            appmasterconfig (unknown): Output passed from the application master_configure call
            jobmasterconfig (tuple): Output from the master job prepare step
        """

        job = app.getJobObject()

        prepared_files = []

        # Setup the command which to be run and the input and output
        input_sand = job.inputsandbox
        output_sand = job.outputsandbox
        data_files = genDataFiles(job)

        input_sand = unique(input_sand + prepared_files + data_files)

        job_command = prepareCommand(app)

        # Generate the script which is to be executed for us on the WN
        scriptToRun = generateWNScript(job_command, job)
        input_sand.append(scriptToRun)

        # Collect all of the prepared files which we want to pass along to this job
        input_files, input_folders = collectPreparedFiles(app)
        if input_folders:
            raise ApplicationConfigurationError('Prepared folders not supported yet, please fix this in future')
        else:
            for f in input_files:
                input_sand.append(File(f))

        # It's this authors opinion that the script should be in the PATH on the WN
        # As it stands policy is that is isn't so we have to call it in a relative way, hence "./"
        c = StandardJobConfig('./'+scriptToRun.name, input_sand, [], output_sand)
        return c

allHandlers.add('GaudiRun', 'Local', GaudiRunRTHandler)
#allHandlers.add('GaudiRun', 'Condor', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Interactive', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Batch', GaudiRunRTHandler)

def generateDiracInput(app):

    input_files, input_folders = collectPreparedFiles(app)

    job = app.getJobObject()

    compressed_file = None
    if input_folders:
        raise ApplicationConfigurationError('Prepared folders not supported yet, please fix this in future')
    else:
        prep_dir = app.getSharedPath()
        add_timeStampFile(prep_dir)
        prep_file = prep_dir + '.tgz'
        compressed_file = os.path.join(tempfile.gettempdir(), os.path.basename(prep_dir) + ".tgz")

        wnScript = generateWNScript(prepareCommand(app), job)
        script_name = os.path.join(tempfile.gettempdir(), wnScript.name)
        wnScript.create(script_name)

        with tarfile.open( compressed_file + ".tgz", "w:gz" ) as tar_file:
            for name in input_files:
                tar.add(name, arcname=os.path.basename(name))
            tar.add(script_name, arcname=os.path.basename(script_name))
        shutil.move(compressed_file, prep_dir)

    new_df = DiracFile(namePattern = os.path.basename(compressed_file), localDir = app.getSharedPath(),
                       remoteDir = os.path.join(DiracFile.diracLFNBase(), 'GangaInputFile/Job_%s' % job.fqid) )
    random_SE = random.choice(getConfig('DIRAC')['allDiracSE'])
    new_df.put(uploadSE = randomSE)

    app.uploadedInput = new_df

class GaudiRunDiracRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Dirac backend"""

    def master_prepare(self, app, appmasterconfig):
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)

        if not app.uploadedInput:
            generateDiracInput(app)

        return StandardJobConfig(inputbox=unique(inputsandbox), outputbox=unique(outputsandbox))


    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """
        Prepare the job in order to submit to the Dirac backend
        Args:
            app (GaudiRun): This application is only expected to handle GaudiRun Applications here
            appconfig (unknown): Output passed from the application configuration call
            appmasterconfig (unknown): Output passed from the application master_configure call
            jobmasterconfig (tuple): Output from the master job prepare step
        """

        # Get the inputdata and input/output sandbox in a sorted way
        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)
        input_data,   parametricinput_data = dirac_inputdata(app)

        job = app.getJobObject()

        opts_file = app.getOptsFile()

        if isinstance(opts_file, DiracFile):
            inputsandbox += ['LFN:'+opts_file.lfn]

        if not app.uploadedInput:
            with prep_lock:
                if job.master:
                    if not job.master.app.uploadedInput:
                        generateDiracInput(job.master.app)
                        app.uploadedInput = job.master.app.uploadedInput
                else:
                    generateDiracInput(app)

        inputsandbox += ['LFN:'+app.uploadedInput.lfn]

        outputfiles = [this_file for this_file in job.outputfiles if isinstance(this_file, DiracFile)]

        # Prepare the command which is to be run on the worker node
        job_command = prepareCommand(app)
        logger.debug('Command line: %s: ', job_command)

        scriptToRun = generateWNScript(job_command, job)
        # Already added to sandbox uploaded as LFN

        dirac_outputfiles = dirac_outputfile_jdl(outputfiles, False)

        # NOTE special case for replicas: replicate string must be empty for no
        # replication
        dirac_script = script_generator(diracAPI_script_template(),
                                        DIRAC_IMPORT='from DIRAC.Interfaces.API.Dirac import Dirac',
                                        DIRAC_JOB_IMPORT='from DIRAC.Interfaces.API.Job import Job',
                                        DIRAC_OBJECT='Dirac()',
                                        JOB_OBJECT='Job()',
                                        NAME=mangle_job_name(app),
                                        EXE=scriptToRun.name,
                                        EXE_ARG_STR='',
                                        EXE_LOG_FILE='Ganga_Executable.log',
                                        ENVIRONMENT=None,  # app.env,
                                        INPUTDATA=input_data,
                                        PARAMETRIC_INPUTDATA=parametricinput_data,
                                        OUTPUT_SANDBOX=API_nullifier(outputsandbox),
                                        OUTPUTFILESSCRIPT=dirac_outputfiles,
                                        OUTPUT_PATH="",  # job.fqid,
                                        OUTPUT_SE=[],
                                        SETTINGS=diracAPI_script_settings(app),
                                        DIRAC_OPTS=job.backend.diracOpts,
                                        REPLICATE='True' if getConfig('DIRAC')['ReplicateOutputData'] else '',
                                        # leave the sandbox for altering later as needs
                                        # to be done in backend.submit to combine master.
                                        # Note only using 2 #s as auto-remove 3
                                        INPUT_SANDBOX='##INPUT_SANDBOX##'
                                        )

        # Return the output needed for the backend to submit this job
        return StandardJobConfig(dirac_script,
                                 inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def gaudiRun_script_template():
    """
    Script to return the contents of the command to be executed on the worker node
    """
    script_template = """#!/usr/bin/env python
'''Script to run Executable application'''
from __future__ import print_function
from os import listdir, system, environ, pathsep, getcwd
from mimetypes import guess_type
from contextlib import closing
import tarfile
import sys

def extractAllTarFiles(path):
    for f in listdir(path):
        print("examining: %s" % f )
        if guess_type(f)[1] in ['gzip', 'bzip2']:
            with closing(tarfile.open(f, "r:*")) as tf:
                print("Extracting: %s" % tf.name)
                tf.extractall('.')

# Main
if __name__ == '__main__':

    extractAllTarFiles('.')

    rc = system('###COMMAND###')

    ###OUTPUTFILESINJECTEDCODE###
    sys.exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('GaudiRun', 'Dirac', GaudiRunDiracRTHandler)

