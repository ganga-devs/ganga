import os
import shutil
from datetime import datetime
import tempfile
import tarfile
import random
import threading
import uuid
import shutil

from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.GPIDev.Base.Proxy import getName
from Ganga.GPIDev.Lib.File.File import File, ShareDir
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from Ganga.GPIDev.Lib.File.OutputFileManager import getWNCodeForOutputPostprocessing
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import unique

from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_settings, API_nullifier
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import master_sandbox_prepare, sandbox_prepare, script_generator
from GangaLHCb.Lib.RTHandlers.RTHUtils import lhcbdiracAPI_script_template, lhcbdirac_outputfile_jdl
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset

logger = getLogger()

prep_lock = threading.Lock()

WN_script_name = 'GaudiRun_Python_WN_script.py'

def add_timeStampFile(given_path):
    """
    This creates a file in this directory given called __timestamp__ which contains the time so that the final file is unique
    I also add 20 unique characters from the ascii and digits pool from SystemRandom which may reduce the risk of collisions between users
    Args:
        given_path (str): Path which we want to create the timestamp within
    """
    fmt = '%Y-%m-%d-%H-%M-%S'
    time_filename = os.path.join(given_path, '__timestamp__')
    logger.info("Constructing: %s" % time_filename)
    with open(time_filename, 'a+') as time_file:
        time_file.write(datetime.now().strftime(fmt))
        time_file.write('\n'+str(uuid.uuid4()))

def genDataFiles(job):
    """
    Generating a data.py file which contains the data we want gaudirun to use
    Args:
        job (Job): This is the job object which contains everything useful for generating the code
    """
    logger.debug("Doing XML Catalog stuff")

    inputsandbox = []

    data = job.inputdata
    if data:
        logger.debug("Returning options String")
        data_str = data.optionsString()
        if data.hasLFNs():
            logger.info("Generating Data catalog for job: %s" % job.fqid)
            logger.debug("Returning Catalogue")
            inputsandbox.append(FileBuffer('catalog.xml', data.getCatalog()))
            cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ["xmlcatalog_file:catalog.xml"]\n'
            data_str += cat_opts

        inputsandbox.append(FileBuffer(GaudiRunDiracRTHandler.data_file, data_str))
    else:
        inputsandbox.append(FileBuffer(GaudiRunDiracRTHandler.data_file, '#dummy_data_file\n'+LHCbDataset().optionsString()))

    return inputsandbox

def generateWNScript(commandline, job):
    """
    Generate the script as a file buffer and return it
    Args:
        commandline (str): This is the command-line argument the script is wrapping
        job (Job): This is the job object which contains everything useful for generating the code
    """

    exe_script_name = 'gaudiRun-script.py'

    if job.application.runWithPython:

        return FileBuffer(name=exe_script_name, contents=script_generator(gaudiRun_script_template(), COMMAND=commandline, WN_SCRIPT_NAME=WN_script_name,
                                                                          SCRIPT_NAME = os.path.basename(job.application.getOptsFile().namePattern),
                                                                          OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')),
                          executable=True)
    else:
        return FileBuffer(name=exe_script_name, contents=script_generator(gaudiRun_script_template(), COMMAND=commandline, WN_SCRIPT_NAME=WN_script_name,
                                                                          OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')),
                          executable=True)

def collectPreparedFiles(app):
    """
    Collect the files from the Application in the prepared state
    Args:
        app (GaudiRun): This expects only the GaudiRun app
    """
    if not isinstance(app.is_prepared, ShareDir):
        raise ApplicationConfigurationError(None, 'Failed to prepare Application Correctly')
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
        raise ApplicationConfigurationError(None, "The filetype: %s is not yet supported for use as an opts file.\nPlease contact the Ganga devs is you wish this implemented." %
                                            getName(opts_file))

    sourceEnv = app.getEnvScript()
        
    if app.runWithPython:
        full_cmd = sourceEnv + './run python %s' %(WN_script_name)
    else:
        full_cmd = sourceEnv + "./run gaudirun.py %s %s" % (opts_name, GaudiRunDiracRTHandler.data_file)
    return full_cmd


def WarnUsers():
    """ A quick method for warning users about the in-development status of the app """
    print("\n\n")
    logger.warning("This GaudiRun Application is still in the testing phase.")
    logger.warning("There is no guarantee that any jobs submitted with it wil remain compatible with the next release of Ganga")
    raw_input("Please Hit the return key to continue with your Job submission\n")

class GaudiRunRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Local backend"""

    def master_prepare(self, app, appmasterconfig):
        """
        Prepare the RTHandler for the master job so that applications to be submitted
        Args:
            app (GaudiRun): This application is only expected to handle GaudiRun Applications here
            appmasterconfig (unknown): Output passed from the application master configuration call
        """

        WarnUsers()

        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)
        return StandardJobConfig(inputbox=unique(inputsandbox), outputbox=unique(outputsandbox))

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

        # Setup the command which to be run and the input and output
        input_sand = genDataFiles(job)
        output_sand = []

        # NB with inputfiles the mechanics of getting the inputfiled to the input of the Localhost backend is taken care of for us
        # We don't have to do anything to get our files when we start running
        # Also we don't manage the outputfiles here!

        job_command = prepareCommand(app)

        # Generate the script which is to be executed for us on the WN
        scriptToRun = generateWNScript(job_command, job)
        input_sand.append(scriptToRun)

        # Collect all of the prepared files which we want to pass along to this job
        input_files, input_folders = collectPreparedFiles(app)
        if input_folders:
            raise ApplicationConfigurationError(None, 'Prepared folders not supported yet, please fix this in future')
        else:
            for f in input_files:
                input_sand.append(File(f))

        logger.debug("input_sand: %s" % input_sand)

        # It's this authors opinion that the script should be in the PATH on the WN
        # As it stands policy is that is isn't so we have to call it in a relative way, hence "./"
        c = StandardJobConfig('./'+scriptToRun.name, input_sand, [], output_sand)
        return c

allHandlers.add('GaudiRun', 'Local', GaudiRunRTHandler)
#allHandlers.add('GaudiRun', 'Condor', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Interactive', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Batch', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'LSF', GaudiRunRTHandler)

def generateDiracInput(app):
    """
    Construct a DIRAC input which must be unique to each job to have unique checksum.
    This generates a unique file, uploads it to DRIAC and then stores the LFN in app.uploadedInput
    Args:
        app (GaudiRun): This expects a GaudiRun app to be passed so that the constructed
    """

    input_files, input_folders = collectPreparedFiles(app)

    job = app.getJobObject()

    if input_folders:
        raise ApplicationConfigurationError(None, 'Prepared folders not supported yet, please fix this in future')
    else:
        prep_dir = app.getSharedPath()
        add_timeStampFile(prep_dir)
        prep_file = prep_dir + '.tgz'
        compressed_file = os.path.join(tempfile.gettempdir(), '__'+os.path.basename(prep_file))

        wnScript = generateWNScript(prepareCommand(app), job)
        script_name = os.path.join(tempfile.gettempdir(), wnScript.name)
        wnScript.create(script_name)

        with tarfile.open(compressed_file, "w:gz") as tar_file:
            for name in input_files:
                tar_file.add(name, arcname=os.path.basename(name))
            tar_file.add(script_name, arcname=os.path.basename(script_name))
        shutil.move(compressed_file, prep_dir)

    new_df = DiracFile(namePattern=os.path.basename(compressed_file), localDir=app.getSharedPath())
    random_SE = random.choice(getConfig('DIRAC')['allDiracSE'])
    logger.info("new File: %s" % new_df)
    new_lfn = os.path.join(DiracFile.diracLFNBase(), 'GangaInputFile/Job_%s' % job.fqid, os.path.basename(compressed_file))
    new_df.put(uploadSE=random_SE, lfn=new_lfn)

    app.uploadedInput = new_df

class GaudiRunDiracRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Dirac backend"""

    data_file = 'data.py'

    def master_prepare(self, app, appmasterconfig):
        """
        Prepare the RTHandler for the master job so that applications to be submitted
        Args:
            app (GaudiRun): This application is only expected to handle GaudiRun Applications here
            appmasterconfig (unknown): Output passed from the application master configuration call
        """

        WarnUsers()

        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)

        if not isinstance(app.uploadedInput, DiracFile):
            generateDiracInput(app)

        return StandardJobConfig(inputbox=unique(inputsandbox), outputbox=unique(outputsandbox))


    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """
        Prepare the RTHandler in order to submit to the Dirac backend
        Args:
            app (GaudiRun): This application is only expected to handle GaudiRun Applications here
            appconfig (unknown): Output passed from the application configuration call
            appmasterconfig (unknown): Output passed from the application master_configure call
            jobmasterconfig (tuple): Output from the master job prepare step
        """

        # NB this needs to be removed safely
        # Get the inputdata and input/output sandbox in a sorted way
        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)
        input_data,   parametricinput_data = dirac_inputdata(app)

        # We know we don't need this one
        inputsandbox = []

        job = app.getJobObject()

        # We can support inputfiles and opts_file here. Locally should be submitted once, remotely can be referenced.

        opts_file = app.getOptsFile()

        if isinstance(opts_file, DiracFile):
            inputsandbox += ['LFN:'+opts_file.lfn]

        # Sort out inputfiles we support
        for file_ in job.inputfiles:
            if isinstance(file_, DiracFile):
                inputsandbox += ['LFN:'+file_.lfn]
            elif isinstance(file_, LocalFile):
                base_name = os.path.basename(file_.namePattern)
                shutil.copyfile(os.path.join(file_.localDir, base_name), os.path.join(app.getSharedPath(), base_name))
            else:
                logger.error("Filetype: %s nor currently supported, please contact Ganga Devs if you require support for this with the DIRAC backend" % getName(file_))
                raise ApplicationConfigurationError(None, "Unsupported filetype: %s with DIRAC backend" % getName(file_))

        if not isinstance(app.uploadedInput, DiracFile):
            with prep_lock:
                if job.master:
                    if not job.master.app.uploadedInput:
                        generateDiracInput(job.master.app)
                        app.uploadedInput = job.master.app.uploadedInput
                else:
                    generateDiracInput(app)


        logger.info("uploadedInput: %s" % app.uploadedInput)

        rep_data = app.uploadedInput.getReplicas()

        logger.info("Replica info: %s" % rep_data)

        inputsandbox += ['LFN:'+app.uploadedInput.lfn]

        logger.info("input_data: %s" % input_data)

        outputfiles = [this_file for this_file in job.outputfiles if isinstance(this_file, DiracFile)]

        # Prepare the command which is to be run on the worker node
        job_command = prepareCommand(app)
        logger.debug('Command line: %s: ', job_command)

        scriptToRun = generateWNScript(job_command, job)
        # Already added to sandbox uploaded as LFN

        # This code deals with the outputfiles as outputsandbox and outputdata for us
        lhcbdirac_outputfiles = lhcbdirac_outputfile_jdl(outputfiles)

        # NOTE special case for replicas: replicate string must be empty for no
        # replication
        dirac_script = script_generator(lhcbdiracAPI_script_template(),
                                        DIRAC_IMPORT='from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb',
                                        DIRAC_JOB_IMPORT='from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob',
                                        DIRAC_OBJECT='DiracLHCb()',
                                        JOB_OBJECT='LHCbJob()',
                                        NAME=mangle_job_name(app),
                                        EXE=scriptToRun.name,
                                        EXE_ARG_STR='',
                                        EXE_LOG_FILE='Ganga_GaudiRun.log',
                                        ENVIRONMENT=None,  # app.env,
                                        INPUTDATA=input_data,
                                        PARAMETRIC_INPUTDATA=parametricinput_data,
                                        OUTPUT_SANDBOX=API_nullifier(outputsandbox),
                                        OUTPUTFILESSCRIPT=lhcbdirac_outputfiles,
                                        OUTPUT_PATH="",  # job.fqid,
                                        OUTPUT_SE=[],
                                        PLATFORM=app.platform,
                                        SETTINGS=diracAPI_script_settings(app),
                                        DIRAC_OPTS=job.backend.diracOpts,
                                        REPLICATE='True' if getConfig('DIRAC')['ReplicateOutputData'] else '',
                                        # leave the sandbox for altering later as needs
                                        # to be done in backend.submit to combine master.
                                        # Note only using 2 #s as auto-remove 3
                                        INPUT_SANDBOX=repr([f for f in inputsandbox]),
                                        )

        # NB
        # inputsandbox here isn't used by the DIRAC backend as we explicitly define the INPUT_SANDBOX here!

        # Return the output needed for the backend to submit this job
        return StandardJobConfig(dirac_script, inputbox=[], outputbox=[])


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def gaudiRun_script_template():
    """
    Script to return the contents of the command to be executed on the worker node
    """
    script_template = """#!/usr/bin/env python
'''Script to run Executable application'''
from __future__ import print_function
from os import listdir, environ, pathsep, getcwd, system
from mimetypes import guess_type
from contextlib import closing
import sys
import subprocess

def extractAllTarFiles(path):
    '''
    This extracts all extractable (g/b)zip files found in the top level dir of the WN
    '''
    for f in listdir(path):
        print("examining: %s" % f )
        file_type = guess_type(f)[1]
        if file_type in ['gzip', 'bzip2']:
            print("Extracting: %s" % f)
            if file_type == 'gzip':
                system("tar -zxf %s" % f)
            elif file_type == 'bzip2':
                system("tar -jxf %s" % f)

def pythonScript(scriptName):
    '''
    This is the actual code in the GaudiPython-like script
    '''
    script = '''
from Gaudi.Configuration import *
importOptions('data.py')
execfile('./%s')
''' % scriptName
    return script

def generatePythonScript(scriptName):
    '''
    Generate the Python script file for the GaudiPython-like running
    '''
    with open('###WN_SCRIPT_NAME###', 'w') as WN_script:
        WN_script.write(pythonScript(scriptName))

def flush_streams(pipe):
    '''
    This flushes the stdout/stderr streams to the stdout/stderr correctly
    '''
    with pipe.stdout or pipe.stderr:
        if pipe.stdout:
            for next_line in iter(pipe.stdout.readline, b''):
                print("%s" % next_line, file=sys.stdout, end='')
                sys.stdout.flush()
        if pipe.stderr:
            for next_line in iter(pipe.stderr.readline, b''):
                print("%s" % next_line, file=sys.stderr, end='')
                sys.stderr.flush()

# Main
if __name__ == '__main__':
    '''
    Main section of code for the GaudiRun script run on the WN
    '''
    # Opening pleasantries
    print("Hello from GaudiRun")
    print("Arrived at workernode: %s" % getcwd())
    print("#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#")
    print("")

    # Extract any/_all_ (b/g)zip files on the WN
    extractAllTarFiles('.')

    # In the case we're wanting to run the Python script here, generate it on the WN and use it there
    generatePythonScript('###SCRIPT_NAME###')


    print("Executing: %s" % '###COMMAND###')
    # Execute the actual command on the WN
    # NB os.system caued the entire stream to be captured before being streamed in some cases
    pipe = subprocess.Popen('###COMMAND###', shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    # Flush the stdout/stderr as the process is running correctly
    flush_streams(pipe)

    # Wait for the process to finish executing
    pipe.wait()

    rc = pipe.returncode

    ###OUTPUTFILESINJECTEDCODE###

    # Final pleasantries
    print("")
    print("#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#")
    print("Goodbye from GaudiRun")

    sys.exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('GaudiRun', 'Dirac', GaudiRunDiracRTHandler)

