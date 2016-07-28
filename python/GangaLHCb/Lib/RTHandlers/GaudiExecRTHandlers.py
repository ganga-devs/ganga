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
from ..Applications.GaudiExecUtils import addTimestampFile
from GangaGaudi.Lib.Applications.GaudiUtils import gzipFile

logger = getLogger()


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

        inputsandbox.append(FileBuffer(GaudiExecDiracRTHandler.data_file, data_str))
    else:
        inputsandbox.append(FileBuffer(GaudiExecDiracRTHandler.data_file, '#dummy_data_file\n'+LHCbDataset().optionsString()))

    return inputsandbox


def generateWrapperScript(app):
    """
    This generates the wrapper script which is run for non GaudiExec type apps
    Args:
        app (GaudiExec): GaudiExec instance which contains the script to run on the WN
    """

    return FileBuffer(name=app.getWrapperScriptName(), contents=app.getWNPythonContents())


def getScriptName(app):
    """
    Returns the name of the script which runs on the WN.
    Args:
        app (Job): This is the app object which contains everything useful for generating the code
    """
    job = app.getJobObject()

    return getName(app)+"_Job_"+job.getFQID('.')+'_script.py'


def generateWNScript(commandline, app):
    """
    Generate the script as a file buffer and return it
    Args:
        commandline (str): This is the command-line argument the script is wrapping
        app (Job): This is the app object which contains everything useful for generating the code
    """
    job = app.getJobObject()
    exe_script_name = getScriptName(app)

    return FileBuffer(name=exe_script_name, contents=script_generator(gaudiRun_script_template(), COMMAND=commandline,
                                                                      OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')),
                      subdir='jobScript', executable=True)


def collectPreparedFiles(app):
    """
    Collect the files from the Application in the prepared state
    Args:
        app (GaudiExec): This expects only the GaudiExec app
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
        app (GaudiExec): This expects only the GaudiExec app
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

    if not app.useGaudiRun:
        full_cmd = sourceEnv + './run python %s' % app.getWrapperScriptName()
    else:
        full_cmd = sourceEnv + "./run gaudirun.py %s %s" % (opts_name, GaudiExecDiracRTHandler.data_file)
        if app.extraOpts:
            full_cmd += ' ' + app.getOptsFileName()
        if app.extraArgs:
            full_cmd += " " + " ".join(app.extraArgs)

    return full_cmd


def WarnUsers():
    """ A quick method for warning users about the in-development status of the app """
    print("\n\n")
    logger.warning("This GaudiExec Application is still in the testing phase.")
    logger.warning("There is no guarantee that any jobs submitted with it wil remain compatible with the next release of Ganga")
    raw_input("Please Hit the return key to continue with your Job submission\n")


class GaudiExecRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Local backend"""

    def master_prepare(self, app, appmasterconfig):
        """
        Prepare the RTHandler for the master job so that applications to be submitted
        Args:
            app (GaudiExec): This application is only expected to handle GaudiExec Applications here
            appmasterconfig (unknown): Output passed from the application master configuration call
        """

        WarnUsers()

        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)

        if isinstance(app.sharedOptsInput, LocalFile):
            app.sharedOptsInput = None

        job = app.getJobObject()
        if job.subjobs:
            rjobs = job.subjobs
        else:
            rjobs = [job]
        for this_job in rjobs:
            logger.debug("RTHandler Preparing: %s" % this_job.fqid)
            this_job.application.constructExtraFiles(this_job)

        optsArchive = os.path.join(app.sharedOptsInput.localDir, app.sharedOptsInput.namePattern)
        gzipFile(optsArchive, optsArchive+'.gz', True)
        app.sharedOptsInput.namePattern = app.sharedOptsInput.namePattern + '.gz'
        optsArchive = os.path.join(app.sharedOptsInput.localDir, app.sharedOptsInput.namePattern)

        inputsandbox.append(File(name=optsArchive))
        return StandardJobConfig(inputbox=unique(inputsandbox), outputbox=unique(outputsandbox))

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        """
        Prepare the job in order to submit to the Local backend
        Args:
            app (GaudiExec): This application is only expected to handle GaudiExec Applications here
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
        scriptToRun = generateWNScript(job_command, app)
        input_sand.append(scriptToRun)

        logger.debug("input_sand: %s" % input_sand)

        # It's this authors opinion that the script should be in the PATH on the WN
        # As it stands policy is that is isn't so we have to call it in a relative way, hence "./"
        c = StandardJobConfig('./'+os.path.join(scriptToRun.subdir, scriptToRun.name), input_sand, [], output_sand)
        return c


allHandlers.add('GaudiExec', 'Local', GaudiExecRTHandler)
allHandlers.add('GaudiExec', 'Condor', GaudiExecRTHandler)
allHandlers.add('GaudiExec', 'Interactive', GaudiExecRTHandler)
allHandlers.add('GaudiExec', 'Batch', GaudiExecRTHandler)
allHandlers.add('GaudiExec', 'LSF', GaudiExecRTHandler)


def generateDiracInput(app):
    """
    Construct a DIRAC input which must be unique to each job to have unique checksum.
    This generates a unique file, uploads it to DRIAC and then stores the LFN in app.uploadedInput
    Args:
        app (GaudiExec): This expects a GaudiExec app to be passed so that the constructed
    """

    input_files, input_folders = collectPreparedFiles(app)

    job = app.getJobObject()

    if input_folders:
        raise ApplicationConfigurationError(None, 'Prepared folders not supported yet, please fix this in future')
    else:
        prep_dir = app.getSharedPath()
        addTimestampFile(prep_dir)
        prep_file = prep_dir + '.tgz'
        compressed_file = os.path.join(tempfile.gettempdir(), '__'+os.path.basename(prep_file))

        if not job.master:
            rjobs = job.subjobs
        else:
            rjobs = [job]

        script_names = []

        for this_job in rjobs:
            this_app = this_job.application
            wnScript = generateWNScript(prepareCommand(this_app), this_app)
            this_script = os.path.join(tempfile.gettempdir(), wnScript.name)
            script_names.append(wnScript)
            wnScript.create(this_script)

        with tarfile.open(compressed_file, "w:gz") as tar_file:
            for name in input_files:
                # FIXME Add support for subfiles here once it's working across multiple IGangaFile objects in a consistent way
                # Not hacking this in for now just in-case we end up with a mess as a result.
                tar_file.add(name, arcname=os.path.basename(name))
            for thisScript in script_names:
                this_file = os.path.join(tempfile.gettempdir(), thisScript.name)
                logger.debug("Adding: '%s' as: '%s'" % (this_file, os.path.join(thisScript.subdir, thisScript.name)))
                tar_file.add(this_file, arcname=os.path.join(thisScript.subdir, thisScript.name))
        shutil.move(compressed_file, prep_dir)

    new_df = uploadLocalFile(job, os.path.basename(compressed_file), app.getSharedPath())

    app.uploadedInput = new_df


def uploadLocalFile(job, namePattern, localDir):
    """
    Upload a locally available file to the grid as a DiracFile

    Args:
        namePattern (str): name of the file
        localDir (str): localDir of the file
    Return
        DiracFile: a DiracFile of the uploaded LFN on the grid
    """

    new_df = DiracFile(namePattern, localDir=localDir)
    random_SE = random.choice(getConfig('DIRAC')['allDiracSE'])
    new_lfn = os.path.join(getInputFileDir(job), namePattern)
    returnable = new_df.put(force=True, uploadSE=random_SE, lfn=new_lfn)[0]

    return returnable


def getInputFileDir(job):
    """
    Return the LFN remote dirname for this job
    """
    return os.path.join(DiracFile.diracLFNBase(), 'GangaInputFile/Job_%s' % job.fqid)


class GaudiExecDiracRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Dirac backend"""

    data_file = 'data.py'

    def master_prepare(self, app, appmasterconfig):
        """
        Prepare the RTHandler for the master job so that applications to be submitted
        Args:
            app (GaudiExec): This application is only expected to handle GaudiExec Applications here
            appmasterconfig (unknown): Output passed from the application master configuration call
        """

        WarnUsers()

        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)


        if not isinstance(app.uploadedInput, DiracFile):
            logger.info("Using DiracFile: '%s' as prepared state." % app.uploadedInput.lfn)
            logger.info("Run job.application.reset()")
        else:
            generateDiracInput(app)
            assert isinstance(app.uploadedInput, DiracFile), "Failed to upload needed file, aborting submit"
        
        rep_data = app.uploadedInput.getReplicas()
        assert rep_data != {}, "Failed to find a replica, aborting submit"


        if isinstance(app.sharedOptsInput, DiracFile):
            app.sharedOptsInput = None

        job = app.getJobObject()
        if job.subjobs:
            rjobs = job.subjobs
        else:
            rjobs = [job]
        for this_job in rjobs:
            logger.debug("RTHandler Preparing: %s" % this_job.fqid)
            this_job.application.constructExtraFiles(this_job)

        optsArchive = os.path.join(app.sharedOptsInput.localDir, app.sharedOptsInput.namePattern[:-3] + '-'+str(uuid.uuid4())+'.tar')
        gzipFile(optsArchive, optsArchive+'.gz', True)

        new_df = DiracFile()
        new_df.namePattern = app.sharedOptsInput.namePattern + '.gz'
        new_df.localDir = app.sharedOptsInput.localDir
        new_df.remoteDir = getInputFileDir(app.getJobObject())
        new_df = new_df.put(force=True)[0]
        app.sharedOptsInput = new_df

        assert isinstance(app.sharedOptsInput, DiracFile), "Failed to upload needed file, aborting submit"

        rep_data = app.sharedOptsInput.getReplicas()
        assert rep_data != {}, "Failed to find a replica, aborting submit"

        return StandardJobConfig(inputbox=unique(inputsandbox), outputbox=unique(outputsandbox))


    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """
        Prepare the RTHandler in order to submit to the Dirac backend
        Args:
            app (GaudiExec): This application is only expected to handle GaudiExec Applications here
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

        app.uploadedInput = job.master.application.uploadedInput
        app.sharedOptsInput = job.master.application.sharedOptsInput

        logger.debug("uploadedInput: %s" % app.uploadedInput)

        rep_data = app.uploadedInput.getReplicas()

        logger.debug("Replica info: %s" % rep_data)

        inputsandbox += ['LFN:'+app.uploadedInput.lfn]
        inputsandbox += ['LFN:'+app.sharedOptsInput.lfn]

        logger.debug("Input Sand: %s" % inputsandbox)

        logger.debug("input_data: %s" % input_data)

        outputfiles = [this_file for this_file in job.outputfiles if isinstance(this_file, DiracFile)]

        scriptToRun = getScriptName(app)
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
                                        EXE=os.path.join('jobScript', scriptToRun),
                                        EXE_ARG_STR='',
                                        EXE_LOG_FILE='Ganga_GaudiExec.log',
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
    This allows us to run a custom python script on the WN
    '''
    script = '''
###WRAPPER_SCRIPT_WORKER_NODE###
'''
    return script

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
    Main section of code for the GaudiExec script run on the WN
    '''
    # Opening pleasantries
    print("Hello from GaudiExec")
    print("Arrived at workernode: %s" % getcwd())
    print("#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#")
    print("")

    print("Files found on WN: %s" % (listdir('.')))

    # Extract any/_all_ (b/g)zip files on the WN
    extractAllTarFiles('.')

    print("Executing: %s" % '###COMMAND###'+' '+' '.join(sys.argv))

    # Execute the actual command on the WN
    # NB os.system caused the entire stream to be captured before being streamed in some cases
    pipe = subprocess.Popen('###COMMAND###'+' '+' '.join(sys.argv), shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    # Flush the stdout/stderr as the process is running correctly
    flush_streams(pipe)

    # Wait for the process to finish executing
    pipe.wait()

    rc = pipe.returncode

    ###OUTPUTFILESINJECTEDCODE###

    # Final pleasantries
    print("")
    print("#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#")
    print("Goodbye from GaudiExec")

    sys.exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('GaudiExec', 'Dirac', GaudiExecDiracRTHandler)

