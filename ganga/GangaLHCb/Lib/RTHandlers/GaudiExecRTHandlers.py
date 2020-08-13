import os
import shutil
from datetime import datetime
import tempfile
import tarfile
import random
import threading
import uuid
import shutil
from GangaCore.Core.exceptions import ApplicationConfigurationError, ApplicationPrepareError, GangaException, GangaFileError
from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GangaCore.GPIDev.Base.Proxy import getName
from GangaCore.GPIDev.Lib.File.File import File, ShareDir
from GangaCore.GPIDev.Lib.File.FileBuffer import FileBuffer
from GangaCore.GPIDev.Lib.File.LocalFile import LocalFile
from GangaCore.GPIDev.Lib.File.OutputFileManager import getWNCodeForOutputPostprocessing
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.util import unique
from GangaCore.GPIDev.Credentials.CredentialStore import credential_store

from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_settings, API_nullifier
from GangaDirac.Lib.Utilities.DiracUtilities import execute, GangaDiracError
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import master_sandbox_prepare, sandbox_prepare, script_generator
from GangaLHCb.Lib.RTHandlers.RTHUtils import lhcbdiracAPI_script_template, lhcbdirac_outputfile_jdl
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from ..Applications.GaudiExecUtils import addTimestampFile
from GangaGaudi.Lib.Applications.GaudiUtils import gzipFile

logger = getLogger()

_pseudo_session_id = str(uuid.uuid4())

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


def getAutoDBTags(job):
    knownApps = ['DaVinci', 'Brunel', 'Moore']
    prefix = 'DaVinci'
    for app in knownApps:
        if app in job.application.directory:
            prefix = app
    inputsandbox = []
    ddb, conddb = execute('getDBtagsFromLFN("{0}")'.format(job.inputdata[0].lfn)) # take the tags only from the first file
    tagOpts = 'from Configurables import ' + prefix +'\n' 
    tagOpts += prefix + '().DDDBtag = ' + "'" + ddb + "'\n"
    tagOpts += prefix + '().CondDBtag = ' + "'" + conddb + "'"
    return tagOpts

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
    return "_".join((getName(app), getConfig('Configuration')['user'], 'Job', job.getFQID('.'), _pseudo_session_id, 'script'))+'.py'


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
        raise ApplicationConfigurationError('Failed to prepare Application Correctly')
    shared_dir = app.getSharedPath()
    input_files, input_folders = [], []
    for root, dirs, files in os.walk(shared_dir, topdown=True):
        for name in files:
            input_files.append(os.path.join(root, name))
        for name in dirs:
            input_folders.append(os.path.join(root, name))

    for file_ in app.getJobObject().inputfiles:
        if isinstance(file_, LocalFile):
            if file_.namePattern == 'data.py':
                raise ApplicationConfigurationError("You should not name any inputfiles 'data.py' to avoid conflict with the generated inputdata. Please rename the file and submit again.")
            shutil.copy(os.path.join(file_.localDir, os.path.basename(file_.namePattern)), shared_dir)
            input_files.append(os.path.join(shared_dir, file_.namePattern))
        elif isinstance(file_, str):
            if 'data.py' in file_:
                raise ApplicationConfigurationError("You should not name any inputfiles 'data.py' to avoid conflict with the generated inputdata. Please rename the file and submit again.")
            new_file = LocalFile(file_)
            shutil.copy(os.path.join(new_file.localDir, os.path.basename(new_file.namePattern)), shared_dir)
            input_files.append(os.path.join(shared_dir, new_file.namePattern))
        elif not isinstance(file_, DiracFile):
            raise ApplicationConfigurationError(None, "File type: %s Not _yet_ supported in GaudiExec" % type(file_))

    return input_files, input_folders


def prepareCommand(app):
    """
    Returns the command which is to be run on the worker node
    Args:
        app (GaudiExec): This expects only the GaudiExec app
    """

    all_opts_files = app.getOptsFiles(True)
    opts_names = []
    for opts_file in all_opts_files:
        if isinstance(opts_file, (LocalFile, DiracFile)):
            # Ideally this would NOT need the basename, however LocalFile is special in this regard.
            # TODO Fix this after fixing LocalFile
            opts_names.append(os.path.basename(opts_file.namePattern))
        elif isinstance(opts_file, str):
            opts_names.append(os.path.basename(opts_file))
        else:
            raise ApplicationConfigurationError("The filetype: %s is not yet supported for use as an opts file.\nPlease contact the Ganga devs is you wish this implemented." %
                                                getName(opts_file))

    #Check if this was checked out with LbEnv or not
    isLbEnv = False
    with open(app.directory+'/Makefile', "r") as makefile:
        if 'LbEnv' in makefile.read():
            isLbEnv = True

    sourceEnv = app.getWNEnvScript(isLbEnv)

    run_cmd = ' export ganga_jobid=%s && ./run ' % app.getJobObject().fqid

    if not app.useGaudiRun:
        full_cmd = sourceEnv + run_cmd + 'python %s' % app.getWrapperScriptName()
    else:
        #If the job does not have inputdata don't include the data.py file in the run command. For Gauss jobs.
        if app.getJobObject().inputdata:
            full_cmd = sourceEnv + run_cmd + "gaudirun.py %s %s" % (' '.join(opts_names), GaudiExecDiracRTHandler.data_file)
        else:
            full_cmd = sourceEnv + run_cmd + "gaudirun.py %s " % (' '.join(opts_names))
        if app.extraOpts:
            full_cmd += ' ' + app.getExtraOptsFileName()
        if app.getMetadata:
            full_cmd += ' summary.py'
        if app.autoDBtags:
            full_cmd += ' dbTags.py'
        if app.extraArgs:
            full_cmd += " " + " ".join(app.extraArgs)

    return full_cmd


class GaudiExecRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Local backend"""

    def master_prepare(self, app, appmasterconfig):
        """
        Prepare the RTHandler for the master job so that applications to be submitted
        Args:
            app (GaudiExec): This application is only expected to handle GaudiExec Applications here
            appmasterconfig (unknown): Output passed from the application master configuration call
        """
        if app.autoDBtags and not app.getJobObject().inputdata[0].lfn.startswith('/lhcb/MC/'):
            logger.warning("This doesn't look like MC! Not automatically adding db tags.")
            app.autoDBtags = False

        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)

        if isinstance(app.jobScriptArchive, LocalFile):
            app.jobScriptArchive = None

        generateJobScripts(app, appendJobScripts=True)

        scriptArchive = os.path.join(app.jobScriptArchive.localDir, app.jobScriptArchive.namePattern)

        inputsandbox.append(File(name=scriptArchive))

        if app.getMetadata:
            logger.info("Adding options to make the summary.xml")
            inputsandbox.append(FileBuffer('summary.py', "\nfrom Gaudi.Configuration import *\nfrom Configurables import LHCbApp\nLHCbApp().XMLSummary='summary.xml'"))

        if app.autoDBtags:
            logger.info("Adding options for auto DB tags")
            inputsandbox.append(FileBuffer('dbTags.py', getAutoDBTags(app.getJobObject())))

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

        # If we are getting the metadata we need to make sure the summary.xml is added to the output sandbox if not there already.
        if app.getMetadata and not 'summary.xml' in output_sand:
            output_sand += ['summary.xml']

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
allHandlers.add('GaudiExec', 'PBS', GaudiExecRTHandler)
allHandlers.add('GaudiExec', 'SGE', GaudiExecRTHandler)


def generateDiracInput(app):
    """
    Construct a DIRAC input which does not need to be unique to each job but is required to have a unique checksum.
    This generates a unique file, uploads it to DRIAC and then stores the LFN in app.uploadedInput
    Args:
        app (GaudiExec): This expects a GaudiExec app to be passed so that the constructed
    """

    input_files, input_folders = collectPreparedFiles(app)

    job = app.getJobObject()

    if input_folders:
        raise ApplicationConfigurationError('Prepared folders not supported yet, please fix this in future')
    else:
        prep_dir = app.getSharedPath()
        addTimestampFile(prep_dir)
        master_no = job.id
        prep_file = '%s_%s.tgz' % (master_no, _pseudo_session_id)
        tmp_dir = tempfile.gettempdir()
        compressed_file = os.path.join(tmp_dir, 'diracInputFiles_'+os.path.basename(prep_file))

        if not job.master:
            rjobs = job.subjobs
        else:
            rjobs = [job]

        with tarfile.open(compressed_file, "w:gz") as tar_file:
            for name in input_files:
                # FIXME Add support for subfiles here once it's working across multiple IGangaFile objects in a consistent way
                # Not hacking this in for now just in-case we end up with a mess as a result
                tar_file.add(name, arcname=os.path.basename(name))

    new_df = uploadLocalFile(job, os.path.basename(compressed_file), tmp_dir)

    app.uploadedInput = new_df
    app.is_prepared.addAssociatedFile(DiracFile(lfn = new_df.lfn))

def generateJobScripts(app, appendJobScripts):
    """
    Construct a DIRAC scripts which must be unique to each job to have unique checksum.
    This generates a unique file, uploads it to DRIAC and then stores the LFN in app.uploadedInput
    Args:
        app (GaudiExec): This expects a GaudiExec app to be passed so that the constructed
        appendJobScripts (bool): Should we add the job scripts to the script archive? (Only makes sense on backends which auto-extact tarballs before running)
    """

    job = app.getJobObject()

    if not job.master:
        rjobs = job.subjobs or [job]
    else:
        rjobs = [job]

    tmp_dir = tempfile.gettempdir()

    # First create the extraOpts files needed 1 per subjob
    for this_job in rjobs:
        logger.debug("RTHandler Making Scripts: %s" % this_job.fqid)
        this_job.application.constructExtraFiles(this_job)

    if not job.master and job.subjobs:
        for sj in rjobs:
            sj.application.jobScriptArchive = sj.master.application.jobScriptArchive

    master_job = job.master or job

    # Now lets get the name of this tar file
    scriptArchive = os.path.join(master_job.application.jobScriptArchive.localDir, master_job.application.jobScriptArchive.namePattern)

    if appendJobScripts:
        # Now lets add the Job scripts to this archive and potentially the extra options to generate the summary.xml
        with tarfile.open(scriptArchive, 'a') as tar_file:
            if app.getMetadata:
                summaryScript = "\nfrom Gaudi.Configuration import *\nfrom Configurables import LHCbApp\nLHCbApp().XMLSummary='summary.xml'"
                summaryPath = os.path.join(job.getInputWorkspace().getPath(), 'summary.py')
                summaryFile = FileBuffer(summaryPath, summaryScript)
                summaryFile.create()
                tar_file.add(summaryPath, arcname = 'summary.py')
            if app.autoDBtags:
                dbScript = getAutoDBTags(job)
                dbPath = os.path.join(job.getInputWorkspace().getPath(), 'dbTags.py')
                dbFile = FileBuffer(dbPath, dbScript)
                dbFile.create()
                tar_file.add(dbPath, arcname = 'dbTags.py')
            for this_job in rjobs:
                this_app = this_job.application
                wnScript = generateWNScript(prepareCommand(this_app), this_app)
                this_script = os.path.join(tmp_dir, wnScript.name)
                wnScript.create(this_script)
                tar_file.add(this_script, arcname=os.path.join(wnScript.subdir, wnScript.name))
                os.unlink(this_script)

    gzipFile(scriptArchive, scriptArchive+'.gz', True)
    app.jobScriptArchive.namePattern = app.jobScriptArchive.namePattern + '.gz'

def generateDiracScripts(app):
    """
    Construct a DIRAC scripts which must be unique to each job to have unique checksum.
    This generates a unique file, uploads it to DRIAC and then stores the LFN in app.uploadedInput
    Args:
        app (GaudiExec): This expects a GaudiExec app to be passed so that the constructed
    """
    generateJobScripts(app, appendJobScripts=True)

    job = app.getJobObject()
    new_df = uploadLocalFile(job, app.jobScriptArchive.namePattern, app.jobScriptArchive.localDir)

    app.jobScriptArchive = new_df

    app.is_prepared.addAssociatedFile(DiracFile(lfn=new_df.lfn))

def uploadLocalFile(job, namePattern, localDir, should_del=True):
    """
    Upload a locally available file to the grid as a DiracFile.
    Randomly chooses an SE.

    Args:
        namePattern (str): name of the file
        localDir (str): localDir of the file
        should_del = (bool): should we delete the local file?
    Return
        DiracFile: a DiracFile of the uploaded LFN on the grid
    """

    new_df = DiracFile(namePattern, localDir=localDir)
    new_df.credential_requirements=job.backend.credential_requirements
    trySEs = getConfig('DIRAC')['allDiracSE']
    random.shuffle(trySEs)
    new_lfn = os.path.join(getInputFileDir(job), namePattern)
    returnable = None
    for SE in trySEs:
        #Check that the SE is writable
        if execute('checkSEStatus("%s", "%s")' % (SE, 'Write')):
            try:
                returnable = new_df.put(force=True, uploadSE=SE, lfn=new_lfn)[0]
                #We have to check the failureReason as DiracFile put doesn't necessarily raise an exception on failure
                if not returnable.failureReason=='':
                    logger.warning("Upload of input file as LFN %s to SE %s failed, trying another SE" % (new_lfn, SE))
                    #Clear the failure reason and continue
                    new_df.failureReason = ''
                    continue
                else:
                    break
            except GangaDiracError as err:
                logger.warning("Upload of input file as LFN %s to SE %s failed, trying another SE" % (new_lfn, SE)) 
    if not returnable:
        raise GangaException("Failed to upload input file to any SE")
    if should_del:
        os.unlink(os.path.join(localDir, namePattern))

    return returnable


def replicateJobFile(fileToReplicate):
    """
    A method to replicate a file to a random SE.
    """

    if not isinstance(fileToReplicate, DiracFile):
        raise GangaDiracError("Can only request replicas of DiracFiles. %s is not a DiracFile" % fileToReplicate)

    if len(fileToReplicate.locations)==0:
        fileToReplicate.getReplicas()

    trySEs = [SE for SE in getConfig('DIRAC')['allDiracSE'] if SE not in fileToReplicate.locations]
    random.shuffle(trySEs)
    success = None
    for SE in trySEs:
        if execute('checkSEStatus("%s", "%s")' % (SE, 'Write')):
            try:
                fileToReplicate.replicate(SE)
                success = True
                break
            except (GangaFileError, GangaDiracError) as err:
                logger.warning("Failed to replicate %s to %s. Trying another SE." % (fileToReplicate.lfn, SE))
    if not success:
        raise GangaException("Failed to replicate %s to any SE" % fileToReplicate.lfn)

def getInputFileDir(job):
    """
    Return the LFN remote dirname for this job
    """
    return os.path.join(DiracFile.diracLFNBase(job.backend.credential_requirements), 'GangaJob_%s/InputFiles' % job.fqid)


def check_creds(cred_req):
    """
    """
    try:
        credential_store[cred_req]
    except KeyError:
        credential_store.create(cred_req)


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

        #First check the remote options or inputfiles exist
        j = app.getJobObject()
        for file_ in j.inputfiles:
            if isinstance(file_, DiracFile):
                try:
                    if file_.getReplicas():
                        continue
                    else:
                        raise GangaFileError("DiracFile inputfile with LFN %s has no replicas" % file_.lfn)
                except GangaFileError as err:
                    raise err
        all_opts_files = app.getOptsFiles(True)
        for opts_file in all_opts_files:
            if isinstance(opts_file, DiracFile):
                try:
                    if opts_file.getReplicas():
                        continue
                    else:
                        raise GangaFileError("DiracFile options file with LFN %s has no replicas" % opts_file.lfn)
                except GangaFileError as err:
                    raise  err

        if app.autoDBtags and not app.getJobObject().inputdata[0].lfn.startswith('/lhcb/MC/'):
            logger.warning("This doesn't look like MC! Not automatically adding db tags.")
            app.autoDBtags = False

        cred_req = app.getJobObject().backend.credential_requirements
        check_creds(cred_req)

        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)

        # If we are getting the metadata we need to make sure the summary.xml is added to the output sandbox if not there already.
        if app.getMetadata and not 'summary.xml' in outputsandbox:
            outputsandbox += ['summary.xml']

        # Check a previously uploaded input is there in case of a job copy
        if isinstance(app.uploadedInput, DiracFile):
            if app.uploadedInput.getReplicas() == {}:
                app.uploadedInput = None
                logger.info("Previously uploaded cmake target missing from Dirac. Uploading it again.")

        if not isinstance(app.uploadedInput, DiracFile):
            generateDiracInput(app)
            try:
                assert isinstance(app.uploadedInput, DiracFile)
            except AssertionError:
                raise ApplicationPrepareError("Failed to upload needed file, aborting submit. Tried to upload to: %s\nIf your Ganga installation is not at CERN your username may be trying to create a non-existent LFN. Try setting the 'DIRAC' configuration 'DiracLFNBase' to your grid user path.\n" % DiracFile.diracLFNBase(cred_req))
        
        rep_data = app.uploadedInput.getReplicas()
        try:
            assert rep_data != {}
        except AssertionError:
            raise ApplicationPrepareError("Failed to find a replica of uploaded file, aborting submit")


        if isinstance(app.jobScriptArchive, (DiracFile, LocalFile)):
            app.jobScriptArchive = None

        generateDiracScripts(app)

        try:
            assert isinstance(app.jobScriptArchive, DiracFile)
        except AssertionError:
            raise ApplicationPrepareError("Failed to upload needed file, aborting submit")
        rep_data = app.jobScriptArchive.getReplicas()
        try:
            assert rep_data != {}
        except AssertionError:
            raise ApplicationPrepareError("Failed to find a replica, aborting submit")

        #Check if the uploaded input already has replicas in case this is a copy of a job.
        if len(app.uploadedInput.locations)==0:
            app.uploadedInput.getReplicas()
        if len(app.uploadedInput.locations) >= 2:
            logger.debug("Uploaded input archive already at two locations, not replicating again")
            return
        else:
            replicateJobFile(app.uploadedInput)

        replicateJobFile(app.jobScriptArchive)

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
        cred_req = app.getJobObject().backend.credential_requirements
        check_creds(cred_req)
        # NB this needs to be removed safely
        # Get the inputdata and input/output sandbox in a sorted way
        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)
        input_data,   parametricinput_data = dirac_inputdata(app)

        # We know we don't need this one
        inputsandbox = []

        job = app.getJobObject()

        # We can support inputfiles and opts_file here. Locally should be submitted once, remotely can be referenced.
        all_opts_files = app.getOptsFiles(True)

        for opts_file in all_opts_files:
            if isinstance(opts_file, DiracFile):
                inputsandbox += ['LFN:'+opts_file.lfn]
        # Sort out inputfiles we support
        for file_ in job.inputfiles:
            if isinstance(file_, DiracFile):
                inputsandbox += ['LFN:'+file_.lfn]
            elif isinstance(file_, LocalFile):
                if job.master is not None and file_ not in job.master.inputfiles:
                    shutil.copy(os.path.join(file_.localDir, file_.namePattern), app.getSharedPath())
                    inputsandbox += [os.path.join(app.getSharedPath(), file_.namePattern)]
            else:
                logger.error("Filetype: %s nor currently supported, please contact Ganga Devs if you require support for this with the DIRAC backend" % getName(file_))
                raise ApplicationConfigurationError("Unsupported filetype: %s with DIRAC backend" % getName(file_))

        master_job = job.master or job

        app.uploadedInput = master_job.application.uploadedInput
        app.jobScriptArchive = master_job.application.jobScriptArchive

        logger.debug("uploadedInput: %s" % app.uploadedInput)

        rep_data = app.uploadedInput.getReplicas()

        logger.debug("Replica info: %s" % rep_data)

        inputsandbox += ['LFN:'+app.uploadedInput.lfn]
        inputsandbox += ['LFN:'+app.jobScriptArchive.lfn]

        #Now add in the missing DiracFiles from the master job
        for file_ in master_job.inputfiles:
            if isinstance(file_, DiracFile) and 'LFN:'+file_.lfn not in inputsandbox:
                inputsandbox += ['LFN:'+file_.lfn]

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
                                        MIN_PROCESSORS=job.backend.minProcessors,
                                        MAX_PROCESSORS=job.backend.maxProcessors,
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

    print("CWD: %s" % getcwd())
    print("Files found on WN: %s" % (listdir('.')))

    if 'LHCb_release_area' not in environ:
        environ['LHCb_release_area'] = '/cvmfs/lhcb.cern.ch/lib/lhcb/'


    # Extract any/_all_ (b/g)zip files on the WN
    extractAllTarFiles('.')

    print("Executing: %s" % '###COMMAND###'+' '+' '.join(sys.argv[1:]))

    # Execute the actual command on the WN
    # NB os.system caused the entire stream to be captured before being streamed in some cases
    pipe = subprocess.Popen('###COMMAND###'+' '+' '.join(sys.argv[1:]), shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

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

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('GaudiExec', 'Dirac', GaudiExecDiracRTHandler)

