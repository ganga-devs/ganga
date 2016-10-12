from os import rename, path, makedirs, chdir, unlink, listdir, chmod
from os import stat as os_stat
import tempfile
import time
import subprocess
import shutil
import tarfile
import threading
import stat
import uuid
from functools import wraps
from StringIO import StringIO

from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError, GangaException
from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Base.Filters import allComponentFilters
from Ganga.GPIDev.Base.Proxy import getName
from Ganga.GPIDev.Lib.File.File import ShareDir
from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, GangaFileItem
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename, fullpath

from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.Backends.DiracBase import DiracBase

from .GaudiExecUtils import getGaudiExecInputData, _exec_cmd, getTimestampContent, gaudiPythonWrapper

logger = getLogger()


def gaudiExecBuildLock(f):
    """ Method used to lock the build methods in GaudiExec so we don't run multiple builds in parallel.
    This is because each new build destorys the target.
    Args:
        f(function): This should be the buildGangaTarget from GaudiExec
    """
    @wraps(f)
    def masterPrepLock(self, *args, **kwds):
        
        # Get the global lock and prepare
        with gaudiExecBuildLock.globalBuildLock:
            return f(self,*args, **kwds)

    return masterPrepLock

# Global lock for all builds
gaudiExecBuildLock.globalBuildLock = threading.Lock()


class GaudiExec(IPrepareApp):
    """

    Welcome to the new GaudiApp for LHCb apps written/constructed making use of the new CMake framework

    =============
    Requirements:
    =============

    Before submitting jobs with this application you will need to run something similar to the following:
    (outside of Ganga at the command line)

    cd $SOMEPATH
    lb-dev DaVinci v40r2
    cd $SOMEPATH/DaVinciDev_v40r2
    getpack 

    This program will perform the following command to `prepare` the application before submission:
        make ganga-input-sandbox
    NB:
    The output from this command can be quite large and Ganga will save it to disk and store it at least once per (master) job
    If your build target is large I would advise that you consider placing your gangadir in your AFS workspace where there is more storage available

    ======
    Usage:
    ======

    This application needs to be configured with the absolute directory of the project and the options you want to pass to gaudirun.py

    e.g.

    j=Job()
    myApp = GaudiExec()
    myApp.directory = "$SOMEPATH/DaVinciDev_v40r2"
    myApp.options = ["$SOMEPATH/DaVinciDev_v40r2/myDaVinciOpts.py"]
    j.application = myApp
    j.submit()

    To setup a minimal application you can also run the helper function:

    prepare_cmake_app(myApp, myVer, myPath, myGetpack)

    =============
    How it works:
    =============

    The actual command run on the WN is::

        ./run gaudirun.py optionsFile.py data.py

    If you would prefer to have your optsfile run as a python application aka like 'GaudiPython' style jobs.
    Set:
        job.application.useGaudiRun = False

    This then changes the command run on the WN to be::

        ./run python OptsFileWrapper.py

    Here the OptsFileWrapper script imports the extraOpts and the data.py describing the data to be run over and executes options in the global namespace with 'execfile'
    The OptsFileWrapper will _execute_ the first file in the job.application.options and will import all other opts files before executing this one.

    """
    _schema = Schema(Version(1, 0), {
        # Options created for constructing/submitting this app
        'directory':    SimpleItem(defvalue='', typelist=[None, str], comparable=1, doc='A path to the project that you\'re wanting to run.'),
        'options':       GangaFileItem(defvalue=[], sequence=1, doc='List of files which contain the options I want to pass to gaudirun.py'),
        'uploadedInput': GangaFileItem(defvalue=None, hidden=1, doc='This stores the input for the job which has been pre-uploaded so that it gets to the WN'),
        'jobScriptArchive': GangaFileItem(defvalue=None, hidden=1, copyable=0, doc='This file stores the uploaded scripts which are generated fron this app to run on the WN'),
        'useGaudiRun':  SimpleItem(defvalue=True, doc='Should \'options\' be run as "python options.py data.py" rather than "gaudirun.py options.py data.py"'),
        'platform' :    SimpleItem(defvalue='x86_64-slc6-gcc49-opt', typelist=[str], doc='Platform the application was built for'),
        'extraOpts':    SimpleItem(defvalue='', typelist=[str], doc='An additional string which is to be added to \'options\' when submitting the job'),
        'extraArgs':    SimpleItem(defvalue=[], typelist=[list], sequence=1, doc='Extra runtime arguments which are passed to the code running on the WN'),

        # Prepared job object
        'is_prepared':  SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, ShareDir], protected=0, comparable=1,
            doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash':         SimpleItem(defvalue=None, typelist=[None, str], hidden=1, doc='MD5 hash of the string representation of applications preparable attributes'),
        })
    _category = 'applications'
    _name = 'GaudiExec'
    _exportmethods = ['prepare', 'unprepare', 'execCmd', 'readInputData']

    cmake_sandbox_name = 'cmake-input-sandbox.tgz'
    build_target = 'ganga-input-sandbox'
    build_dest = 'input-sandbox.tgz'
    sharedOptsFile_baseName = 'jobScripts-%s.tar'


    def __setattr__(self, attr, value):
        """
        This overloads the baseclass setter method and allows for dynamic evaluation of a parameter on assignment
        Args:
            attr (str): Name of the attribute which is being assigned for this class
            value (unknown): The raw value which is being passed to this class for assigning to the attribute
        """

        actual_value = value
        if attr == 'directory':
            if value:
                actual_value = path.abspath(fullpath(expandfilename(value)))
        elif attr == 'options':
            if isinstance(value, str):
                new_file = allComponentFilters['gangafiles'](value, None)
                actual_value = [ new_file ]
            elif isinstance(value, IGangaFile):
                actual_value = [ value ]
            elif not isinstance(value, (list, tuple, GangaList, type(None))):
                logger.warning("Possibly setting wrong type for options: '%s'" % type(value))

        super(GaudiExec, self).__setattr__(attr, actual_value)


    def unprepare(self, force=False):
        """
        Unprepare the GaudiExec App
        Args:
            force (bool): Forces an un-prepare
        """
        logger.debug('Running unprepare in GaudiExec app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None
        self.uploadedInput = None
        self.jobScriptArchive = None


    def prepare(self, force=False):
        """
        This method creates a set of prepared files for the application to pass to the RTHandler
        Args:
            force (bool): Forces a prepare to be run
        """

        if (self.is_prepared is not None) and not force:
            raise ApplicationPrepareError('%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        # lets use the same criteria as the configure() method for checking file existence & sanity
        # this will bail us out of prepare if there's somthing odd with the job config - like the executable
        # file is unspecified, has a space or is a relative path
        self.configure(self)
        logger.info('Preparing %s application.' % getName(self))
        self.is_prepared = ShareDir()
        logger.info('Created shared directory: %s' % (self.is_prepared.name))

        this_build_target = self.buildGangaTarget()

        try:
            # copy any 'preparable' objects into the shared directory
            send_to_sharedir = self.copyPreparables()
            # add the newly created shared directory into the metadata system
            # if the app is associated with a persisted object
            self.checkPreparedHasParent(self)

            self.copyIntoPrepDir(this_build_target)
            all_opts_files = self.getOptsFiles()
            for opts_file in all_opts_files:
                if isinstance(opts_file, LocalFile):
                    self.copyIntoPrepDir(path.join( opts_file.localDir, path.basename(opts_file.namePattern) ))
                elif isinstance(opts_file, DiracFile):
                    # NB safe to put it here as should have expressly setup a path for this job by now.
                    # We cannot _not_ place this here based upon the backend.
                    # Always have to put it here regardless of if we're on DIRAC or Local so prepared job can be copied.
                    opts_file.get(localPath=self.getSharedPath())
                else:
                    raise ApplicationConfigurationError(None, "Opts file type %s not yet supported please contact Ganga devs if you require this support" % getName(opts_file))
            self.post_prepare()

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise

        self.cleanGangaTargetArea(this_build_target)

        return 1


    def getExtraOptsFileName(self):
        """
        Returns the name of the opts file which corresponds to the job which owns this app
        This places the script of interest in a subdir to not overly clutter the WN
        """
        return path.join('opts', 'extra_opts_%s_.py' % self.getJobObject().getFQID('.'))


    def getWrapperScriptName(self):
        """
        Returns the name of the wrapper script file which corresponds to the job which owns this app
        This places the script of interest in a subdir to not overly clutter the WN
        """
        return path.join('wrapper', 'job_%s_optsFileWrapper.py' % self.getJobObject().getFQID('.'))


    def constructExtraFiles(self, job):
        """
        This constructs or appends to an uncompressed archive containing all of the opts files which are required to run on the grid
        Args:
            job (Job): The parent job of this application, we don't care if it's unique or not
        """

        master_job = job.master or job

        df = master_job.application.jobScriptArchive

        folder_dir = master_job.getInputWorkspace(create=True).getPath()

        if not df or df.namePattern == '':
            unique_name = GaudiExec.sharedOptsFile_baseName % uuid.uuid4()
            master_job.application.jobScriptArchive = LocalFile(namePattern=unique_name, localDir=folder_dir)
            tar_filename = path.join(folder_dir, unique_name)
            if not path.isfile(tar_filename):
                with tarfile.open(tar_filename, "w"):
                    pass
            with tarfile.open(tar_filename, "a") as tar_file:
                tinfo = tarfile.TarInfo('__timestamp__')
                tinfo.mtime = time.time()
                fileobj = StringIO(getTimestampContent())
                tinfo.size = fileobj.len
                tar_file.addfile(tinfo, fileobj)
        else:
            unique_name = master_job.application.jobScriptArchive.namePattern

        extra_opts_file = self.getExtraOptsFileName()

        # First construct if needed
        if not path.isfile(path.join(folder_dir, unique_name)):
            with tarfile.open(path.join(folder_dir, unique_name), "w"):
                pass

        # Now append the extra_opts file here when needed
        with tarfile.open(path.join(folder_dir, unique_name), "a") as tar_file:
            # Add the extra opts file to the job
            tinfo = tarfile.TarInfo(extra_opts_file)
            tinfo.mtime = time.time()
            fileobj = StringIO(self.extraOpts)
            tinfo.size = fileobj.len
            tar_file.addfile(tinfo, fileobj)

            if not self.useGaudiRun:
                # Add the WN script for wrapping the job
                logger.info("Constructing: %s" % self.getWrapperScriptName())
                tinfo2 = tarfile.TarInfo(self.getWrapperScriptName())
                tinfo2.mtime = time.time()
                fileobj2 = StringIO(self.getWNPythonContents())
                tinfo2.size = fileobj2.len
                tar_file.addfile(tinfo2, fileobj2)


    def cleanGangaTargetArea(self, this_build_target):
        """
        Method to remove the build target and other files not needed to reproduce the same build target again
        Args:
            this_build_target (str): This is the full path of the build target
        """
        logger.debug("Cleaning up area after prepare")

        # Don't delete these
        preserved_set = set(['run'])

        build_dir = path.dirname(this_build_target)
        for obj in set(listdir(build_dir)) - preserved_set:
            logger.debug("del: %s of %s" %(obj, set(listdir(build_dir)) - preserved_set))
            if path.isfile(path.join(build_dir, obj)):
                unlink(path.join(build_dir, obj))
            elif path.isdir(path.join(build_dir, obj)):
                shutil.rmtree(path.join(build_dir, obj), ignore_errors=True)


    def configure(self, masterappconfig):
        """
        Required even though nothing is done in this step for this App
        Args:
            masterappconfig (unknown): This is the output from the master_configure from the parent app
        """
        # Lets test the inputs
        opt_file = self.getOptsFiles()
        dir_name = self.directory
        return (None, None)


    def getOptsFiles(self):
        """
        This function returns a sanitized absolute path to the self.options file from user input
        """
        if self.options:
            for this_opt in self.options:
                if isinstance(this_opt, LocalFile):
                    ## FIXME LocalFile should return the basename and folder in 2 attibutes so we can piece it together, now it doesn't
                    full_path = path.join(this_opt.localDir, this_opt.namePattern)
                    if not path.exists(full_path):
                        raise ApplicationConfigurationError(None, "Opts File: \'%s\' has been specified but does not exist please check and try again!" % full_path)
                elif isinstance(this_opt, DiracFile):
                    pass
                else:
                    logger.error("opts: %s" % self.options)
                    raise ApplicationConfigurationError(None, "Opts file type %s not yet supported please contact Ganga devs if you require this support" % getName(this_opt))
            return self.options
        else:
            raise ApplicationConfigurationError(None, "No Opts File has been specified, please provide one!")


    def getEnvScript(self):
        """
        Return the script which wraps the running command in a correct environment
        """
        return 'export CMTCONFIG=%s; source LbLogin.sh --cmtconfig=%s && ' % (self.platform, self.platform)


    def execCmd(self, cmd):
        """
        This method executes a command within the namespace of the project. The cmd is placed in a bash script which is executed within the env
        This will adopt the platform associated with this application.
        Any explicit calls to be run within the project env have to be prepended with './run '. This is not added automatically

        e.g. The following will execute a 'make' command within the given project dir

            app = GaudiExec('some/path')
            app.execCmd('make')

        Args:
            cmd (str): This is the command(s) which are to be executed within the project environment and directory
        """

        cmd_file = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)

        cmd_file.write("#!/bin/bash")
        cmd_file.write("\n")
        cmd_file.write(self.getEnvScript())
        cmd_file.write(cmd)
        cmd_file.flush()
        cmd_file.close()
        st = os_stat(cmd_file.name)
        chmod(cmd_file.name, st.st_mode | stat.S_IEXEC)

        logger.debug("Running: %s" % cmd_file.name)

        # I would have preferred to execute all commands against inside `./run` so we have some sane behaviour
        # but this requires a build to have been run before we can use this command reliably... so we're just going to be explicit

        rc, stdout, stderr = _exec_cmd(cmd_file.name, self.directory)

        if rc != 0:
            logger.error("Failed to execute command: %s" % cmd_file.name)
            logger.error("Tried to execute command in: %s" % self.directory)
            logger.error("StdErr: %s" % str(stderr))
            raise GangaException("Failed to Execute command")

        unlink(cmd_file.name)

        return rc, stdout, stderr


    @gaudiExecBuildLock
    def buildGangaTarget(self):
        """
        This builds the ganga target 'ganga-input-sandbox' for the project defined by self.directory
        This returns the absolute path to the file after it has been created. It will fail if things go wrong or the file fails to generate
        """
        logger.info("Make-ing target '%s'     (This may take a few minutes depending on the size of your project)" % GaudiExec.build_target)
        # Up to the user to run something like make clean... (Although that would avoid some potential CMake problems)
        self.execCmd('make %s' % GaudiExec.build_target)

        targetPath = path.join(self.directory, 'build.%s' % self.platform, 'ganga')
        if not path.isdir(targetPath):
            raise GangaException("Target Path: %s NOT found!" % targetPath)
        sandbox_str = '%s' % GaudiExec.build_dest
        targetFile = path.join(targetPath, sandbox_str)
        if not path.isfile(targetFile):
            raise GangaException("Target File: %s NOT found!" % targetFile)
        wantedTargetFile = path.join(targetPath, GaudiExec.cmake_sandbox_name)
        rename(targetFile, wantedTargetFile)
        if not path.isfile(wantedTargetFile):
            raise GangaException("Wanted Target File: %s NOT found" % wantedTargetFile)

        logger.info("Built %s" % wantedTargetFile)
        return wantedTargetFile


    def readInputData(self, opts):
        """
        This reads the inputdata from a file and assigns it to the inputdata field of the parent job.

        Or you can use BKQuery and the box repo to save having to do this over and over
        Args:
            opts (str): This is the file which contains the inputdata we want to read in
        """
        input_dataset = getGaudiExecInputData(opts, self)
        try:
            job = self.getJobObject()
        except:
            raise GangaException("This makes no sense without first belonging to a job object as I can't assign input data!")

        if job.inputdata is not None and len(job.inputdata) > 0:
            logger.warning("Warning Job %s already contained inputdata, overwriting" % job.fqid)

        job.inputdata = input_dataset


    def getWNPythonContents(self):
        """
        Return the wrapper script which is used to run GaudiPython type jobs on the WN
        """
        # FIXME should there be a more central definition of 'data.py' string to rename this for all of LHCb if it ever changes for LHCbDirac
        from ..RTHandlers.GaudiExecRTHandlers import GaudiExecDiracRTHandler

        all_names = [this_o.namePattern  for this_o in self.options]
        return gaudiPythonWrapper(repr(self.extraArgs), self.getExtraOptsFileName(),
                                  GaudiExecDiracRTHandler.data_file, all_names)

