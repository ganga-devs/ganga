from os import rename, path, makedirs, chdir, unlink
import tempfile
import time
import subprocess

from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError, GangaException
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Base.Proxy import getName
from Ganga.GPIDev.Lib.File.File import ShareDir
from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, GangaFileItem
from Ganga.Runtime.GPIexport import exportToGPI
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.Backends.DiracBase import DiracBase

logger = getLogger()

def _exec_cmd(cmd, cwdir):
    """
    This is taken from the code which runs SetupProject
    TODO: Replace me with a correct call to execute once the return code is known to work
    
    Args:
        cmd (str): This is the full command which is to be executed
        cwdir (str): The folder the command is to be run in
    """
    pipe = subprocess.Popen(cmd,
            shell=True,
            env=None,
            cwd=cwdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    while pipe.poll() is None:
        time.sleep(0.5)
    return pipe.returncode, stdout, stderr

def prepare_cmake_app(myApp, myVer, myPath='$HOME/cmtuser', myGetpack=None):
    """
    Short helper function for setting up minimal application environments on disk for job submission
    Args:
        myApp (str): This is the name of the app to pass to lb-dev
        myVer (str): This is the version of 'myApp' to pass tp lb-dev
        myPath (str): This is where lb-dev will be run
        myGepPack (str): This is a getpack which will be run once the lb-dev has executed
    """
    full_path = expandfilename(myPath, True)
    if not path.exists(full_path):
        makedirs(full_path)
    chdir(full_path)
    _exec_cmd('lb-dev %s %s' % (myApp, myVer), full_path)
    logger.info("Set up App Env at: %s" % full_path)
    if myGetpack:
        dev_dir = path.join(full_path, myApp + 'Dev_' + myVer)
        _exec_cmd('getpack %s' % myGetpack, dev_dir)
        logger.info("Ran Getpack: %s" % myGetpack)

exportToGPI('prepare_cmake_app', prepare_cmake_app, 'Functions')

class GaudiRun(IPrepareApp):

    """
    Welcome to the new GaudiApp for LHCb apps written/constructed making use of the new CMake framework

    Before submitting jobs with this application you will need to run something similar to the following:

    cd $SOMEPATH
    lb-dev DaVinci v40r2
    cd $SOMEPATH/DaVinciDev_v40r2
    getpack 

    This program will perform the following command to `prepare` the application before submission:
        make ganga-input-sandbox
    NB:
    The output from this command can be quite large and Ganga will save it to disk and store it at least once per (master) job
    If your build target is large I would advise that you consider placing your gangadir in your AFS workspace where there is more storage available

    This application needs to be configured with the absolute directory of the project and the options you want to pass to gaudirun.py

    e.g.

    j=Job()
    myApp = GaudiRun()
    myApp.directory = "$SOMEPATH/DaVinciDev_v40r2"
    myApp.myOpts = "$SOMEPATH/DaVinciDev_v40r2/myDaVinciOpts.py"
    j.application = myApp
    j.submit()

    To setup a minimal application you can also run the helper function:

    prepare_cmake_app(myApp, myVer, myPath, myGetpack)

    """
    _schema = Schema(Version(1, 0), {
        # Options created for constructing/submitting this app
        'directory':    SimpleItem(preparable=1, defvalue=None, typelist=[None, str], comparable=1,
            doc='A path to the project that you\'re wanting to run.'),
        'build_opts':   SimpleItem(defvalue=[""], typelist=[str], sequence=1, strict_sequence=0,
            doc="Options to be passed to 'make ganga-input-sandbox'"),
        'myOpts':       GangaFileItem(defvalue=None, doc='File which contains the extra opts I want to pass to gaudirun.py'),
        'uploadedInput': GangaFileItem(defvalue=None, doc='This stores the input for the job which has been pre-uploaded so that it gets to the WN'),

        # Prepared job object
        'is_prepared':  SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, bool, ShareDir], protected=0, comparable=1,
            doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash':         SimpleItem(defvalue=None, typelist=[None, str], hidden=0,
            doc='MD5 hash of the string representation of applications preparable attributes'),
        'arch':         SimpleItem(defvalue="x86_64-slc6-gcc49-opt", typelist=[str], doc='Arch the application was built for'),
        })
    _category = 'applications'
    _name = 'GaudiRun'
    _exportmethods = ['prepare', 'unprepare', 'exec_cmd', 'getDir']

    cmake_sandbox_name = 'cmake-input-sandbox.tgz'
    build_target = 'ganga-input-sandbox'
    build_dest = 'input-sandbox.tgz'

    def unprepare(self, force=False):
        """
        Unprepare the GaudiRun App
        Args:
            force (bool): Forces an un-prepare
        """
        logger.debug('Running unprepare in GaudiRun app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None
        ## FIXME Add some configurable object which controls whether a file should be removed from storage
        ## Here if the is_prepared count reaches zero
        self.uploadedInput = None

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
            opts_file = self.getOptsFile()
            if isinstance(opts_file, LocalFile):
                self.copyIntoPrepDir(path.join( opts_file.localDir, path.basename(opts_file.namePattern) ))
            elif isinstance(opts_file, DiracFile) and self.master and not isinstance(self.getJobObject().backend, DiracBase):
                # NB safe to put it here as should have expressly setup a path for this job by now
                opts_file.get(localPath=self.getSharedPath())
            else:
                raise ApplicationConfigurationError(None, "Opts file type %s not yet supported please contact Ganga devs if you require this support" % getName(opts_file))
            self.post_prepare()

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise

        return 1

    def configure(self, masterappconfig):
        """
        Required even though nothing is done in this step for this App
        Args:
            masterappconfig (unknown): This is the output from the master_configure from the parent app
        """
        # Lets test the inputs
        opt_file = self.getOptsFile()
        dir_name = self.getDir()
        return (None, None)

    def getOptsFile(self):
        """
        This function returns a sanitized absolute path to the self.myOpts file from user input
        """
        if self.myOpts:
            if isinstance(self.myOpts, LocalFile):
                ## FIXME LocalFile should return the basename and folder in 2 attibutes so we can piece it together, now it doesn't
                full_path = expandfilename(path.join(self.myOpts.localDir, path.basename(self.myOpts.namePattern)), force=True)
                if not path.exists(full_path):
                    raise ApplicationConfigurationError(None, "Opts File: \'%s\' has been specified but does not exist please check and try again!")
                self.myOpts = LocalFile(namePattern=path.basename(full_path), localDir=path.dirname(full_path))
                return self.myOpts
            elif isinstance(self.myOpts, DiracFile):
                return self.myOpts
            else:
                raise ApplicationConfigurationError(None, "Opts file type %s not yet supported please contact Ganga devs if you require this support" % getName(self.myOpts))
        else:
            raise ApplicationConfigurationError(None, "No Opts File has been specified, please provide one!")

    def getDir(self):
        """
        This function returns a sanitized absolute path of the self.directory method from user input
        """
        if self.directory:
            return path.abspath(expandfilename(self.directory))
        else:
            raise ApplicationConfigurationError(None, "No Opts File has been specified, please provide one!")

    def exec_cmd(self, cmd):
        """
        This method executes a command within the namespace of the project. The cmd is placed in a bash script which is executed within the env
        Args:
            cmd (str): This is the command(s) which are to be executed within the project environment and directory
        """

        cmd_file = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)

        cmd_wrapper = './run bash -c "%s"' % cmd

        cmd_file.write(cmd_wrapper)
        cmd_file.flush()

        script_run = 'bash %s' % cmd_file.name

        rc, stdout, stderr = _exec_cmd(script_run, self.getDir())

        unlink(cmd_file.name)

        if rc != 0:
            logger.error("Failed to execute command: %s" % script_run)
            logger.error("Tried to execute command in: %s" % self.getDir())
            logger.error("StdErr: %s" % str(stderr))
            raise GangaException("Failed to Execute command")

    def buildGangaTarget(self):
        """
        This builds the ganga target 'ganga-input-sandbox' for the project defined by self.directory
        This returns the absolute path to the file after it has been created. It will fail if things go wrong or the file fails to generate
        """
        logger.info("Make-ing target '%s'     (This may take a few minutes depending on the size of your project)" % GaudiRun.build_target)
        self.exec_cmd('make clean && make %s' % GaudiRun.build_target)

        targetPath = path.join(self.getDir(), 'build.%s' % self.arch, 'ganga')
        if not path.isdir(targetPath):
            raise GangaException("Target Path: %s NOT found!" % targetPath)
        sandbox_str = '%s' % GaudiRun.build_dest
        targetFile = path.join(targetPath, sandbox_str)
        if not path.isfile(targetFile):
            raise GangaException("Target File: %s NOT found!" % targetFile)
        wantedTargetFile = path.join(targetPath, GaudiRun.cmake_sandbox_name)
        rename(targetFile, wantedTargetFile)
        if not path.isfile(wantedTargetFile):
            raise GangaException("Wanted Target File: %s NOT found" % wantedTargetFile)

        logger.info("Built %s" % wantedTargetFile)
        return wantedTargetFile

