
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import File, ShareDir
from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError, GangaException

from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Base.Proxy import getName, isType, stripProxy

from os import rename
from os.path import join, isdir, isfile, abspath, expanduser, expandvars, split, isabs, basename, exists
import shutil
from Ganga.Utility.files import expandfilename

import tempfile
import time
import subprocess

logger = getLogger()
cmake_sandbox_name = 'cmake-input-sandbox.tgz'
build_target = 'ganga-input-sandbox'
build_dest = 'input-sandbox.tgz'

def _exec_cmd(cmd, cwdir):
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

class GaudiRun(IPrepareApp):

    """
        New GaudiApp for LHCb apps written/constructed making use of the new CMake application
    """
    _schema = Schema(Version(1, 0), {
        'directory':    SimpleItem(preparable=1, defvalue='$HOME/MyProjectPath', typelist=['str'], comparable=1,
            doc='A path to the project that you\'re wanting to run.'),
        'build_opts':   SimpleItem(defvalue=["Hello World"], typelist=['str', 'Ganga.GPIDev.Lib.File.File.File', 'int'], sequence=1, strict_sequence=0,
            doc="Options to be passed to 'make ganga-input-sandbox'"),
        'is_prepared':  SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=['type(None)', 'bool', ShareDir], protected=0, comparable=1,
            doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash':         SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=0,
            doc='MD5 hash of the string representation of applications preparable attributes'),
        'arch':         SimpleItem(defvalue="x86_64-slc6-gcc49-opt", typelist=['str'], doc='Arch the application was built for'),
        'build_target': SimpleItem(defvalue='', typelist=['str'], doc='Target which is to be added to the prepared state of an application', hidden=1, preparable=1),
        })
    _category = 'applications'
    _name = 'GaudiRun'
    _exportmethods = ['prepare', 'unprepare', 'exec_cmd', 'getDir']

    def __init__(self):
        super(GaudiRun, self).__init__()

    def unprepare(self, force=False):
        logger.debug('Running unprepare in GaudiRun app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):

        if (self.is_prepared is not None) and (force is not True):
            raise ApplicationPrepareError('%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        # lets use the same criteria as the configure() method for checking file existence & sanity
        # this will bail us out of prepare if there's somthing odd with the job config - like the executable
        # file is unspecified, has a space or is a relative path
        self.configure(self)
        logger.info('Preparing %s application.' % getName(self))
        setattr(self, 'is_prepared', ShareDir())
        logger.info('Created shared directory: %s' % (self.is_prepared.name))

        self.build_target = self.buildGangaTarget()

        try:
            # copy any 'preparable' objects into the shared directory
            send_to_sharedir = self.copyPreparables()
            # add the newly created shared directory into the metadata system
            # if the app is associated with a persisted object
            self.checkPreparedHasParent(self)
            self.post_prepare()

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise err

        return 1

    def configure(self, masterappconfig):

        return (None, None)

    def getDir(self):
        myDir = self.directory
        myDir = abspath(expanduser(expandvars(myDir)))
        return myDir

    def exec_cmd(self, cmd):

        cmd_file = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)

        cmd_wrapper = './run bash -c "%s"' % cmd

        cmd_file.write(cmd_wrapper)
        cmd_file.flush()

        script_run = 'bash %s' % cmd_file.name

        rc, stdout, stderr = _exec_cmd(script_run, self.getDir())

        if rc != 0:
            logger.error("Failed to execute command: %s" % script_run)
            logger.error("Tried to execute command in: %s" % self.getDir())
            logger.error("StdErr: %s" % str(stderr))
            raise GangaException("Failed to Execute command")

    def buildGangaTarget(self):

        logger.info("Make-ing target '%s'" % build_target)
        self.exec_cmd('make clean && make %s' % build_target)

        targetPath = join(self.getDir(), 'build.%s' % self.arch, 'ganga')
        if not isdir(targetPath):
            raise GangaException("Target Path: %s NOT found!" % targetPath)
        sandbox_str = '%s' % build_dest
        targetFile = join(targetPath, sandbox_str)
        if not isfile(targetFile):
            raise GangaException("Target File: %s NOT found!" % targetFile)
        wantedTargetFile = join(targetPath, cmake_sandbox_name)
        rename(targetFile, wantedTargetFile)
        if not isfile(wantedTargetFile):
            raise GangaException("Wanted Target File: %s NOT found" % wantedTargetFile)

        logger.info("Built %s" % wantedTargetFile)
        return wantedTargetFile

    def get_prepared_files(self):
        return [File(self.build_target)]

