#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Parent for all Gaudi and GaudiPython applications in LHCb.'''

import os
import tempfile
import gzip
import shutil
from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.GPIDev.Schema import SimpleItem, Schema, Version
from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
import GangaCore.Utility.logging
from GangaCore.Utility.files import expandfilename, fullpath
from .GaudiUtils import get_user_platform, fillPackedSandbox, get_user_dlls
from GangaCore.GPIDev.Lib.File import File
from GangaCore.Core.exceptions import ApplicationConfigurationError
import GangaCore.Utility.Config
from GangaCore.Utility.execute import execute
from GangaCore.GPIDev.Lib.File import ShareDir
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Base.Proxy import getName
import copy
logger = GangaCore.Utility.logging.getLogger()


class GaudiBase(IPrepareApp):

    '''Parent for all Gaudi and GaudiPython applications, should not be used
    directly.'''

    schema = {}
    docstr = 'The version of the application (like "v19r2")'
    schema['version'] = SimpleItem(preparable=1, defvalue=None,
                                   typelist=['str', 'type(None)'], doc=docstr)
    docstr = 'The platform the application is configured for (e.g. ' \
             '"slc4_ia32_gcc34")'
    schema['platform'] = SimpleItem(preparable=1, defvalue=None,
                                    typelist=['str', 'type(None)'], doc=docstr)
    docstr = 'The user path to be used. After assigning this'  \
             ' you can do j.application.getpack(\'Phys DaVinci v19r2\') to'  \
             ' check out into the new location. This variable is used to '  \
             'identify private user DLLs by parsing the output of "cmt '  \
             'show projects".'
    schema['user_release_area'] = SimpleItem(preparable=1, defvalue=None,
                                             typelist=['str', 'type(None)'],
                                             doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['appname'] = SimpleItem(preparable=1, defvalue=None, typelist=['str', 'type(None)'],
                                   hidden=1, doc=docstr)
    docstr = 'Location of shared resources. Presence of this attribute implies'\
             'the application has been prepared.'
    schema['is_prepared'] = SimpleItem(defvalue=None,
                                       strict_sequence=0,
                                       visitable=1,
                                       copyable=1,
                                       typelist=['type(None)', 'str', ShareDir],
                                       hidden=0,
                                       protected=1,
                                       doc=docstr)
    docstr = 'The env'
    schema['env'] = SimpleItem(preparable=1, transient=1, defvalue=None,
                               hidden=1, doc=docstr, typelist=['type(None)', 'dict'])
    docstr = 'MD5 hash of the string representation of applications preparable attributes'
    schema['hash'] = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=1)

    schema['newStyleApp'] = SimpleItem(defvalue=False, typelist=['bool'], doc="Is this app a 'new Style' CMake app?")

    _name = 'GaudiBase'
    _exportmethods = ['getenv', 'getpack', 'make', 'projectCMD', 'cmt']
    _schema = Schema(Version(0, 1), schema)
    _hidden = 1

    
    #def __init__(self):
    #    super(GaudiBase, self).__init__(None)

    def _get_default_version(self, gaudi_app):
        raise NotImplementedError

    def _get_default_platform(self):
        return get_user_platform(self)

    #def _init(self, set_ura):
    def _init(self):
        if self.appname is None:
            raise ApplicationConfigurationError("appname is None")
        if (not self.version):
            self.version = self._get_default_version(self.appname)
        if (not self.platform):
            self.platform = self._get_default_platform()
        #if not set_ura:
        #    return
        if not self.user_release_area:
            expanded = os.path.expandvars("$User_release_area")
            if expanded == "$User_release_area":
                self.user_release_area = ""
            else:
                self.user_release_area = expanded.split(os.pathsep)[0]

        from GangaCore.Utility.Shell import expand_vars
        env = expand_vars(os.environ)

        env['User_release_area'] = self.user_release_area
        env['CMTCONFIG'] = self.platform
        return env

    def getenv(self, cache_env=False):
        '''Returns a copy of the environment used to flatten the options, e.g.
        env = DaVinci().getenv(), then calls like env[\'DAVINCIROOT\'] return
        the values.

        Note: Editing this does not affect the options processing.
        '''
        if self.env is None:
            shell = None
            try:
                job = self.getJobObject()
            except Exception as err:
                logger.debug("Error: %s" % str(err))
                pass
            else:
                env_file_name = job.getDebugWorkspace().getPath() + \
                    '/gaudi-env.py.gz'
                if os.path.exists(env_file_name):
                    in_file = gzip.GzipFile(env_file_name, 'rb')
                    exec(in_file.read())
                    in_file.close()
                    shell = gaudi_env

            if shell is None:
                shell = self._getshell()
            if cache_env:
                self.env = copy.deepcopy(shell)
            return shell

        return self.env

    def _export_getenv(self):
        return self.getenv(False)

    def getpack(self, options=''):
        """Execute a getpack command. If as an example dv is an object of
        type DaVinci, the following will check the Analysis package out in
        the project area pointed to by the dv object.

        dv.getpack('Tutorial/Analysis v6r2')
        """
        # Make sure project user area is there
        project_path = expandfilename(self.user_release_area)
        if project_path:
            if not os.path.exists(project_path):
                try:
                    os.makedirs(project_path)
                except Exception as err:
                    logger.error("Can not create project user directory: " + project_path)
                    logger.debug("%s" % str(err))
                    return

        execute('getpack %s' % options,
                shell=True,
                timeout=None,
                env=self.getenv(False),
                cwd=self.user_release_area)

    def make(self, argument=''):
        """Build the code in the release area the application object points to."""
        from GangaGaudi.Lib.Application.GaudiUtils import make
        make(self, argument)

    def projectCMD(self, command):
        """Eecute a given command at the top level of a requested project."""

        execute('%s' % command,
                shell=True,
                timeout=None,
                env=self.getenv(False),
                cwd=self.user_release_area)

    def cmt(self, command):
        """Execute a cmt command in the cmt user area pointed to by the
        application. Will execute the command "cmt <command>" after the
        proper configuration. Do not include the word "cmt" yourself."""

        if self.newStyleApp is True:
            logger.warning("Cannot Use this in combination with cmake!")
            return

        execute('cmt %s' % command,
                shell=True,
                timeout=None,
                env=self.getenv(False),
                cwd=self.user_release_area)

    def unprepare(self, force=False):
        self._unregister()

    def _unregister(self):
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared)
            self.is_prepared = None
        self.hash = None
        self.env = None

    def prepare(self, force=False):
        # Define the ShareDir
        self._register(force)
        try:
            # Try to prepare the application fully
            self._really_prepare(force)
        except Exception as err:
            # Cleanup after self on fail as self.is_prepared is used as test to
            # see if I'm prepared
            logger.error("Prepare Error:\n%s" % str(err))
            self._unregister()
            raise

    def _really_prepare(self, force=False):
        if (not self.is_prepared):
            raise ApplicationConfigurationError(
                "Could not establish sharedir")

        if (not self.user_release_area):
            return  # GaudiPython and Bender dont need the following.
        if (not self.appname):
            raise ApplicationConfigurationError("appname is None")
        if (not self.version):
            raise ApplicationConfigurationError("version is None")
        if (not self.platform):
            raise ApplicationConfigurationError("platform is None")

        share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                 'shared',
                                 getConfig('Configuration')['user'],
                                 self.is_prepared.name)

        dlls, pys, subpys = get_user_dlls(self, self.appname, self.version,
                                          self.user_release_area, self.platform,
                                          self.getenv(True))
        InstallArea = []

        for f in dlls:
            logger.debug("UserDLLs DLLs: %s" % expandfilename(f))
            InstallArea.append(File(name=expandfilename(f), subdir='lib'))
        for f in pys:
            tmp = f.split('InstallArea')[-1]
            subdir = 'InstallArea' + tmp[:tmp.rfind('/') + 1]
            logger.debug("UserDLLs PYS: %s" % expandfilename(f))
            InstallArea.append(File(name=expandfilename(f), subdir=subdir))

        for dir, files in subpys.items():
            for f in files:
                tmp = f.split('InstallArea')[-1]
                subdir = 'InstallArea' + tmp[:tmp.rfind('/') + 1]
                logger.debug("UserDLLs SUBPYS: %s" % expandfilename(f))
                InstallArea.append(File(name=expandfilename(f), subdir=subdir))

        # add the newly created shared directory into the metadata system if the app is associated with a persisted object
        # also call post_prepare for hashing
        # commented out here as inherrited from this class with extended
        # perpare

        fillPackedSandbox(InstallArea, os.path.join(share_dir, 'inputsandbox', '_input_sandbox_%s.tar' % self.is_prepared.name))

    def _register(self, force):
        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.' % (getName(self)))

        try:
            logger.info('Job %s: Preparing %s application.' % (stripProxy(self).getJobObject().getFQID('.'), getName(self)))
        except AssertionError as err:
            ## No Job associated with Object!!
            logger.info("Preparing %s application." % getName(self))
        self.is_prepared = ShareDir()

    def master_configure(self):
        '''Handles all common master_configure actions.'''
        raise NotImplementedError

    def configure(self, appmasterconfig):
        raise NotImplementedError


