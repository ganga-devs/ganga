from __future__ import print_function

import sys
import shutil
import os.path
try:
    import unittest2 as unittest
except ImportError:
    import unittest

ganga_test_dir_name = 'gangadir testing'
ganga_config_file = '/not/a/file'

def _getGangaPath():
    """
    Determine what the path of the Ganga code is based upon where this file is in the repo
    """
    file_path = os.path.dirname(os.path.realpath(__file__))
    ganga_python_dir = os.path.join(file_path, '..', '..')
    ganga_python_dir = os.path.realpath(ganga_python_dir)
    return ganga_python_dir


def _setupGangaPath():
    """
    Add the path of the Ganga to the PYTHONDIR upon import
    """
    ganga_python_dir = _getGangaPath()
    if len(sys.path) >= 1 and ganga_python_dir != sys.path[0]:
        sys.path.insert(0, ganga_python_dir)

        print("Adding: %s to Python Path\n" % ganga_python_dir)


def load_config_files():
    """
    Load the config files as a normal Ganga session would, taking
    into account environment variables etc.
    """
    from Ganga.Utility.Config import getConfig, setSessionValuesFromFiles
    from Ganga.Runtime import GangaProgram
    system_vars = {}
    for opt in getConfig('System'):
        system_vars[opt] = getConfig('System')[opt]
    config_files = GangaProgram.get_config_files(ganga_config_file)
    setSessionValuesFromFiles(config_files, system_vars)


def clear_config():
    """
    Reset all the configs back to their default values
    """
    from Ganga.Utility.Config import allConfigs
    for package in allConfigs.values():
        package._user_handlers = []
        package._session_handlers = []
        package.revertToDefaultOptions()

_setupGangaPath()


def start_ganga(gangadir_for_test, extra_opts=[]):
    """
    Startup Ganga by calling the same set of 'safe' functions each time
    Args:
        gangadir_for_test (str): This is the directory which the GangaUnitTest is to be run, a new gangadir has been created per test to avoid collisions
        extra_opts (list): A list of tuples which are used to pass command line style options to Ganga
    """

    import Ganga.PACKAGE
    Ganga.PACKAGE.standardSetup()

    # End taken from the ganga binary

    import Ganga.Runtime
    from Ganga.Utility.logging import getLogger
    logger = getLogger()

    # Start ganga by passing some options for unittesting

    logger.info("Starting ganga")

    logger.info("Parsing Command Line options")
    this_argv = [
        'ganga',  # `argv[0]` is usually the name of the program so fake that here
    ]

    # These are the default options for all test instances
    # They can be overridden by extra_opts
    default_opts = [
        ('Configuration', 'RUNTIME_PATH', 'GangaTest'),
        ('Configuration', 'gangadir', gangadir_for_test),
        ('Configuration', 'user', 'testframework'),
        ('Configuration', 'repositorytype', 'LocalXML'),
        ('Configuration', 'UsageMonitoringMSG', False),  # Turn off spyware
        ('TestingFramework', 'ReleaseTesting', True),
        ('Queues', 'NumWorkerThreads', 2),
    ]

    # FIXME Should we need to add the ability to load from a custom .ini file
    # to configure tests without editting this?

    # Actually parse the options
    Ganga.Runtime._prog = Ganga.Runtime.GangaProgram(argv=this_argv)
    Ganga.Runtime._prog.default_config_file = ganga_config_file
    Ganga.Runtime._prog.parseOptions()

    # For all the default and extra options, we set the session value
    from Ganga.Utility.Config import setConfigOption
    for opt in default_opts + extra_opts:
        setConfigOption(*opt)

    # The configuration is currently created at module import and hence can't be
    # regenerated.
    # The values read in from any .ini file or from command line will change this
    # but the configuration can't be obliterated and re-created. (yet, 16.06.16)

    # Perform the configuration and bootstrap steps in ganga
    logger.info("Parsing Configuration Options")
    Ganga.Runtime._prog.configure()

    logger.info("Initializing")
    Ganga.Runtime._prog.initEnvironment()

    logger.info("Bootstrapping")
    Ganga.Runtime._prog.bootstrap(interactive=False)

    # We need to test if the internal services need to be reinitialized
    from Ganga.Core.InternalServices import Coordinator
    if not Coordinator.servicesEnabled:
        # Start internal services
        logger.info("InternalServices restarting")

        from Ganga.Core.InternalServices.Coordinator import enableInternalServices
        enableInternalServices()
    else:
        logger.info("InternalServices still running")

    # Adapted from the Coordinator class, check for the required credentials and stop if not found
    # Hopefully stops us falling over due to no AFS access of something similar
    from Ganga.Core.InternalServices import Coordinator
    missing_cred = Coordinator.getMissingCredentials()

    logger.info("Checking Credentials")

    if missing_cred:
        raise Exception("Failed due to missing credentials %s" % str(missing_cred))

    # Make sure that all the config options are really set.
    # Some from plugins may not have taken during startup
    for opt in default_opts + extra_opts:
        setConfigOption(*opt)

    logger.info("Passing to Unittest")


def emptyRepositories():
    """
    A method which attempts to remove jobs from various repositories in a sane manner,
    This is preferred to just shutting down and runnning rm -fr ... as it catches a few errors hard to test for
    """
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    # empty repository so we start again at job 0 when we restart
    logger.info("Clearing the Job and Template repositories")

    from Ganga.GPI import jobs, templates, tasks
    for j in jobs:
        try:
            j.remove()
        except:
            pass
    for t in templates:
        try:
            t.remove()
        except:
            pass
    for t in tasks:
        try:
            t.remove(remove_jobs=True)
        except:
            pass
    if hasattr(jobs, 'clean'):
        jobs.clean(confirm=True, force=True)
    if hasattr(templates, 'clean'):
        templates.clean(confirm=True, force=True)
    if hasattr(tasks, 'clean'):
        tasks.clean(confirm=True, force=True)


def stop_ganga():
    """
    This test stops Ganga and shuts it down

    Most of the logic is weapped in ShutdownManager._ganga_run_exitfuncs but additional code is used to cleanup repos and such between tests
    """

    from Ganga.Utility.logging import getLogger
    logger = getLogger()

    logger.info("Deciding how to shutdown")

    # Do we want to empty the repository on shutdown?
    from Ganga.Utility.Config import getConfig
    if 'AutoCleanup' in getConfig('TestingFramework'):
        whole_cleanup = getConfig('TestingFramework')['AutoCleanup']
    else:
        whole_cleanup = True
    logger.info("AutoCleanup: %s" % whole_cleanup)

    if whole_cleanup is True:
        emptyRepositories()

    logger.info("Shutting Down Internal Services")

    # Disable internal services such as monitoring and other tasks
    #from Ganga.Core.InternalServices import Coordinator
    # if Coordinator.servicesEnabled:
    #    Coordinator.disableInternalServices()
    #    Coordinator.servicesEnabled = False

    logger.info("Mimicking ganga exit")
    from Ganga.Core.InternalServices import ShutdownManager

    # make sure we don't have an interactive shutdown policy
    from Ganga.Core.GangaThread import GangaThreadPool
    GangaThreadPool.shutdown_policy = 'batch'

    # This should now be safe
    ShutdownManager._ganga_run_exitfuncs()

    # Undo any manual editing of the config and revert to defaults
    clear_config()

    # Finished
    logger.info("Test Finished")


class GangaUnitTest(unittest.TestCase):
    """
    This class is the class which all new-style Ganga tests should inherit from
    """

    @classmethod
    def gangadir(cls):
        """
        Return the directory that this test should store its registry and repository in
        """
        return os.path.join(_getGangaPath(), ganga_test_dir_name, cls.__name__)

    @classmethod
    def setUpClass(cls):
        """
        This removes all trace of any previous tests on disk
        TODO, would it be better to move the folder first, then remove it incase of broken locks etc,?
        """
        shutil.rmtree(cls.gangadir(), ignore_errors=True)

    def setUp(self, extra_opts=[]):
        """
        Setup the unit test which is about to run
        Args:
            extra_opts (list): This is a list of tuples which are similar to command line arguments passed to Ganga
        """
        unittest.TestCase.setUp(self)
        # Start ganga and internal services
        # This is called before each unittest

        gangadir = self.gangadir()
        if not os.path.isdir(gangadir):
            os.makedirs(gangadir)
        print("\n") # useful when watching stdout from tests
        print("Starting Ganga in: %s" % gangadir)
        start_ganga(gangadir_for_test=gangadir, extra_opts=extra_opts)

    def tearDown(self):
        """
        This tears down Ganga in a nice way at the end of each test
        """
        unittest.TestCase.tearDown(self)
        # Stop ganga and mimick an exit to shutdown all internal processes
        stop_ganga()
        sys.stdout.flush()

    @classmethod
    def tearDownClass(cls):
        """
        This is used for cleaning up anything at a module level of higher
        """
        # NB maybe we shouldn't delete tests here as failed tests require debugging
        #    this is better cleaned prior to running the next job
        #shutil.rmtree(cls.gangadir(), ignore_errors=True)
