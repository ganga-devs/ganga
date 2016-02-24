from __future__ import print_function
try:
    import unittest2 as unittest
except ImportError:
    import unittest


def start_ganga(gangadir_for_test='$HOME/gangadir_testing', extra_opts=[]):

    import sys
    import os.path


    file_path = os.path.dirname(os.path.realpath(__file__))
    ganga_python_dir = os.path.join(file_path, '..', '..', '..')
    ganga_python_dir = os.path.realpath(ganga_python_dir)
    if len(sys.path) >= 1 and ganga_python_dir != sys.path[0]:
        sys.path.insert(0, ganga_python_dir)

        print("Adding: %s to Python Path\n" % ganga_python_dir)

    import Ganga.PACKAGE
    Ganga.PACKAGE.standardSetup()

    # End taken from the ganga binary

    import Ganga.Runtime
    from Ganga.Utility.logging import getLogger
    logger = getLogger()

    # Start ganga by passing some options for unittesting

    logger.info("Starting ganga")

    logger.info("Parsing Command Line options")
    import Ganga.Runtime
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
        ('TestingFramework', 'ReleaseTesting', True),
        ('Queues', 'NumWorkerThreads', 2),
    ]

    # FIXME Should we need to add the ability to load from a custom .ini file
    # to configure tests without editting this?

    # Actually parse the options
    Ganga.Runtime._prog = Ganga.Runtime.GangaProgram(argv=this_argv)
    Ganga.Runtime._prog.parseOptions()

    # Determine if ganga has actually finished initializing...
    # This is here to protect against the startGanga being called on an
    # initialized ganga environment
    try:
        do_config = not Ganga.Utility.Config.Config._after_bootstrap
    except:
        do_config = True

    # For all the default and extra options, we set the session value
    from Ganga.Utility.Config import setConfigOption
    for opt in default_opts + extra_opts:
        setConfigOption(*opt)

    if do_config:
        # Perform the configuration and bootstrap steps in ganga
        logger.info("Parsing Configuration Options")
        Ganga.Runtime._prog.configure()
        logger.info("Initializing")
        Ganga.Runtime._prog.initEnvironment(opt_rexec=False)
    else:
        # We need to test if the internal services need to be reinitialized
        from Ganga.Core.InternalServices import Coordinator
        if not Coordinator.servicesEnabled:
            # Start internal services
            logger.info("InternalServices restarting")

            from Ganga.GPI import reactivate
            reactivate()
        else:
            logger.info("InternalServices still running")

        # The queues are shut down by the atexit handlers so we need to start them here
        from Ganga.Core.GangaThread.WorkerThreads import startUpQueues
        startUpQueues()

    logger.info("Bootstrapping")
    Ganga.Runtime._prog.bootstrap(interactive=False)

    # Adapted from the Coordinator class, check for the required credentials and stop if not found
    # Hopefully stops us falling over due to no AFS access of something similar
    from Ganga.Core.InternalServices import Coordinator
    missing_cred = Coordinator.getMissingCredentials()

    logger.info("Checking Credentials")

    if missing_cred:
        raise Exception("Failed due to missing credentials %s" % str(missing_cred))

    logger.info("Passing to Unittest")


def stop_ganga():

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
        # empty repository so we start again at job 0 when we restart
        logger.info("Clearing the Job and Template repositories")

        from Ganga.GPI import jobs, templates
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
        if hasattr(jobs, 'clean'):
            jobs.clean(confirm=True, force=True)
        if hasattr(templates, 'clean'):
            templates.clean(confirm=True, force=True)

    logger.info("Shutting Down Internal Services")

    # Disable internal services such as monitoring and other tasks
    #from Ganga.Core.InternalServices import Coordinator
    # if Coordinator.servicesEnabled:
    #    Coordinator.disableInternalServices()
    #    Coordinator.servicesEnabled = False

    logger.info("Mimicking ganga exit")
    from Ganga.Core.InternalServices import ShutdownManager

    import Ganga.Core
    Ganga.Core.change_atexitPolicy(interactive_session=False, new_policy='batch')
    # This should now be safe
    ShutdownManager._ganga_run_exitfuncs()

    # Undo any manual editing of the config and revert to defaults
    from Ganga.Utility.Config import allConfigs
    for package in allConfigs.values():
        package.revertToDefaultOptions()

    # Finished
    logger.info("Test Finished")


class GangaUnitTest(unittest.TestCase):

    wipe_repo = None
    gangadir = None

    def setUp(self, gangadir=None, wipe_repo=None, extra_opts=[]):
        unittest.TestCase.setUp(self)
        # Start ganga and internal services
        # This is called before each unittest
        if gangadir is None:
            import os
            gangadir = os.path.join('$HOME/gangadir_testing')  # TODO make separate dirs per test suite, `self.__class__.__name__`
            gangadir = os.path.expanduser(os.path.expandvars(gangadir))
            if not os.path.isdir(gangadir):
                os.makedirs(gangadir)
        if wipe_repo is None:
            self.wipe_repo=True
        else:
            self.wipe_repo=wipe_repo
        self.gangadir = gangadir
        self.__class__.gangadir = self.gangadir
        self.__class__.wipe_repo = self.wipe_repo
        start_ganga(gangadir_for_test=gangadir, extra_opts=extra_opts)

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        # Stop ganga and mimick an exit to shutdown all internal processes
        stop_ganga()
        import sys
        sys.stdout.flush()

    @classmethod
    def tearDownClass(cls):
        if cls.wipe_repo:
            import shutil
            shutil.rmtree(cls.gangadir, ignore_errors=True)

