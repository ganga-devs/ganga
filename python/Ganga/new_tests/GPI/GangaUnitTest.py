try:
    import unittest2 as unittest
except ImportError:
    import unittest


def start_ganga():

    import sys
    import os.path

    try:
        ganga_sys_root = os.environ.get('GANGASYSROOT')
    except Exception, err:
        print "Exception Raised finding GANGASYSROOT,\n\tPLEASE DEFINE THIS IN YOUR ENVIRONMENT TO RUN THE TESTS\n"
        raise err

    if ganga_sys_root is None:
        raise Exception(
            "GANGASYSROOT evaluated to None, please check Ganga setup")

    python_rel_path = 'python'

    ganga_dir = os.path.abspath(os.path.join(ganga_sys_root, python_rel_path))

    if not os.path.isdir(ganga_dir):
        python_rel_path = '../install/ganga/python'
        ganga_dir = os.path.abspath(os.path.join(ganga_sys_root, python_rel_path))

    print "Adding: %s to Python Path" % ganga_dir
    sys.path.insert(0, ganga_dir)

    import Ganga.PACKAGE
    Ganga.PACKAGE.standardSetup()

    # End taken from the ganga binary

    import Ganga.Runtime
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()

    # Start ganga by passing some options for unittesting

    print "\n"
    logger.info("Starting ganga")

    logger.info("Parsing Command Line options")
    import Ganga.Runtime
    this_argv = [
        'ganga',  # `argv[0]` is usually the name of the program so fake that here
        '-o[Configuration]RUNTIME_PATH=GangaTest',
        '-o[Configuration]user=testframework',
        '-o[Configuration]gangadir=$HOME/gangadir_testing',
        '-o[Configuration]repositorytype=LocalXML',
        '-o[PollThread]autostart_monThreads=False',
        '-o[TestingFramework]ReleaseTesting=True',
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

    logger.info("Bootstrapping")
    Ganga.Runtime._prog.bootstrap(interactive=False)

    # [PollThread]autostart_monThreads=False has turned this off being done automatically.
    # The thread pool is emptied by _ganga_run_exitfuncs
    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import _makeThreadPool
    _makeThreadPool()

    # Adapted from the Coordinator class, check for the required credentials and stop if not found
    # Hopefully stops us falling over due to no AFS access of something similar
    from Ganga.Core.InternalServices import Coordinator
    missing_cred = Coordinator.getMissingCredentials()

    logger.info("Checking Credentials")

    if missing_cred:
        raise Exception("Failed due to missing credentials %s" %
                        str(missing_cred))

    logger.info("Passing to Unittest")


def stop_ganga():

    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()

    logger.info("Deciding how to shutdown")

    # Do we want to empty the repository on shutdown?
    from Ganga.Utility.Config import getConfig
    if 'AutoCleanup' in getConfig('TestingFramework'):
        whole_cleanup = getConfig('TestingFramework')['AutoCleanup']
    else:
        whole_cleanup = True

    if whole_cleanup:
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

    # Finished
    logger.info("Test Finished")


class GangaUnitTest(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        # Start ganga and internal services
        # This is called before each unittest
        start_ganga()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        # Stop ganga and mimick an exit to shutdown all internal processes
        stop_ganga()
