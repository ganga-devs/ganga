
import unittest

def startGanga():

    ## Taken from  the ganga 'binary'
    def standardSetup():
        """Function to perform standard setup for Ganga.
        """
        import sys, os.path

        # insert the path to Ganga itself
        exeDir = os.path.abspath( "/afs/cern.ch/user/r/rcurrie/cmtuser/GANGA/GANGA_v600r99/install/ganga/bin" ) # which ganga

        gangaDir = "/afs/cern.ch/user/r/rcurrie/cmtuser/GANGA/GANGA_v600r99/install/ganga/python" #os.path.join( os.path.dirname(exeDir), 'python' )
        sys.path.insert(0, gangaDir)

        import Ganga.PACKAGE
        Ganga.PACKAGE.standardSetup()

    standardSetup()
    del standardSetup

    ## End taken from the ganga binary

    import Ganga.Runtime, sys
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()

    ## Start ganga by passing some options for unittesting

    print "\n"
    logger.info( "Starting ganga" )

    logger.info( "Parsing Command Line options" )
    import Ganga.Runtime, sys
    this_argv = sys.argv[1:]
    this_argv.append( '-o[Configuration]RUNTIME_PATH=GangaTest' )
    this_argv.append( '-o[Configuration]UsageMonitoringURL=""' )
    this_argv.append( '-o[Configuration]user=testframework' )
    this_argv.append( '-o[Configuration]gangadir=$HOME/gangadir_testing' )
    this_argv.append( '-o[Configuration]repositorytype=LocalXML' )
    this_argv.append( '-o[TestingFramework]ReleaseTesting=True' )

    ## FIXME Should we need to add the ability to load from a custom .ini file to configure tests without editting this?

    ## Actually parse the options
    Ganga.Runtime._prog = Ganga.Runtime.GangaProgram(argv=this_argv)
    Ganga.Runtime._prog.parseOptions()

    ## Determine if ganga has actually finished initializing...
    ## This is here to protect against the startGanga being called on an initialized ganga environment
    try:
        doConfig = not Ganga.Utility.Config.Config._after_bootstrap
    except:
        doConfig = True

    if doConfig:
        ## Perform the configuration and bootstrap steps in ganga
        logger.info( "Parsing Configuration Options" )
        Ganga.Runtime._prog.configure()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption( 'PollThread', 'forced_shutdown_policy', 'batch' )
        logger.info( "Initializing" )
        Ganga.Runtime._prog.initEnvironment()
        logger.info( "Bootstrapping" )
        Ganga.Runtime._prog.bootstrap()
    else:
        ## No need to perform the bootstrap but we need to test if the internal services need to be reinitialized
        from Ganga.Utility.Config import setConfigOption
        setConfigOption( 'PollThread', 'forced_shutdown_policy', 'batch' )
        from Ganga.Core.InternalServices import Coordinator
        if not Coordinator.servicesEnabled:
            ## Start internal services
            logger.info( "InternalServices restarting" )
            def testing_cb(t_total, critical_thread_ids, non_critical_thread_ids):
                return True
            from Ganga.Core.GangaThread import GangaThreadPool
            thread_pool = GangaThreadPool.getInstance()
            thread_pool.shutdown( should_wait_cb=testing_cb )
            from Ganga.GPI import reactivate
            reactivate()
        else:
            logger.info( "InternalServices still running" )

    ## Adapted from the Coordinator class, check for the required credentials and stop if not found
    ## Hopefully stops us falling over due to no AFS access of something similar
    from Ganga.Core.InternalServices import Coordinator
    missing_cred = Coordinator.getMissingCredentials()

    logger.info( "Checking Credentials" )

    if missing_cred:
        raise Exception( "Failed due to missing credentials %s" % str(missing_cred) )

    logger.info( "Passing to Unittest" )

def stopGanga():

    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()

    logger.info( "Deciding how to shutdown" )

    ## Do we want to empty the repository on shutdown?
    from Ganga.Utility.Config import getConfig
    if 'AutoCleanup' in getConfig( 'TestingFramework' ):
        wholeCleanup = getConfig( 'TestingFramework' )[ 'AutoCleanup' ]
    else:
        wholeCleanup = True

    if wholeCleanup:
        ## empty repository so we start again at job 0 when we restart
        logger.info( "Clearing the Job and Template repositories" )

        from Ganga.GPI import jobs, templates
        for j in jobs: j.remove()
        for t in templates: t.remove()
        if hasattr(jobs,'clean'):
            jobs.clean(confirm=True, force=True)
        if hasattr(templates,'clean'):
            templates.clean(confirm=True, force=True)

    logger.info( "Shutting Down Internal Services" )

    ## Disable internal services such as monitoring and other tasks
    from Ganga.Core.InternalServices import Coordinator
    if Coordinator.servicesEnabled:
        Coordinator.disableInternalServices()

    logger.info( "Mimicking ganga exit" )
    from Ganga.Core.InternalServices import ShutdownManager

    import Ganga.Core
    Ganga.Core.change_atexitPolicy( 'batch' )
    ## This should now be safe
    ShutdownManager._ganga_run_exitfuncs()

    ## Finished
    logger.info( "Test Finished" )



class GangaUnitTest(unittest.TestCase):

    def setUp(self):
        ## Start ganga and internal services
        ## This is called before each unittest
        startGanga()

    def tearDown(self):
        ## Stop ganga and mimick an exit to shutdown all internal processes
        stopGanga()

## Not sure if required but I think it is
if __name__ == "__main__":
    try:
        unittest.main()
    except:
        pass
