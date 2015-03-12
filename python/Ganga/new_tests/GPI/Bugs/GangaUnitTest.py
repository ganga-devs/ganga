
import unittest

def startGanga():

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

    import Ganga.Runtime, sys
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()

    logger.info( "Starting ganga" )

    logger.info( "Parsing Command Line options" )
    import Ganga.Runtime, sys
    this_argv = sys.argv[1:]
    #this_argv.append( '--test' )
    this_argv.append( '-o[Configuration]RUNTIME_PATH=GangaTest' )
    this_argv.append( '-o[Configuration]UsageMonitoringURL=""' )
    this_argv.append( '-o[Configuration]user=testframework' )
    this_argv.append( '-o[Configuration]gangadir=$HOME/gangadir_testing' )
    this_argv.append( '-o[Configuration]repositorytype=LocalXML' )
    this_argv.append( '-o[TestingFramework]ReleaseTesting=True' )

    Ganga.Runtime._prog = Ganga.Runtime.GangaProgram(argv=this_argv)
    Ganga.Runtime._prog.parseOptions()

    try:
        doConfig = not Ganga.Utility.Config.Config._after_bootstrap
    except:
        doConfig = True

    if doConfig:
        logger.info( "Parsing Configuration Options" )
        Ganga.Runtime._prog.configure()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption( 'PollThread', 'forced_shutdown_policy', 'batch' )
        logger.info( "Initializing" )
        Ganga.Runtime._prog.initEnvironment()
        logger.info( "Bootstrapping" )
        Ganga.Runtime._prog.bootstrap()
    else:
        from Ganga.Utility.Config import setConfigOption
        setConfigOption( 'PollThread', 'forced_shutdown_policy', 'batch' )
        from Ganga.Core.InternalServices import Coordinator
        if not Coordinator.servicesEnabled:
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

    from Ganga.Core.InternalServices import Coordinator
    missing_cred = Coordinator.getMissingCredentials()

    logger.info( "Checking Credentials" )

    if missing_cred:
        raise Exception( "Failed due to missing credentials %s" % str(missing_cred) )

    Ganga.Core.change_atexitPolicy( interactive_session=False, new_policy='batch' )

    logger.info( "Passing to Unittest" )


def stopGanga():

    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()

    logger.info( "Deciding how to shutdown" )

    from Ganga.Utility.Config import getConfig
    if 'AutoCleanup' in getConfig( 'TestingFramework' ):
        wholeCleanup = getConfig( 'TestingFramework' )[ 'AutoCleanup' ]
    else:
        wholeCleanup = True
    if wholeCleanup:

        logger.info( "Clearing the Job and Template repositories" )

        from Ganga.GPI import jobs, templates
        for j in jobs: j.remove()
        for t in templates: t.remove()
        if hasattr(jobs,'clean'):
            jobs.clean(confirm=True, force=True)
        if hasattr(templates,'clean'):
            templates.clean(confirm=True, force=True)

    logger.info( "Shutting Down Internal Services" )

    from Ganga.Core.InternalServices import Coordinator
    if Coordinator.servicesEnabled:
        Coordinator.disableInternalServices()

    logger.info( "Mimicking ganga exit" )
    import Ganga.Core

    def testing_cb(t_total, critical_thread_ids, non_critical_thread_ids):
        return True
    from Ganga.Core.GangaThread import GangaThreadPool
    thread_pool = GangaThreadPool.getInstance()
    thread_pool.shutdown( should_wait_cb=testing_cb )

    logger.info( "Test Finished" )

class GangaUnitTest(unittest.TestCase):

    def setUp(self):
        startGanga()

    def tearDown(self):
        stopGanga()

if __name__ == "__main__":
    try:
        unittest.main()
    except:
        pass
