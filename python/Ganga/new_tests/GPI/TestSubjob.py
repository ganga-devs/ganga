from GangaUnitTest import GangaUnitTest
import stacktracer
stacktracer.trace_start("/home/mws/Ganga/install/ganga/stacktracer.html")

class TestSubjobs(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSubjobs, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def testLargeJobSubmission(self):
        """
        Create lots of subjobs and submit it
        """
        from Ganga.GPI import Job, GenericSplitter, Local
        j = Job()
        j.application.exe = "sleep"
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [ ['400'] for i in range(0, 100) ]
        j.backend = Local()
        j.submit()

        self.assertEqual( len(j.subjobs), 100 )

    def testSetParentOnLoad(self):
        """
        Test that the parents are set correctly on load
        """
        from Ganga.GPI import jobs, queues, Executable, Local
        from Ganga.GPIDev.Base.Proxy import isType

        def flush_full_job():
            j = jobs(0)
            j._impl._setDirty()
            j._impl._getRegistry()._flush([j])

        # fire off a load of threads to flush
        for i in range(0, 100):
            queues.add( flush_full_job )

        # Now loop over and force the load of all the subjobs
        j = jobs(0)
        for sj in j.subjobs:
            assert sj.splitter == None
            assert isType(sj.application, Executable)
            assert isType(sj.backend, Local)
            assert sj.application.exe == "sleep"
            assert sj.application.args == ['400']
            assert sj._impl._getRoot() is j._impl

