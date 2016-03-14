from GangaUnitTest import GangaUnitTest

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
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        import random
        import string

        def flush_full_job():
            j = jobs(0)
            j.comment = "Make sure I'm dirty " + ''.join( random.choice( string.ascii_uppercase) for _ in range(5))
            stripProxy(j)._getRegistry()._flush([j])

        # Make sure the main job is fully loaded
        j = jobs(0)
        self.assertTrue( isType(j.application, Executable) )
        self.assertTrue( isType(j.backend, Local) )
        self.assertEqual( j.application.exe, "sleep" )

        # fire off a load of threads to flush
        for i in range(0, 100):
            queues.add( flush_full_job )

        # Now loop over and force the load of all the subjobs
        for sj in j.subjobs:
            self.assertEqual( sj.splitter, None )
            self.assertTrue( isType(sj.application, Executable) )
            self.assertTrue( isType(sj.backend, Local) )
            self.assertEqual( sj.application.exe, "sleep" )
            self.assertEqual( sj.application.args, ['400'] )
            self.assertIs( stripProxy(sj)._getRoot(), stripProxy(j))


