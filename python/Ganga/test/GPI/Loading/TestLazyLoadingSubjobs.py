from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

global_subjob_num = 5
default_CleanUp = None

class TestLazyLoadingSubjobs(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestLazyLoadingSubjobs, self).setUp()
        from Ganga.Utility.Config import getConfig
        default_CleanUp = getConfig('TestingFramework')['AutoCleanup']
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs, ArgSplitter
        j=Job()
        self.assertEqual(len(jobs), 1) # Don't really gain anything from assertEqual...

        j.splitter = ArgSplitter(args=[[i] for i in range(global_subjob_num)])
        j.submit()

        self.assertEqual(len(j.subjobs), global_subjob_num)
        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j, 60)

    def test_b_JobNotLoaded(self):
        """ Second get the job and check that getting it via jobs doesn't cause it to be loaded"""
        from Ganga.GPI import jobs

        self.assertEqual(len(jobs), 1)

        print("len: %s" % str(len(jobs)))

        j = jobs(0)

        from Ganga.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        self.assertFalse(has_loaded_job)

    def test_c_JobLoaded(self):
        """ Third do something to trigger a loading of a Job and then test if it's loaded"""
        from Ganga.GPI import jobs

        self.assertEqual(len(jobs), 1)

        j = jobs(0)

        from Ganga.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        ## ANY COMMAND TO LOAD A JOB CAN BE USED HERE
        raw_j.printSummaryTree()

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        for i in range(len(j.subjobs)):
            self.assertFalse(raw_j.subjobs.isLoaded(i))

        self.assertTrue(has_loaded_job)

        stripProxy(j.subjobs(0)).printSummaryTree()

        self.assertTrue(raw_j.subjobs.isLoaded(0))

        for i in range(1, len(j.subjobs)):
            self.assertFalse(raw_j.subjobs.isLoaded(i))


    def test_d_JobRemoval(self):
        """ Fourth make sure that we get rid of the jobs safely"""
        from Ganga.GPI import jobs

        self.assertEqual(len(jobs), 1)

        jobs(0).remove()

        self.assertEqual(len(jobs), 0)

        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', default_CleanUp)

