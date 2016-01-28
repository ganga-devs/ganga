from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

class LazyLoadingTest(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(LazyLoadingTest, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs
        j=Job()
        self.assertTrue(len(jobs) == 1)

    def test_b_JobNotLoaded(self):
        """ Second get the job and check that getting it via jobs doesn't cause it to be loaded"""
        from Ganga.GPI import jobs

        self.assertTrue(len(jobs) == 1)

        print("len: %s" % str(len(jobs)))

        j = jobs(0)

        from Ganga.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        self.assertTrue(has_loaded_job == False)

    def test_c_JobLoaded(self):
        """ Third do something to trigger a loading of a Job and then test if it's loaded"""
        from Ganga.GPI import jobs

        self.assertTrue(len(jobs) == 1)

        j = jobs(0)

        from Ganga.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        ## ANY COMMAND TO LOAD A JOB CAN BE USED HERE
        raw_j.printSummaryTree()

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        self.assertTrue(has_loaded_job == True)

    def test_d_JobRemoval(self):
        """ Fourth make sure that we get rid of the jobs safely"""
        from Ganga.GPI import jobs

        self.assertTrue(len(jobs) == 1)

        jobs(0).remove()

        self.assertTrue(len(jobs) == 0)

        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'True')

