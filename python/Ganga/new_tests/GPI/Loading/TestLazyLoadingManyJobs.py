from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

numJobs = 10

class TestLazyLoadingManyJobs(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestLazyLoadingManyJobs, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs
        for _ in range(numJobs):
            j=Job()
        self.assertEqual(len(jobs), numJobs) # Don't really gain anything from assertEqual...

        jobs(9).submit()

    def test_b_JobNotLoaded(self):
        """ Second get the job and check that getting it via jobs doesn't cause it to be loaded"""
        from Ganga.GPI import jobs

        from Ganga.GPIDev.Base.Proxy import stripProxy

        from Ganga.GPIDev.Lib.Job.Job import lazyLoadJobApplication, lazyLoadJobBackend, lazyLoadJobStatus, lazyLoadJobFQID

        self.assertEqual(len(jobs), numJobs)

        print("len: %s" % len(jobs))

        for j in jobs:

            if j.id != 9:
                raw_j = stripProxy(j)

                has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

                self.assertFalse(has_loaded_job)

        for j in jobs:

            if j.id != 9:
                raw_j = stripProxy(j)

                print("job: %s status = %s" % (j.id, j.status))

                app = lazyLoadJobApplication(raw_j) 
                back = lazyLoadJobBackend(raw_j)
                stat = lazyLoadJobStatus(raw_j)
                fq = lazyLoadJobFQID(raw_j)

                has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

                self.assertFalse(has_loaded_job)

    def test_c_JobLoaded(self):
        """ Third do something to trigger a loading of a Job and then test if it's loaded"""
        from Ganga.GPI import jobs

        self.assertEqual(len(jobs), numJobs)

        j = jobs(0)

        from Ganga.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        ## ANY COMMAND TO LOAD A JOB CAN BE USED HERE
        raw_j.printSummaryTree()

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        self.assertTrue(has_loaded_job)

    def test_d_JobRemoval(self):
        """ Fourth make sure that we get rid of the jobs safely"""
        from Ganga.GPI import jobs

        self.assertEqual(len(jobs), numJobs)

        for j in jobs:
            j.remove()

        self.assertEqual(len(jobs), 0)

        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'True')

