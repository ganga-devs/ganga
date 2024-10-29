import time
from GangaCore.testlib.GangaUnitTest import GangaUnitTest

master_timeout = 300.

def dummySleep(someJob):
    my_timeout = 0.
    while someJob.status not in ['completed', 'failed', 'killed', 'removed'] and my_timeout < master_timeout:
        time.sleep(1.)
        my_timeout += 1.

class TestMonitoring(GangaUnitTest):

    def setUp(self):
        """Set up configurations for monitoring tests."""
        extra_opts = [('PollThread', 'autostart', 'False'), ('PollThread', 'base_poll_rate', 1)]
        super(TestMonitoring, self).setUp(extra_opts=extra_opts)

    def tearDown(self):
        super(TestMonitoring, self).tearDown()

    def test_a_runMonitoring_withJobSlice(self):
        from GangaCore.GPI import enableMonitoring, Job, jobs, runMonitoring
        
        enableMonitoring()
        j = Job()
        j.submit()
        dummySleep(j)

        result = runMonitoring(steps=3, jobs=jobs[:])
        self.assertTrue(result, "runMonitoring with job slice failed to execute successfully.")

    def test_b_runMonitoring_withJobID(self):
        from GangaCore.GPI import enableMonitoring, Job, jobs, runMonitoring

        enableMonitoring()
        j = Job()
        j.submit()
        dummySleep(j)

        job_id = j.id
        result = runMonitoring(steps=3, jobs=job_id)
        self.assertTrue(result, "runMonitoring with job ID failed to execute successfully.")

    def test_c_runMonitoring_withJobIDList(self):
        from GangaCore.GPI import enableMonitoring, Job, runMonitoring

        enableMonitoring()
        job_ids = []
        for _ in range(2):
            j = Job()
            j.submit()
            dummySleep(j)
            job_ids.append(j.id)
        
        result = runMonitoring(steps=3, jobs=job_ids)
        self.assertTrue(result, "runMonitoring with list of job IDs failed to execute successfully.")

    def test_d_runMonitoring_withJobObject(self):
        from GangaCore.GPI import enableMonitoring, Job, runMonitoring

        enableMonitoring()
        j = Job()
        j.submit()
        dummySleep(j)

        result = runMonitoring(steps=3, jobs=j)
        self.assertTrue(result, "runMonitoring with job object failed to execute successfully.")

    def test_e_disableAndEnableMonitoring(self):
        from GangaCore.GPI import disableMonitoring, enableMonitoring, Job, runMonitoring

        disableMonitoring()
        enableMonitoring()
        
        j = Job()
        j.submit()
        dummySleep(j)

        result = runMonitoring(steps=3, jobs=j)
        self.assertTrue(result, "Re-enabling monitoring and running on a job object failed.")

    def test_f_monitoringLoopStatus(self):
        from GangaCore.GPI import disableMonitoring, enableMonitoring, Job, runMonitoring

        disableMonitoring()
        j = Job()
        j.submit()
        self.assertEqual(j.status, 'submitted')

        enableMonitoring()
        dummySleep(j)

        result = runMonitoring(steps=3, jobs=j)
        self.assertTrue(result, "Final monitoring loop status check failed.")
