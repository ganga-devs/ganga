from __future__ import absolute_import

import time

from ..GangaUnitTest import GangaUnitTest

master_timeout = 300.

def dummySleep(someJob):
    my_timeout = 0.
    while someJob.status not in ['completed', 'failed', 'killed', 'removed'] and my_timeout < master_timeout:
        time.sleep(1.)
        my_timeout+=1.

    return

class TestMonitoring(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts=[('PollThread', 'autostart', 'False'), ('PollThread', 'base_poll_rate', 1)]
        super(TestMonitoring, self).setUp(extra_opts=extra_opts)

    def tearDown(self):
        from Ganga.Utility.Config import getConfig
        super(TestMonitoring, self).tearDown()

    def test_a_JobConstruction(self):
        from Ganga.GPI import Job, jobs, disableMonitoring

        j=Job()

        self.assertEqual(len(jobs), 1)

        j.submit()

        self.assertNotEqual(j.status, 'new')

    def test_b_EnableMonitoring(self):
        from Ganga.GPI import enableMonitoring, Job, jobs

        enableMonitoring()

        j=Job()
        j.submit()

        dummySleep(j)

        self.assertNotEqual(jobs(0).status, 'submitted')

    def test_c_disableMonitoring(self):

        from Ganga.GPI import disableMonitoring

        disableMonitoring()


    def test_d_anotherNewJob(self):

        from Ganga.GPI import Job, jobs

        j=Job()

        j.submit()
        self.assertNotEqual(j.status, 'new')

    def test_e_reEnableMon(self):

        from Ganga.GPI import disableMonitoring, enableMonitoring, Job, jobs

        disableMonitoring()
        enableMonitoring()
        disableMonitoring()
        enableMonitoring()

        j=Job()
        j.submit()

        dummySleep(j)

        self.assertEqual(j.status, 'completed')


    def test_f_reallyDisabled(self):

        from Ganga.GPI import disableMonitoring, enableMonitoring, Job

        disableMonitoring()
        j=Job()
        j.submit()

        time.sleep(20.)

        self.assertEqual(j.status, 'submitted')

        enableMonitoring()

        dummySleep(j)

        self.assertEqual(j.status, 'completed')

