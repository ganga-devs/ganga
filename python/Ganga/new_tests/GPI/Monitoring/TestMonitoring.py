from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

timeForComplete = 10.

class TestMonitoring(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts=[('PollThread', 'autostart', 'False')]
        super(TestMonitoring, self).setUp(extra_opts=extra_opts)

    def tearDown(self):
        from Ganga.Utility.Config import getConfig
        getConfig('PollThread').getOption('autostart').revertToDefault()
        super(TestMonitoring, self).tearDown()

    def test_a_JobConstruction(self):
        from Ganga.GPI import Job, jobs

        j=Job()

        self.assertEqual(len(jobs), 1)

        j.submit()

        self.assertEqual(j.status, 'submitted')

    def test_b_EnableMonitoring(self):
        """ Fourth make sure that we get rid of the jobs safely"""
        from Ganga.GPI import enableMonitoring, Job, jobs

        enableMonitoring()

        j=Job()
        j.submit()

        import time
        time.sleep(20.)

        self.assertEqual(jobs(0).status, 'completed')

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

        import time
        time.sleep(timeForComplete)

        self.assertEqual(j.status, 'completed')

