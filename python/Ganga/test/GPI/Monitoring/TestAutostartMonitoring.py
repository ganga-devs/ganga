from __future__ import absolute_import

import time

from Ganga.testlib.GangaUnitTest import GangaUnitTest

master_timeout = 300.

def dummySleep(someJob):
    my_timeout = 0.
    while someJob.status not in ['completed', 'failed', 'killed', 'removed'] and my_timeout < master_timeout:
        time.sleep(1.)
        my_timeout+=1.

    return

class TestAutostartMonitoring(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts=[('PollThread', 'autostart', 'True'), ('PollThread', 'base_poll_rate', 1)]
        super(TestAutostartMonitoring, self).setUp(extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        from Ganga.GPI import Job, jobs

        j = Job()

        self.assertEqual(len(jobs), 1)

        j.submit()

        self.assertNotEqual(j.status, 'new')
