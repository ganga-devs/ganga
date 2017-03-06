from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestAutostartMonitoring(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts=[('PollThread', 'autostart', 'True'), ('PollThread', 'base_poll_rate', 1)]
        super(TestAutostartMonitoring, self).setUp(extra_opts=extra_opts)

    def test_a_JobConstruction(self):
        from Ganga.GPI import Job, jobs

        j = Job()
        assert len(jobs) == 1

        j.submit()
        assert j.status != 'new'
