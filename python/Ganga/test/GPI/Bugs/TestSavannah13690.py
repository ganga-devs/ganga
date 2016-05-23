from Ganga.testlib.GangaUnitTest import GangaUnitTest

class TestSavannah13690(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts=[('PollThread', 'base_poll_rate', 1)]
        super(TestSavannah13690, self).setUp(extra_opts=extra_opts)

    def test_SubmitAndRemoval(self):
        from Ganga.GPI import Job, TestSubmitter
        
        j = Job()
        j.backend = TestSubmitter(time=1, update_delay=5)
        j.submit()
        all = [j]

        for i in range(10):
            j2 = j.copy()
            j2.submit()
            all.append(j2)

        for j in all:
            j.remove()

