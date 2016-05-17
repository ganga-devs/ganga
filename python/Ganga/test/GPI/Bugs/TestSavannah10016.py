from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah10016(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSavannah10016, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_TestJobDirs(self):
        from Ganga.GPI import Job

        j = Job()
        assert j.inputdir != ''
        assert j.outputdir != ''

    def test_b_RetestJobDirs(self):
        from Ganga.GPI import jobs

        j = jobs(0)
        assert j.inputdir != ''
        assert j.outputdir != ''