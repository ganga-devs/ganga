from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah9713(GangaUnitTest):

    def setUp(self):
        super(TestSavannah9713, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_Savannah9713(self):
        from Ganga.GPI import JobTemplate
        t = JobTemplate()
        assert t.status == 'template'

    def test_b_Savannah9713(self):
        from Ganga.GPI import templates

        # Check the created template is still present
        t = templates(0)
        assert t.status == 'template'
