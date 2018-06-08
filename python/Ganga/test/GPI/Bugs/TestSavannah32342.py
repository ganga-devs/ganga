

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah32342(GangaUnitTest):

    def setUp(self):
        super(TestSavannah32342, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_Savannah32342(self):
        """Basic splitting test"""
        from Ganga.GPI import Job, ArgSplitter, jobs

        j = Job()
        j.splitter = ArgSplitter(args=[['A'], ['B']])
        j.submit()

        assert len(j.subjobs) == 2, 'Splitting must have occured'
        for jj in j.subjobs:
            assert jj._impl._getParent(), 'Parent must be set'

        # make sure we have out job in the repository
        job_seen = False
        for jj in jobs:
            if j is jj:
                job_seen = True
                break

        assert job_seen, 'Job must be in the repository'

    def test_b_Savannah32342(self):
        from Ganga.GPI import jobs
        
        j = jobs(0)

        assert j, 'job should not be null'
        assert len(j.subjobs) == 2, 'Splitting must have occured'
        for jj in j.subjobs:
            assert jj._impl._getParent(), 'Parent must be set'
