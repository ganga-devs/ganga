from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah9779(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSavannah9779, self).setUp()
        from GangaCore.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_CreateJob(self):
        from GangaCore.GPI import jobs, Job, Executable, Local

        jobs.remove()
        j = Job()
        j.application = Executable()
        j.backend = Local()

    def test_b_ModifyJob(self):
        from GangaCore.GPI import jobs, Batch

        j = jobs(0)
        j.backend = Batch()
        j.application.exe = 'myexecutable'
        self.check_job(j)

    def test_c_CheckJob(self):
        from GangaCore.GPI import jobs

        j = jobs(0)
        self.check_job(j)

    def check_job(self, j):
        from GangaCore.GPI import Executable, Batch

        assert isinstance(j.application, Executable)
        assert j.application.exe == 'myexecutable'
        assert isinstance(j.backend, Batch)
