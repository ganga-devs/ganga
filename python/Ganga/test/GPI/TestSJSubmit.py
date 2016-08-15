from __future__ import absolute_import
from Ganga.testlib.GangaUnitTest import GangaUnitTest

class TestSJSubmit(GangaUnitTest):

    n_subjobs = 10

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSJSubmit, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('Output', 'FailJobIfNoOutputMatched', 'True')
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
        setConfigOption('Configuration', 'resubmitOnlyFailedSubjobs', 'True')

    @staticmethod
    def _getSplitter():
        """
        Setup and return splitter
        """
        from Ganga.GPI import GenericSplitter
        splitter = GenericSplitter()
        splitter.attribute = 'application.args'
        splitter.values = [['1'] for _ in range(0, TestSJSubmit.n_subjobs)]
        return splitter

    def test_a_JobSubmission(self):
        """
        Create lots of subjobs and submit it
        """
        from Ganga.GPI import Job, Local
        j = Job()
        j.application.exe = "sleep"
        j.splitter = self._getSplitter()
        j.backend = Local()
        j.submit()

        # Test we can submit a job and we're going to check the sj are created

        assert len(j.subjobs) == TestSJSubmit.n_subjobs

    def test_b_SJCompleted(self):
        """
        Test the subjobs complete
        """
        from Ganga.GPI import jobs

        assert len(jobs) == 1
        assert len(jobs(0).subjobs) == TestSJSubmit.n_subjobs

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(0))

        for sj in jobs(0).subjobs:
            assert sj.status in ['completed']

        # Check that we can load the subjobs and that they are now in a complete status

    def test_c_SJResubmit_FailRequired(self):
        """
        Resubmit the subjobs when the required status is needed to do something
        """
        from Ganga.GPI import jobs

        jobs(0).resubmit()

        # Test that resubmitting a job with SubJobXMLList subjobs doesn't stall
        # Test them with all subjobs completed and resubmitOnlyFailedSubjobs = True

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(0))

        # Test that resubmitting a subjob from SubJobXMLList that subjob doesn't stall
        jobs(0).subjobs(0).resubmit()

        jobs(0).subjobs(0).force_status('failed', force=True)

        jobs(0).subjobs(0).resubmit()

        # Test that we can resubmit 1 subjob from the subjob itself when the subjob is failed
        # resubmitOnlyFailedSubjobs = True

        sleep_until_completed(jobs(0))

        assert jobs(0).subjobs(0).status == 'completed'

        jobs(0).subjobs(0).force_status('failed')

        jobs(0).resubmit()

        # Test that we can resubmit 1 subjob from the master job when the subjob is failed
        # resubmitOnlyFailedSubjobs = True

        sleep_until_completed(jobs(0))

        assert jobs(0).subjobs(0).status == 'completed'

    def test_d_SJResubmit_FailNotRequired(self):
        """
        Resubmit when jobs are not required to have failed 
        """
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('Configuration', 'resubmitOnlyFailedSubjobs', 'False')

        from Ganga.GPI import jobs

        # Test that resubmitting a job with SubJobXMLList subjobs doesn't stall
        # Test them with all subjobs completed and resubmitOnlyFailedSubjobs = False

        jobs(0).resubmit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(0))

        # Test that resubmitting a subjob from SubJobXMLList that subjob doesn't stall
        jobs(0).subjobs(0).resubmit()

        jobs(0).subjobs(0).force_status('failed', force=True)

        jobs(0).subjobs(0).resubmit()

        sleep_until_completed(jobs(0))

        assert jobs(0).subjobs(0).status == 'completed'

        # Test that we can resubmit 1 subjob from the subjob itself when the subjob is failed
        # resubmitOnlyFailedSubjobs = False

        jobs(0).subjobs(0).force_status('failed')

        jobs(0).resubmit()

        sleep_until_completed(jobs(0))

        assert jobs(0).subjobs(0).status == 'completed'

        # Test that we can resubmit 1 subjob from the master job when the subjob is failed
        # resubmitOnlyFailedSubjobs = False

    def test_e_testInMemory(self):
        """
        Test the resubmit on a job in memory vs a job which has been loaded from disk
        """
        from Ganga.GPI import Job, Local

        j=Job()
        j.splitter = self._getSplitter()
        j.backend = Local()
        j.submit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j)

        # Job has ben created, split, run and now exists in Memory (NOT SJXML)

        from Ganga.Utility.Config import setConfigOption
        setConfigOption('Configuration', 'resubmitOnlyFailedSubjobs', 'True')

        j.resubmit()

        sleep_until_completed(j)

        j.subjobs(0).resubmit()

        # We should get here if calling resubmit doesn't stall

        j.subjobs(0).force_status('failed', force=True)

        j.resubmit()

        sleep_until_completed(j)

        assert j.subjobs(0).status == 'completed'

        # Test resubmit from the master job worked

        j.subjobs(0).force_status('failed')

        j.subjobs(0).resubmit()

        sleep_until_completed(j)

        assert j.subjobs(0).status == 'completed'

        # Test that the resubmit from the subjob worked

        setConfigOption('Configuration', 'resubmitOnlyFailedSubjobs', 'False')

        j.resubmit()

        sleep_until_completed(j)

        j.subjobs(0).force_status('failed')

        j.resubmit()

        sleep_until_completed(j)

        # Test that this works when resubmitOnlyFailedSubjobs = False

