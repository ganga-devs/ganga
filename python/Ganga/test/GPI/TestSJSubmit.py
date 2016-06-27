from __future__ import absolute_import
from Ganga.testlib.GangaUnitTest import GangaUnitTest
import random
import string


class TestSJSubmit(GangaUnitTest):

    n_subjobs = 10

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSJSubmit, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('Output', 'FailJobIfNoOutputMatched', 'True')
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
	setConfigOption('Configuration', 'resubmitOnlyFailedSubjobs', 'True')

    def _getSplitter(self):
        """
        Setup and return splitter
        """
        from Ganga.GPI import GenericSplitter
        splitter = GenericSplitter()
        splitter.attribute = 'application.args'
        splitter.values = [['2'] for _ in range(0, TestSJSubmit.n_subjobs)]
        return splitter

    def test_a_LargeJobSubmission(self):
        """
        Create lots of subjobs and submit it
        """
        from Ganga.GPI import Job, Local
        j = Job()
        j.application.exe = "sleep"
        j.splitter = self._getSplitter()
        j.backend = Local()
        j.submit()

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

    def test_c_SJResubmit_FailRequired(self):
        """
        Resubmit the subjobs when the required status is needed to do something
        """
        from Ganga.GPI import jobs

        jobs(0).resubmit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(0))

	jobs(0).subjobs(0).force_status('failed')

	jobs(0).subjobs(0).resubmit()

        sleep_until_completed(jobs(0))

	jobs(0).subjobs(0).force_status('failed')

	jobs(0).resubmit()

        sleep_until_completed(jobs(0))

    def test_d_SJResubmit_FailNotRequired(self):
        """
        Resubmit when jobs are not required to have failed 
	"""
	from Ganga.Utility.Config import setConfigOption
	setConfigOption('Configuration', 'resubmitOnlyFailedSubjobs', 'False')

        from Ganga.GPI import jobs

        jobs(0).resubmit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(0))

        jobs(0).subjobs(0).force_status('failed')

        jobs(0).subjobs(0).resubmit()

        sleep_until_completed(jobs(0))

        jobs(0).subjobs(0).force_status('failed')

        jobs(0).resubmit()

        sleep_until_completed(jobs(0))

