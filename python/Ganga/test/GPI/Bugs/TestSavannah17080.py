from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.Core.exceptions import IncompleteJobSubmissionError
import pytest

class TestSavannah17080(GangaUnitTest):
    # These tests for failures during submission

    def test_CondorConfigDefaults(self):
        # A test with sequential submission
        from Ganga.GPI import Job, TestSplitter, TestSubmitter

        j = Job()
        j.splitter = TestSplitter()
        j.splitter.backs = [TestSubmitter(),TestSubmitter(),TestSubmitter()]
        j.backend= TestSubmitter()
        b = j.splitter.backs[1]
        b.fail = 'submit'
        j.parallel_submit = False

        assert j.status == 'new'

        with pytest.raises(IncompleteJobSubmissionError):
            j.submit(keep_going=True)
        assert j.subjobs[0].status in ['submitted', 'running']

        assert j.subjobs[1].status == 'new'
        assert j.subjobs[2].status == 'new'

    def test_CondorConfigDefaultsParallel(self):
        # A test with parallel submission
        from Ganga.GPI import Job, TestSplitter, TestSubmitter

        j = Job()
        j.splitter = TestSplitter()
        j.splitter.backs = [TestSubmitter(),TestSubmitter(),TestSubmitter()]
        j.backend= TestSubmitter()
        b = j.splitter.backs[1]
        b.fail = 'submit'
        j.parallel_submit = True

        assert j.status == 'new'

        with pytest.raises(IncompleteJobSubmissionError):
            j.submit(keep_going=True)

        assert j.status =='failed'
        assert j.subjobs[0].status in ['submitted', 'running']

        assert j.subjobs[1].status == 'new'
        assert j.subjobs[2].status in ['submitted', 'running']
