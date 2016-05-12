from __future__ import absolute_import
import pytest

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestJob(GangaUnitTest):

    def testSubmitLocal(self):
        from Ganga.GPI import DaVinci, Job, TestSubmitter, JobError
        from GangaLHCb.testlib import addLocalTestSubmitter

        ap = DaVinci()
        j = Job(application=ap, backend=TestSubmitter())

        # Test that submission fails before adding runtime handler
        with pytest.raises(JobError):
            j.submit()

        # Test that submission succeeds after adding it.
        addLocalTestSubmitter()
        assert j.submit()

