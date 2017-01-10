from __future__ import absolute_import
from Ganga.testlib.mark import external
import pytest

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestJob(GangaUnitTest):

    # Adding this locally puts us in a state where we're vulnerable to LHCb projects having problems
    # This is not good from a test perspective! making it external for now until we sort out what's going on
    @external
    def testSubmitLocal(self):
        from Ganga.GPI import DaVinci, Job, TestSubmitter, JobError
        from GangaLHCb.testlib import addLocalTestSubmitter

        ap = DaVinci()
        j = Job(application=ap, backend=TestSubmitter())

        # Test that submission fails before adding runtime handler
        with pytest.raises(JobError):
            j.submit()

        # Test that submission succeeds after adding it.
        #addLocalTestSubmitter()
        #assert j.submit()

