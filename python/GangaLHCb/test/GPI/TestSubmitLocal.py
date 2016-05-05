from __future__ import absolute_import

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestJob(GangaUnitTest):

    def testSubmitLocal(self):
        from Ganga.GPI import DaVinci, Job, TestSubmitter, JobError
        from GangaLHCb.testlib import addLocalTestSubmitter

        ap = DaVinci()
        j = Job(application=ap, backend=TestSubmitter())

        # Test that submission fails before adding runtime handler
        try:
          j.submit()
        except JobError:
          pass

        # Test that submission succeeds after adding it.
        addLocalTestSubmitter()
        assert(j.submit())



