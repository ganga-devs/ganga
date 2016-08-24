from __future__ import absolute_import

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.monitoring import run_until_completed, run_until_state


class TestSavannah28511(GangaUnitTest):
    def test_Savannah28511(self):
        from Ganga.GPI import Job
        from Ganga.GPI import TestSubmitter

        j = Job()

        j.submit()
        assert run_until_completed(j, timeout=20), 'Job is not completed'

        j.resubmit()
        assert run_until_completed(j, timeout=30), 'Job is not completed after fail during resubmit'

        j._impl.updateStatus('failed')
        assert run_until_state(j, timeout=20, state='failed'), 'Job is not failed'

        j.resubmit()
        assert run_until_completed(j, timeout=20), 'Job is not completed after resubmit'
