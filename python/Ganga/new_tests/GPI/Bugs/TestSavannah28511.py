from __future__ import absolute_import

from Ganga.new_tests.lib.GangaUnitTest import GangaUnitTest


class TestSavannah28511(GangaUnitTest):
    def test_Savannah28511(self):
        from Ganga.GPI import Job

        from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state

        j = Job()

        j.submit()
        self.assertTrue(sleep_until_completed(j, 20), 'Job is not completed')

        j.resubmit()
        self.assertTrue(sleep_until_completed(j, 30), 'Job is not completed after fail during resubmit')

        j._impl.updateStatus('failed')
        self.assertTrue(sleep_until_state(j, 20, 'failed'), 'Job is not failed')

        j.resubmit()
        self.assertTrue(sleep_until_completed(j, 20), 'Job is not completed after resubmit')
