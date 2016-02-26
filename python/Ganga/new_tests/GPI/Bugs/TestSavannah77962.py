from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah77962(GangaUnitTest):
    def test_Savannah77962(self):
        from Ganga.GPI import Job

        # The submit_counter was being incremented by 2 when j.resubmit was called.
        from GangaTest.Framework.utils import sleep_until_completed

        j = Job()

        self.assertEqual(j.info.submit_counter, 0)

        j.submit()

        self.assertEqual(j.info.submit_counter, 1)

        self.assertTrue(sleep_until_completed(j), 'Timeout on job submission: job is still not finished')

        j.resubmit()

        self.assertEqual(j.info.submit_counter, 2)
