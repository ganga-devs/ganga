from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah9289(GangaUnitTest):
    def test_Savannah9289(self):
        from Ganga.GPI import Job, jobs

        for repeat in range(5):

            for start in range(1):
                j = Job()

            ids1 = jobs.ids()
            ids2 = sorted(jobs.ids())
            self.assertEqual(ids1, ids2)
            jobs.remove()
