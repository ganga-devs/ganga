from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah33303(GangaUnitTest):
    def test_Savannah33303(self):
        from Ganga.GPI import Job

        j = Job()
        id = j.id

        for i in xrange(1, 20):
            j = j.copy()
            self.assertEqual(j.id, id + i)
