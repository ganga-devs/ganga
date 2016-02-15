from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah8111(GangaUnitTest):

    def test_Savannah8111(self):
        from Ganga.GPI import Job

        j1 = Job()
        j1.name = 'Gauss Job'
        jj = j1.copy()
        self.assertEqual(jj.name, 'Gauss Job')
