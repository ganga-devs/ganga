from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah9120(GangaUnitTest):
    def test_Savannah9120(self):
        from Ganga.GPI import TestApplication

        t = TestApplication()
        t.sequence += ['1']
        self.assertEqual(t.sequence, ['1'])
        t.sequence += ['2']
        self.assertEqual(t.sequence, ['1', '2'])

        t.sequence.append('3')
        t2 = t.sequence
        t2.append('4')
        self.assertEqual(t.sequence, ['1', '2', '3', '4'])
