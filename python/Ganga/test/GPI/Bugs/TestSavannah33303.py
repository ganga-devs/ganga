

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah33303(GangaUnitTest):
    def test_Savannah33303(self):
        from Ganga.GPI import Job

        j = Job()
        id = j.id

        for i in range(1, 20):
            j = j.copy()
            self.assertEqual(j.id, id + i)
