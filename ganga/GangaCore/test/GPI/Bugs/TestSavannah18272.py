from __future__ import absolute_import

from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah18272(GangaUnitTest):
    def test_Savannah18272(self):
        from GangaCore.GPI import Job, File

        j = Job()
        j.application.exe = File('/hello')
        self.assertEqual(j.application.exe, File('/hello'))
