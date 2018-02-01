from __future__ import absolute_import

from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah13685(GangaUnitTest):
    def test_Savannah13685(self):
        from GangaCore.GPI import Job, JobError

        j = Job()
        j.application.exe = 'sleep 6'

        self.assertRaises(JobError, j.submit)

        j.application.exe = 'bin/hostname'

        self.assertRaises(JobError, j.submit)
