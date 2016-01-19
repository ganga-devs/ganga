from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest
from Ganga.GPIDev.Lib.Job.Job import JobError


class Savannah13685(GangaUnitTest):
    def test_Savannah13685(self):
        from Ganga.GPI import Job

        j = Job()
        j.application.exe = 'sleep'
        j.application.args = '6'

        self.assertRaises(JobError, j.submit)

        j.application.exe = 'bin/hostname'

        self.assertRaises(JobError, j.submit)
