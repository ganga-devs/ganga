from __future__ import absolute_import

from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah43249(GangaUnitTest):
    def test_Savannah43249(self):
        from GangaCore.Core.exceptions import GangaException
        from GangaCore.GPI import Job, jobs

        Job()
        Job()
        Job()

        self.assertRaises(GangaException, jobs.remove, 10)

        self.assertRaises(TypeError, jobs.remove, x=True)

        jobs.remove(keep_going=True)
