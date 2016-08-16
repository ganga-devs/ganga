from __future__ import absolute_import

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestAthena(GangaUnitTest):

    def testAthena(self):

        from Ganga.GPI import Athena, Job

        j = Job()
        j.application = Athena()

