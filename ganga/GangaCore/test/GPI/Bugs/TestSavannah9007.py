from __future__ import absolute_import

from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah9007(GangaUnitTest):
    def test_Savannah9007(self):
        from GangaCore.GPI import Job

        j = Job()

        from GangaCore.Utility.Config import getConfig
        if not getConfig('Output')['ForbidLegacyInput']:
            j.inputsandbox = ['x']
        else:
            j.inputfiles = ['x']
