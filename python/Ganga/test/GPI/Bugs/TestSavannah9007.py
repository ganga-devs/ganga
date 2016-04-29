from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah9007(GangaUnitTest):
    def test_Savannah9007(self):
        from Ganga.GPI import Job

        j = Job()

        from Ganga.Utility.Config import getConfig
        if not getConfig('Output')['ForbidLegacyInput']:
            j.inputsandbox = ['x']
        else:
            j.inputfiles = ['x']
