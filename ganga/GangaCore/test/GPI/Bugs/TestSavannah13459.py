from __future__ import absolute_import

from GangaCore.testlib.GangaUnitTest import GangaUnitTest

class TestSavannah13459(GangaUnitTest):

    def test_Savannah13459(self):
        from GangaCore.GPI import Job, config
        from GangaCore.GPI import Job, Executable
        j = Job()
        j.application = Executable()
        j.application.args = ['1', '2', '3']
        self.assertEqual(j.application.args, ['1', '2', '3'])
        j.application.args[0] = '0'

        self.assertEqual(j.application.args, ['0', '2', '3'])

