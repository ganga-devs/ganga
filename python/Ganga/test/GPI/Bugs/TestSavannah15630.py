from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah15630(GangaUnitTest):
    def test_Savannah15630(self):
        from Ganga.GPI import Job, Executable, Local, LocalFile

        from GangaTest.Framework.utils import sleep_until_completed
        j = Job()
        j.application = Executable(exe='touch', args=['out.dat'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.dat')]
        j.submit()
        self.assertTrue(sleep_until_completed(j, 60), 'Timeout on job submission: job is still not finished')

        import os.path
        p = os.path.join(j.outputdir, j.application.args[0])

        self.assertTrue(os.path.exists(p))
