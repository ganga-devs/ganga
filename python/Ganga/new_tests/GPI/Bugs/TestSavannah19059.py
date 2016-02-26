from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah19059(GangaUnitTest):
    def test_Savannah19059(self):
        from Ganga.GPI import Executable, Job, Interactive, LocalFile

        import os.path
        from GangaTest.Framework.utils import sleep_until_completed

        # Test if Interactive backend copies back sandbox
        app = Executable()
        app.exe = 'touch'
        self.fname = 'abc'
        app.args = [self.fname]
        self.j = Job(backend=Interactive(), application=app, outputfiles=[LocalFile(self.fname)])
        self.j.submit()

        self.assertTrue(sleep_until_completed(self.j, 60), 'Timeout on registering Interactive job as completed')

        self.assertTrue(os.path.exists(os.path.join(self.j.outputdir, self.fname)))
