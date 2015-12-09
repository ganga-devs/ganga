from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class Savannah13979(GangaUnitTest):
    def Savannah13979(self):
        from Ganga.GPI import Job, Executable, export, load

        import os
        fname = 'test_savannah_13979.ganga'
        j = Job(application=Executable())
        # One-line parameter
        j.application.args = ['a']
        export(j, fname)
        self.assertTrue(load(fname))
        # Two-line parameter
        j.application.args = ['''a
        b''']
        export(j, fname)
        self.assertTrue(load(fname))

        os.remove(fname)
