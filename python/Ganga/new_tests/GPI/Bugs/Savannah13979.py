from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class Savannah13979(GangaUnitTest):
    def Savannah13979(self):
        from Ganga.GPI import Job, Executable, export, load

        import os
        self.fname = 'test_savannah_13979.ganga'
        j = Job(application=Executable())
        # One-line parameter
        j.application.args = ['a']
        export(j, self.fname)
        self.assertTrue(load(self.fname))
        # Two-line parameter
        j.application.args = ['''a
        b''']
        export(j, self.fname)
        self.assertTrue(load(self.fname))

        os.remove(self.fname)

    def tearDown(self):
        import os
        os.remove(self.fname)

        super(Savannah13979, self).tearDown()