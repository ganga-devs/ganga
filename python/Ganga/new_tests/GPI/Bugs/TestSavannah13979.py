from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah13979(GangaUnitTest):

    def test_Savannah13979(self):
        from Ganga.GPI import Job, Executable, export, load

        self.fname = 'test_savannah_13979.ganga'
        j = Job(application=Executable())

        args_set = [
            ['a'],

            ['''a
                b'''],

            ['''a
                b''', 'simple', 'normal\nnewline', """another

                multiline"""]
        ]

        for args in args_set:
            j.application.args = args
            export(j, self.fname)
            j2 = load(self.fname)[0]
            self.assertEqual(j2.application.args, args)

    def tearDown(self):
        import os
        os.remove(self.fname)

        super(TestSavannah13979, self).tearDown()
