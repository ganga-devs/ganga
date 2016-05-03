from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah40220(GangaUnitTest):
    def test_Savannah40220(self):
        from Ganga.GPI import Job, LCG, export, load

        j = Job(backend=LCG())
        import tempfile
        f, self.fname = tempfile.mkstemp()

        self.assertTrue(export(j, self.fname))

        self.assertTrue(load(self.fname))

    def tearDown(self):
        import os
        os.remove(self.fname)

        super(TestSavannah40220, self).tearDown()
