from __future__ import absolute_import

from Ganga.new_tests.lib.GangaUnitTest import GangaUnitTest


class TestSavannah10064(GangaUnitTest):

    def test_Savannah10064(self):
        from Ganga.GPI import Job, templates
        j = Job()
        j.submit()
        import os.path
        self.assertTrue(os.path.exists(j.inputdir))
        templates.remove()
        self.assertTrue(os.path.exists(j.inputdir))
