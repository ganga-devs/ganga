from __future__ import absolute_import, print_function

from ..GangaUnitTest import GangaUnitTest


class TestSavannah87262(GangaUnitTest):
    def test_Savannah87262(self):
        from Ganga.GPI import Executable, Job, shareref

        e = Executable()
        e.prepare()
        j = Job(application=e)
        s = shareref._impl.name
        print("%s" % s)
        print("%s" % e.is_prepared.name)
        print("%s" % j.application)
        self.assertEqual(s[e.is_prepared.name], 1)
