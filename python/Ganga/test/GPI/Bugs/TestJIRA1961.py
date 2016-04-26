from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestJIRA1961(GangaUnitTest):

    def test_unprepareTrue(self):

        from Ganga.Utility.Config import setConfigOption
        from Ganga.GPI import Job, Executable
        setConfigOption('Preparable', 'unprepare_on_copy', 'True')
        j = Job(application=Executable(exe='/bin/echo', args=['hello']))
        j.submit()

        assert(j.application.is_prepared != None)

        j2 = j.copy()

        assert(j2.application.is_prepared == None)

        j3 = Job(j)

        assert(j3.application.is_prepared == None)

    def test_unprepareFalse(self):

        from Ganga.Utility.Config import setConfigOption
        from Ganga.GPI import Job, Executable
        setConfigOption('Preparable', 'unprepare_on_copy', 'False')
        k = Job(application=Executable(exe='/bin/echo', args=['hello']))
        k.submit()

        assert(k.application.is_prepared != None)

        k2 = k.copy()

        assert(k2.application.is_prepared != None)

        k3 = Job(k)

        assert(k.application.is_prepared != None)
