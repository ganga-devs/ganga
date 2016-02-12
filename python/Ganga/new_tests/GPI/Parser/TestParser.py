from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

class TestParser(GangaUnitTest):

    def test_heavilyNested(self):
        from Ganga.GPI import Job, jobs

        j = Job(Job(Job(Job(Job()))))

        assert(j.id == 4)
        assert(len(jobs) == 5)

    def test_OptionsOptions(self):
        from Ganga.GPI import Job, Executable

        j = Job(application=Executable(args=['hello']))

        assert(j.application.args[0] == 'hello')

    def test_AddRemove(self):
        from Ganga.GPI import Job, jobs

        j = Job()
        assert(len(jobs) == 1)
        j.remove()
        assert(len(jobs) == 0)

    def test_AddSubmitRemove(self):
        from Ganga.GPI import Job

        j = Job()
        j.submit()
        j.remove()

    def test_Jobs(self):
        from Ganga.GPI import Job, jobs

        j = Job()
        k = jobs(j.id)

        from Ganga.GPIDev.Base.Objects import GangaObject
        assert(not isinstance(j, GangaObject))
        assert(not isinstance(k, GangaObject))

