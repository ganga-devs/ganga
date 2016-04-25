from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

class TestSavannah10006(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSavannah10006, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_pass1(self):
        from Ganga.GPI import TestApplication, Job
        a = TestApplication()
        assert(not a.modified)
        a.modify()
        assert(a.modified)

        j = Job(application=TestApplication())
        assert(not j.application.modified)
        j.application.modify()
        assert(j.application.modified)

        return j.id

    def test_pass2(self):
        from Ganga.GPI import jobs
        j = jobs(0)

        assert(j.application.modified)

