from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah8529(GangaUnitTest):

    def test_Savannah8529(self):
        from Ganga.GPI import Job, TestApplication

        # make sure that _auto__init__ is called correctly in all cases
        j1 = Job()
        j1.application = TestApplication()

        j2 = Job()
        j2.application = "TestApplication"

        j3 = Job(application=TestApplication())

        j4 = Job(application="TestApplication")

        self.assertEqual(j1.application.derived_value, j2.application.derived_value)
        self.assertEqual(j2.application.derived_value, j3.application.derived_value)
        self.assertEqual(j3.application.derived_value, j4.application.derived_value)

        self.assertNotEqual(j1.application.derived_value, None)
        self.assertNotEqual(j1.application.derived_value.find(j1.application.exe), -1)
