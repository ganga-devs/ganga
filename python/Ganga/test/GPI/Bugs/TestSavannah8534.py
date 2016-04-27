from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah8534(GangaUnitTest):
    def test_Savannah8534(self):
        from Ganga.GPI import Job, TestApplication, TestSubmitter

        app = TestApplication()

        j = Job(backend=TestSubmitter(time=30), application=app)
        j.submit()

        self.assertNotEqual(j.status, 'new')

        j2 = j.copy()

        # make sure that the status is reset correctly as well as the output parameters
        self.assertEqual(j2.status, 'new')
        self.assertEqual(j2.backend.start_time, 0)
