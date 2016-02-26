from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah44116(GangaUnitTest):
    def test_Savannah44116(self):
        from Ganga.GPI import Job, TestApplication, TestSubmitter

        from GangaTest.Framework.utils import sleep_until_state

        j = Job()
        j.application = TestApplication()
        j.application.postprocess_mark_as_failed = True
        j.backend = TestSubmitter()
        j.backend.time = 1

        j.submit()

        self.assertTrue(sleep_until_state(j, 10, 'failed'), 'Job is not marked as failed despite app.postprocess() hook')
