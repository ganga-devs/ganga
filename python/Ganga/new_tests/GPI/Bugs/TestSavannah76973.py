from __future__ import absolute_import

from Ganga.new_tests.lib.GangaUnitTest import GangaUnitTest


class TestSavannah76973(GangaUnitTest):
    def test_Savannah76973(self):
        from Ganga.GPI import Job, JobTemplate, JobError

        j = Job()
        t = JobTemplate(j)
        self.assertEqual(t.status, 'template')

        self.assertRaises(JobError, t.submit)
