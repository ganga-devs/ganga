from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah8009(GangaUnitTest):

    def test_Savannah8009(self):
        from Ganga.GPI import Executable, Job, jobs, templates

        from GangaTest.Framework.utils import sleep_until_completed

        j = Job()
        j.submit()

        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(templates), 0)

        if not sleep_until_completed(j, timeout=120):
            assert(not "Timeout on job submission: job is still not finished")

        t = j.copy()

        # make sure that copy creates a new job (and not the template)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(len(templates), 0)

        # make sure that output parameters are not carried forward
        self.assertNotEqual(j.backend.id, t.backend.id)
        self.assertNotEqual(j.backend.exitcode, t.backend.exitcode)

        # make sure that input parameters are carried forward
        self.assertEqual(j.application.exe, t.application.exe)
