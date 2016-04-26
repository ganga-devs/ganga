from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

import os


class TestSavannah31691(GangaUnitTest):
    def test_Savannah31691(self):
        from Ganga.GPI import config, Job, jobs

        config['Configuration']['autoGenerateJobWorkspace'] = True

        import Ganga.Runtime.Workspace_runtime

        localDir = Ganga.Runtime.Workspace_runtime.getLocalRoot()

        # Create 5 default jobs, then list content of local workspace
        for i in range(5):
            Job()

        self.assertEqual(len(os.listdir(localDir)), 5)

        # Delete job 0, then try again to list content of local workspace
        jobs(0).remove()

        self.assertEqual(len(os.listdir(localDir)), 4)
