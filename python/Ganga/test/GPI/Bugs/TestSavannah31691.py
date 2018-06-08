

from Ganga.testlib.GangaUnitTest import GangaUnitTest

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

        dir_list = os.listdir(localDir)
        for i in range(5):
            assert str(i) in dir_list

        # Delete job 0, then try again to list content of local workspace
        jobs(0).remove()

        dir_list = os.listdir(localDir)
        for i in range(1, 5):
            assert str(i) in dir_list

        assert '0' not in dir_list

