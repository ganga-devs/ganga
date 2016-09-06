from __future__ import absolute_import
from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.file_utils import generate_unique_temp_file

import datetime
import time
import os
import shutil

class TestLocalFile(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    _managed_files = []

    # Num of sj in tests
    sj_len = 3

    # This sets up a LocalFileConfiguration which works by placing a file on local storage somewhere we can test using standard tools
    LocalFileConfig = {'fileExtensions': [''],
                             'uploadOptions': {},
                             'backendPostprocess': {'LSF': 'client', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'client', 'Interactive': 'client', 'Local': 'client', 'CREAM': 'client'}}

    def setUp(self):
        """
        Configure the LocalFile for the test
        """
        extra_opts=[('PollThread', 'autostart', 'False'),
                    ('Local', 'remove_workdir', 'False'),
                    ('TestingFramework', 'AutoCleanup', 'False'),
                    ('Output', 'LocalFile', TestLocalFile.LocalFileConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestLocalFile, self).setUp(extra_opts=extra_opts)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp objects """

        from Ganga.GPI import jobs
        for j in jobs:
            shutil.rmtree(j.backend.workdir, ignore_errors=True)
            j.remove()

        for file_ in TestLocalFile._managed_files:
            os.unlink(file_)
        TestLocalFile._managed_files = []

    def test_a_testClientSideSubmit(self):
        """Test the client side code whilst stil using the Local backend"""

        from Ganga.GPI import LocalFile, Job, ArgSplitter

        from Ganga.Utility.Config import getConfig

        TestLocalFile.cleanUp()

        assert getConfig('Output')['LocalFile']['backendPostprocess']['Local'] == 'client'

        _ext = '.root'
        file_1 = generate_unique_temp_file(_ext)
        file_2 = generate_unique_temp_file(_ext)
        TestLocalFile._managed_files.append(file_1)
        TestLocalFile._managed_files.append(file_2)

        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestLocalFile.sj_len) ])
        j.outputfiles = [LocalFile(namePattern='*'+_ext)]
        j.submit()

    def test_b_testClientSideComplete(self):
        """Test the client side code whilst stil using the Local backend"""

        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        from Ganga.Utility.Config import getConfig

        assert getConfig('Output')['LocalFile']['backendPostprocess']['Local'] == 'client'

        j = jobs[-1]

        sleep_until_completed(j)

        assert j.status == 'completed'

        for sj in j.subjobs:
            output_dir = stripProxy(sj).getOutputWorkspace(create=False).getPath()
            assert os.path.isdir(output_dir) == True

            # Check that the files were placed in the correct place on storage
            for file_ in j.inputfiles:
                assert os.path.isfile(os.path.join(output_dir, file_.namePattern))

            # Check that wildcard expansion happened correctly
            assert len(stripProxy(stripProxy(sj).outputfiles[0]).subfiles) == 2

            assert len(sj.outputfiles) == 2

        TestLocalFile.cleanUp()

