from __future__ import absolute_import
import os
import shutil
import copy

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.file_utils import generate_unique_temp_file

class TestLocalFileClient(GangaUnitTest):
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
                    ('Output', 'LocalFile', self.LocalFileConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestLocalFileClient, self).setUp(extra_opts=extra_opts)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp jobs """

        from Ganga.GPI import jobs
        for j in jobs:
            shutil.rmtree(j.backend.workdir, ignore_errors=True)
            j.remove()

    @classmethod
    def tearDownClass(cls):
        """ Cleanup the current temp objects """
        for file_ in TestLocalFileClient._managed_files:
            os.unlink(file_)
        TestLocalFileClient._managed_files = []

    def test_a_testClientSideSubmit(self):
        """Test the client side code whilst stil using the Local backend"""

        from Ganga.GPI import LocalFile, Job, ArgSplitter

        _ext = '.root'
        file_1 = generate_unique_temp_file(_ext)
        file_2 = generate_unique_temp_file(_ext)
        TestLocalFileClient._managed_files.append(file_1)
        TestLocalFileClient._managed_files.append(file_2)

        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(TestLocalFileClient.sj_len)])
        j.outputfiles = [LocalFile(namePattern='*'+_ext)]
        j.submit()

    def test_b_testClientSideComplete(self):
        """Test the client side code whilst stil using the Local backend"""

        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs[-1]

        assert sleep_until_completed(j)

        for sj in j.subjobs:
            output_dir = stripProxy(sj).getOutputWorkspace(create=False).getPath()
            assert os.path.isdir(output_dir)

            # Check that the files were placed in the correct place on storage
            for file_ in j.inputfiles:
                assert os.path.isfile(os.path.join(output_dir, file_.namePattern))

            for sf_ in stripProxy(sj).outputfiles[0].subfiles:
                print("sf: %s" % str(sf_))

            # Check that wildcard expansion happened correctly
            assert len(stripProxy(sj).outputfiles[0].subfiles) == 2

            assert len(sj.outputfiles) == 2

        self.cleanUp()

class TestLocalFileWN(TestLocalFileClient):
    """test for sjid in filename names explain each test"""

    LocalFileConfig = copy.deepcopy(TestLocalFileClient.LocalFileConfig)
    LocalFileConfig['backendPostprocess']['Local'] = 'WN'

