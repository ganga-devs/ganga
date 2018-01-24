from __future__ import absolute_import
import os
import shutil
import tempfile

from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.testlib.file_utils import generate_unique_temp_file
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Base.Proxy import stripProxy, addProxy
from GangaTest.Framework.utils import sleep_until_completed
from GangaCore.GPIDev.Lib.File.MassStorageFile import MassStorageFile, SharedFile
from GangaCore.GPIDev.Base.Objects import _getName

class TestMassStorageClient(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    _managed_files = []

    # Num of sj in tests
    sj_len = 3

    fileClass = addProxy(MassStorageFile)

    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/Test' + _getName(fileClass) + 'Client'

    # This sets up a MassStorageConfiguration which works by placing a file on local storage somewhere we can test using standard tools
    MassStorageTestConfig = {'defaultProtocol': 'file://',
                             'fileExtensions': [''],
                             'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir'},
                             'backendPostprocess': {'LSF': 'client', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'client', 'Interactive': 'client', 'Local': 'client', 'CREAM': 'client'}}

    _ext = '.root'

    def setUp(self):
        """
        Configure the MassStorageFile for the test
        """
        extra_opts=[('PollThread', 'autostart', 'False'),
                    ('Local', 'remove_workdir', 'False'),
                    ('TestingFramework', 'AutoCleanup', 'False'),
                    ('Output', _getName(self.fileClass), TestMassStorageClient.MassStorageTestConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestMassStorageClient, self).setUp(extra_opts=extra_opts)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp objects """

        from GangaCore.GPI import jobs
        for j in jobs:
            shutil.rmtree(j.backend.workdir, ignore_errors=True)
            j.remove()

    @classmethod
    def setUpClass(cls):
        """ This creates a safe place to put the files into 'mass-storage' """
        cls.outputFilePath = tempfile.mkdtemp()
        cls.MassStorageTestConfig['uploadOptions']['path'] = cls.outputFilePath

    @classmethod
    def tearDownClass(cls):
        """ Cleanup the current temp objects """

        for file_ in cls._managed_files:
            os.unlink(file_)
        cls._managed_files = []

        shutil.rmtree(cls.outputFilePath, ignore_errors=True)

    def test_a_testClientSideSubmit(self):
        """Test the client side code whilst stil using the Local backend"""

        MassStorageFile = self.fileClass

        from GangaCore.GPI import LocalFile, Job, ArgSplitter

        TestMassStorageClient.cleanUp()

        assert getConfig('Output')[_getName(self.fileClass)]['backendPostprocess']['Local'] == 'client'

        file_1 = generate_unique_temp_file(TestMassStorageClient._ext)
        file_2 = generate_unique_temp_file(TestMassStorageClient._ext)
        TestMassStorageClient._managed_files.append(file_1)
        TestMassStorageClient._managed_files.append(file_2)

        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(TestMassStorageClient.sj_len)])
        j.outputfiles = [MassStorageFile(namePattern='*'+TestMassStorageClient._ext)]
        j.submit()

    def test_b_testClientSideComplete(self):
        """Test the client side code whilst stil using the Local backend"""

        from GangaCore.GPI import jobs

        assert getConfig('Output')[_getName(self.fileClass)]['backendPostprocess']['Local'] == 'client'

        j = jobs[-1]

        assert sleep_until_completed(j)

        for sj in j.subjobs:
            output_dir = stripProxy(sj).getOutputWorkspace(create=False).getPath()
            assert os.path.isdir(output_dir) == True

            # Check that the files have been removed from the output worker dir
            for input_f in j.inputfiles:
                assert not os.path.isfile(os.path.join(output_dir, input_f.namePattern))

            # Check that the files were placed in the correct place on storage
            output_dir = os.path.join(self.outputFilePath, str(j.id), str(sj.id))
            for file_ in j.inputfiles:
                assert os.path.isfile(os.path.join(output_dir, file_.namePattern))

            # Check that wildcard expansion happened correctly
            assert len(stripProxy(stripProxy(sj).outputfiles[0]).subfiles) == 2

            assert len(sj.outputfiles) == 2

    def test_c_testCopyJob(self):
        """ Test copying a completed job with a wildcard in the outputfiles """

        from GangaCore.GPI import jobs

        j = jobs[-1]

        j2 = j.copy()

        assert len(j2.outputfiles) == 1

        MassStorageFile = self.fileClass

        assert j2.outputfiles == [MassStorageFile(namePattern='*'+TestMassStorageClient._ext)]

        assert len(j2.inputfiles) == 2

        self.cleanUp()


class TestSharedClient(TestMassStorageClient):
    """test for sjid in filename names explain each test"""
    fileClass = addProxy(SharedFile)

