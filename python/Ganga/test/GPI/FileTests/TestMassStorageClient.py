from __future__ import absolute_import
<<<<<<< HEAD
import os
import shutil

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.file_utils import generate_unique_temp_file

=======
from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.file_utils import generate_unique_temp_file

import datetime
import time
import os
import shutil

>>>>>>> Adding test for MassStorageFile (#735)
class TestMassStorageClient(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    _managed_files = []

    # Num of sj in tests
    sj_len = 3

<<<<<<< HEAD
    fileName = 'MassStorageFile'

    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/' + fileName + 'Client'
=======
    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/MassStorageClient'
>>>>>>> Adding test for MassStorageFile (#735)

    # This sets up a MassStorageConfiguration which works by placing a file on local storage somewhere we can test using standard tools
    MassStorageTestConfig = {'defaultProtocol': 'file://',
                             'fileExtensions': [''],
<<<<<<< HEAD
                             'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir'},
                             'backendPostprocess': {'LSF': 'client', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'client', 'Interactive': 'client', 'Local': 'client', 'CREAM': 'client'}}


=======
                             'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir -p'},
                             'backendPostprocess': {'LSF': 'client', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'client', 'Interactive': 'client', 'Local': 'client', 'CREAM': 'client'}}

>>>>>>> Adding test for MassStorageFile (#735)
    def setUp(self):
        """
        Configure the MassStorageFile for the test
        """
<<<<<<< HEAD

        extra_opts=[('PollThread', 'autostart', 'False'),
                    ('Local', 'remove_workdir', 'False'),
                    ('TestingFramework', 'AutoCleanup', 'False'),
                    ('Output', TestMassStorageClient.fileName, TestMassStorageClient.MassStorageTestConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestMassStorageClient, self).setUp(extra_opts=extra_opts)

    @classmethod
    def getFileObject(cls):
        """ Return an instance of the file we're wanting to test """
        import Ganga.GPI as gpi
        return getattr(gpi, cls.fileName)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp jobs """
=======
        extra_opts=[('PollThread', 'autostart', 'False'),
                    ('Local', 'remove_workdir', 'False'),
                    ('TestingFramework', 'AutoCleanup', 'False'),
                    ('Output', 'MassStorageFile', TestMassStorageClient.MassStorageTestConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestMassStorageClient, self).setUp(extra_opts=extra_opts)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp objects """
>>>>>>> Adding test for MassStorageFile (#735)

        from Ganga.GPI import jobs
        for j in jobs:
            shutil.rmtree(j.backend.workdir, ignore_errors=True)
            j.remove()

<<<<<<< HEAD
    @classmethod
    def tearDownClass(cls):
        """ Cleanup the current temp objects """

=======
>>>>>>> Adding test for MassStorageFile (#735)
        for file_ in TestMassStorageClient._managed_files:
            os.unlink(file_)
        TestMassStorageClient._managed_files = []

        shutil.rmtree(TestMassStorageClient.outputFilePath, ignore_errors=True)

    def test_a_testClientSideSubmit(self):
        """Test the client side code whilst stil using the Local backend"""

<<<<<<< HEAD
        MassStorageFile = TestMassStorageClient.getFileObject()

        from Ganga.GPI import LocalFile, Job, ArgSplitter
=======
        from Ganga.GPI import LocalFile, MassStorageFile, Job, ArgSplitter
>>>>>>> Adding test for MassStorageFile (#735)

        from Ganga.Utility.Config import getConfig

        TestMassStorageClient.cleanUp()

<<<<<<< HEAD
        assert getConfig('Output')[TestMassStorageClient.fileName]['backendPostprocess']['Local'] == 'client'
=======
        assert getConfig('Output')['MassStorageFile']['backendPostprocess']['Local'] == 'client'
>>>>>>> Adding test for MassStorageFile (#735)

        _ext = '.root'
        file_1 = generate_unique_temp_file(_ext)
        file_2 = generate_unique_temp_file(_ext)
        TestMassStorageClient._managed_files.append(file_1)
        TestMassStorageClient._managed_files.append(file_2)

        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2)]
<<<<<<< HEAD
        j.splitter = ArgSplitter(args = [[_] for _ in range(TestMassStorageClient.sj_len)])
=======
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorageClient.sj_len) ])
>>>>>>> Adding test for MassStorageFile (#735)
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext)]
        j.submit()

    def test_b_testClientSideComplete(self):
        """Test the client side code whilst stil using the Local backend"""

        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        from Ganga.Utility.Config import getConfig

<<<<<<< HEAD
        assert getConfig('Output')[TestMassStorageClient.fileName]['backendPostprocess']['Local'] == 'client'

        j = jobs[-1]

        assert sleep_until_completed(j)
=======
        assert getConfig('Output')['MassStorageFile']['backendPostprocess']['Local'] == 'client'

        j = jobs[-1]

        sleep_until_completed(j)

        assert j.status == 'completed'
>>>>>>> Adding test for MassStorageFile (#735)

        for sj in j.subjobs:
            output_dir = stripProxy(sj).getOutputWorkspace(create=False).getPath()
            assert os.path.isdir(output_dir) == True

            # Check that the files have been removed from the output worker dir
            for input_f in j.inputfiles:
                assert not os.path.isfile(os.path.join(output_dir, input_f.namePattern))

            # Check that the files were placed in the correct place on storage
            output_dir = os.path.join(TestMassStorageClient.outputFilePath, str(j.id), str(sj.id))
            for file_ in j.inputfiles:
                assert os.path.isfile(os.path.join(output_dir, file_.namePattern))

            # Check that wildcard expansion happened correctly
            assert len(stripProxy(stripProxy(sj).outputfiles[0]).subfiles) == 2

            assert len(sj.outputfiles) == 2

<<<<<<< HEAD
        self.cleanUp()

class TestSharedClient(TestMassStorageClient):
    """test for sjid in filename names explain each test"""

    fileName = 'SharedFile'
=======
        TestMassStorageClient.cleanUp()
>>>>>>> Adding test for MassStorageFile (#735)

