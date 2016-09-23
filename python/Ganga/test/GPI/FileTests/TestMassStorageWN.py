from __future__ import absolute_import
import os
import shutil
import tempfile

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.file_utils import generate_unique_temp_file

from Ganga.GPIDev.Base.Proxy import stripProxy, getProxyClass
from GangaTest.Framework.utils import sleep_until_completed
from Ganga.GPIDev.Lib.File.MassStorageFile import MassStorageFile, SharedFile
from Ganga.GPIDev.Base.Objects import _getName

class TestMassStorageWN(GangaUnitTest):
    """Testing MassStorage when completing a file"""

    _managed_files = []

    # Num of sj in tests
    sj_len = 3

    fileClass = getProxyClass(MassStorageFile)

    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/Test' + _getName(fileClass) + 'WN'

    # This sets up a MassStorageConfiguration which works by placing a file on local storage somewhere we can test using standard tools
    MassStorageTestConfig = {'defaultProtocol': 'file://',
                             'fileExtensions': [''],
                             'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir'},
                             'backendPostprocess': {'LSF': 'WN', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'WN', 'Interactive': 'client', 'Local': 'WN', 'CREAM': 'client'}}

    standardFormat = '{jid}/{fname}'
    extendedFormat = '{jid}/{sjid}/{fname}'
    customOutputFormat = '{jid}_{sjid}_{fname}'

    def setUp(self):
        """
        Configure the MassStorageFile for the test
        """
        extra_opts=[('PollThread', 'autostart', 'False'),
                    ('Local', 'remove_workdir', 'False'),
                    ('TestingFramework', 'AutoCleanup', 'False'),
                    ('Output', _getName(self.fileClass), TestMassStorageWN.MassStorageTestConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestMassStorageWN, self).setUp(extra_opts=extra_opts)

    @staticmethod
    def cleanUp():
        """ Cleanup the current job objects """

        from Ganga.GPI import jobs
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

    def test_a_Submit(self):
        """Test the ability to submit a job with some LocalFiles"""

        MassStorageFile = self.fileClass
        from Ganga.GPI import jobs, Job, LocalFile

        _ext = '.txt'

        file_1 = generate_unique_temp_file(_ext)
        TestMassStorageWN._managed_files.append(file_1)

        j = Job()
        j.inputfiles = [LocalFile(file_1)]
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat=self.standardFormat)]
        j.submit()

        for f in j.outputfiles:
            assert f.outputfilenameformat == self.standardFormat

    def test_b_Completed(self):
        """Test the job completed and the output files exit `in storage`"""
        from Ganga.GPI import jobs

        j = jobs[-1]

        assert sleep_until_completed(j)

        # Check that we've still got 1 file everywhere we expect 1
        assert len(j.inputfiles) == 1
        assert len(j.outputfiles) == 1
        # 1 file after wildcard expansion
        assert len(stripProxy(stripProxy(j).outputfiles[0]).subfiles) == 1
        assert len(j.outputfiles) == 1

        # Test that these strings are sensible
        assert j.outputfiles[0].namePattern != '' and j.outputfiles[0].namePattern[0] != '*'
        assert j.outputfiles[0].locations != [''] and isinstance(j.outputfiles[0].locations[0], str) is True
        assert j.outputfiles[0].accessURL() != [''] and isinstance(j.outputfiles[0].accessURL()[0], str) is True

        # Check that the output file exists on 'storage'
        output_dir = os.path.join(self.outputFilePath, str(j.id))
        assert os.path.isdir(output_dir)
        assert os.path.isfile(os.path.join(output_dir, j.inputfiles[0].namePattern))

        self.cleanUp()

    def test_c_SplitJob(self):
        """Test submitting subjobs"""
        MassStorageFile = self.fileClass
        from Ganga.GPI import Job, LocalFile, ArgSplitter

        _ext = '.txt2'

        file_1 = generate_unique_temp_file(_ext)
        TestMassStorageWN._managed_files.append(file_1)

        j = Job()
        j.inputfiles = [LocalFile(file_1)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorageWN.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat=self.extendedFormat)]
        j.submit()

        for f in j.outputfiles:
            assert f.outputfilenameformat == self.extendedFormat

    def test_d_CompletedSJ(self):
        """Test that the subjobs ave completed"""
        from Ganga.GPI import jobs

        j = jobs[-1]

        assert sleep_until_completed(j)

        assert len(j.subjobs) == TestMassStorageWN.sj_len

        assert len(stripProxy(stripProxy(j.subjobs[0]).outputfiles[0]).subfiles) == 1
        assert len(j.subjobs[0].outputfiles) == 1

        for i in range(0, TestMassStorageWN.sj_len):
            output_dir = os.path.join(self.outputFilePath, str(j.id), str(i))
            assert os.path.isdir(output_dir)
            # Check each inputfile has been placed in storage like we asked
            for _input_file in j.inputfiles:
                assert os.path.isfile(os.path.join(output_dir, _input_file.namePattern))

        self.cleanUp()

    def test_e_MultipleFiles(self):
        """Test that the wildcards work"""

        MassStorageFile = self.fileClass
        from Ganga.GPI import LocalFile, Job, ArgSplitter

        _ext = '.root'
        _ext2 = '.txt'
        file_1 = generate_unique_temp_file(_ext)
        file_2 = generate_unique_temp_file(_ext)
        file_3 = generate_unique_temp_file(_ext2)
        TestMassStorageWN._managed_files.append(file_1)
        TestMassStorageWN._managed_files.append(file_2)
        TestMassStorageWN._managed_files.append(file_3)

        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2), LocalFile(file_3)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorageWN.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}/{sjid}/{fname}'),
                         MassStorageFile(namePattern='*'+_ext2)]
        j.submit()

    def test_f_MultiUpload(self):
        """Test that multiple 'uploads' work"""

        from Ganga.GPI import jobs

        j = jobs[-1]

        assert sleep_until_completed(j)

        assert len(j.subjobs) == TestMassStorageWN.sj_len

        for i in range(0, TestMassStorageWN.sj_len):
            # Check that the subfiles were expended correctly
            assert len(stripProxy(stripProxy(j.subjobs[i]).outputfiles[0]).subfiles) == 2
            assert len(stripProxy(stripProxy(j.subjobs[i]).outputfiles[1]).subfiles) == 1
            # Check we have the correct total number of files
            assert len(j.subjobs[i].outputfiles) == 3
            output_dir = os.path.join(self.outputFilePath, str(j.id), str(i))
            assert os.path.isdir(output_dir)
            # Checl all of the files were put into storage
            for file_ in j.inputfiles: 
                assert os.path.isfile(os.path.join(output_dir, file_.namePattern))

        self.cleanUp()

    def test_g_MultipleFiles(self):
        """Test that the wildcards work"""

        MassStorageFile = self.fileClass
        from Ganga.GPI import LocalFile, Job, ArgSplitter

        _ext = '.root'
        file_1 = generate_unique_temp_file(_ext)
        file_2 = generate_unique_temp_file(_ext)
        TestMassStorageWN._managed_files.append(file_1)
        TestMassStorageWN._managed_files.append(file_2)

        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorageWN.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat=self.customOutputFormat)]
        
        for f in j.outputfiles:
            assert f.outputfilenameformat == self.customOutputFormat

        j.submit()

        for f in j.outputfiles:
            assert f.outputfilenameformat == self.customOutputFormat

    def test_h_MultiUpload(self):
        """Test that multiple 'uploads' work"""

        from Ganga.GPI import jobs

        j = jobs[-1]

        assert sleep_until_completed(j)

        assert len(j.subjobs) == TestMassStorageWN.sj_len

        for i in range(0, TestMassStorageWN.sj_len):
            # Check that we correctly have expanded the wildcard still
            assert len(stripProxy(stripProxy(j.subjobs[i]).outputfiles[0]).subfiles) == 2
            assert len(j.subjobs[i].outputfiles) == 2
            file_prep = os.path.join(self.outputFilePath, str(j.id) + '_' + str(i) + '_')
            # Check that the files were placed in the correct place on storage
            print("Found: %s" % str(os.listdir(self.outputFilePath)))
            assert j.outputfiles[i].outputfilenameformat == self.customOutputFormat
            for file_ in j.inputfiles:
                assert os.path.isfile(file_prep + file_.namePattern)

        self.cleanUp()


class TestSharedWN(TestMassStorageWN):
    """Testing SharedFile when completing a file"""
    fileClass = getProxyClass(SharedFile)

