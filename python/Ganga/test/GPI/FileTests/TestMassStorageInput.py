
import os
import shutil
import copy
import tempfile

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.file_utils import generate_unique_temp_file
from GangaTest.Framework.utils import sleep_until_completed
from Ganga.GPIDev.Lib.File.MassStorageFile import MassStorageFile, SharedFile
from Ganga.GPIDev.Base.Objects import _getName
from Ganga.GPIDev.Base.Proxy import addProxy

class TestMassStorageClientInput(GangaUnitTest):
    """Testing MassStorage on input to a job"""

    _managed_files = []

    # Num of sj in tests
    sj_len = 3

    fileClass = addProxy(MassStorageFile)

    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/Test' + _getName(fileClass) + 'Input'

    # This sets up a MassStorageConfiguration which works by placing a file on local storage somewhere we can test using standard tools
    MassStorageTestConfig = {'defaultProtocol': 'file://',
                             'fileExtensions': [''],
                             'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir -p'},
                             'backendPostprocess': {'LSF': 'client', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'client', 'Interactive': 'client', 'Local': 'client', 'CREAM': 'client'}}

    def setUp(self):
        """
        Configure the MassStorageFile for the test
        """
        extra_opts=[('PollThread', 'autostart', 'False'),
                    ('Local', 'remove_workdir', 'False'),
                    ('TestingFramework', 'AutoCleanup', 'False'),
                    ('Output', _getName(self.fileClass), self.MassStorageTestConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestMassStorageClientInput, self).setUp(extra_opts=extra_opts)

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

    def test_a_testClientInputSubmit(self):
        """Test that a job can be submitted with inputfiles in the input"""

        MassStorageFile = self.fileClass
        from Ganga.GPI import LocalFile, Job, ArgSplitter

        _ext = '.root'
        file_1 = generate_unique_temp_file(_ext)
        file_2 = generate_unique_temp_file(_ext)
        self._managed_files.append(file_1)
        self._managed_files.append(file_2)
        msf_1 = MassStorageFile(file_1)
        msf_2 = MassStorageFile(file_2)
        msf_1.put()
        msf_2.put()

        j = Job()
        j.inputfiles = [msf_1, msf_2]
        j.splitter = ArgSplitter(args = [[_] for _ in range(self.sj_len)])
        j.outputfiles = [LocalFile(namePattern='*'+_ext)]
        j.submit()

    def test_b_testClientInputComplete(self):
        """Test that the files were made accessible to the WN area and collected as LocalFile objects in outputfiles"""

        from Ganga.GPI import jobs

        j = jobs[-1]

        assert sleep_until_completed(j)

        for sj in j.subjobs:
            for file_ in j.inputfiles:
                assert os.path.isfile(os.path.join(sj.outputdir, file_.namePattern))

        self.cleanUp()

class TestMassStorageWNInput(TestMassStorageClientInput):
    """Testing SharedFile on input to a job"""
    fileClass = addProxy(SharedFile)

