from __future__ import absolute_import
import os
import shutil
import copy
import tempfile

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.file_utils import generate_unique_temp_file
from Ganga.Core.exceptions import GangaException

class TestMassStorageGetPut(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    _temp_files = []
    _managed_files = []

    # Num of sj in tests
    sj_len = 3

    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/TestMassStorageGetPut'

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
                    ('Output', 'MassStorageFile', self.MassStorageTestConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestMassStorageGetPut, self).setUp(extra_opts=extra_opts)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp jobs """

        from Ganga.GPI import jobs
        for j in jobs:
            shutil.rmtree(j.backend.workdir, ignore_errors=True)
            j.remove()

    @classmethod
    def setUpClass(cls):
        """ """
        cls.outputFilePath = tempfile.mkdtemp()
        cls.MassStorageTestConfig['uploadOptions']['path'] = cls.outputFilePath

    @classmethod
    def tearDownClass(cls):
        """ Cleanup the current temp objects """

        for file_ in cls._temp_files:
            os.unlink(file_)
        cls._temp_files = []
        cls._managed_files = []

        shutil.rmtree(cls.outputFilePath, ignore_errors=True)

    def test_a_test_put(self):
        """Test that a job can be submitted with inputfiles in the input"""

        if not os.path.isdir(self.outputFilePath):
            os.makedirs(self.outputFilePath)

        from Ganga.GPI import MassStorageFile

        _ext = '.root'
        file_1 = generate_unique_temp_file(_ext)
        file_2 = generate_unique_temp_file(_ext)
        self._temp_files.append(file_1)
        self._temp_files.append(file_2)
        msf_1 = MassStorageFile(file_1)
        msf_2 = MassStorageFile(file_2)
        self._managed_files.append(msf_1)
        self._managed_files.append(msf_2)
        msf_1.put()
        msf_2.put()

        for file_ in [msf for msf in (msf_1, msf_2)]:
            assert os.path.isfile(os.path.join(self.outputFilePath, file_.namePattern))
            file_.localDir = ''
            assert file_.localDir == ''

    def test_b_test_get(self):
        """Test that the files were made accessible to the WN area and collected as LocalFile objects in outputfiles"""

        from Ganga.GPIDev.Base.Proxy import stripProxy
        from Ganga.GPI import Job

        tmpdir = tempfile.mkdtemp()
        if not os.path.isdir(tmpdir):
            os.makedirs(tmpdir)

        # Test in the case that the files don't have a parent or a localDir
        for file_ in self._managed_files:
            file_.localDir = ''
            try:
                assert file_.localDir == ''
                file_.get()
                print("Unexpected localDir: %s" % file_.localDir)
                failed = False
            except GangaException:
                failed = True
            assert failed

        # Test in the case that the localDir has been set
        for file_ in self._managed_files:
            file_.localDir = tmpdir
            print("localDir: %s" % file_.localDir)
            file_.get()
            assert os.path.isfile(os.path.join(tmpdir, file_.namePattern))
            file_.localDir = ''
            assert file_.localDir == ''

        # Test in the case that the object is 'owned' by a Job

        j=Job()
        outputdir = stripProxy(j).getOutputWorkspace(create=True).getPath()
        j.outputfiles = self._managed_files
        for file_ in j.outputfiles:
            assert stripProxy(file_).getJobObject() is stripProxy(j)
            assert file_.localDir == ''
            file_.get()
            assert os.path.isfile(os.path.join(outputdir, file_.namePattern))

        shutil.rmtree(tmpdir, ignore_errors=True)

        self.cleanUp()

    def test_c_test_copyTo(self):
        """ Test the new copyTo interface"""

        tmpdir = '/tmp/testMassStorageGet'
        if not os.path.isdir(tmpdir):
            os.makedirs(tmpdir)

        for file_ in self._managed_files:
            file_.localDir = ''
            file_.copyTo(tmpdir)
            assert os.path.isfile(os.path.join(tmpdir, file_.namePattern))


