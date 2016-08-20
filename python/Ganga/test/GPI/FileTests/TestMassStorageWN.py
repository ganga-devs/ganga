from __future__ import absolute_import
from Ganga.testlib.GangaUnitTest import GangaUnitTest

import tempfile
import datetime
import time
import random
import os
import shutil
import copy
import string

def generateUniqueTempFile( ext = '.txt' ):
    """ Generate a unique file with a given filename with some random contents and return the name of the file on disk
    Args:
        ext (str): This is the extension (including '.') to give to the file of interest
    """

    with tempfile.NamedTemporaryFile(mode='w',suffix=ext,delete=False) as myFile:

        myFile.write( ''.join(random.choice(string.ascii_uppercase+string.digits) for _ in range(20)) )

        TestMassStorageWN._managed_files.append(myFile.name)

        return myFile.name

class TestMassStorageWN(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    _managed_files = []

    # Num of sj in tests
    sj_len = 3

    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/MassStorageWN'

    # This sets up a MassStorageConfiguration which works by placing a file on local storage somewhere we can test using standard tools
    MassStorageTestConfig = {'defaultProtocol': 'file://',
                             'fileExtensions': [''],
                             'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir -p'},
                             'backendPostprocess': {'LSF': 'WN', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'WN', 'Interactive': 'client', 'Local': 'WN', 'CREAM': 'client'}}

    def setUp(self):
        """
        Configure the MassStorageFile for the test
        """
        extra_opts=[('PollThread', 'autostart', 'False'),
                    ('Local', 'remove_workdir', 'False'),
                    ('TestingFramework', 'AutoCleanup', 'False'),
                    ('Output', 'MassStorageFile', TestMassStorageWN.MassStorageTestConfig),
                    ('Output', 'FailJobIfNoOutputMatched', 'True')]
        super(TestMassStorageWN, self).setUp(extra_opts=extra_opts)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp objects """

        from Ganga.GPI import jobs
        for j in jobs:
            shutil.rmtree(j.backend.workdir, ignore_errors=True)
            j.remove()

        for file_ in TestMassStorageWN._managed_files:
            os.unlink(file_)
        TestMassStorageWN._managed_files = []

        shutil.rmtree(TestMassStorageWN.outputFilePath, ignore_errors=True)

    def test_a_Submit(self):
        """Test the ability to submit a job with some LocalFiles"""
        from Ganga.GPI import jobs, Job, LocalFile, MassStorageFile

        TestMassStorageWN.cleanUp()

        _ext = '.txt'

        file_1 = generateUniqueTempFile(_ext)

        j = Job()
        j.inputfiles = [LocalFile(file_1)]
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}/{fname}')]
        j.submit()

    def test_b_Completed(self):
        """Test the job completed and the output files exit `in storage`"""
        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs[-1]

        sleep_until_completed(j)

        # Just has to have reached completed state for checks to make sense
        assert j.status == 'completed'

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
        output_dir = os.path.join(TestMassStorageWN.outputFilePath, str(j.id))
        assert os.path.isdir(output_dir)
        assert os.path.isfile(os.path.join(output_dir, j.inputfiles[0].namePattern))

        TestMassStorageWN.cleanUp()

    def test_c_SplitJob(self):
        """Test submitting subjobs"""
        from Ganga.GPI import Job, LocalFile, MassStorageFile, ArgSplitter

        _ext = '.txt2'

        file_1 = generateUniqueTempFile(_ext)

        j = Job()
        j.inputfiles = [LocalFile(file_1)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorageWN.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}/{sjid}/{fname}')]
        j.submit()

    def test_d_CompletedSJ(self):
        """Test that the subjobs ave completed"""
        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs[-1]

        sleep_until_completed(j)

        assert j.status == 'completed'

        assert len(j.subjobs) == TestMassStorageWN.sj_len

        assert len(stripProxy(stripProxy(j.subjobs[0]).outputfiles[0]).subfiles) == 1
        assert len(j.subjobs[0].outputfiles) == 1

        for i in range(0, TestMassStorageWN.sj_len):
            output_dir = os.path.join(TestMassStorageWN.outputFilePath, str(j.id), str(i))
            assert os.path.isdir(output_dir)
            # Check each inputfile has been placed in storage like we asked
            for _input_file in j.inputfiles:
                assert os.path.isfile(os.path.join(output_dir, _input_file.namePattern))

        TestMassStorageWN.cleanUp()

    def test_e_MultipleFiles(self):
        """Test that the wildcards work"""

        from Ganga.GPI import LocalFile, MassStorageFile, Job, ArgSplitter

        _ext = '.root'
        _ext2 = '.txt'
        file_1 = generateUniqueTempFile(_ext)
        file_2 = generateUniqueTempFile(_ext)
        file_3 = generateUniqueTempFile(_ext2)
        
        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2), LocalFile(file_3)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorageWN.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}/{sjid}/{fname}'),
                         MassStorageFile(namePattern='*'+_ext2)]
        j.submit()

    def test_f_MultiUpload(self):
        """Test that multiple 'uploads' work"""

        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs[-1]

        sleep_until_completed(j)

        assert j.status == 'completed'

        assert len(j.subjobs) == TestMassStorageWN.sj_len

        for i in range(0, TestMassStorageWN.sj_len):
            # Check that the subfiles were expended correctly
            assert len(stripProxy(stripProxy(j.subjobs[i]).outputfiles[0]).subfiles) == 2
            assert len(stripProxy(stripProxy(j.subjobs[i]).outputfiles[1]).subfiles) == 1
            # Check we have the correct total number of files
            assert len(j.subjobs[i].outputfiles) == 3
            output_dir = os.path.join(TestMassStorageWN.outputFilePath, str(j.id), str(i))
            assert os.path.isdir(output_dir)
            # Checl all of the files were put into storage
            for file_ in j.inputfiles: 
                assert os.path.isfile(os.path.join(output_dir, file_.namePattern))

        TestMassStorageWN.cleanUp()

    def test_g_MultipleFiles(self):
        """Test that the wildcards work"""

        from Ganga.GPI import LocalFile, MassStorageFile, Job, ArgSplitter

        _ext = '.root'
        file_1 = generateUniqueTempFile(_ext)
        file_2 = generateUniqueTempFile(_ext)

        j = Job()
        j.inputfiles = [LocalFile(file_1), LocalFile(file_2)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorageWN.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}_{sjid}_{fname}')]
        j.submit()

    def test_h_MultiUpload(self):
        """Test that multiple 'uploads' work"""

        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs[-1]

        sleep_until_completed(j)

        assert j.status == 'completed'

        assert len(j.subjobs) == TestMassStorageWN.sj_len

        for i in range(0, TestMassStorageWN.sj_len):
            # Check that we correctly have expanded the wildcard still
            assert len(stripProxy(stripProxy(j.subjobs[i]).outputfiles[0]).subfiles) == 2
            assert len(j.subjobs[i].outputfiles) == 2
            file_prep = os.path.join(TestMassStorageWN.outputFilePath, str(j.id) + '_' + str(i) + '_')
            # Check that the files were placed in the correct place on storage
            for file_ in j.inputfiles:
                assert os.path.isfile(file_prep + file_.namePattern)

        TestMassStorageWN.cleanUp()

