from __future__ import absolute_import
from Ganga.testlib.GangaUnitTest import GangaUnitTest

import tempfile
import datetime
import time
import random
import os
import shutil
import copy

def generateUniqueTempFile( ext = '.txt' ):
    """ Generate a unique file with a given filename with some random contents and return the name of the file on disk
    Args:
        ext (str): This is the extension (including '.') to give to the file of interest
    """

    myFile = tempfile.NamedTemporaryFile(mode='w',delete=False)

    t = datetime.datetime.now()
    unix_t = time.mktime(t.timetuple())

    file_string = str(unix_t) + "\n"

    random.seed( unix_t )
    rand = random.random() * 1E10

    file_string = file_string + str( rand ) + "\n"

    urand = os.urandom(20)

    file_string = file_string + str( urand ) + "\n"

    myFile.write( file_string )

    myFile.close()

    returnableName = myFile.name+str(ext)

    os.rename( myFile.name, returnableName )

    TestMassStorage._managed_files.append(returnableName)

    return returnableName

class TestMassStorage(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    _managed_files = []

    # Num of sj in tests
    sj_len = 3

    # Where on local storage we want to have our 'MassStorage solution'
    outputFilePath = '/tmp/MassStorage'

    # This sets up a MassStorageConfiguration which works by placing a file on local storage somewhere we can test using standard tools
    MassStorageTestConfig = {'defaultProtocol': 'file://',
                             'fileExtensions': [''],
                             'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir -p'},
                             'backendPostprocess': {'LSF': 'WN', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                    'PBS': 'WN', 'Interactive': 'client', 'Local': 'WN', 'CREAM': 'client'}}

    # This is used for testing the client-side code using the Local backend
    MassStorageTestConfig2 = copy.deepcopy(MassStorageTestConfig)
    MassStorageTestConfig2['backendPostprocess']['Local'] = 'client'


    def setUp(self):
        """
        Configure the MassStorageFile for the test
        """
        extra_opts=[('PollThread', 'autostart', 'False')]
        super(TestMassStorage, self).setUp(extra_opts=extra_opts)
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
        setConfigOption('Output', 'MassStorageFile', TestMassStorage.MassStorageTestConfig)
        setConfigOption('Output', 'FailJobIfNoOutputMatched', True)
        setConfigOption('Local', 'remove_workdir', False)

    @staticmethod
    def cleanUp():
        """ Cleanup the current temp objects """

        from Ganga.GPI import jobs
        for j in jobs:
            shutil.rmtree(j.backend.workdir, ignore_errors=True)
            j.remove()

        for file_ in TestMassStorage._managed_files:
            os.unlink(file_)
        TestMassStorage._managed_files = []

        shutil.rmtree(TestMassStorage.outputFilePath, ignore_errors=True)

    def test_a_Submit(self):
        """Test the ability to submit a job with some LocalFiles"""
        from Ganga.GPI import jobs, Job, LocalFile, MassStorageFile

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

        j = jobs(0)

        sleep_until_completed(j)

        assert j.status == 'completed'

        assert len(j.inputfiles) == 1
        assert len(j.outputfiles) == 1
        # FIXME?
        #assert len(stripProxy(j.outputfiles[0]).subfiles) == 1

        #print("namePattern: %s" % (j.outputfiles[0].namePattern, )) 
        #print("Locations: %s" % (j.outputfiles[0].locations, ))
        #print("accessURL: %s" % (j.outputfiles[0].accessURL(), ))

        assert j.outputfiles[0].namePattern != '' and j.outputfiles[0].namePattern[0] != '*'
        assert j.outputfiles[0].locations != [''] and isinstance(j.outputfiles[0].locations[0], str) is True
        assert j.outputfiles[0].accessURL() != [''] and isinstance(j.outputfiles[0].accessURL()[0], str) is True

        output_dir = os.path.join(TestMassStorage.outputFilePath, '0')
        assert os.path.isdir(output_dir)
        assert os.path.isfile(os.path.join(output_dir, j.inputfiles[0].namePattern))

        TestMassStorage.cleanUp()

    def test_c_SplitJob(self):
        """Test submitting subjobs"""
        from Ganga.GPI import Job, LocalFile, MassStorageFile, ArgSplitter

        _ext = '.txt2'

        file_1 = generateUniqueTempFile(_ext)

        j = Job()
        j.inputfiles = [LocalFile(file_1)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorage.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}/{sjid}/{fname}')]
        j.submit()

    def test_d_CompletedSJ(self):
        """Test that the subjobs ave completed"""
        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs(1)

        sleep_until_completed(j)

        assert j.status == 'completed'

        assert len(j.subjobs) == TestMassStorage.sj_len

        # FIXME?
        #assert (stripProxy(j.subjobs[0].outputfiles[0]).subfiles) == 1

        for i in range(0, TestMassStorage.sj_len):
            output_dir = os.path.join(TestMassStorage.outputFilePath, '1', str(i))
            assert os.path.isdir(output_dir)
            for _input_file in j.inputfiles:
                assert os.path.isfile(os.path.join(output_dir, _input_file.namePattern))

        TestMassStorage.cleanUp()

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
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorage.sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}/{sjid}/{fname}'),
                         MassStorageFile(namePattern='*'+_ext2)]
        j.submit()

    def test_f_MultiUpload(self):
        """Test that multiple 'uploads' work"""

        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs(2)

        sleep_until_completed(j)

        assert j.status == 'completed'

        assert len(j.subjobs) == TestMassStorage.sj_len

        for i in range(0, TestMassStorage.sj_len):
            # FIXME?
            #assert (stripProxy(j.subjobs[i].outputfiles[0]).subfiles) == 2
            output_dir = os.path.join(TestMassStorage.outputFilePath, '2', str(i))
            assert os.path.isdir(output_dir)
            for file_ in j.inputfiles: 
                assert os.path.isfile(os.path.join(output_dir, file_.namePattern))

        TestMassStorage.cleanUp()


# FIXME: The following 2 tets are designed to test the 'client-side' code of the MassStorageFile
#        This currently will _NOT_ work as the code in Ganga/GPIDev/Lib/File/OutputFileManager.py has been written with too much special cases for
#        among other things the Local backend and LocalFile file objects.
#        These special cases may have been needed in the past but are only getting in the way of development now and should ideally go away ASAP!
#        The changes required to fix this are invasive and effect pretty much 100% of users but this code is being included in a PR before 6.2.0
#        Ideally these tests would be enabled prior to 6.3.0 so that we can test the MassStorageFile completely in an automated way
#
#
#    def test_g_testClientSideSubmit(self):
#        """Test the client side code whilst stil using the Local backend"""
#
#        from Ganga.GPI import LocalFile, MassStorageFile, Job, ArgSplitter
#
#        from Ganga.Utility.Config import setConfigOption
#        setConfigOption('Output', 'MassStorageFile', TestMassStorage.MassStorageTestConfig2)
#
#        _ext = '.root'
#        file_1 = generateUniqueTempFile(_ext)
#        file_2 = generateUniqueTempFile(_ext)
#        
#        j = Job()
#        j.inputfiles = [LocalFile(file_1), LocalFile(file_2)]
#        j.splitter = ArgSplitter(args = [[_] for _ in range(0, TestMassStorage.sj_len) ])
#        j.outputfiles = [MassStorageFile(namePattern='*'+_ext)]
#        j.submit()
#
#    def test_h_testClientSideComplete(self):
#        """Test the client side code whilst stil using the Local backend"""
#
#        from Ganga.GPI import jobs
#
#        from GangaTest.Framework.utils import sleep_until_completed
#        from Ganga.Utility.Config import setConfigOption
#        setConfigOption('Output', 'MassStorageFile', TestMassStorage.MassStorageTestConfig2)
#
#        j = jobs(3)
#
#        sleep_until_completed(j)
#
#        assert j.status == 'completed'
#
