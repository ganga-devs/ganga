from __future__ import absolute_import
from Ganga.testlib.GangaUnitTest import GangaUnitTest

import tempfile
import datetime
import time
import random
import os

outputFilePath = '/tmp/MassStorage'

MassStorageTestConfig = {'defaultProtocol': 'file://',
                         'fileExtensions': [''],
                         'uploadOptions': {'path': outputFilePath, 'cp_cmd': 'cp', 'ls_cmd': 'ls', 'mkdir_cmd': 'mkdir'},
                         'backendPostprocess': {'LSF': 'WN', 'LCG': 'client', 'ARC': 'client', 'Dirac': 'client',
                                                'PBS': 'WN', 'Interactive': 'client', 'Local': 'client', 'CREAM': 'client'}}

sj_len = 3

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

    return returnableName

class TestMassStorage(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    def setUp(self):
        """
        Configure the MassStorageFile for the test
        """
        super(TestMassStorage, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
        setConfigOption('Output', 'MassStorageFile', MassStorageTestConfig)
        setConfigOption('Output', 'FailJobIfNoOutputMatched', True)

    def test_a_Submit(self):
        """Test the ability to submit a job with some LocalFiles"""
        from Ganga.GPI import Job, LocalFile, MassStorageFile

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

        assert len(j.inputfiles) == 1
        assert len(j.outputfiles) == 1
        #assert len(stripProxy(j.outputfiles[0]).subfiles) == 1

        print("namePattern: %s" % (j.outputfiles[0].namePattern, )) 
        print("Locations: %s" % (j.outputfiles[0].locations, ))
        print("accessURL: %s" % (j.outputfiles[0].accessURL(), ))

        print("on disk: %s" % str(os.listdir(outputFilePath)))

        output_dir = os.path.join(outputFilePath, '0')
        assert os.path.isdir(output_dir)
        assert os.path.isfile(os.path.join(output_dir, j.inputfiles[0].namePattern))

    def test_c_SplitJob(self):
        """Test submitting subjobs"""
        from Ganga.GPI import Job, LocalFile, MassStorageFile, ArgSplitter

        _ext = '.txt2'

        file_1 = generateUniqueTempFile(_ext)

        j = Job()
        j.inputfiles = [LocalFile(file_1)]
        j.splitter = ArgSplitter(args = [[_] for _ in range(0, sj_len) ])
        j.outputfiles = [MassStorageFile(namePattern='*'+_ext, outputfilenameformat='{jid}/{sjid}/{fname}')]
        j.submit()

    def test_d_CompletedSJ(self):

        from Ganga.GPI import jobs
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from GangaTest.Framework.utils import sleep_until_completed

        j = jobs(1)

        sleep_until_completed(j)

        assert len(j.subjobs) == sj_len

        output_dir = os.path.join(outputFilePath, '1', '0')
        assert os.path.isdir(output_dir)
        assert os.path.isfile(os.path.join(output_dir, j.inputfiles[0].namePattern))


