from __future__ import absolute_import
import time
from Ganga.testlib.GangaUnitTest import GangaUnitTest

class TestSJIDName(GangaUnitTest):
    """test for sjid in filename names explain each test"""

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSJIDName, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')
        setConfigOption('Configuration', 'resubmitOnlyFailedSubjobs', 'False')

    def test_a_jobSubmit(self):
        """here for testing a submit"""
        from Ganga.GPI import Job, Executable, ArgSplitter, MassStorageFile

        j=Job()
        j.application=Executable(exe='echo')
        j.splitter=ArgSplitter(args=[['abc.txt'], ['def.txt']])
        j.outputfiles=[MassStorageFile(outputfilenameformat = '/test/{sjid}-{fname}', namePattern = '*.txt')]
        j.submit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j)

    def test_b_jobResubmit(self):
        """here for testing a re-submit"""
        from Ganga.GPI import jobs

        jobs(0).resubmit()

        assert jobs(0).status not in ['completed', 'failed', 'killed']

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(0))

    def test_c_onlyCreate(self):
        """here for testing job create"""
        from Ganga.GPI import Job, Executable, ArgSplitter, MassStorageFile

        j=Job()
        j.application=Executable(exe='echo')
        j.splitter=ArgSplitter(args=[['abc.txt'], ['def.txt']])
        j.outputfiles=[MassStorageFile(outputfilenameformat = '/test/{sjid}-{fname}', namePattern = '*.txt')]

    def test_d_loadSubmit(self):
        """here for testing a loaded submit"""
        from Ganga.GPI import jobs

        jobs(1).submit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(1))

