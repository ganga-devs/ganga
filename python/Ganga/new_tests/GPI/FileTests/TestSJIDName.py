from __future__ import absolute_import
from ..GangaUnitTest import GangaUnitTest

class TestSJIDName(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSJIDName, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_jobSubmit(self):
        from Ganga.GPI import Job, Executable, ArgSplitter, MassStorageFile

        j=Job()
        j.application=Executable(exe='touch')
        j.splitter=ArgSplitter(args=[['abc.txt'], ['def.txt']])
        j.outputfiles=[MassStorageFile(outputfilenameformat = '/test/{sjid}-{fname}', namePattern = '*.txt')]
        j.submit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(j)

    def test_b_jobResubmit(self):
        from Ganga.GPI import jobs

        jobs(0).resubmit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(0))

    def test_c_onlyCreate(self):

        from Ganga.GPI import Job, Executable, ArgSplitter, MassStorageFile

        j=Job()
        j.application=Executable(exe='touch')
        j.splitter=ArgSplitter(args=[['abc.txt'], ['def.txt']])
        j.outputfiles=[MassStorageFile(outputfilenameformat = '/test/{sjid}-{fname}', namePattern = '*.txt')]

    def test_d_loadSubmit(self):

        from Ganga.GPI import jobs

        jobs(1).submit()

        from GangaTest.Framework.utils import sleep_until_completed
        sleep_until_completed(jobs(1))

