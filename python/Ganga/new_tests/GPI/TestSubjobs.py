from __future__ import absolute_import
from .GangaUnitTest import GangaUnitTest
import random
import string


class TestSubjobs(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestSubjobs, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def testLargeJobSubmission(self):
        """
        Create lots of subjobs and submit it
        """
        from Ganga.GPI import Job, GenericSplitter, Local
        j = Job()
        j.application.exe = "sleep"
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['400'] for _ in range(0, 20)]
        j.backend = Local()
        j.submit()

        assert len(j.subjobs) == 20

    def testSetParentOnLoad(self):
        """
        Test that the parents are set correctly on load
        """
        from Ganga.GPI import jobs, queues, Executable, Local
        from Ganga.GPIDev.Base.Proxy import isType

        def flush_full_job():
            mj = jobs(0)
            mj.comment = "Make sure I'm dirty " + ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
            mj._impl._getRegistry()._flush([j])

        # Make sure the main job is fully loaded
        j = jobs(0)
        assert isType(j.application, Executable)
        assert isType(j.backend, Local)
        assert j.application.exe == "sleep"

        # fire off a load of threads to flush
        for i in range(0, 20):
            queues.add(flush_full_job)

        # Now loop over and force the load of all the subjobs
        for sj in j.subjobs:
            assert sj.splitter is None
            assert isType(sj.application, Executable)
            assert isType(sj.backend, Local)
            assert sj.application.exe == "sleep"
            assert sj.application.args == ['400']
            assert sj._impl._getRoot() is j._impl
