##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestRootMerger.py,v 1.2 2009-03-18 10:46:01 wreece Exp $
##########################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, write_file
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
import os


class TestLHCbMetaDataChecker(GangaGPITestCase):

    """
    Test the LHCbMetaDataChecker for protection against bad input and do a standard check.
    """

    def __init__(self):
        self.c = LHCbMetaDataChecker()
        self.davinci_job = None
        self.exe_job = None

    def setUp(self):
        self.exe_job = Job()
        self.davinci_job = Job(application=DaVinci())

        self.exe_job.submit()
        if not sleep_until_completed(self.exe_job):
            assert False, 'Test timed out'
        assert self.exe_job.status == 'completed'

        self.davinci_job.submit()
        if not sleep_until_completed(self.davinci_job):
            assert False, 'Test timed out'
        assert self.davinci_job.status == 'completed'

    def checkFail(self, job, message):
        try:
            self.c.check(job)
        except PostProcessException:
            pass
        else:
            assert False, 'Should have thrown exception: ' + message

    def testLHCbMetaDataChecker_badInput(self):

        self.checkFail(self.davinci_job, 'no expression set')

        self.c.expression = 'will not recognise'

        self.checkFail(self.davinci_job, 'should not recognise expression')

        self.c.expression = '4'

        self.checkFail(self.davinci_job, 'not an expression')

        self.c.expression = 'inputevents'

        self.checkFail(self.exe_job, 'should not recognise inputevents')

    def testLHCbMetaDataChecker_standardCheck(self):

        self.c.expression = 'inputevents == 0'
        assert self.c.check(self.davinci_job)

        self.c.expression = 'outputevents > 0'
        assert not self.c.check(self.davinci_job)

