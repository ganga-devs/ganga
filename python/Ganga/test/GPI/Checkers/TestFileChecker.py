################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestRootMerger.py,v 1.2 2009-03-18 10:46:01 wreece Exp $
################################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,write_file
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
import os

class TestFileChecker(GangaGPITestCase):
    """
    Test the file checker for protection against bad input and do a standard check.
    """
    def __init__(self):
        self.c = FileChecker()
        self.jobslice = []

    def setUp(self):
        args = ['1','2','12']
        for arg in args:
            j = Job(application=Executable(),backend=Local())
            #write string to tmpfile
            j.application.args = [arg]
            self.jobslice.append(j)

        for j in self.jobslice:
            j.submit()
            if not sleep_until_completed(j):
                assert False, 'Test timed out' 
            assert j.status == 'completed'

    def checkFail(self,message):
        try: self.c.check(self.jobslice[0])
        except PostProcessException:
            pass
        else:
            assert False, 'Should have thrown exception: '+message

    def testFileChecker_badInput(self):

        self.c.files = ['stdout']

        self.checkFail('no searchString sepcified')

        self.c.files = []
        self.c.searchStrings = ['buttary']

        self.checkFail('no files specified')

        self.c.files = ['not_a_file']

        self.checkFail('file does not exist')

    def testFileChecker_standardCheck(self):

        self.c.files = ['stdout']
        self.c.searchStrings = ['1']
        self.c.failIfFound = False
        assert self.c.check(self.jobslice[0])
        assert not self.c.check(self.jobslice[1])
        assert self.c.check(self.jobslice[2])
        
        self.c.searchStrings = ['1','2']
        
        assert not self.c.check(self.jobslice[0])
        assert not self.c.check(self.jobslice[1])
        assert self.c.check(self.j.jobslice[2])



