from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestFileChecker(GangaUnitTest):
    def setUp(self):
        super(TestFileChecker, self).setUp()
        from Ganga.GPI import Job, FileChecker, Executable, Local
        from GangaTest.Framework.utils import sleep_until_completed

        self.c = FileChecker()
        self.jobslice = []
        args = ['1', '2', '12']
        for arg in args:
            j = Job(application=Executable(), backend=Local())
            # write string to tmpfile
            j.application.args = [arg]
            self.jobslice.append(j)

        for j in self.jobslice:
            j.submit()
            self.assertTrue(sleep_until_completed(j), 'Timeout on job submission: job is still not finished')
            self.assertEqual(j.status, 'completed')

    def checkFail(self, message):
        from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
        try:
            self.c.check(self.jobslice[0])
        except PostProcessException:
            pass
        else:
            self.fail('Should have thrown exception: ' + message)

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
        self.c.check(self.jobslice[0])
        self.c.check(self.jobslice[1])
        self.c.check(self.jobslice[2])

        self.c.searchStrings = ['1', '2']

        self.c.check(self.jobslice[0])
        self.c.check(self.jobslice[1])
        self.c.check(self.jobslice[2])
