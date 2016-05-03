from __future__ import absolute_import

import os
import tempfile

from ..GangaUnitTest import GangaUnitTest


class TestCustomChecker(GangaUnitTest):

    def setUp(self):
        super(TestCustomChecker, self).setUp()
        from Ganga.GPI import Job, CustomChecker
        from GangaTest.Framework.utils import sleep_until_completed
        self.c = CustomChecker()
        self.j = None
        self.file_name_stdout = None
        self.file_name_fail = None

        # write string to tmpfile
        self.j = Job()
        self.j.submit()
        self.assertTrue(sleep_until_completed(self.j), 'Timeout on job submission: job is still not finished')
        self.assertEqual(self.j.status, 'completed')

        file_obj, file_name = tempfile.mkstemp()
        os.close(file_obj)
        os.unlink(file_name)
        os.mkdir(file_name)

        self.file_name_stdout = os.path.join(file_name, 'check_stdout.py')
        with open(self.file_name_stdout, 'w') as module_stdout:
            module_stdout.write("""import os
def check(j):
        stdout = os.path.join(j.outputdir,'stdout')
        return os.path.exists(stdout)
""")

        self.file_name_fail = os.path.join(file_name, 'check_fail.py')
        with open(self.file_name_fail, 'w') as module_fail:
            module_fail.write('will not run')

    def checkFail(self, message):
        from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
        try:
            self.c.check(self.j)
        except PostProcessException:
            pass
        else:
            self.fail('Should have thrown exception: ' + message)

    def testCustomChecker_badInput(self):

        self.checkFail('no module specified')

        self.c.module = '/not/a/file'

        self.checkFail('file does not exist')

        self.c.module = self.file_name_fail

        self.checkFail('module will not run')

    def testCustomChecker_standardCheck(self):

        print('%s' % str(self.file_name_stdout))
        print('%s' % str(self.file_name_fail))
        self.c.module = self.file_name_stdout
        print('%s' % type(self.c.module))
        assert self.c.check(self.j)
