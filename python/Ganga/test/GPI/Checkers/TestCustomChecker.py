##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestRootMerger.py,v 1.2 2009-03-18 10:46:01 wreece Exp $
##########################################################################
from __future__ import print_function
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
import os
import tempfile


class TestCustomChecker(GangaGPITestCase):

    """
    Test the customchecker for bad input and do a standard check
    """

    def __init__(self):
        self.c = CustomChecker()
        self.j = None
        self.file_name_stdout = None
        self.file_name_fail = None

    def setUp(self):
        args = ['1', '2', '12']
        # write string to tmpfile
        self.j = Job()
        self.j.submit()
        if not sleep_until_completed(self.j):
            assert False, 'Test timed out'
        assert self.j.status == 'completed'

        (file_obj, file_name) = tempfile.mkstemp()
        os.close(file_obj)
        os.unlink(file_name)
        os.mkdir(file_name)

        self.file_name_stdout = os.path.join(file_name, 'check_stdout.py')
        module_stdout = open(self.file_name_stdout, 'w')
        module_stdout.write("""import os
def check(j):
        stdout = os.path.join(j.outputdir,'stdout')
        return os.path.exists(stdout)
""")

        self.file_name_fail = os.path.join(file_name, 'check_fail.py')
        module_fail = open(self.file_name_fail, 'w')
        module_fail.write("will not run")

    def checkFail(self, message):
        try:
            self.c.check(self.j)
        except PostProcessException:
            pass
        else:
            assert False, 'Should have thrown exception: ' + message

    def testCustomChecker_badInput(self):

        self.checkFail('no module specified')

        self.c.module = '/not/a/file'

        self.checkFail('file does not exist')

        self.c.module = self.file_name_fail

        self.checkFail('module will not run')

    def testCustomChecker_standardCheck(self):

        print("%s" % str(self.file_name_stdout))
        print("%s" % str(self.file_name_fail))
        self.c.module = self.file_name_stdout
        print("%s" % type(self.c.module))
        assert self.c.check(self.j)

