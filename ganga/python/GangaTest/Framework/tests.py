import unittest

class GangaGPITestCase(unittest.TestCase):
        """
        Base class for GPI test-cases
        """
        def __init__(self, methodName='runTest'):
                unittest.TestCase.__init__(self,methodName)


        def setUp(self):
                pass

        def tearDown(self):
                pass

        def runTest():
                pass

from pytf.lib import MultipassTest as MP 
class MultipassTest(MP):
        """
        Base class for multi-pass tests
        """
        def setUp(self):
                pass

        def tearDown(self):
                pass


class GangaGPIPTestCase(unittest.TestCase):
    """
    The parent test case of the REAL GPIP tests.
    """
    def __init__(self, testCmd, descr, output_path, report_outputpath, parent_report_name, releaseTesting=False):
        unittest.TestCase.__init__(self,'run_pyunit_testcases')
        self.testCmd = testCmd
        self.description = descr
        self.enableReleaseTesting = releaseTesting
        self.output_path_template = output_path
        self.output_path = output_path % 'ALL'
        self.report_outputpath = report_outputpath
        self.parent_report_name = parent_report_name
        self._initializeTestCmd()

    def _initializeTestCmd(self):
        self.testCmd = '%s --output-path=%s --description=\'%s\'' % (self.testCmd, self.output_path_template, self.description.replace('ALL', '%s'))
        if self.enableReleaseTesting:
            self.testCmd = '%s %s' % (self.testCmd, '--release-test=True')
            self.testCmd = '%s --report-outputpath=%s' % (self.testCmd, self.report_outputpath)
            self.testCmd = '%s --parent-report-name=%s' % (self.testCmd, self.parent_report_name)

    def getDescription(self):
        return self.description

    def shortDescription(self):
        return self.description

    def isReleaseTesting(self):
        self.enableReleaseTesting = True

    def __str__(self):
        return self.description

    def run_pyunit_testcases(self):
        import os
        import sys
        from subprocess import PIPE,Popen,STDOUT
        print '\n**** %s' % self.description
        print '\tLog file: %s' % self.output_path

        #print 'test cmd : %s' % self.testCmd

        output = open(self.output_path,'w')
        pytf_runner = os.path.join(os.getenv('PYTF_TOP_DIR',''),'cmd_run.sh')
        script_runner = [pytf_runner,'{ ' + self.testCmd + '; }']
        process = Popen(script_runner, shell=False, bufsize=0,
            stdin=PIPE, stdout=output, stderr=STDOUT, close_fds=True)
        process.wait()
        output.close()

import time

class GPIPPreparationTestCase(unittest.TestCase):
    """
    The test case for running the test preparation.
    """
    def __init__(self, pytest_instance, description, method_name, timeout, outputpath):
        unittest.TestCase.__init__(self,'run_pytest_function')
        self.pytest_instance = pytest_instance
        self.method_name = method_name
        self.timeout = timeout
        self.outputpath = outputpath
        self.description = description #'%s_preparation' % description
        #self.description_org = description

    def run_pytest_function(self):
        print '%s starts to run' % self.method_name 
        checkTest = getattr(self.pytest_instance, self.method_name)()
        if checkTest:
            self.checkTest = checkTest

    def getCheckTest(self):
        try:
            return self.checkTest
        except AttributeError:
            raise Exception, 'Failed to get the instance of "CheckTest".(Should get this after running the testcase.)'

    def getDescription(self):
        return self.description

    def shortDescription(self):
        return self.description

    def __str__(self):
        return self.description


class GPIPCheckTestCase(unittest.TestCase):
    """
    The test case for running the result check.
    """
    def __init__(self, checkTest, description, preError=None, timeoutError=None):
        unittest.TestCase.__init__(self,'runCheckTest')
        self.checkTest = checkTest
        self.description = description
        self.preError = preError
        self.timeoutError = timeoutError

    def runCheckTest(self):
        if self.timeoutError:
            raise self.timeoutError

        if self.preError:
            raise self.preError

        assert(self.checkTest != None), 'No instance of checktest, this should happen while the preparation of test is failed.'

        self.checkTest.checkTest()

    def getDescription(self):
        return self.description

    def shortDescription(self):
        return self.description

    def __str__(self):
        return self.description

from GangaTest.Framework.utils import is_job_completed
class ICheckTest(object):
    """
    The default ICheckTest interface implementation.
    """
    def __init__(self, j):
        self.j = j
    """Interface class for getting the status of result and checking result"""
    def checkTest(self):
        """This checkTest method will be invoked by the driver"""
        assert(self.j.status == 'completed')
    def isReadyForCheck(self):
        """This check method will be invoked by the driver"""
        return is_job_completed(self.j)

from testoob.running.convenience import apply_decorators
from testoob.extracting import full_extractor as _full_extractor

class SimpleRunnerControl(object):
    """
    The instance of this class will control one real GPIP test case, for running "prepare" -> "monitoring" -> "check result"
    """
    def __init__(self, testName, runner, preparationTestCase, fixture_decorators, timeout):
        self.testName = testName
        self.runner = runner
        self.preparationTestCase = preparationTestCase
        self.fixture_decorators = fixture_decorators
        self.timeout = timeout
        self.isTimeout = False
        self.error = None
        self.timeoutError = None
        self.finished = False

    def runPreparationTestCase(self):
        self._printBegin('runPreparation')
        run_status =  self.runner.run(self.preparationTestCase)
        try:
            self.checkTest = self.preparationTestCase.get_fixture().getCheckTest()
        except Exception, e:
            print 'WARNING: Failed to get the instance of check test, because of the fail of the preparation of test, won\'t do the check test.'
            self.checkTest = None 
            if run_status:
                print 'WARNING: The test preparation is done successfully, did you forget to return the instance of check test?'
            run_status = False # Mark the run status as fail, in order to set to finish by driver.py.
        self._printEnd('runPreparation')
        return run_status

    def isFinished(self):
        return self.finished

    def setFinished(self, finished=False):
        self.runner.done()
        self.finished = finished

    def isReadyForCheck(self):
         ready = None
         try:
             if self.checkTest:
                 ready = self.checkTest.isReadyForCheck()
             else:
                 ready = True
         except AssertionError, e:
             self.error = e
             #raise e

         if ready is None:
             return False
         else:
             return ready

    def getError(self):
         if self.erorr:
             return self.error
         else:
             return None

    def setTimeout(self, flag=False):
        self.isTimeout = flag

    def runCheckTest(self):
        self._printBegin('runCheckTest')
        if self.isTimeout:
            self.timeoutError = unittest.TestCase.failureException("Test Timeout(%s sec). Consider increasing the value of '[TestingFramework]timeout' parameter in your test configuration file" % self.timeout)
        checkTestCase = GPIPCheckTestCase(self.checkTest, "CheckTest", self.error, self.timeoutError)
        checkTestSuite = unittest.TestSuite([checkTestCase])

        test_extractor = apply_decorators(_full_extractor, [])

        for fixture in test_extractor(checkTestSuite):
            decorated_fixture = apply_decorators(fixture, self.fixture_decorators)
            self.runner.run(decorated_fixture)
        self.runner.done()
        self._printEnd('runCheckTest')

    def _printBegin(self, method):
        print '@@@@ [%s] BEGIN of %s' % (self.testName, method)

    def _printEnd(self, method):
        print '@@@@ [%s] END of %s' % (self.testName, method)
