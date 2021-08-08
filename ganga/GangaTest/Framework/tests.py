import unittest
import traceback
import io

from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)

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


class FailedTestsException(Exception):
    """The exception shows the failed tests"""
    def __init__(self, failedTests):
        self.failedTests = failedTests

    def __str__(self):
        msg = "\n**** FAIL ****\n\n"
        for name in self.failedTests:
            msg += "%s\nFAIL: %s, please check the error message below\n%s" % (('-' * 40), name[0], name[1])
        msg += '%s' % ('-' * 40)
        return "%s\n**** END ****" % msg

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
        logger.info('\n***** %s' % self.description)
        logger.info('\tLog file: %s' % self.output_path)

        output = open(self.output_path,'w')
        pytf_paths = str.split(os.getenv('PYTF_TOP_DIR'),':')
        pytf_runner = str()
        if type(pytf_paths) is str:
            pytf_runner = os.path.join( pytf_paths, 'cmd_run.sh' )
        else:
            for i in pytf_paths:
                if os.path.isfile( os.path.join( i, 'cmd_run.sh') ):
                    pytf_runner = os.path.join( i, 'cmd_run.sh')
                    break;
        script_runner = [pytf_runner,'{ ' + self.testCmd + '; }']
        process = Popen(script_runner, shell=False, bufsize=0,
            stdin=PIPE, stdout=output, stderr=STDOUT, close_fds=True)
        process.wait()
        output.close()

        
        text = None
        f = open(self.output_path)
        try: text = f.read()
        finally: f.close()

        start_index = text.find("**** FAIL ****")
        end_index = text.find("**** END ****")
        if start_index > -1:start_index = start_index + 15
        err = "%s" % text[start_index:end_index]
        if len(err) > 0:
            raise unittest.TestCase.failureException(err)

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
        self.preparationError = None
        #self.description_org = description

    def run_pytest_function(self):
        logger.info('%s starts to run' % self.method_name)
        try:
            checkTest = getattr(self.pytest_instance, self.method_name)()
        except Exception as preparationError:
            sio = io.StringIO()
            traceback.print_exc(file=sio)
            self.preparationError = sio.getvalue()
            sio.close()
            raise preparationError

        if checkTest:
            self.checkTest = checkTest

    def getCheckTest(self):
        try:
            return self.checkTest
        except AttributeError:
            raise Exception('Failed to get the instance of "CheckTest".(Should get this after running the testcase.)')

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
        self.runCheckError = None
        self.errorTraceback = None

    def runCheckTest(self):
        try:
            if self.timeoutError:
                raise self.timeoutError

            if self.preError:
                raise self.preError

            assert(self.checkTest is not None), 'No instance of checktest, this should happen while the preparation of test is failed.'
        except Exception as error:
            sio = io.StringIO()
            traceback.print_exc(file=sio)
            self.errorTraceback = sio.getvalue()
            sio.close()
            raise error

        try:
            self.checkTest.checkTest()
        except Exception as runCheckError:
            self.runCheckError = runCheckError
            sio = io.StringIO()
            traceback.print_exc(file=sio)
            self.errorTraceback = sio.getvalue()
            sio.close()
            raise runCheckError

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

from pytf.testoob.running.convenience import apply_decorators
from pytf.testoob.extracting import full_extractor as _full_extractor

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
        self.preparationError = None
        self.runCheckError = None
        self.errorTraceback = None

    def runPreparationTestCase(self):
        self._printBegin('runPreparation')
        run_status =  self.runner.run(self.preparationTestCase)
        if self.preparationTestCase.get_fixture().preparationError:
            self.preparationError = self.preparationTestCase.get_fixture().preparationError
        try:
            self.checkTest = self.preparationTestCase.get_fixture().getCheckTest()
        except Exception as e:
            logger.warning("Failed to get the instance of check test, because of the fail of the preparation of test, won't do the check test.")
            self.checkTest = None 
            if run_status:
                logger.warning('The test preparation is done successfully, did you forget to return the instance of check test?')
            run_status = False # Mark the run status as fail, in order to set to finish by driver.py.
        self._printEnd('runPreparation')
        return run_status

    def isFinished(self):
        return self.finished

    def setFinished(self, finished=False):
        self.runner.done()
        self.finished = finished

    def isReadyForCheck(self):
         ready = False
         try:
             if self.checkTest:
                 ready = self.checkTest.isReadyForCheck()
             else:
                 ready = True
         except Exception as e:
             #Change to catch generic exception.
             sio = io.StringIO()
             traceback.print_exc(file=sio)
             self.error = sio.getvalue()
             sio.close()
             #raise e

         #if ready is None:
         #    return False
         #else:
         return ready

    def getError(self):
         if self.erorr:
             return self.error
         else:
             return None

    def setTimeout(self, flag=False):
        self.isTimeout = flag

    def runCheckTest(self):
        run_status = False
        self._printBegin('runCheckTest')
        if self.isTimeout:
            self.timeoutError = unittest.TestCase.failureException("Test Timeout(%s sec). Consider increasing the value of '[TestingFramework]timeout' parameter in your test configuration file" % self.timeout)

        #if self.error is not None:

        checkTestCase = GPIPCheckTestCase(self.checkTest, "CheckTest", self.error, self.timeoutError)
        checkTestSuite = unittest.TestSuite([checkTestCase])

        test_extractor = apply_decorators(_full_extractor, [])

        for fixture in test_extractor(checkTestSuite):
            decorated_fixture = apply_decorators(fixture, self.fixture_decorators)
            run_status = self.runner.run(decorated_fixture)

        if checkTestCase.runCheckError:
            self.runCheckError = checkTestCase.runCheckError

        if checkTestCase.errorTraceback:
            self.errorTraceback = checkTestCase.errorTraceback

        self.runner.done()
        self._printEnd('runCheckTest')
        return run_status

    def _printBegin(self, method):
        logger.info('@@@@ [%s] BEGIN of %s' % (self.testName, method))

    def _printEnd(self, method):
        logger.info('@@@@ [%s] END of %s' % (self.testName, method))
