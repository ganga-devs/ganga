from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Core.GangaThread.WorkerThreads.WorkerThreadPool import WorkerThreadPool
from Ganga.GPI import *
# GangaTest.Framework.utils defines some utility methods
# from GangaTest.Framework.utils import file_contains#,
# sleep_until_completed,sleep_until_state
import unittest
import tempfile
import os
from Ganga.Utility.Config import getConfig

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


class TestWorkerThreadPool(GangaGPITestCase):

    def setUp(self):
        self.w = WorkerThreadPool()

    def test__init__(self):
        from Queue import PriorityQueue

        def constructor_test(w,
                             num_worker_threads=getConfig(
                                 'Queues')['NumWorkerThreads'],
                             worker_thread_prefix='Worker_'):
            self.assertTrue(
                isinstance(w._WorkerThreadPool__queue, PriorityQueue))
            self.assertTrue(w._WorkerThreadPool__queue.empty())
            self.assertEqual(
                len(w._WorkerThreadPool__worker_threads), num_worker_threads)

            i = 0
            for worker in w._WorkerThreadPool__worker_threads:
                self.assertEqual(worker._name, worker_thread_prefix + str(i))
                self.assertEqual(worker._command, 'idle')
                self.assertEqual(worker._timeout, 'N/A')
                i = i + 1

        # test default
        constructor_test(self.w)

        # test positional
        w1 = WorkerThreadPool(3, 'Hello_')
        constructor_test(w1, 3, 'Hello_')

        # test keyword
        w2 = WorkerThreadPool(worker_thread_prefix='World_',
                              num_worker_threads=2)
        constructor_test(w2, 2, 'World_')

    def test__worker_thread(self):
        from Ganga.Core.GangaThread.WorkerThreads.WorkerThreadPool import QueueElement, CommandInput, FunctionInput
        from Ganga.Utility.logging import getLogger
        import datetime
        import difflib
        ###################################################
        w = WorkerThreadPool(0)

        class dummythread(object):

            def __init__(self, name='test_thread'):
                self.name = name

            def should_stop(self):
                if not hasattr(self, '_stop'):
                    logger.info("Entering the loop for the first time.")
                    self._stop = True
                    return False
                logger.info("second time round the loop exiting.")
                return self._stop

            def register(self):
                self._registered = datetime.datetime.now()

            def unregister(self):
                self._unregistered = datetime.datetime.now()

        error_chk = []

        def error(this, message, *args, **kwargs):
            expected = error_chk.pop(0)
            if message != expected:
                logger.info('#####################################')
                logger.info('####             DIFF            ####')
                logger.info('#####################################')
            for line in difflib.unified_diff(expected.splitlines(), message.splitlines(), fromfile='expected', tofile='found'):
                logger.info(line)
            else:
                if message != expected:
                    logger.info('#####################################')
            self.assertEqual(message, expected)

        setattr(getLogger().__class__, 'error', error)

        ##################################################

        error_chk.append("Unrecognised queue element: '%s'" % repr('bob'))
        error_chk.append("                  expected: 'QueueElement'")
        d = dummythread()
        w._WorkerThreadPool__queue.put('bob')
        w._WorkerThreadPool__worker_thread(d)
        self.assertTrue(hasattr(d, '_registered'))
        self.assertTrue(hasattr(d, '_unregistered'))
        self.assertTrue(d._registered < d._unregistered)
        self.assertTrue(w._WorkerThreadPool__queue.empty())
        self.assertTrue(len(error_chk) == 0)

        #######################################################

        q = QueueElement(5, 'bob', None, None)
        error_chk.append(
            "Unrecognised input command type: '%s'" % repr(q.command_input))
        error_chk.append(
            "                       expected: ('FunctionInput' or 'CommandInput')")
        d = dummythread()
        w._WorkerThreadPool__queue.put(q)
        w._WorkerThreadPool__worker_thread(d)
        self.assertTrue(hasattr(d, '_registered'))
        self.assertTrue(hasattr(d, '_unregistered'))
        self.assertTrue(d._registered < d._unregistered)
        self.assertTrue(w._WorkerThreadPool__queue.empty())
        self.assertTrue(len(error_chk) == 0)

        #######################################################

        def test_func(arg0, arg1, arg2):
            self.assertEqual(arg0, 12)
            self.assertEqual(type(arg1), dummythread)
            self.assertEqual(arg2, 42)
            self.assertEqual(arg1._command, 'test_func')
            raise Exception('help!')

        d = dummythread()
        import sys
        import os
        # try:
        #error_chk.append("Exception raised executing 'test_func' in Thread 'test_thread':\n%s" % 'l')
        error_chk.append('Exception raised executing \'test_func\' in Thread \'test_thread\':\nTraceback (most recent call last):\n  File "%s", line 92, in __worker_thread\n    result = item.command_input.function(*item.command_input.args, **item.command_input.kwargs)\n  File "TestWorkerThreadPool.py", line 114, in test_func\n    raise Exception(\'help!\')\nException: help!\n' %
                         os.path.join(os.getcwd(), sys.modules['Ganga.Core.GangaThread.WorkerThreads.WorkerThreadPool'].__file__).replace('WorkerThreadPool.pyc', 'WorkerThreadPool.py'))
        # except:
        #    pass

        q = QueueElement(5,
                         FunctionInput(test_func, (12, d), {'arg2': 42}),
                         FunctionInput(None, None, None),
                         FunctionInput(None, None, None))
        w._WorkerThreadPool__queue.put(q)
        w._WorkerThreadPool__worker_thread(d)
        self.assertTrue(hasattr(d, '_registered'))
        self.assertTrue(hasattr(d, '_unregistered'))
        self.assertTrue(d._registered < d._unregistered)
        self.assertTrue(w._WorkerThreadPool__queue.empty())
        self.assertEqual(d._command, 'idle')
        self.assertTrue(len(error_chk) == 0)
