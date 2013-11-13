from GangaTest.Framework.tests                     import GangaGPITestCase
from GangaDirac.Lib.Server.WorkerThreadPool        import WorkerThreadPool
from Ganga.GPI                                     import *
#GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import file_contains#, sleep_until_completed,sleep_until_state
import unittest, tempfile, os
from Ganga.Utility.Config import getConfig

class TestWorkerThreadPool(GangaGPITestCase):
    def setUp(self):
        self.w = WorkerThreadPool()
 
    def test__init__(self):
        from Queue import PriorityQueue

        def constructor_test( w,
                              num_worker_threads = getConfig('DIRAC')['NumWorkerThreads'],
                              worker_thread_prefix = 'Worker_' ):
            self.assertTrue(isinstance(w._WorkerThreadPool__queue, PriorityQueue))
            self.assertTrue(w._WorkerThreadPool__queue.empty())
            self.assertEqual(len(w._WorkerThreadPool__worker_threads), num_worker_threads )
        
            i=0
            for worker in w._WorkerThreadPool__worker_threads:
                print worker._name, i
                self.assertEqual(worker._name, worker_thread_prefix + str(i))
                self.assertEqual(worker._command, 'idle')
                self.assertEqual(worker._timeout, 'N/A')
                i=i+1

        # test default
        constructor_test(self.w)

        # test positional
        w1 = WorkerThreadPool(3, 'Hello_')
        constructor_test(w1, 3, 'Hello_')
        
        # test keyword
        w2 = WorkerThreadPool( worker_thread_prefix = 'World_',
                               num_worker_threads = 2 )
        constructor_test(w2, 2, 'World_')

    def test__worker_thread(self):
        from Ganga.Utility.logging import getLogger
        error_chk = ''
        def error(this, message, *args, **kwargs):
            self.assertEqual(message, error_chk)

        setattr(getLogger().__class__, 'error', error)

        error_chk = 'Unrecognised queue element'
        self.w._WorkerThreadPool__queue.put('bob')
