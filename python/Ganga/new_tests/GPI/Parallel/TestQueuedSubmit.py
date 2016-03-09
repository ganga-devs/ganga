from __future__ import absolute_import, print_function

from ..GangaUnitTest import GangaUnitTest

import time

global_num_threads = 20
global_num_jobs = global_num_threads*5


class TestQueuedSubmit(GangaUnitTest):

    def setUp(self):
        extra_opts = [('Queues', 'NumWorkerThreads', global_num_threads)]
        super(TestQueuedSubmit, self).setUp(extra_opts=extra_opts)
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_TestNumThreads(self):
        from Ganga.GPI import queues
        num_threads = len(queues._user_threadpool.worker_status())
        print("num_threads: %s" % num_threads)
        assert num_threads == global_num_threads

    def test_b_SetupJobs(self):
        from Ganga.GPI import Job, jobs, Executable

        # Just in-case, I know this shouldn't be here, but if the repo gets polluted this is a sane fix
        for j in jobs:
            try:
                j.remove()
            except:
                pass

        for i in range(global_num_jobs):
            print('creating job', end=' ')
            j = Job()
            print('%s' % j.id)

        print('job len: %s' % len(jobs))

        assert len(jobs) == global_num_jobs

    def test_c_QueueSubmits(self):
        from Ganga.GPI import jobs, queues

        for j in jobs:
            print('adding job %s to queue for submission' % j.id)
            queues.add(j.submit)

        while queues.totalNumUserThreads() > 0:
            print('remaining threads: %s' % queues.totalNumUserThreads())
            time.sleep(1)

        print('remaining threads: %s' % queues.totalNumUserThreads())

        # All user threads should have terminated by now
        for j in jobs:
            print('checking job %s' % j.id)
            assert j.status != 'new'

    def test_d_Finished(self):
        from Ganga.GPI import jobs, queues
        from GangaTest.Framework.utils import sleep_until_completed

        print('waiting on job', end=' ')
        for j in jobs:
            if j.status in ['submitted', 'running', 'completing']:
                print('%s' % j.id, end=' ')
                import sys
                sys.stdout.flush()

                assert sleep_until_completed(j), 'Timeout on job submission: job is still not finished'
            assert j.status == 'completed'

    def test_e_Cleanup(self):
        from Ganga.GPI import jobs

        for j in jobs:
            try:
                j.remove()
            except:
                pass

