from __future__ import absolute_import, print_function

from ..GangaUnitTest import GangaUnitTest

import time

global_num_threads = 5
global_num_jobs = global_num_threads*5


class TestQueuedSJSubmit(GangaUnitTest):

    def setUp(self):
        extra_opts = [('Queues', 'NumWorkerThreads', global_num_threads)]
        super(TestQueuedSJSubmit, self).setUp(extra_opts=extra_opts)
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_TestNumThreads(self):
        from Ganga.GPI import queues
        num_threads = len(queues._user_threadpool.worker_status())
        print("num_threads: %s" % num_threads)
        assert num_threads == global_num_threads

    def test_b_SetupJobs(self):
        from Ganga.GPI import Job, jobs, Executable, ArgSplitter

        # Just in-case, I know this shouldn't be here, but if the repo gets polluted this is a sane fix
        for j in jobs:
            try:
                j.remove()
            except:
                pass

        for i in range(global_num_jobs):
            print('creating job', end=' ')
            j = Job(splitter=ArgSplitter(args=[[0], [1], [2]]))
            print(j.id)

        print('job len:', len(jobs))

        assert len(jobs) == global_num_jobs

    def test_c_QueueSubmits(self):
        from Ganga.GPI import jobs, queues

        for j in jobs:
            print('adding job', j.id, 'to queue for submission')
            queues.add(j.submit)

        while queues.totalNumUserThreads() > 0:
            print('remaining threads:', queues.totalNumUserThreads())
            time.sleep(1)

        print('remaining threads:', queues.totalNumUserThreads())

        # All user threads should have terminated by now
        for j in jobs:
            print('checking job', j.id)
            assert j.status != 'new'

    def test_d_Finished(self):
        from Ganga.GPI import jobs, queues
        from GangaTest.Framework.utils import sleep_until_completed

        print('waiting on job', end=' ')
        for j in jobs:
            print(j.id, end=' ')
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
