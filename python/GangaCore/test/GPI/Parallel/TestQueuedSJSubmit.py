from __future__ import absolute_import, print_function

import time

import pytest

from GangaCore.testlib.decorators import add_config
from GangaCore.testlib.monitoring import run_until_completed

global_num_threads = 5
global_num_jobs = global_num_threads * 5


@add_config([('TestingFramework', 'AutoCleanup', False),
             ('Queues', 'NumWorkerThreads', global_num_threads)])
@pytest.mark.usefixtures('gpi')
class TestQueuedSJSubmit(object):

    def test_a_TestNumThreads(self):
        from GangaCore.GPI import queues
        num_threads = len(queues._user_threadpool.worker_status())
        print("num_threads: %s" % num_threads)
        assert num_threads == global_num_threads

    def test_b_SetupJobs(self):
        from GangaCore.GPI import Job, jobs, Executable, ArgSplitter

        for i in range(global_num_jobs):
            print('creating job', end=' ')
            j = Job(splitter=ArgSplitter(args=[[0], [1], [2]]))
            print(j.id)

        print('job len:', len(jobs))

        assert len(jobs) == global_num_jobs

    def test_c_QueueSubmits(self):
        from GangaCore.GPI import jobs, queues

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
        from GangaCore.GPI import jobs, queues

        print('waiting on job', end=' ')
        for j in jobs:
            print(j.id, end=' ')
            import sys
            sys.stdout.flush()
            assert run_until_completed(j, sleep_period=0.1), 'Timeout on job submission: job is still not finished'
