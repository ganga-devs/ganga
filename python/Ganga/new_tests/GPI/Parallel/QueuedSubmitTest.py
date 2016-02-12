from __future__ import absolute_import, print_function

from ..GangaUnitTest import GangaUnitTest

import time

global_num_threads = 20
global_num_jobs = global_num_threads*5

class QueuedSubmitTest(GangaUnitTest):

    def setUp(self):
        extra_opts = [('Queues', 'NumWorkerThreads', global_num_threads)]
        super(QueuedSubmitTest, self).setUp(extra_opts=extra_opts)
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_TestNumThreads(self):
        from Ganga.GPI import queues
        num_threads = len(queues._user_threadpool.worker_status())
        print("num_threads: %s" % str(num_threads))
        assert(num_threads == global_num_threads)

    def test_b_SetupJobs(self):
        from Ganga.GPI import Job, jobs

        ## Just incase, I know this shouldn't be here, but if the repo gets polluted this is a sane fix
        for j in jobs:
            try:
                j.remove()
            except:
                pass

        for i in range(global_num_jobs):
            j=Job()

        print("job len: %s" % str(len(jobs)))

        assert(len(jobs) == global_num_jobs)

    def test_c_QueueSubmits(self):
        from Ganga.GPI import jobs, queues

        for j in jobs:
            queues.add(j.submit)

        while queues.totalNumUserThreads() > 0:
            time.sleep(1.)

        ## All user threads should have terminated by now
        for j in jobs:
            assert(j.status != "new")

    def test_d_Finished(self):
        from Ganga.GPI import jobs, queues

        time.sleep(3.)

        while queues.totalNumIntThreads() > 0:
            time.sleep(1.)

        for j in jobs:
            assert(j.status == "completed")


    def test_e_Cleanup(self):
        from Ganga.GPI import jobs

        for j in jobs:
            try:
                j.remove()
            except:
                pass

