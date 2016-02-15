from __future__ import absolute_import, print_function

from ..GangaUnitTest import GangaUnitTest

import time

global_num_threads = 5
global_num_jobs = global_num_threads*5

class TestQueuedDiracSubmit(GangaUnitTest):

    def setUp(self):
        extra_opts = [('Queues', 'NumWorkerThreads', global_num_threads)]
        super(TestQueuedDiracSubmit, self).setUp(extra_opts=extra_opts)
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_TestNumThreads(self):
        from Ganga.GPI import queues
        num_threads = len(queues._user_threadpool.worker_status())
        print("num_threads: %s" % str(num_threads))
        assert(num_threads == global_num_threads)

    def test_b_SetupJobs(self):
        from Ganga.GPI import Job, jobs, Dirac

        ## Just incase, I know this shouldn't be here, but if the repo gets polluted this is a sane fix
        for j in jobs:
            try:
                j.remove()
            except:
                pass

        for i in range(global_num_jobs):
            j=Job(backend=Dirac())

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

    def test_d_update(self):
        from Ganga.GPI import jobs, queues, Dirac
        from Ganga.GPIDev.Base.Proxy import stripProxy

        for j in jobs:
            ## ala Ganga/GPIDev/Adapters/IBackend
            queues.add(stripProxy(Dirac).master_updateMonitoringInformation, ([stripProxy(j)],))

        while queues.totalNumUserThreads() > 0:
            time.sleep(1.)

        ## Dirac backend updates the statusInfo to always be something not '' when the monitoring has updated correctly
        for j in jobs:
            assert(j.backend.statusInfo != '')

        ## Flush any waiting/pending 'completing' tasks
        queues._stop_all_threads(True)
        queues.lock()
        while queues.totalNumIntThreads() > 0:
            time.sleep(1.)

    def test_e_Cleanup(self):
        from Ganga.GPI import jobs, queues

        ## Flush any waiting/pending 'completing' tasks
        queues._stop_all_threads(True)
        queues.lock()
        while queues.totalNumIntThreads() > 0:
            time.sleep(1.)

        for j in jobs:
            try:
                j.remove()
            except:
                pass

