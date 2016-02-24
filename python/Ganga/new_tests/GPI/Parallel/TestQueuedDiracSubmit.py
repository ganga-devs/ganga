from __future__ import absolute_import, print_function

from ..GangaUnitTest import GangaUnitTest

import time

global_num_threads = 5
global_num_jobs = global_num_threads*5

default_AutoCleanUp = None

class TestQueuedDiracSubmit(GangaUnitTest):

    def setUp(self):
        extra_opts = [('Queues', 'NumWorkerThreads', global_num_threads)]
        super(TestQueuedDiracSubmit, self).setUp(extra_opts=extra_opts)
        global default_AutoCleanUp
        from Ganga.Utility.Config import getConfig
        default_AutoCleanUp = getConfig('TestingFramework')['AutoCleanup']

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

        print("Creating: %s jobs with Dirac backend" % global_num_jobs)
        for i in range(global_num_jobs):
            j=Job(backend=Dirac())

        print("job len: %s" % str(len(jobs)))

        assert(len(jobs) == global_num_jobs)

    def test_c_QueueSubmits(self):
        from Ganga.GPI import jobs, queues

        print("Submitting all jobs via queues")
        for j in jobs:
            queues.add(j.submit)

        print("Waiting for queue to finish processing submit")
        while queues.totalNumUserThreads() > 0:
            time.sleep(1.)

        ## All user threads should have terminated by now
        for j in jobs:
            assert(j.status != "new")

    def test_d_update(self):
        from Ganga.GPI import jobs, queues, Dirac
        from Ganga.GPIDev.Base.Proxy import stripProxy

        print("Forcing a status update on all jobs regardless of their status via queues")
        for j in jobs:
            ## ala Ganga/GPIDev/Adapters/IBackend
            queues.add(stripProxy(Dirac).master_updateMonitoringInformation, ([stripProxy(j)],))

        print("Waiting for all tasks to complete")
        ## Wait for all queues to finish processing this step
        while queues.totalNumUserThreads() > 0:
            time.sleep(1.)

        print("Asserting that all jobs should have had their status updated at least once")
        ## Dirac backend updates the statusInfo to always be something not '' when the monitoring has updated correctly
        for j in jobs:
            assert(j.backend.statusInfo != '')

        print("Delaying shutdown until all running queues have finished")
        ## Flush any waiting/pending 'completing' tasks
        queues._stop_all_threads(True)
        queues.lock()
        while queues.totalNumIntThreads() > 0:
            time.sleep(1.)

    def test_e_Cleanup(self):
        from Ganga.GPI import jobs, queues

        print("Stopping all running queues")
        ## Flush any waiting/pending 'completing' tasks
        queues._stop_all_threads(True)
        queues.lock()
        while queues.totalNumIntThreads() > 0:
            time.sleep(1.)

        print("Removing all jobs in repo")
        for j in jobs:
            try:
                j.remove()
            except:
                pass
        print("job len: %s " % str(len(jobs)))

        from Ganga.Utility.Config import getConfig, setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', default_AutoCleanUp)
        getConfig('Queues').getOption('NumWorkerThreads').revertToDefault()

