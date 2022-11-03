import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import traceback

from GangaCore.Core.GangaRepository.Registry import RegistryKeyError, RegistryLockError
from GangaCore.Core.GangaThread import GangaThread
from GangaCore.GPIDev.Base.Proxy import getName, stripProxy
from GangaCore.GPIDev.Lib.Job.Job import lazyLoadJobBackend, lazyLoadJobStatus
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger

config = getConfig("PollThread")
THREAD_POOL_SIZE = config['update_thread_pool_size']
POLL_RATE = config['base_poll_rate']  # in seconds
log = getLogger()


async def log_exceptions(awaitable):
    try:
        return await awaitable
    except Exception:
        traceback.print_exc()


class AsyncMonitoringService(GangaThread):
    def __init__(self, registry_slice=None):
        GangaThread.__init__(self, name="AsyncMonitoringService")
        self.daemon = True
        self.loop = asyncio.new_event_loop()
        self.enabled = False
        self.alive = True
        self.registry_slice = registry_slice
        self.active_backends = {}
        self.scheduled_backend_checks = {}
        self.thread_executor = None

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.enabled = True
        self.thread_executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
        self._check_active_backends()
        self.loop.run_forever()

    def _check_active_backends(self, job_slice=None):
        if not self.enabled:
            return

        if job_slice:
            fixed_ids = job_slice.ids()
        else:
            fixed_ids = self.registry_slice.ids()

        found_active_backends = {}
        for i in fixed_ids:
            try:
                # This is safe as it's addressing a job which _better_ be in the job repo
                j = stripProxy(self.registry_slice(i))
                job_status = lazyLoadJobStatus(j)
                if job_status in ['submitted', 'running'] or (j.master and (job_status in ['submitting'])):
                    backend_obj = lazyLoadJobBackend(j)
                    backend_name = getName(backend_obj)
                    found_active_backends.setdefault(backend_name, [])
                    found_active_backends[backend_name].append(j)

            except RegistryKeyError as err:
                log.debug("RegistryKeyError: The job was most likely removed")
                log.debug("RegError %s" % str(err))
            except RegistryLockError as err:
                log.debug("RegistryLockError: The job was most likely removed")
                log.debug("Reg LockError%s" % str(err))

        # If a backend is newly found as active, trigger its monitoring
        previously_active_backends = self.active_backends
        self.active_backends = found_active_backends
        for backend_name in self.active_backends:
            if backend_name not in previously_active_backends:
                log.debug(f'Adding {backend_name} to list of backends to monitor.')
                self._check_backend(backend_obj)

        self._log_backend_summary(found_active_backends)

        if previously_active_backends:
            self._cleanup_finished_backends(previously_active_backends, found_active_backends)

        self.loop.call_later(POLL_RATE, self._check_active_backends)

    def _log_backend_summary(self, active_backends):
        summary = "{"
        for backend, these_jobs in active_backends.items():
            summary += '"' + str(backend) + '" : ['
            for this_job in these_jobs:
                summary += str(stripProxy(this_job).id) + ', '  # getFQID('.')) + ', '
            summary += '], '
        summary += '}'
        log.debug(f"Active backends: {summary}")

    def run_monitoring_task(self, monitoring_task, jobs):
        if not self.enabled:
            return
        if asyncio.iscoroutinefunction(monitoring_task):
            self.loop.create_task(log_exceptions(monitoring_task(jobs)))
        else:
            self.loop.create_task(self._run_in_threadpool(monitoring_task, jobs))

    def _check_backend(self, backend):
        if not self.enabled:
            return
        backend_name = getName(backend)
        if backend_name not in self.active_backends:
            return

        job_slice = self.active_backends[backend_name]
        log.debug(f'Checking backend {backend_name}')
        backend.master_updateMonitoringInformation(job_slice)
        self._schedule_next_backend_check(backend)

    def _schedule_next_backend_check(self, backend):
        backend_name = getName(backend)
        if backend in config:
            pRate = config[backend_name]
        else:
            pRate = config['default_backend_poll_rate']
        timer_handle = self.loop.call_later(pRate, self._check_backend, backend)
        self.scheduled_backend_checks.setdefault(backend_name, [])
        self.scheduled_backend_checks[backend_name].append(timer_handle)

    async def _run_in_threadpool(self, task, jobs):
        await self.loop.run_in_executor(
            self.thread_executor, functools.partial(task, jobs=jobs))

    def _cleanup_finished_backends(self, previously_active_backends, found_active_backends):
        # Check for backends which have no more jobs to monitor and trigger their cleanup.
        for backend, jobs in previously_active_backends.items():
            if backend not in found_active_backends:
                for job in jobs:
                    if job.status == 'completing':
                        self.loop.call_later(1, self._cleanup_finished_backends,
                                             previously_active_backends, found_active_backends)
                        return
                log.debug(f'Removing {getName(backend)} from active backends')
                backend_obj = lazyLoadJobBackend(jobs[0])
                self._cleanup_backend(backend_obj)

    def _cleanup_backend(self, backend):
        backend_name = getName(backend)
        for timer_handle in self.scheduled_backend_checks[backend_name]:
            timer_handle.cancel()
        del self.scheduled_backend_checks[backend_name]
        try:
            backend.tear_down_monitoring()
        except NotImplementedError:
            pass

    def _cleanup_scheduled_tasks(self):
        scheduled_tasks = [task for task in asyncio.all_tasks(self.loop) if task is not asyncio.current_task(self.loop)]
        for task in scheduled_tasks:
            task.cancel()

    def disable(self):
        if not self.alive:
            log.error("Cannot disable monitoring loop. It has already been stopped")
            return False
        self.thread_executor.shutdown()
        self._cleanup_scheduled_tasks()
        self.enabled = False
        return True

    def enable(self):
        if not self.alive:
            log.error("Cannot start monitoring loop. It has already been stopped")
            return False
        self.enabled = True
        self.thread_executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
        self.loop.call_soon_threadsafe(self._check_active_backends)
        return True

    def stop(self):
        self.alive = False
        self.enabled = False
        self.thread_executor.shutdown()
        self._cleanup_scheduled_tasks()
        self.loop.stop()
