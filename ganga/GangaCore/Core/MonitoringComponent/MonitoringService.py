import asyncio
from threading import get_ident

from GangaCore.Core.GangaRepository.Registry import RegistryKeyError, RegistryLockError
from GangaCore.Core.GangaThread import GangaThread
from GangaCore.GPIDev.Base.Proxy import getName, stripProxy
from GangaCore.GPIDev.Lib.Job.Job import lazyLoadJobBackend, lazyLoadJobStatus
from GangaCore.Utility.logging import getLogger

POLL_RATE = 1  # in seconds
log = getLogger()


class AsyncMonitoringService(GangaThread):
    def __init__(self, registry_slice=None):
        GangaThread.__init__(self, name="AsyncMonitoringService")
        self.daemon = True
        self.loop = asyncio.new_event_loop()
        self.enabled = False
        self.alive = True
        self.registry_slice = registry_slice
        self.active_backends = {}

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.enabled = True
        self.loop.call_later(POLL_RATE, self._check_active_backends)
        self.loop.run_forever()

    def _check_active_backends(self, job_slice=None):
        if not self.enabled:
            return
        active_backends = {}

        if job_slice is not None:
            fixed_ids = job_slice.ids()
        else:
            fixed_ids = self.registry_slice.ids()
        for i in fixed_ids:
            try:
                # This is safe as it's addressing a job which _better_ be in the job repo
                j = stripProxy(self.registry_slice(i))

                job_status = lazyLoadJobStatus(j)

                if job_status in ['submitted', 'running'] or (j.master and (job_status in ['submitting'])):
                    if self.enabled is True and self.alive is True:
                        backend_obj = lazyLoadJobBackend(j)
                        backend_name = getName(backend_obj)
                        active_backends.setdefault(backend_name, [])
                        active_backends[backend_name].append(j)

            except RegistryKeyError as err:
                log.debug("RegistryKeyError: The job was most likely removed")
                log.debug("RegError %s" % str(err))
            except RegistryLockError as err:
                log.debug("RegistryLockError: The job was most likely removed")
                log.debug("Reg LockError%s" % str(err))

        summary = '{'
        for backend, these_jobs in active_backends.items():
            summary += '"' + str(backend) + '" : ['
            for this_job in these_jobs:
                summary += str(stripProxy(this_job).id) + ', '  # getFQID('.')) + ', '
            summary += '], '
        summary += '}'

        for backend, these_jobs in active_backends.items():
            backend_obj = these_jobs[0].backend
            stripProxy(backend_obj).master_updateMonitoringInformation(these_jobs)
        self.loop.call_later(POLL_RATE, self._check_active_backends)

    def _cleanup_scheduled_tasks(self):
        scheduled_tasks = [task for task in asyncio.all_tasks(self.loop) if task is not asyncio.current_task(self.loop)]
        for task in scheduled_tasks:
            task.cancel()

    def stop(self):
        self.alive = False
        self.enabled = False
        self._cleanup_scheduled_tasks()
        self.loop.stop()
