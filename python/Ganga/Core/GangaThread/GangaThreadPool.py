from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig
import time

logger = getLogger('GangaThread')


class GangaThreadPool(object):

    _attributes = ('SHUTDOWN_TIMEOUT')

    # GangaThreadPool singleton instance
    _instance = None

    class SingletonHelper(object):

        def __call__(self, *args, **kw):

            if GangaThreadPool._instance is None:
                object = GangaThreadPool()
                GangaThreadPool._instance = object

            return GangaThreadPool._instance

    getInstance = SingletonHelper()
    shutdown_policy = 'interactive'

    def __init__(self):

        if not GangaThreadPool._instance == None:
            raise RuntimeError(
                'Only one instance of GangaThreadPool is allowed!')

        GangaThreadPool._instance = self

        self.SHUTDOWN_TIMEOUT = 1
        self.__threads = []

    def addServiceThread(self, t):
        #logger.debug('service thread "%s" added to the GangaThreadPool', t.getName())
        ##   HERE TO AVOID AN IMPORT ERROR!
        from Ganga.Core.GangaThread.MTRunner import DuplicateDataItemError
        try:
            self.__threads.append(t)
        except DuplicateDataItemError as e:
            self.logger.debug("%s" % e)

    def delServiceThread(self, t):
        #logger.debug('service thread "%s" deleted from the GangaThreadPool', t.getName())
        try:
            if t in self.__threads:
                self.__threads.remove(t)
        except ValueError as e:
            logger.debug("%s" % e)

    def shutdown(self):
        """Shutdown the Ganga session.

        @param should_wait_cb: A callback function with the following signature
            should_wait_cb(total_time, critical_thread_ids, non_critical_thread_ids)
            where
                total_time is the time in seconds since shutdown started
                critical_thread_ids is a list of alive critical thread names
                non_critical_thread_ids is a list of alive non-critical threads names.
            and
                return value is evaluated as a boolean. 

        A shutdown thread is started that calls stop() on each GangaThread and
        waits for them all to die. A loop waits for the shutdown thread to
        die, periodically calling the should_wait_cb function to ask if it
        should continue to wait or shutdown anyway.

        """

        try:
            self._really_shutdown()
        except Exception as err:
            from Ganga.Utility.logging import getLogger
            logger = getLogger('GangaThread')
            logger.error("Error shutting down thread Pool!")
            logger.error("\n%s" % err)
        return

    def _really_shutdown(self):

        from Ganga.Utility.logging import getLogger
        logger = getLogger('GangaThread')

        logger.debug('shutting down GangaThreadPool with timeout %d sec' % self.SHUTDOWN_TIMEOUT)

        # run shutdown thread in background
        import threading
        shutdown_thread = threading.Thread(target=self.__do_shutdown__, args=(self.__threads,), name='GANGA_Update_Thread_shutdown')
        shutdown_thread.setDaemon(True)
        shutdown_thread.start()

        t_start = time.time()

        # Note that this is negative for the logic below to work
        t_last = -t_start

        def __cnt_alive_threads__(_all_threads):
            num_alive_threads = 0
            for t in _all_threads:
                if t.isAlive():
                    num_alive_threads += 1
            return num_alive_threads

        # wait for the background shutdown thread to finish
        config = getConfig('PollThread')
        while shutdown_thread.isAlive():
            logger.debug('Waiting for max %d seconds for threads to finish' % self.SHUTDOWN_TIMEOUT)
            logger.debug('There are %d alive background threads' % __cnt_alive_threads__(self.__threads))
            logger.debug('%s' % self.__alive_critical_thread_ids())
            logger.debug('%s' % self.__alive_non_critical_thread_ids())
            shutdown_thread.join(self.SHUTDOWN_TIMEOUT)

            # Have all the threads finished?
            if not shutdown_thread.isAlive():
                logger.debug('GangaThreadPool shutdown properly')
                break

            # Nope, so decide what we should do based on shutdown policy
            total_time = time.time() - t_start
            critical_thread_ids = self.__alive_critical_thread_ids()
            non_critical_thread_ids = self.__alive_non_critical_thread_ids()

            if GangaThreadPool.shutdown_policy == 'batch':
                # we have batch shutdown policy so wait until PollThread.forced_shutdown_timeout for critical threads
                # and PollThread.forced_shutdown_first_prompt_time for non-critical threads
                if critical_thread_ids:
                    if total_time > config['forced_shutdown_timeout']:
                        getLogger().warning('Shutdown was forced after waiting for %d seconds for background '
                                            'activities to finish (monitoring, output download, etc). This may '
                                            'result in some jobs being corrupted.', total_time)
                        break

                elif non_critical_thread_ids:
                    if total_time > config['forced_shutdown_first_prompt_time']:
                        break
                else:
                    break
            else:
                # we have interactive shutdown policy so for critical threads, wait until
                # PollThread.forced_shutdown_first_prompt_time before asking every
                # PollThread.forced_shutdown_prompt_time before asking to force exit. For non-critical threads wait
                # for PollThread.forced_shutdown_first_prompt_time before forcing the exit.
                if critical_thread_ids:
                    if ((t_last < 0 and time.time() + t_last > config['forced_shutdown_first_prompt_time']) or
                            (t_last > 0 and time.time() - t_last > config['forced_shutdown_prompt_time'])):
                        msg = 'Job status update or output download still in progress (shutdown not completed ' \
                              'after %d seconds). %d background thread(s) still running: %s. Do you want to force ' \
                              'the exit (y/[n])? ' % (total_time, len(critical_thread_ids), critical_thread_ids)
                        resp = raw_input(msg)
                        t_last = time.time()
                        if resp.lower() == 'y':
                            break
                elif non_critical_thread_ids:
                    if total_time > config['forced_shutdown_first_prompt_time']:
                        break
                else:
                    break

        # log warning message if critical thread still alive
        critical_thread_ids = self.__alive_critical_thread_ids()
        if critical_thread_ids:
            logger.debug('GangaThreadPool shutdown anyway after %d sec.' % (time.time() - t_start))
            logger.warning('Shutdown forced. %d background thread(s) still running: %s', len(critical_thread_ids), critical_thread_ids)

        # log debug message if critical thread still alive
        non_critical_thread_ids = self.__alive_non_critical_thread_ids()
        if non_critical_thread_ids:
            logger.debug('GangaThreadPool shutdown anyway after %d sec.' % (time.time() - t_start))
            logger.debug('Shutdown forced. %d non-critical background thread(s) still running: %s', len(non_critical_thread_ids), non_critical_thread_ids)

        # set singleton instance to None
        self._instance = None
        for i in self.__threads:
            del i
        self.__threads = []

    def __alive_critical_thread_ids(self):
        """Return a list of alive critical thread names."""
        return [t.gangaName for t in self.__threads if t.isAlive() and t.isCritical()]

    def __alive_non_critical_thread_ids(self):
        """Return a list of alive non-critical thread names."""
        return [t.gangaName for t in self.__threads if t.isAlive() and not t.isCritical()]

    @staticmethod
    def __do_shutdown__(_all_threads):

        from Ganga.Utility.logging import getLogger
        logger = getLogger('GangaThread')

        from Ganga.Core.GangaThread.WorkerThreads import _global_queues as queues

        if queues is not None:
            queues._purge_all()
            queues._stop_all_threads(shutdown=True)

            logger.debug("ExternalQueues still running: %s" % queues.threadStatus())

        logger.debug('Service threads to shutdown: %s' % ([i for i in reversed(list(_all_threads))]))

        logger.debug('Service threads to shutdown: %s' % ([i for i in reversed(list(_all_threads))]))

        # shutdown each individual threads in the pool
        nonCritThreads = []
        critThreads = []

        for t in _all_threads:
            if t.isCritical():
                critThreads.append(t)
            else:
                nonCritThreads.append(t)

        # while len( _all_threads ) != 0:
        # Shutdown NON critical threads first as these can cause some critical
        # threads to hang
        for t in reversed(nonCritThreads):
            logger.debug('shutting down Thread: %s' % t.getName())
            t.stop()
            logger.debug('shutdown Thread: %s' % t.getName())
            # t.unregister()

        # Shutdown critical threads now assuming that the non-critical ones
        # have disappeared
        for t in reversed(critThreads):
            logger.debug('shutting down Thread: %s' % t.getName())
            t.stop()
            logger.debug('shutdown Thread: %s' % t.getName())
            # t.unregister()

        #    nonCritThreads = []
        #    critThreads = []

        #    for t in _all_threads:
        #        if t.isCritical():
        #            critThreads.append( t )
        #        else:
        #            nonCritThreads.append( t )

        def __cnt_alive_threads__(_all_threads):
            num_alive_threads = 0
            for t in _all_threads:
                if t.isAlive():
                    num_alive_threads += 1
            return num_alive_threads

        num_alive_threads = __cnt_alive_threads__(_all_threads)

        while num_alive_threads > 0:
            from Ganga.Utility.logging import getLogger
            logger = getLogger('GangaThread')
            # fix for bug #62543 https://savannah.cern.ch/bugs/?62543
            # following 2 lines swapped so that we access no globals between
            # sleep and exit test
            num_alive_threads = __cnt_alive_threads__(_all_threads)
            logger.debug('number of alive threads: %d' % num_alive_threads)
            time.sleep(0.3)
            num_alive_threads = __cnt_alive_threads__(_all_threads)

