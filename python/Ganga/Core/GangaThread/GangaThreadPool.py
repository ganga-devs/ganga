from Ganga.Utility.logging import getLogger
import time

logger = getLogger('GangaThread')

class GangaThreadPool:

    _attributes = ('SHUTDOWN_TIMEOUT')

    ## GangaThreadPool singleton instance
    _instance = None

    class SingletonHelper:

        def __call__(self, *args, **kw):

            if GangaThreadPool._instance is None:
                object = GangaThreadPool()
                GangaThreadPool._instance = object

            return GangaThreadPool._instance

    getInstance = SingletonHelper()

    def __init__(self):

        if not GangaThreadPool._instance == None :
            raise RuntimeError, 'Only one instance of GangaThreadPool is allowed!'

        GangaThreadPool._instance=self

        self.SHUTDOWN_TIMEOUT = 1
        self.__threads = []

    def addServiceThread(self,t):
        logger.debug('service thread "%s" added to the GangaThreadPool',t.getName())
        try:
            self.__threads.append(t)
        except DuplicateDataItemError, e:
            self.logger.debug(str(e))
            pass

    def delServiceThread(self,t):
        logger.debug('service thread "%s" deleted from the GangaThreadPool',t.getName())
        try:
            self.__threads.remove(t)
        except ValueError,e:
            logger.debug(str(e))
            pass

    def shutdown(self, should_wait_cb = None):
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

        logger.debug('shutting down GangaThreadPool with timeout %d sec' % self.SHUTDOWN_TIMEOUT)

        ## run shutdown thread in background
        import threading
        shutdown_thread = threading.Thread(target=self.__do_shutdown__, name='GANGA_Update_Thread_shutdown')
        shutdown_thread.setDaemon(True)
        shutdown_thread.start()

        t_start = time.time()

        ## wait for the background shutdown thread to finish
        while shutdown_thread.isAlive():
            logger.debug('Waiting for max %d seconds for threads to finish' % self.SHUTDOWN_TIMEOUT)
            logger.debug('There are %d alive background threads' % self.__cnt_alive_threads__())
            logger.debug('%s' % self.__alive_critical_thread_ids() )
            logger.debug('%s' % self.__alive_non_critical_thread_ids() )
            shutdown_thread.join(self.SHUTDOWN_TIMEOUT)

            if shutdown_thread.isAlive():
                # if should_wait_cb callback exists then ask if we should wait
                if should_wait_cb:
                    total_time = time.time()-t_start
                    critical_thread_ids = self.__alive_critical_thread_ids()
                    non_critical_thread_ids = self.__alive_non_critical_thread_ids()
                    if not should_wait_cb(total_time, critical_thread_ids, non_critical_thread_ids):
                        logger.debug('GangaThreadPool shutdown anyway after %d sec.' % (time.time()-t_start))
                        break
            else:
                logger.debug('GangaThreadPool shutdown properly')

        # log warning message if critical thread still alive
        critical_thread_ids = self.__alive_critical_thread_ids()
        if critical_thread_ids:
            logger.warning('Shutdown forced. %d background thread(s) still running: %s', len(critical_thread_ids), critical_thread_ids)

        # log debug message if critical thread still alive
        non_critical_thread_ids = self.__alive_non_critical_thread_ids()
        if non_critical_thread_ids:
            logger.debug('Shutdown forced. %d non-critical background thread(s) still running: %s', len(non_critical_thread_ids), non_critical_thread_ids)

        ## set singleton instance to None
        self._instance = None
        for i in self.__threads:
            del i
        self.__threads = []

        # Potentailly DANGEROUS only use if certain it's safe
        #from Ganga.Runtime import bootstrap
        #bootstrap.safeCloseOpenFiles()

    def __alive_critical_thread_ids(self):
        """Return a list of alive critical thread names."""
        return [t.name for t in self.__threads if t.isAlive() and t.isCritical()]

    def __alive_non_critical_thread_ids(self):
        """Return a list of alive non-critical thread names."""
        return [t.name for t in self.__threads if t.isAlive() and not t.isCritical()]

    def __do_shutdown__(self):

        from Ganga.GPI import queues

        queues._purge_all()
        queues._stop_all_threads()

        #while queues.totalNumAllThreads() != 0:
        #    queues._stop_all_threads()
        #    logger.warning( "Tasks still running: %s" % queues.threadStatus() )
        #    import time
        #    time.sleep( 0.1 )

        logger.debug( "ExternalTasks still running: %s" % queues.threadStatus() )

        logger.debug('Service threads to shutdown: %s' % list(self.__threads))

        ## shutdown each individual threads in the pool
        nonCritThreads = []
        critThreads = []

        for t in self.__threads:
            if t.isCritical():
                critThreads.append( t )
            else:
                nonCritThreads.append( t )

        #while len( self.__threads ) != 0:
        ## Shutdown NON critical threads first as these can cause some critical threads to hang
        for t in nonCritThreads:
            if not t.isCritical():
                logger.debug('shutting down Thread: %s' % t.getName())
                t.stop()
                logger.debug('shutdown Thread: %s' % t.getName())
                #t.unregister()

        ## Shutdown critical threads now assuming that the non-critical ones have disappeared
        for t in critThreads:
            logger.debug('shutting down Thread: %s' % t.getName())
            t.stop()
            logger.debug('shutdown Thread: %s' % t.getName())
            #t.unregister()

        #    nonCritThreads = []
        #    critThreads = []

        #    for t in self.__threads:
        #        if t.isCritical():
        #            critThreads.append( t )
        #        else:
        #            nonCritThreads.append( t )



        ## counting the number of alive threads
        num_alive_threads = self.__cnt_alive_threads__()

        while num_alive_threads > 0:
            # fix for bug #62543 https://savannah.cern.ch/bugs/?62543
            # following 2 lines swapped so that we access no globals between sleep and exit test
            logger.debug('number of alive threads: %d' % num_alive_threads)
            time.sleep(0.3)
            num_alive_threads = self.__cnt_alive_threads__()

    def __cnt_alive_threads__(self):

        num_alive_threads = 0
        for t in self.__threads:
            if t.isAlive():
                num_alive_threads += 1

        return num_alive_threads
