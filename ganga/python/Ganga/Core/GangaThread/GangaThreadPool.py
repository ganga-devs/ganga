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

        logger.debug('shuting down GangaThreadPool with timeout %d sec' % self.SHUTDOWN_TIMEOUT)
        
        ## run shutdown thread in background
        import threading
        t = threading.Thread(target=self.__do_shutdown__, name='GANGA_Update_Thread_shutdown')
        t.setDaemon(True)
        t.start()

        t_start = time.time()

        ## wait for the background shutdown thread to finish
        while t.isAlive():
            logger.debug('Waiting for max %d seconds for threads to finish'%self.SHUTDOWN_TIMEOUT)
            logger.debug('There are %d alive background threads'%self.__cnt_alive_threads__())
            t.join(self.SHUTDOWN_TIMEOUT)

            if t.isAlive():
                if should_wait_cb:
                    if not should_wait_cb(time.time()-t_start):
                        logger.debug('GangaThreadPool shutdown anyway after %d sec.' % (time.time()-t_start))
                        break

            else:
                logger.debug('GangaThreadPool shutdown properly')

        unfinished_threads = [t for t in self.__threads if t.isAlive()]
        if unfinished_threads:
            logger.warning('Shutdown forced. There are %d background threads still running: %s',len(unfinished_threads),[t.getName() for t in unfinished_threads])

        ## set singleton instance to None
        self._instance = None
        self.__threads = []

    def __do_shutdown__(self):

        ## shutdown each individual threads in the pool
        for t in self.__threads:
            logger.debug('shutting down Thread: %s' % t.getName())
            t.stop()

        ## counting the number of alive threads
        num_alive_threads = self.__cnt_alive_threads__()

        while num_alive_threads > 0:
            time.sleep(0.3)
            logger.debug('number of alive threads: %d' % num_alive_threads)
            num_alive_threads = self.__cnt_alive_threads__()

    def __cnt_alive_threads__(self):

        num_alive_threads = 0
        for t in self.__threads:
            if t.isAlive():
                num_alive_threads += 1

        return num_alive_threads
