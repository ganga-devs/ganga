from Ganga.Utility.logging import getLogger

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

        self.SHUTDOWN_TIMEOUT = 10
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

    def shutdown(self):

        logger.debug('shuting down GangaThreadPool with timeout %d sec' % self.SHUTDOWN_TIMEOUT)
        
        ## run shutdown thread in background
        import threading
        t = threading.Thread(target=self.__do_shutdown__, name='GANGA_Update_Thread_shutdown')
        t.setDaemon(True)
        t.start()

        ## wait for the background shutdown thread to finish
        while t.isAlive():
            
            t.join(self.SHUTDOWN_TIMEOUT)

            if t.isAlive():
                logger.debug('GangaThreadPool shutdown anyway after %d sec.' % self.SHUTDOWN_TIMEOUT)
                break
            else:
                logger.debug('GangaThreadPool shutdown properly')

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
            logger.debug('number of alive threads: %d' % num_alive_threads)
            num_alive_threads = self.__cnt_alive_threads__()

    def __cnt_alive_threads__(self):

        num_alive_threads = 0
        for t in self.__threads:
            if t.isAlive():
                num_alive_threads += 1

        return num_alive_threads