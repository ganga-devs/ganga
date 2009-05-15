from threading import Thread
from GangaThreadPool import GangaThreadPool
from Ganga.Utility.logging import getLogger

logger = getLogger('GangaThread')

class GangaThread(Thread):

    def __init__(self, name, auto_register=True, **kwds):

        name = 'GANGA_Update_Thread_%s' % name

        Thread.__init__(self, name=name, **kwds)
        self.setDaemon(True)
        self.__should_stop_flag = False

        if auto_register:
            tpool = GangaThreadPool.getInstance()
            tpool.addServiceThread(self)

    def should_stop(self):
        return self.__should_stop_flag

    def stop(self):
        if not self.__should_stop_flag:
            logger.debug("Stopping: %s",self.getName())
            self.__should_stop_flag = True

    def unregister(self):
        GangaThreadPool.getInstance().delServiceThread(self)

