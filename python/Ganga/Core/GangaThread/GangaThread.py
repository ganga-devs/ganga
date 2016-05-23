from __future__ import absolute_import
from threading import Thread, RLock
from .GangaThreadPool import GangaThreadPool
from Ganga.Utility.logging import getLogger
import functools
logger = getLogger()

def synchronised(f):
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
        with self._lock:
            return f(self, *args, **kwargs)
    return decorated


class GangaThread(Thread):

    def __init__(self, name, auto_register=True, critical=True, **kwds):

        self.gangaName = str(name)  # want to copy actual not by ref!
        name = 'GANGA_Update_Thread_%s' % name

        Thread.__init__(self, args=list(), name=name, **kwds)
        self.setDaemon(True)
        self.__should_stop_flag = False
        self.__critical = critical

        if auto_register:
            tpool = GangaThreadPool.getInstance()
            tpool.addServiceThread(self)
        self._lock = RLock()

    @synchronised
    def isCritical(self):
        """Return critical flag.

        @return: Boolean critical flag.
        """
        return self.__critical

    @synchronised
    def setCritical(self, critical):
        """Set critical flag, which can be used for example in shutdown
        algorithms. See Ganga/Core/__init__.py for example.

        @param critical: Boolean critical flag.
        """
        self.__critical = critical

    @synchronised
    def should_stop(self):
        return self.__should_stop_flag

    @synchronised
    def stop(self):
        if not self.__should_stop_flag:
            logger.debug("Stopping: %s", self.gangaName)
            self.__should_stop_flag = True

    @synchronised
    def unregister(self):
        GangaThreadPool.getInstance().delServiceThread(self)

    @synchronised
    def register(self):
        GangaThreadPool.getInstance().addServiceThread(self)
