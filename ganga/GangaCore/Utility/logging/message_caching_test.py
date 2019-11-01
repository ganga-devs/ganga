from GangaCore.Core.GangaThread import GangaThread
import time
from GangaCore.Utility.logging import getLogger
l = getLogger()


def f(name=''):
    for i in range(5):
        l.warning(name + str(i))
        time.sleep(1)


f('Main:')


class MyThread(GangaThread):

    def run(self):
        f('GangaThread:')


t = MyThread('GangaThread')
t.start()
