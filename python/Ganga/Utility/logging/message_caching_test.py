from Ganga.Utility.logging import getLogger
l = getLogger()
import time


def f(name=''):
    for i in range(5):
        l.warning(name + str(i))
        time.sleep(1)

f('Main:')

from Ganga.Core.GangaThread import GangaThread


class MyThread(GangaThread):

    def run(self):
        f('GangaThread:')

t = MyThread('GangaThread')
t.start()
