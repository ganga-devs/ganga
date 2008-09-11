#!/usr/bin/env python
import time
from Algorithm import *
from MTRunner import *
from Data import *

class MyAlgorithm(Algorithm):

    def process(self, item):
        """
        prints what is given to it.
        """
        time.sleep(1)

        self.__appendResult__(item, 'item %s processed' % item)

        return True

if __name__ == '__main__':

    myAlg  = MyAlgorithm()
    myData = Data(collection=range(0,1000))

    runner = MTRunner(myAlg, myData)
    runner.debug = True
    runner.start()
    runner.join()

    doneList = runner.getDoneList()
    results  = runner.getResults()

    print doneList
    print results
