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
        time.sleep(0.01)

        self.__appendResult__(item, 'item %s processed' % item)

        return True

if __name__ == '__main__':

    myAlg  = MyAlgorithm()
    myData = Data(collection=range(0,1000))

    runner = MTRunner(myAlg, myData)
    runner.debug = True
    ### start the runner as a daemon and keep it alive as long as possible
    runner.setDaemon(True)
    runner.keepAlive = True
    runner.start()

    ### add more items here
    for i in range(1001,2000):
        myData.addItem(i)

    ### lets run until the runner is fully stopped
    cnt = 0
    while runner.isAlive():

        try: 
            print 'check again ... %d' % cnt
         
            time.sleep(5)
         
            cnt += 1
         
            if cnt in [1, 3, 5]:
                print '%d: add more items to be processed' % cnt
                for i in range(cnt*2000, cnt*2000+100):
                    myData.addItem(i)
         
            ### stop the runner if running over 10 checking loops 
            if cnt >= 10 and myData.isEmpty():
                print 'running over 10 checking loops, stop the MTRunner'
                runner.stop()

        except KeyboardInterrupt,e:
            print 'interrupted by keyboard ... safely stop the MTRunner'
            runner.stop()

    doneList = runner.getDoneList()
    results  = runner.getResults()

    print doneList
    print results
