#!/usr/bin/env python
from threading import Thread, Lock
from Queue import Queue, Empty
from Algorithm import AlgorithmError

class MTRunnerError(Exception):
    """
    Class for general MTRunner errors.
    """

    def __init__(self, message):
        self.message = message 

class MTRunner(Thread):
    """
    Class to handle multiple concurrent threads running on the same algorithm. 
    
    @since: 0.0.1
    @author: Hurng-Chun Lee 
    @contact: hurngchunlee@gmail.com

    The class itself is a thread. To run it; doing the following:

        runner = MTRunner(myAlgorithm, myData)
        runner.start()
        ... you can do something in parallel in your main program ...
        runner.join()

    where 'myAlorithm' and 'myData' are two objects defining your own
    algorithm running on a dataset.
    """

    _attributes = ('algorithm','data','numThread','debug','doneList')

    def __init__(self, algorithm=None, data=None, numThread=10):
        """
        initializes the MTRunner object. 
        
        @since: 0.0.1
        @author: Hurng-Chun Lee 
        @contact: hurngchunlee@gmail.com

        @param algorithm is an Algorithm object defining how to process on the data
        @param data is an Data object defining what to be processed by the algorithm
        """
        Thread.__init__(self)

        if (not algorithm) or (not data):
            raise MTRunnerError('algorithm and data must not be None') 

        self.algorithm = algorithm
        self.data      = data
        self.numThread = numThread
        self.debug     = False
        self.doneList  = []

    def getDoneList(self):
        """
        gets the data items that have been processed correctly by the algorithm.
        """
        return self.doneList

    def getResults(self):
        """
        gets the overall results (e.g. output) from the algorithm.
        """
        return self.algorithm.getResults()

    def run(self):
        """
        runs the MTRunner thread.
        """

        # preparing the queue
        collection = self.data.getCollection()
        wq = Queue(len(collection))
        for item in collection:
            wq.put(item)

        myLock = Lock()
 
        def worker(id):
            while not wq.empty():
                try:
                    item = wq.get(block=True, timeout=1)
                    if self.debug:
                        print 'worker %d get item %s' % (id, item)
                    rslt = self.algorithm.process(item)
                    if rslt:
                        myLock.acquire()
                        self.doneList.append(item)
                        myLock.release()
                except NotImplementedError:
                    break
                except AlgorithmError:
                    break
                except Empty:
                    pass
        
        # creating new threads to run the algorithm
        threads = []
        for i in range(self.numThread):
            t = Thread(target=worker, kwargs={'id': i})
            t.setDaemon(False)
            threads.append(t)

        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
