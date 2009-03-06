#!/usr/bin/env python
import time
from threading import Thread, Lock
from Queue import Empty
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

    _attributes = ('algorithm', 'data', 'numThread', 'debug', 'doneList', 'keepAlive', 'doStop')

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
        self.keepAlive = False
        self.doStop    = False
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

    def stop(self):
        """
        stops the MTRunner.
        """
        self.doStop = True

    def join(self, timeout=None):
        """
        overrides the join method.
        """
        Thread.join(self, timeout)

    def run(self):
        """
        runs the MTRunner thread.
        """

        myLock = Lock()
 
        def worker(id, keepAlive=False):

            while not self.doStop:

                if self.data.isEmpty():

                    if keepAlive:
                        #if self.debug:
                        #    print 'data queue is empty, check again in 0.5 sec.'
                        time.sleep(0.5)
                        continue
                    else:
                        if self.debug:
                            print 'data queue is empty, stop worker'
                        break
                else:
                    try:
                        item = self.data.getNextItem()
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

        # don't keep the worker thread alive if the MTRunner is not a daemon-type thread
        if not self.isDaemon:
            self.keepAlive = False

        for i in range(self.numThread):
            t = Thread(target=worker, kwargs={'id': i, 'keepAlive': self.keepAlive })
            t.setName(self.getName() + '_agent_%d' % i)
            #t.setDaemon(False)
            t.setDaemon(True)
            threads.append(t)

        # starting worker threads
        for t in threads:
            t.start()

        # checking periodically and trying to join the splitted worker thread  
        num_alive_threads = self.numThread
        while num_alive_threads > 0:
            if self.debug:
                print 'number of running threads: %s' % num_alive_threads
            num_alive_threads = 0
            for t in threads:
                t.join(1)
                if t.isAlive():
                    num_alive_threads += 1

        # when entering this line, MTRunner stops
        if self.debug:
            print 'MTRunner stop'
