#!/usr/bin/env python2.2
#----------------------------------------------------------------------------
# Name:         PipeReader.py
# Purpose:      Read from pipe in non blocking manner.
#
# Author:       Alexander Soroko
#
# Created:      20/10/2003     
#----------------------------------------------------------------------------

"""Read from pipe in non blocking manner. It is portable to both
posix and windows environments."""

import os
import time
import Queue
import threading

MIN_TIMEOUT = 0.01

################################################################################
class PipeReader:
    
    def __init__(self, readfile, timeout=None, pipesize=0, blocksize=1024):
        """Initialise a non-blocking pipe object, given a real file readfile.
        timeout = the default timeout (in seconds) at which read will decide
                  that there is no more data in the queue.
                  timeout = None, or < 0 stands for indefinite waiting time. 
        pipesize = the size (in blocks) of the queue used to buffer the
                  blocks read
        blocksize = the maximum block size for a raw read."""

        self.rfile = readfile
        # default timeout allowed between blocks   
        if timeout:
            self.timeout = timeout
        else:
            self.timeout = -1
        self.pipesize = pipesize
        self.blocksize = blocksize
        self._q = Queue.Queue(self.pipesize)
        self._stop = threading.Event()
        self._stop.clear()
        self._thread = threading.Thread(target = self._readtoq)
        self._thread.start()

#-------------------------------------------------------------------------------        
    def __del__(self): 
        self.stop()
        
#-------------------------------------------------------------------------------        
    def stop(self): 
        self._stop.set()
        self._thread.join()
        
#-------------------------------------------------------------------------------        
    def _readtoq(self):
        try:
            while 1:
                item = self.rfile.read(self.blocksize)
                if item == '':
                    break
                else:
                    self._q.put(item)
                if self._stop.isSet():
                    break    
        except:
            return
        
#-------------------------------------------------------------------------------                       
    def has_data(self):
        return not self._q.empty()
    
#-------------------------------------------------------------------------------                       
    def isReading(self):
        return self._thread.isAlive()
    
#------------------------------------------------------------------------------- 
    def instantRead(self, maxblocks=0):
        """Read data from the queue, to a maximum of maxblocks (0 = infinite).
        Does not block."""
        data = ''
        blockcount = 0
        while self.has_data():
            data += self._q.get()
            blockcount += 1
            if blockcount == maxblocks:
                break
        return data
    
#-------------------------------------------------------------------------------    
    def read(self, maxblocks=0, timeout=None, condition = None):
        """Read data from the queue, allowing timeout seconds between block arrival.
        if timeout = None, then use default timeout. If timeout<0,
        then wait indefinitely.
        Returns '' if we are at the EOF, or no data turns up within the timeout.
        If condition is not None and condition() == False returns.
        Reads at most maxblocks (0 = infinite).
        Does not block."""
        def keepReading(endtime):
            if endtime < 0:
                return 1
            else:
                return time.time() < endtime
            
        data = ''
        blockcount = 0
        if timeout == None:
            timeout = self.timeout
            
        if timeout < 0:
            endtime = -1
        else:
            endtime = time.time() + timeout
        
        while keepReading(endtime):
            block = self.instantRead(1)
            if block != '':
                blockcount += 1
                data += block
                if blockcount == maxblocks:
                    break
                if endtime != -1:
                    endtime = time.time() + timeout #reset endtime
                continue
            else:
                time.sleep(MIN_TIMEOUT)
                if self.isReading():
                    if condition and condition() or not condition:
                        continue
                    else:
                        # process exited
                        # take a chance of reading data
                        time.sleep(MIN_TIMEOUT)
                # stopping
                if not self.has_data():
                    break
                
        return data

