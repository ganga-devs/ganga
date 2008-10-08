#!/usr/bin/env python
from Queue import Queue

class Data:
    """
    Class to define user dataset collection.
    """

    _attributes = ('collection','queue')

    def __init__(self, collection=[]):
        self.collection = collection
        self.queue = Queue(maxsize=-1)
        for item in collection:
            self.queue.put(item)

    def getCollection(self):
        return self.collection

    def isEmpty(self):
        '''
        checks if the bounded queue is empty.
        '''
        return self.queue.empty()

    def addItem(self, item):
        '''
        try to put a new item in the queue. As the queue is defined with infinity number
        of slots, it should never throw "Queue.Full" exception.
        '''
        self.queue.put(item)

    def getNextItem(self):
        '''
        try to get the next item in the queue after waiting in max. 1 sec.

        if nothing available, the exception "Queue.Empty" will be thrown. 
        '''
        return self.queue.get(block=True, timeout=1)
