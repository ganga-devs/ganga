#!/usr/bin/env python
from queue import Queue
import threading


class DuplicateDataItemError(Exception):

    """
    Class raised when adding the same item in the Data object.
    """

    def __init__(self, message):
        self.message = message


class Data(object):

    """
    Class to define user dataset collection.
    """

    _attributes = ('collection', 'queue')

    def __init__(self, collection=None):
        if collection is None:
            collection = []

        self.collection = collection
        self.queue = Queue(maxsize=-1)
        self.lock = threading.Lock()

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

        self.lock.acquire()
        try:
            if item not in self.collection:
                self.collection.append(item)

                self.queue.put(item)
            else:
                raise DuplicateDataItemError(
                    'data item \'%s\' already in the task queue' % str(item))
        finally:
            self.lock.release()

    def getNextItem(self):
        '''
        try to get the next item in the queue after waiting in max. 1 sec.

        if nothing available, the exception "Queue.Empty" will be thrown. 
        '''

        theItem = None

        self.lock.acquire()
        try:
            theItem = self.queue.get(block=True, timeout=1)
        finally:
            self.lock.release()

        return theItem
