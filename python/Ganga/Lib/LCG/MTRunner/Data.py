#!/usr/bin/env python

class Data:
    """
    Class to define user dataset collection.
    """

    _attributes = ('collection', 'size')

    def __init__(self, collection=[]):
        self.collection = collection
        self.size = len(collection)

    def getCollection(self):
        return self.collection

    def getSize(self):
        return self.size
