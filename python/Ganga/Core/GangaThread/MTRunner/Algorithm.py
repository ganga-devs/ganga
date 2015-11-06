#!/usr/bin/env python
class AlgorithmError(Exception):

    """
    Class for Algorithm errors.
    """

    def __init__(self, message):
        self.message = message


class Algorithm(object):

    """
    Class to define user algorithm.
    """

    _attributes = ('results')

    def __init__(self):
        self.results = {}

    def __appendResult__(self, item, result):
        self.results[item] = result

    def process(self, item):
        """
        implements how to deal with the given data item. The output of the process can be
        appended to the self.results dictionary by calling self.__appendResult__(item, result). 

        @since: 0.0.1
        @author: Hurng-Chun Lee 
        @contact: hurngchunlee@gmail.com

        @param item is the sigle item from the data collection 
        @return True if the item is properly processed; otherwise False
        """
        raise NotImplementedError(
            'algorithm needs to be implemented to deal with the data item')

    def getResults(self):
        """
        returns the up-to-date results from what has been processed by this algorithm object.
        """
        return self.results
