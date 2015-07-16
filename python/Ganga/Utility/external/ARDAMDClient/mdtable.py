from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: mdtable.py,v 1.1 2008-07-17 16:41:02 moscicki Exp $
##########################################################################
import os
import time
from .extendedLists import Entries, Attributes
from .diskutils import RLock
from .mdinterface import CommandException


class EmptyTableException(Exception):

    def __init__(self):
        self.raised = True


class MDTable:

    def __init__(self, dirname,
                 attributes=None,
                 entries=None,
                 blocklength=1000,
                 cache_size=3,
                 tries_limit=200):
        """dirname is system path to the table files"""
        self.dirname = dirname
        if attributes == None:
            attributes = Attributes(dirname=dirname,
                                    blocklength=blocklength,
                                    cache_size=cache_size,
                                    tries_limit=tries_limit)
        if entries == None:
            entries = Entries(dirname=dirname,
                              blocklength=blocklength,
                              cache_size=cache_size,
                              tries_limit=tries_limit)
        self.attributes = attributes
        self.entries = entries
        self.timestamp = time.time()
        self.update()
        self._lock = RLock(lockfile=os.path.join(dirname, 'LOCK'),
                           tries_limit=tries_limit)

    def lock(self):
        return self._lock.acquire()

    def unlock(self):
        self._lock.release()

    def load(self):
        if os.path.isdir(self.dirname):
            if self.lock():
                try:
                    self.attributes.load(self.dirname)
                    self.entries.load(self.dirname)
                    self.timestamp = time.time()
                    self.update()
                finally:
                    self.unlock()
            else:
                raise CommandException(
                    9, 'Could not acquire table lock %s' % self.dirname)

    def save(self):
        #        mkdir(self.dirname)
        if self.lock():
            try:
                self.update()
                self.attributes.save(self.dirname)
                self.entries.save(self.dirname)
            finally:
                self.unlock()
        else:
            raise CommandException(
                9, 'Could not acquire table lock %s' % self.dirname)

    def update(self):
        self.attributeDict = {}
        self.typeDict = {}
        for i in range(0, len(self.attributes)):
            # Was a, t=self.attributes[i].split(' ', 1)
            a, t = self.attributes[i]
            self.attributeDict[a] = i
            self.typeDict[a] = t
        self.initRowPointer()

    def initRowPointer(self):
        self.currentRow = 0
        if not len(self.entries):
            self.currentRow = -1
