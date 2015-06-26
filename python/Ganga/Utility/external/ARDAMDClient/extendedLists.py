from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: extendedLists.py,v 1.1 2008-07-17 16:41:02 moscicki Exp $
##########################################################################
import os
import types
import time

from .diskutils import *


class Storage(object):

    def __init__(self, dirname, tries_limit=200):
        # dirname is the name of the directory to store blocks
        # tries_limit is the maximum allowed limit for attempts
        # to move, remove or write files
        self.dirname = dirname
        self.tries_limit = tries_limit


class Block(object):

    def __init__(self, entries=None, entrlen=None, name=None, storage=None):
        if entries is None:
            entries = []
        if entrlen is None:
            entrlen = len(entries)
        self.entries = entries
        self.name = name
        self.storage = storage
        self.in_memory = True
        self.on_disk = False  # if modified: self.on_disk = False
        self.setLen(entrlen)
        self.updateKeys()

    def __getitem__(self, i):
        if i < self.__len__():
            return self.entries[i]
        else:
            raise IndexError('list index out of range')

    def __setitem__(self, i, item):
        if i < self.__len__():
            if self.entries[i][0] != item[0]:
                self.delKey(self.entries[i][0])
                self.addKey(item[0])
            self.entries[i] = item
            self.on_disk = False
        else:
            raise IndexError('list index out of range')

    def __delitem__(self, i):
        entrlen = self.__len__()
        if i < entrlen:
            self.delKey(self.entries[i][0])
            del self.entries[i]
            self.setLen(entrlen - 1)
            self.on_disk = False
        else:
            raise IndexError('list index out of range')

    def __len__(self):
        return self.entrlen

    def append(self, item):
        entrlen = self.__len__()
        self.addKey(item[0])
        if entrlen < len(self.entries):
            self.entries[entrlen] = item
        else:
            self.entries.append(item)
        self.setLen(entrlen + 1)
        self.on_disk = False

    def insert(self, i, item):
        entrlen = self.__len__()
        if i < entrlen:
            self.addKey(item[0])
            self.entries.insert(i, item)
            self.setLen(entrlen + 1)
            self.on_disk = False
        else:
            self.append(item)

    def genericName(self):
        if self.name:
            return self.name.split('_')[0]

    def setLen(self, entrlen):
        self.entrlen = entrlen

    def addKey(self, key):
        self.entry_keys[key] = {}

    def delKey(self, key):
        del self.entry_keys[key]

    def hasKey(self, key):
        return key in self.entry_keys

    def updateKeys(self):
        self.entry_keys = {}
        for i in xrange(self.__len__()):
            self.addKey(self.entries[i][0])

    def memoryRelease(self):
        # release memory, so that self.entries list can be reused by another block
        # this is done to fight memory leak in Python
        if self.in_memory:
            if not self.on_disk:
                self.save()
            self.in_memory = False
            entries = self.entries
            self.entries = []
            return entries

    def load(self, forced=False, entries=None):
        if self.storage:
            dirname = self.storage.dirname
            generic_name = self.genericName()
            if generic_name:
                try:
                    last = getLast(os.path.join(dirname, generic_name))
                except OSError as e:
                    self.on_disk = False
                else:
                    self.on_disk = True
                    lfn = os.path.basename(last)
                    if self.name != lfn or forced:
                        self.entries, entrlen = read(last, entries)
                        self.name = lfn
                        self.in_memory = True
                        self.setLen(entrlen)
                        self.updateKeys()

    def save(self):
        if not self.on_disk:
            if self.in_memory:
                if self.storage:
                    dirname = self.storage.dirname
                    tries_limit = self.storage.tries_limit
                    generic_name = self.genericName()
                    if generic_name:
                        fn = os.path.join(dirname, generic_name)
                        entrlen = self.__len__()
                        if entrlen > 0:
                            self.name = os.path.basename(
                                write(self.entries, fn, entrlen, tries_limit))
                            self.on_disk = True
                        else:
                            remove(fn)
                            self.on_disk = False


class EntryBlock(Block):

    def addKey(self, key):
        # key will be slit using "." separator and saved in a tree like structure
        # {k1:{k2:{}}, kk:{}}
        kk = key.split('.')
        dd = self.entry_keys
        for k in kk:
            if k not in dd:
                dd[k] = {}
            dd = dd[k]

    def delKey(self, key):
        # will delete empty dictionaries
        kk = key.split('.')
        dd = self.entry_keys
        visited = []
        for k in kk:
            if k in dd:
                visited.append((dd, k))
                dd = dd[k]
            else:
                break
        else:
            visited.reverse()
            for dd, k in visited:
                del dd[k]
                if len(dd) > 0:
                    break

    def hasKey(self, key):
        kk = key.split('.')
        dd = self.entry_keys
        for k in kk:
            if k not in dd:
                return False
            else:
                dd = dd[k]
        return True


class BlockCache(object):

    def __init__(self, maxsize=3):
        # maxsize is in blocks
        # if maxsize <= 0, there is no limit
        self.maxsize = maxsize
        self.blocks = []

    def put(self, block):
        if block.in_memory:
            if block not in self.blocks:
                self.blocks.append(block)

    def get(self):
        if self.maxsize > 0:
            if len(self.blocks) >= self.maxsize:
                return self.blocks.pop(0).memoryRelease()


class BlockedList(object):
    block_prefix = 'BLOCK-'
    block_class = Block

    def __init__(self, ll=None,
                 dirname='',
                 blocklength=100,
                 cache_size=3,
                 tries_limit=200):
        self.storage = Storage(dirname, tries_limit)
        self.blocklength = blocklength
        if ll == None:
            ll = []
        self.cache = BlockCache(cache_size)
        self.blocks = []
        start = 0
        stop = self.blocklength
        while True:
            entries = ll[start:stop]
            if not entries:
                break
            self._addNewBlock(entries)
            start = stop
            stop += self.blocklength

    def _addNewBlock(self, entries):
        bni = len(self.blocks)
        if bni > 0:
            last_blk = self.blocks[-1]
            gn = last_blk.genericName()
            if gn:
                bni = int(gn.split('-')[1]) + 1
        generic_name = self.block_prefix + str(bni)
        entrlen = len(entries)
        free_entries = self.cache.get()
        if free_entries is not None:
            free_entries[:entrlen] = entries
            entries = free_entries
        blk = self.block_class(entries=entries,
                               entrlen=entrlen,
                               name=generic_name,
                               storage=self.storage)
        self.blocks.append(blk)
        self.cache.put(blk)

    def _loadBlock(self, blk, forced=True):
        # if block is in memory the forced loading is suppressed
        if not (blk.in_memory and forced):
            free_entries = self.cache.get()
            blk.load(forced, free_entries)
            self.cache.put(blk)

    def _translateIndex(self, i):
        if i < 0:
            i += self.__len__()
            i = max(0, i)
        blk_i = i
        for blk in self.blocks:
            tt = blk_i - len(blk)
            if tt < 0:
                return (blk, blk_i)
            blk_i = tt
        raise IndexError('list index out of range')

    def __getitem__(self, i):
        if isinstance(i, types.SliceType):
            ss = i.indices(self.__len__())
            ii = range(*ss)
            return map(self.__getitem__, ii)
        else:
            (blk, blk_i) = self._translateIndex(i)
            self._loadBlock(blk)
            return blk[blk_i]

    def __setitem__(self, i, item):
        if isinstance(i, types.SliceType):
            ss = i.indices(self.__len__())
            ii = range(*ss)
            len_ii = len(ii)
            len_item = len(item)
            if i.step != None:  # extended slice
                if len_ii != len_item:
                    msg = "attempt to assign sequence of size %d to extended slice of size %d" % (
                        len_item, len_ii)
                    raise ValueError(msg)
            else:
                delete = ii[len_item:]
                delete.reverse()  # items to delete
                insert = item[len_ii:]
                insert.reverse()  # items to insert
                for d in delete:
                    del self[d]
                for s in insert:
                    self.insert(len_ii, s)
            map(lambda x: self.__setitem__(*x), zip(ii, item))
        else:
            (blk, blk_i) = self._translateIndex(i)
            self._loadBlock(blk)
            blk[blk_i] = item

    def __delitem__(self, i):
        if isinstance(i, types.SliceType):
            ss = i.indices(self.__len__())
            ii = range(*ss)
            if ss[0] < ss[1]:
                ii.reverse()
            map(self.__delitem__, ii)
        else:
            (blk, blk_i) = self._translateIndex(i)
            self._loadBlock(blk)
            del blk[blk_i]

    def __len__(self):
        blen = 0
        for blk in self.blocks:
            blen += len(blk)
        return blen

    def append(self, item):
        if not self.blocks:
            self._addNewBlock([])
        blk = self.blocks[-1]
        if len(blk) >= self.blocklength:
            self._addNewBlock([])
            blk = self.blocks[-1]
        self._loadBlock(blk)
        blk.append(item)

    def extend(self, iter):
        for item in iter:
            self.append(item)

    def insert(self, i, item):
        # can increase block length over self.blocklength
        (blk, blk_i) = self._translateIndex(i)
        self._loadBlock(blk)
        blk.insert(blk_i, item)

    def _getBlockNames(self):
        dd = {}
        for blk in self.blocks:
            generic_name = blk.genericName()
            if generic_name != None:
                dd[generic_name] = blk
        return dd

    def _sorter(self, x, y):
        x, y = map(lambda f: int(f.split('-')[1]), (x, y))
        if x < y:
            return -1
        elif x == y:
            return 0
        elif x > y:
            return 1

    def _getStoredBlockNames(self):
        dirname = self.storage.dirname
        names = []
        for fn in listFiles(dirname):
            if fn.startswith(self.block_prefix):
                ff = fn.split('_')
                if len(ff) == 2:
                    if ff[1] != '':
                        fn = ff[0]
                        if fn not in names:
                            names.append(fn)
        names.sort(self._sorter)
        return names

    def load(self, dirname=''):
        if dirname:
            self.storage.dirname = dirname
        blocks = []
        bns = self._getStoredBlockNames()
        ndict = self._getBlockNames()
        for bn in bns:
            if bn in ndict:
                blk = ndict[bn]
            else:
                blk = self.block_class(name=bn, storage=self.storage)
            self._loadBlock(blk, forced=False)
            blocks.append(blk)
        self.blocks = blocks

    def save(self, dirname=''):
        if dirname:
            self.storage.dirname = dirname
        for i in xrange(len(self.blocks) - 1, -1, -1):
            blk = self.blocks[i]
            blk.save()
            if len(blk) == 0:
                del self.blocks[i]

    def mark(self):
        map(lambda x: setattr(x, 'on_disk', False), self.blocks)

    def has_key(self, key):
        for blk in self.blocks:
            if blk.hasKey(key):
                return True
        return False


class Attributes(BlockedList):
    block_prefix = 'Attr-'

    def __init__(self, ll=None,
                 dirname='',
                 blocklength=20,
                 cache_size=3,
                 tries_limit=200):
        super(Attributes, self).__init__(
            ll, dirname, blocklength, cache_size, tries_limit)


class Entries(BlockedList):
    block_prefix = 'Entr-'
    block_class = EntryBlock

    def __init__(self, ll=None,
                 dirname='',
                 blocklength=100,
                 cache_size=3,
                 tries_limit=200):
        super(Entries, self).__init__(
            ll, dirname, blocklength, cache_size, tries_limit)
