from os import stat
import time
import new
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError

try:
    from threading import Lock
except ImportError:
    from dummy_threading import Lock

NOT_INITIALIZED = object()

log = getLogger()
config = makeConfig('Caching','Caching for DQ2 dataset')
config.addOption('CacheLifeTime', 150, 'Cache refresh time in seconds')
config.addOption('CacheMaxEntry', 10, 'For CacheMaxEntry == 0,  the cache is unbounded, otherwise  the Least Recently Used (LRU) entry is discarded.')


class Entry(object):
    """ A cache entry, mostly an internal object. """
    def __init__(self, key):
        object.__init__(self)
        self._key=key
        self._timestamp = time.time()
        self._value=NOT_INITIALIZED
        self._lock=Lock()

class Cache(object):
    """ An abstract, multi-threaded cache object. """

    cacheDict = {}
    
    def __init__(self, dset, max_size=0):
        """ Builds a cache with a limit of max_size entries.
            If this limit is exceeded, the Least Recently Used entry is discarded.
            if max_size==0, the cache is unbounded (no LRU rule is applied).
        """
        object.__init__(self)
        self._maxsize=max_size
        self._dict=self.cacheDict
        self._dset=dset
        self._lock=Lock()
        
        # Header of the access list
        if self._maxsize:
            self._head=Entry(None)
            self._head._previous=self._head
            self._head._next=self._head

    def __setitem__(self, name, value):
        """ Populates the cache with a given name and value. """
        key = self.key(name)
        
        entry = self._get_entry(key)
        
        entry._lock.acquire()
        try:
            self._pack(entry,value)
            self.commit()
        finally:
            entry._lock.release()

    def __getitem__(self, name):
        """ Gets a value from the cache, builds it if required.
        """
        return self._checkitem(name)[2]

    def __delitem__(self, name):
        self._lock.acquire()
        try:
            key = self.key(name)
            del self._dict[key]
        finally:
            self._lock.release()

    def _get_entry(self,key):
        self._lock.acquire()
        try:
            key = str(key)
            entry = self._dict.get(key)
            if not entry:
                entry = Entry(key)
                self._dict[key]=entry
                if self._maxsize:
                    entry._next = entry._previous = None
                    self._access(entry)
                    self._checklru()
            elif self._maxsize:
                self._access(entry)
            return entry
        finally:
            self._lock.release()

    def _checkitem(self, name):
        """ Gets a value from the cache, builds it if required.
            Returns a tuple is_new, key, value, entry.
            If is_new is True, the result had to be rebuilt.
        """
        key = self.key(name)
        entry = self._get_entry(key)

        entry._lock.acquire()
        try:
            value = self._unpack(entry)
            is_new = False
            if value is NOT_INITIALIZED:
                opened = self.check(key[1:], name, entry)
                value = self.build(key[1:], name, opened, entry)
                is_new = True
                self._pack(entry, value)
                self.commit()
            else:
                opened = self.check(key[1:], name, entry)
                if opened is not None:
                    value = self.build(key[1:], name, opened, entry)
                    is_new = True
                    self._pack(entry, value)
                    self.commit()
                    log.debug("Cache is being refreshed for dataset %s" % str(key))
                
                log.debug("Value is reading from cache for its key %s" % str(key))
            
            return is_new, key[1:], value, entry
        finally:
            entry._lock.release()

    def mru(self):
        """ Returns the Most Recently Used key """
        if self._maxsize:
            self._lock.acquire()
            try:
                return self._head._previous._key
            finally:
                self._lock.release()
        else:
            return None

    def lru(self):
        """ Returns the Least Recently Used key """
        if self._maxsize:
            self._lock.acquire()
            try:
                return self._head._next._key
            finally:
                self._lock.release()
        else:
            return None

    def key(self, name):
        """ Override this method to extract a key from the name passed to the [] operator """
        dsetN = self._dset
        
        if (dsetN.__class__.__name__ == 'GangaList'):
            dataset = tuple(dsetN)
        else:
            dataset = (dsetN,)
        
        parg = dataset + name 
        return parg

    def commit(self):
        """ Override this method if you want to do something each time the underlying dictionary is modified (e.g. make it persistent). """
        pass

    def clear(self):
        """ Clears the cache """
        self._lock.acquire()
        try:
            self._dict.clear()
            if self._maxsize:
                self._head._next=self._head
                self._head._previous=self._head
        finally:
            self._lock.release()

    def check(self, key, name, entry):
        """ Override this method to check whether the entry with the given name is stale. Return None if it is fresh
            or an opened resource if it is stale. The object returned will be passed to the 'build' method as the 'opened' parameter.
            Use the 'entry' parameter to store meta-data if required. Don't worry about multiple threads accessing the same name,
            as this method is properly isolated.
        """
        return None

    def build(self, key, name, opened, entry):
        """ Build the cached value with the given name from the given opened resource. Use entry to obtain or store meta-data if needed.
             Don't worry about multiple threads accessing the same name, as this method is properly isolated.
        """
        raise NotImplementedError()
           
    def _access(self, entry):
        " Internal use only, must be invoked within a cache lock. Updates the access list. """
        if entry._next is not self._head:
            if entry._previous is not None:
                # remove the entry from the access list
                entry._previous._next=entry._next
                entry._next._previous=entry._previous
            # insert the entry at the end of the access list
            entry._previous=self._head._previous
            entry._previous._next=entry
            entry._next=self._head
            entry._next._previous=entry
            if self._head._next is self._head:
                self._head._next=entry

    def _checklru(self):
        " Internal use only, must be invoked within a cache lock. Removes the LRU entry if needed. """
        if len(self._dict)>self._maxsize:
            lru=self._head._next
            lru._previous._next=lru._next
            lru._next._previous=lru._previous
            del self._dict[lru._key]
            log.debug("Old entry %s in cache is being deleted for creating space for new one" %str(lru._key))

    def _pack(self, entry, value):
        """ Store the value in the entry. """
        entry._value=value

    def _unpack(self, entry):
        """ Recover the value from the entry, returns NOT_INITIALIZED if it is not OK. """
        return entry._value

class FunctionCache(Cache):
    def __init__(self, function, dset, max_size=config['CacheMaxEntry']):
        Cache.__init__(self, dset, max_size)
        self.function=function
    
    def __call__(self, *args, **kw):
        if kw:
            # a dict is not hashable so we build a tuple of (key, value) pairs
            kw = tuple(kw.iteritems())
            return self[args, kw]
        else:
            return self[args, ()]

    def check(self, key, name, entry):
        
        if entry._value is NOT_INITIALIZED:
            return None
        else:
            timediff = time.time() - entry._timestamp
            if timediff > config['CacheLifeTime']:
                entry._timestamp = time.time()
                return "Replacement of  key in cache" 
            else:
                return None
    
    def build(self, key, name, opened, entry):
        args, kw = key
        return self.function(*args, **dict(kw))


"""
#Function for caching can be used in the following way ...
def compute(n):
        print "Sleeping for ", n, " seconds"
        time.sleep(n)
        return "Done ........"

func =  FunctionCache(compute)
print(func(2))
print(func(3))
print(func(2))
"""

