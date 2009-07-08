################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GangaRepository.py,v 1.1.2.3 2009-07-08 14:40:42 ebke Exp $
################################################################################
#
# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking
#
# Note: No functions may access _objects or _objects_id_map more than once (or both once)
# since this could lead to avoid inconsistent states. At the moment the locking is left to python.

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
import threading

# Empty Ganga Object. This should never be saved back to file.
class EmptyGangaObject(GangaObject):
    """Empty Ganga Object. Is used to construct incomplete jobs"""
    _schema = Schema(Version(0,0), {})
    _name   = "Unknown"
    _category = "unknownObjects"
    _hidden = 1

# Error raised on schema version error
class SchemaVersionError(Exception):
    pass

class GangaRepository(object):
    """GangaRepository is the base class for repository backend implementation.
       It provides an interface for developers for having a persistent, locked list of 
       Ganga objects.
    """

## Functions that should not be overridden
    def __init__(self, registry, dirty_flush_counter=10):
        """ GangaRepository abstract constructor. 
        name is: box, jobs, templates, tasks
        location is: gangadir/.../...
        """
        self.name = registry.name
        self.location = registry.location
        self.type = registry.type
        self.registry = registry
        self._objects = {}
        self._object_id_map = {}
        self._lock = threading.RLock()
        self.dirty_flush_counter = dirty_flush_counter
        self.dirty_objs = {}
        self.dirty_hits = 0

    def __getitem__(self,id):
        """ Returns the Ganga Object with the given id.
            May return a not fully initialized object
            Raise KeyError"""
        if not id in self._objects:
            self.load([id])
        return self._objects[id]

    def __len__(self):
        return len(self._objects)

    def __contains__(self,id):
        """ Returns True if the given id is in the repository"""
        return id in self._objects

    def ids(self):
        """ Returns the list of ids of this repository """
        k = self._objects.keys()
        k.sort()
        return k

    def keys(self):
        """ Returns the list of ids of this repository """
        return self.ids()

    def values(self):
        its = self._objects.items()
        its.sort()
        return [it[1] for it in its]

    def __iter__(self):
        return self.ids().__iter__()

    def find(self,obj):
        """ Returns the index of the given object in the repository.
        Returns -1 if the object is not in the repository """
        return self._object_id_map.get(obj,-1)

    def _internal_setitem__(self, id, obj):
        """ Internal function for derived classes to add items to the repository """
        self._objects[id] = obj
        self._object_id_map[obj] = id
        if obj._data and "id" in obj._data.keys():
            obj.id = id
        obj._setRegistry(self.registry)
        
    def _dirty(self,obj):
        """ mark an object as dirty
            trigger automatic obj flush after specified number of dirty hits """
        self.dirty_objs[obj] = obj
        self.dirty_hits += 1
        if self.dirty_hits % self.dirty_flush_counter == 0:
            self.flush([self.find(o) for o in self.dirty_objs.keys()])

## Functions that must be overridden and implemented by derived classes.
## Direct access to _object and _object_id_map must be avoided or locked.
    def startup(self):
        """startup(self) ---> None
        Raise RepositoryError
        Tries to take the repository lock if applicable, and connects to 
        the persistency layer. 
        After this function self._objects should be fully initialized, but the
        individual objects do not have to be fully loaded (_data can be None)
        """
        # MANAGE the locks: session lock, and counter lock
        # this may spawn a thread
        pass

    def shutdown(self):
        """shutdown(self) ---> None
        Releases all the locks and flushes the repository to persistent storage
        This is called by an atexit handler registered by the registry runtime.
        Raise RepositoryError
        """
        self.flush([self.find(o) for o in self.dirty_objs.keys()])

    def add(self, objs):
        """add(self, objs) --> list of unique ids
        Raise RepositoryError
        objs -- list of ganga objects to register
        Returns a list of unique ids for the items in order.
        Does not check if objs are already registered.
        """
        raise NotImplementedError

    def flush(self, ids):
        """commit(self, ids) --> None
        Raise RepositoryError
        Raise KeyError
        Flush the objects with the given ids to disk.
        """
        raise NotImplementedError

    def load(self, ids):
        """checkout(self, ids) --> list of objects
        Raise RepositoryError
        Raise KeyError
        ids -- list of object ids to fully load.
        """
        raise NotImplementedError

    def delete(self, ids):
        """delete(self, ids) --> None
        Raise RepositoryError
        Raise KeyError
        ids -- list of job ids
        """
        raise NotImplementedError

    def acquireWriteLock(self,ids):
        """ acquireWriteLock -> bool
        Obtain write lock on object [id]. It is OK to acquire lock multiple times.
        Returns True if successful, False if not. Does not block.
        (TODO: Set obj.write_lock to the timestamp.)
        This function should be thread safe.
        Raise RepositoryError
        Raise KeyError
        """
        pass

    def releaseWriteLock(self,id):
        """ This method is currently only used atexit. 
        The write lock is taken for the whole duration of a session (for the moment).
        Raise RepositoryError
        Raise KeyError
        """
        pass


