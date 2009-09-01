################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GangaRepository.py,v 1.1.2.9 2009-07-16 14:04:17 ebke Exp $
################################################################################

# Only the corresponding registry may access the methods of a GangaRepository.
# It should only raise RepositoryErrors and LockingErrors

from Ganga.Core import GangaException
from Ganga.Utility.Plugin import PluginManagerError, allPlugins
from Ganga.Core.InternalServices.Coordinator import disableInternalServices

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version

# Empty Ganga Object. This should never be saved back to file.
class EmptyGangaObject(GangaObject):
    """Empty Ganga Object. Is used to construct incomplete jobs"""
    _schema = Schema(Version(0,0), {})
    _name   = "Unknown"
    _category = "internal"
    _hidden = 1

# Error raised on schema version error
class SchemaVersionError(GangaException):
    pass


class RepositoryError(GangaException):
    def __init__(self,repo,what):
        GangaException.__init__(self,what)
        self.what=what
        self.repository = repo
        logger.error("A severe error occurred in the Repository '%s': %s" % (repo.registry.name, what))
        logger.error('If you believe the problem has been solved type "reactivate()" to re-enable ')
        disableInternalServices()

class GangaRepository(object):
    """ GangaRepository is the base class for repository backend implementations.
        It provides an interface for developers of new backends.
        The base class implements a transient Ganga Repository for testing purposes.
    """

    def __init__(self, registry):
        """GangaRepository constructor. Initialization should be done in startup()"""
        self.registry = registry

## Functions that should be overridden and implemented by derived classes.
    def startup(self):
        """startup() --> None
        Connect to the repository.
        Raise RepositoryError
        """
        raise NotImplementedError

    def update_index(self, id = None):
        """update_index(id = None) --> None
        Read the index containing the given ID (or all indices if id is None).
        Create objects as needed , and set the _index_cache for all objects 
        that are not fully loaded.
        Raise RepositoryError
        """
        raise NotImplementedError

    def shutdown(self):
        """shutdown() --> None
        Disconnect from the repository.
        Raise RepositoryError
        """
        raise NotImplementedError

    def add(self, objs):
        """add(objects) --> list of object IDs in this repository
        Add the given objects to the repository and return their IDs 
        After successfully determining the id call _internal_setitem__(id,obj)
        for each id/object pair.
        Raise RepositoryError
        """
        raise NotImplementedError
        
    def delete(self, ids):
        """delete(ids) --> None
        Delete the objects specified by the ids from the repository.
        Assumes that the objects associated to the ids are locked (!)
        Call _internal_del__(id) for each id to remove the GangaObject
        from the Registry 
        Raise KeyError
        Raise RepositoryError
        """
        raise NotImplementedError

    def load(self, ids):
        """load(ids) --> None
        Load the objects specified by the ids from the persistency layer.
        Raise KeyError
        Raise RepositoryError
        """
        raise NotImplementedError

    def flush(self, ids):
        """flush(ids) --> None
        Writes the objects specified by the ids to the persistency layer.
        Raise KeyError
        Raise RepositoryError
        """
        raise NotImplementedError

    def lock(self,ids):
        """lock(ids) --> bool
        Locks the specified IDs against modification from other Ganga sessions
        Raise RepositoryError
        Returns True on success, False if one of the ids is already locked by another session
        Also returns False if one of the ids is associated with a deleted object
        """
        raise NotImplementedError

    def unlock(self,ids):
        """unlock(ids) --> None
        Unlock the specified IDs to allow another Ganga session to modify them
        EXPERIMENTAL - does not have to be implemented.
        """
        pass

    def _make_empty_object_(self, id, category, classname):
        """Internal helper: adds an empty GangaObject of the given class to the repository.
        Raise RepositoryError
        Raise PluginManagerError if the class name is not found"""
        cls = allPlugins.find(category, classname)
        obj  = super(cls, cls).__new__(cls)
        obj._proxyObject = None
        obj._data = None
        self._internal_setitem__(id,obj)
        return obj

    def _internal_setitem__(self, id, obj):
        """ Internal function for repository classes to add items to the repository."""
        if id == 0:
            self._metadata = obj
        else:
            self._objects[id] = obj
        obj.__dict__["_registry_id"] = id
        obj.__dict__["_registry_locked"] = False
        if obj._data and "id" in obj._data.keys():
            obj.id = id
        obj._setRegistry(self.registry)

    def _internal_del__(self, id):
        """ Internal function for repository classes to delete items to the repository."""
        if id == 0:
            self._metadata._setRegistry(None)
            self._metadata = None
        else:
            self._objects[id]._setRegistry(None)
            del self._objects[id].__dict__["_registry_id"]
            del self._objects[id].__dict__["_registry_locked"]
            del self._objects[id]

    def _getMetadataObject(self):
        raise NotImplementedError

    def _setMetadataObject(self, obj):
        raise NotImplementedError

class GangaRepositoryTransient(object):
    """This class implements a transient Ganga Repository for testing purposes.
    """
## Functions that should be overridden and implemented by derived classes.
    def startup(self):
        self._next_id = 0
        self._metadata = None

    def update_index(self, id = None):
        pass

    def shutdown(self):
        pass

    def add(self, objs):
        ids = []
        for obj in objs:
            self._internal_setitem__(self._next_id, obj)
            ids.append(self._next_id)
            self._next_id += 1
        return ids
        
    def delete(self, ids):
        for id in ids:
            self._internal_del__(id)

    def load(self, ids):
        pass

    def flush(self, ids):
        pass

    def lock(self,ids):
        return True

    def unlock(self,ids):
        pass

    def _getMetadataObject(self):
        return self._metadata

    def _setMetadataObject(self, obj):
        self._metadata = obj
