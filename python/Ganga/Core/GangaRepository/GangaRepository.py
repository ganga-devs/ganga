##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GangaRepository.py,v 1.1.2.9 2009-07-16 14:04:17 ebke Exp $
##########################################################################

# Only the corresponding registry may access the methods of a GangaRepository.
# It should only raise RepositoryErrors and LockingErrors

# there are two "MAGIC" variables in an object: id and status.
# If a root object has an id field, it will be set to the repository id
# if a root object has a status field and some load error occurs, it will
# be set to "incomplete"

from Ganga.Utility.logging import getLogger

from Ganga.Utility.Plugin import allPlugins
from Ganga.Core.exceptions import SchemaVersionError, RepositoryError
from Ganga.GPIDev.Base.Proxy import getName

logger = getLogger()

class GangaRepository(object):

    """ GangaRepository is the base class for repository backend implementations.
        It provides an interface for developers of new backends.
        The base class implements a transient Ganga Repository for testing purposes.
    """

    def __init__(self, registry, locking=True):
        """GangaRepository constructor. Initialization should be done in startup()"""
        super(GangaRepository, self).__init__()
        self.registry = registry
        self.objects = {}
        self.incomplete_objects = []
        self._found_classes = {}

# Functions that should be overridden and implemented by derived classes.
    def startup(self):
        """startup() --> None
        Connect to the repository.
        Raise RepositoryError
        """
        raise NotImplementedError

    def update_index(self, id=None):
        """update_index(id = None) --> iterable of ids
        Read the index containing the given ID (or all indices if id is None).
        Create objects as needed , and set the _index_cache
        for all objects that are not fully loaded.
        Returns a list of ids of jobs that changed/removed/added
        Raise RepositoryError
        Args:
            id (int): id of the object which needs it's index updated
        """
        raise NotImplementedError

    def shutdown(self):
        """shutdown() --> None
        Disconnect from the repository.
        Raise RepositoryError
        """
        raise NotImplementedError

    def add(self, objs, force_ids=None):
        """add(objects) --> list of object IDs in this repository
        Add the given objects to the repository and return their IDs 
        After successfully determining the id call _internal_setitem__(id,obj)
        for each id/object pair.
        WARNING: If forcing the IDs, no locking is done here!
        Raise RepositoryError
        Args:
            objs (list): The objects which are to be added to the repo
            force_ids (list, None): The ids which we want to assign, None for auto-assign
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
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        raise NotImplementedError

    def load(self, ids):
        """load(ids) --> None
        Load the objects specified by the ids from the persistency layer.
        Raise KeyError
        Raise RepositoryError
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        raise NotImplementedError

    def flush(self, ids):
        """flush(ids) --> None
        Writes the objects specified by the ids to the persistency layer.
        Raise KeyError
        Raise RepositoryError
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        raise NotImplementedError

    def lock(self, ids):
        """lock(ids) --> bool
        Locks the specified IDs against modification from other Ganga sessions
        Raise RepositoryError
        Returns successfully locked ids
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        raise NotImplementedError

    def unlock(self, ids):
        """unlock(ids) --> None
        Unlock the specified IDs to allow another Ganga session to modify them
        EXPERIMENTAL - does not have to be implemented.
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def clean(self):
        """clear() --> None
        Remove the Repository completely (rm -rf) and set it up again.
        Very violent. Use with caution.
        """
        pass

    def isObjectLoaded(self, obj):
        """
        Returns if an object is loaded into memory
        Args:
            obj (GangaObject): object we want to know if it's in memory or not
        """
        raise NotImplementedError


# Optional but suggested functions
    def get_lock_session(self, id):
        """get_lock_session(id)
        Tries to determine the session that holds the lock on id for information purposes, and return an informative string.
        Returns None on failure
        Args:
            id (int): This is the id which we want to check is locked
        """
        return None

    def get_other_sessions(self):
        """get_session_list()
        Tries to determine the other sessions that are active and returns an informative string for each of them.
        """
        return []

    def reap_locks(self):
        """reap_locks() --> True/False
        Remotely clear all foreign locks from the session.
        WARNING: This is not nice.
        Returns True on success, False on error."""
        return False

    # Internal helper functions for derived classes
    def _make_empty_object_(self, this_id, category, classname):
        """Internal helper: adds an empty GangaObject of the given class to the repository.
        Raise RepositoryError
        Raise PluginManagerError if the class name is not found
        Args:
            this_id (int): This is the id to assign to the empty object
            category (str): This is the category the object belongs to
            classname (str): This is the name of the class of the object which is used to construct it
        """
        compound_name = str(category+"_"+classname)
        if compound_name not in self._found_classes:
            cls = allPlugins.find(category, classname)
            self._found_classes[compound_name] = cls
        cls = self._found_classes[compound_name]
        obj = cls()
        obj._data = {}

        obj._setFlushed()
        self._internal_setitem__(this_id, obj)
        return obj

    def _internal_setitem__(self, this_id, obj):
        """ Internal function for repository classes to add items to the repository.
        Should not raise any Exceptions
        Args:
            this_id (int): This is the id of the object we're to assign
            obj (GangaObject): This is the Object which we're assigning
        """
        if this_id in self.incomplete_objects:
            self.incomplete_objects.remove(this_id)
        self.objects[this_id] = obj
        obj._registry_id = this_id
        obj._registry_locked = False
        obj._id = this_id
        if 'id' in obj._schema.allItemNames():
            obj.setSchemaAttribute('id', this_id)  # Don't set the object as dirty
        obj._setRegistry(self.registry)

    def _internal_del__(self, id):
        """ Internal function for repository classes to (logically) delete items to the repository.
        Args:
            id (int):
        """
        if id in self.incomplete_objects:
            self.incomplete_objects.remove(id)
        else:
            self.objects[id]._setRegistry(None)
            del self.objects[id]


class GangaRepositoryTransient(object):

    """This class implements a transient Ganga Repository for testing purposes.
    """
# Functions that should be overridden and implemented by derived classes.

    def startup(self):
        """
        Startup a minimal in-memory repo
        """
        self._next_id = 0

    def update_index(self, id=None):
        """
        Nop the updating of the index of this in-memory repo
        Args:
            id (int, None): The id which we want to update the index for
        """
        pass

    def shutdown(self):
        """
        Nop the shutdown of this in-memory repo
        """
        pass

    def add(self, objs, force_ids=None):
        """
        Add the object to the main dict
        Args:
            objs (list): Objects we want to store in memory
            force_ids (list, None): IDs to assign to the objects, None for auto-assign
        """
        assert force_ids is None or len(force_ids) == len(objs)
        ids = []
        for i in range(len(objs)):
            obj = objs[i]
            if force_ids:
                id = force_ids[i]
            else:
                id = self._next_id
            self._internal_setitem__(id, obj)
            ids.append(id)
            self._next_id = max(self._next_id + 1, id + 1)
        return ids

    def delete(self, ids):
        """
        Remove the object from the main dict
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        for id in ids:
            self._internal_del__(id)

    def load(self, ids):
        """
        Nop the load of these ids to disk. We don't
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def flush(self, ids):
        """
        Nop the flushing of these ids to disk. We don't
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def lock(self, ids):
        """
        Has the list of IDs been locked from other ganga instances (True by def)
        """
        return True

    def unlock(self, ids):
        """
        Nop the unlocking of disk locks for this repo
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def isObjectLoaded(self, obj):
        """
        Returns if an object is loaded into memory
        Args:
            obj (GangaObject): object we want to know if it's in memory or not
        """
        return True

