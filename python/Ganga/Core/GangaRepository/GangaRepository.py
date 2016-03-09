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
from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Proxy import getName

logger = getLogger()

# Error raised on schema version error


class SchemaVersionError(GangaException):

    def __init__(self, what=''):
        GangaException.__init__(self, what)
        self.what = what

    def __str__(self):
        return "SchemaVersionError: %s" % self.what


class InaccessibleObjectError(GangaException):

    def __init__(self, repo=None, id='', orig=None):
        GangaException.__init__(self, "Inaccessible Object")
        self.repo = repo
        self.id = id
        self.orig = orig

    def __str__(self):
        if str(self.orig).find('comments') > -1:
            return "Please restart Ganga in order to reload the object"
        else:
            return "Repository '%s' object #%s is not accessible because of an %s: %s" % (self.repo.registry.name, self.id, getName(self.orig), str(self.orig))


class RepositoryError(GangaException):

    """ This error is raised if there is a fatal error in the repository."""

    def __init__(self, repo=None, what=''):
        GangaException.__init__(self, what)
        self.what = what
        self.repository = repo
        logger.error("A severe error occurred in the Repository '%s': %s" % (repo.registry.name, what))
        logger.error('If you believe the problem has been solved, type "reactivate()" to re-enable ')
        try:
            from Ganga.Core.InternalServices.Coordinator import disableInternalServices
            disableInternalServices()
            logger.error("Shutting Down Repository_runtime")
            from Ganga.Runtime import Repository_runtime
            Repository_runtime.shutdown()
        except:
            logger.error("Unable to disable Internal services, they may have already been disabled!")


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
        Create objects as needed , and set the _index_cache through setNodeIndexCache
        for all objects that are not fully loaded.
        Returns a list of ids of jobs that changed/removed/added
        Raise RepositoryError
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

    def lock(self, ids):
        """lock(ids) --> bool
        Locks the specified IDs against modification from other Ganga sessions
        Raise RepositoryError
        Returns successfully locked ids
        """
        raise NotImplementedError

    def unlock(self, ids):
        """unlock(ids) --> None
        Unlock the specified IDs to allow another Ganga session to modify them
        EXPERIMENTAL - does not have to be implemented.
        """
        pass

    def clean(self):
        """clear() --> None
        Remove the Repository completely (rm -rf) and set it up again.
        Very violent. Use with caution.
        """
        pass

# Optional but suggested functions
    def get_lock_session(self, id):
        """get_lock_session(id)
        Tries to determine the session that holds the lock on id for information purposes, and return an informative string.
        Returns None on failure
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
        Raise PluginManagerError if the class name is not found"""
        compound_name = str(category+"_"+classname)
        if compound_name not in self._found_classes:
            cls = allPlugins.find(category, classname)
            self._found_classes[compound_name] = cls
        cls = self._found_classes[compound_name]
        obj = cls()
        #setattr(obj, '_parent', None)
        #obj.__init__()
        obj.setNodeData({})
        obj.setNodeAttribute('id', this_id)

        self._internal_setitem__(this_id, obj)
        return obj

    def _internal_setitem__(self, this_id, obj):
        """ Internal function for repository classes to add items to the repository.
        Should not raise any Exceptions
        """
        if this_id in self.incomplete_objects:
            self.incomplete_objects.remove(this_id)
        self.objects[this_id] = obj
        setattr(obj, "_registry_id", this_id)
        setattr(obj, "_registry_locked", False)
        obj.setNodeAttribute('id', this_id)
        obj._setRegistry(self.registry)


    def _internal_del__(self, id):
        """ Internal function for repository classes to (logically) delete items to the repository."""
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
        self._next_id = 0

    def update_index(self, id=None):
        pass

    def shutdown(self):
        pass

    def add(self, objs, force_ids=None):
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
        for id in ids:
            self._internal_del__(id)

    def load(self, ids):
        pass

    def flush(self, ids):
        pass

    def lock(self, ids):
        return True

    def unlock(self, ids):
        pass
