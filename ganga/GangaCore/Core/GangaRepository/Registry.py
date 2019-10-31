

import functools
from GangaCore.Utility.logging import getLogger

from GangaCore.Core.exceptions import (GangaException,
                                   InaccessibleObjectError,
                                   RepositoryError)

import time
import threading

from GangaCore.Core.GangaThread.GangaThread import GangaThread
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.GPIDev.Base.Proxy import isType, getName
from GangaCore.Utility.Config import getConfig

logger = getLogger()

class RegistryError(GangaException):

    def __init__(self, what=''):
        super(RegistryError, self).__init__(self, what)
        self.what = what

    def __str__(self):
        return "RegistryError: %s" % self.what


class RegistryAccessError(RegistryError):

    """ This error is raised if the request is valid in principle, 
        but the Registry cannot be accessed at the moment."""

    def __init__(self, what=''):
        super(RegistryAccessError, self).__init__(what)

    def __str__(self):
        return "RegistryAccessError: %s" % self.what


class RegistryLockError(RegistryError):

    """ This error is raised if the request is valid in principle,
        but the object is locked by another Ganga session"""

    def __init__(self, what=''):
        super(RegistryLockError, self).__init__(what)

    def __str__(self):
        return "RegistryLockError: %s" % self.what


class ObjectNotInRegistryError(RegistryError):

    """ This error is raised if an object has been associated to this registry,
        but is not actually in the registry. This most probably indicates an internal Ganga error."""

    def __init__(self, what=''):
        super(ObjectNotInRegistryError, self).__init__(what)

    def __str__(self):
        return "ObjectNotInRegistryError: %s" % self.what


class RegistryKeyError(RegistryError, KeyError):

    """ This error is raised if the given id is not found in the registry """

    def __init__(self, what=''):
        super(RegistryKeyError, self).__init__(what)

    def __str__(self):
        return "RegistryKeyError: %s" % self.what


class RegistryIndexError(RegistryError, IndexError):

    """ This error is raised if the given id is not found in the registry """

    def __init__(self, what=''):
        super(RegistryIndexError, self).__init__(what)

    def __str__(self):
        return "RegistryIndexError: %s" % self.what


def makeRepository(registry):
    """Factory that selects, imports and instantiates the correct GangaRepository
    Args:
        registry (Registry): This maps the Registry type to the correct Repository
    """
    if registry.type in ["LocalXML", "LocalPickle"]:
        from GangaCore.Core.GangaRepository.GangaRepositoryXML import GangaRepositoryLocal
        return GangaRepositoryLocal(registry)
    elif registry.type in ["SQLite"]:
        from GangaCore.Core.GangaRepository.GangaRepositorySQLite import GangaRepositorySQLite
        return GangaRepositorySQLite(registry)
    elif registry.type in ["Transient"]:
        from GangaCore.Core.GangaRepository.GangaRepository import GangaRepository
        return GangaRepository(registry)
    elif registry.type in ["ImmutableTransient"]:
        from GangaCore.Core.GangaRepository.GangaRepositoryImmutableTransient import GangaRepositoryImmutableTransient
        return GangaRepositoryImmutableTransient(registry, registry.location, registry.file_ext, registry.pickle_files)
    else:
        raise RegistryError("Repository %s: Unknown repository type %s" % (registry.name, registry.type))


class IncompleteObject(GangaObject):

    """ This class represents an object that could not be loaded on startup"""

    _schema = Schema(Version(0, 0), {})
    _name = "IncompleteObject"
    _category = "internal"
    _hidden = 1

    _exportmethods = ['reload', 'remove', '__repr__']

    _additional_slots = ('registry', 'id')

    def __init__(self, registry, this_id):
        """
        This constructs an object which is placed into the objects dict when a repo fails to load an object due to some error
        Args:
            registry (Registry): This is the registry the object belongs to
            this_id (int): This is the registry/repo id of the object in the objects dict
        """
        super(IncompleteObject, self).__init__()
        self.registry = registry
        self.id = this_id

    def reload(self):
        """
        This will trigger a re-load of the object from disk which is useful if the object was locked but accessible by Ganga
        TODO work ouf if this is still called anywhere
        """
        with self.registry._flush_lock:
            with self.registry._read_lock:
                self.registry._load(self)
                logger.debug("Successfully reloaded '%s' object #%i!" % (self.registry.name, self.id))

    def remove(self):
        """
        This will trigger a delete of the the object itself from within the given Repository but not registry
        TODO work out if this is safe and still called
        """
        with self.registry._flush_lock:
            with self.registry._read_lock:
                if len(self.registry.repository.lock([self.id])) == 0:
                    errstr = "Could not lock '%s' object #%i!" % (self.registry.name, self.id)
                    try:
                        errstr += " Object is locked by session '%s' " % self.registry.repository.get_lock_session(self.id)
                    except Exception as err:
                        logger.debug("Remove Lock error: %s" % err)
                    raise RegistryLockError(errstr)
                self.registry.repository.delete([self.id])

    def __repr__(self):
        """
        This returns a repr of the object in question as inaccessible
        """
        return "Incomplete object in '%s', ID %i. Try reload() or remove()." % (self.registry.name, self.id)


def synchronised_flush_lock(f):
    """
    Specific flush lock as flushing can take a long time. This ensures no changes occur while flushing though reads can.

    Args:
        f (func): Function to decorate
    """
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
        with self._flush_lock:
            return f(self, *args, **kwargs)
    return decorated

def synchronised_read_lock(f):
    """
    General read lock for quick functions that won't take a while but won't change anything either

    Args:
        f (func): Function to decorate
    """
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
        with self._read_lock:
            return f(self, *args, **kwargs)
    return decorated

def synchronised_complete_lock(f):
    """
    Entirely lock the registry. Make sure locks are acquired in the right order!

    Args:
        f (func): Function to decorate
    """
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
        with self._flush_lock:
            with self._read_lock:
                return f(self, *args, **kwargs)
    return decorated

class RegistryFlusher(GangaThread):
    """
    This class is intended to be used by the registry to perfom
    automatic flushes on a fixed schedule so that information is not
    lost if Ganga is shut down abruptly.
    """

    __slots__ = ('registry', '_stop_event')

    def __init__(self, registry, *args, **kwargs):
        """
        This inits the RegistryFlusher and makes use of threading events
        Args:
            registry (Registry): The registry this Registry Flusher is flushing
            args (list): Args passed to the constructor of a new RegistryFlusher thread
            kwargs (dict): Kwds passed to the constructor of a new RegistryFlusher thread
        """
        super(RegistryFlusher, self).__init__(*args, **kwargs)
        self.registry = registry
        self._stop_event = threading.Event()

    def stop(self):
        """
        Ask the thread to stop what it is doing and it will finish
        the next chance it gets.
        TODO, does this need to be exposed as a method if only used internally?
        """
        self._stop_event.set()

    @property
    def stopped(self):
        """
        Returns if the flusher has stopped as a boolean?
        """
        return self._stop_event.isSet()

    def join(self, *args, **kwargs):
        """
        Called on thread shutdown to stop the active thread
        Args:
            args (list): Args passed to the constructor of a new RegistryFlusher thread
            kwargs (dict) : Kwds passed to the constructor of a new RegistryFlusher thread
        """
        self.stop()
        super(RegistryFlusher, self).join(*args, **kwargs)

    def run(self):
        """
        This will run an indefinite loop which periodically checks
        whether it should stop. In between calls to ``flush_all`` it
        will wait for a fixed period of time.
        """
        sleeps_per_second = 10  # This changes the granularity of the sleep.
        regConf = getConfig('Registry')
        while not self.stopped:
            sleep_period = regConf['AutoFlusherWaitTime']
            for i in range(sleep_period*sleeps_per_second):
                time.sleep(1/sleeps_per_second)
                if self.stopped:
                    return
            # This will trigger a flush on all dirty objects in the repo,
            # It will lock all objects dirty as a result of the nature of the flush command
            logger.debug('Auto-flushing: %s', self.registry.name)
            if regConf['EnableAutoFlush']:
                self.registry.flush_all()
        logger.debug("Auto-Flusher shutting down for Registry: %s" % self.registry.name)


class Registry(object):

    """Ganga Registry
    Base class providing a dict-like locked and lazy-loading interface to a Ganga repository
    """

    __slots__ = ('name', 'doc', '_hasStarted', '_needs_metadata', 'metadata', '_read_lock', '_flush_lock', '_parent', 'repository', '_objects', '_incomplete_objects', 'flush_thread', 'type', 'location')

    def __init__(self, name, doc):
        """Registry constructor, giving public name and documentation
        Args:
            name (str): Name of teh registry e.g. 'jobs', 'box', 'prep'
            doc (str): This is the doc string of the registry describing it's use and contents
        """
        self.name = name
        self.doc = doc
        self._hasStarted = False
        self._needs_metadata = False
        self.metadata = None
        self._read_lock = threading.RLock()
        self._flush_lock = threading.RLock()

        self._parent = None

        self.repository = None
        self._objects = None
        self._incomplete_objects = None

        self.flush_thread = None

    def hasStarted(self):
        """
        Wrapper function to return _hasStarted boolen
        TODO is this needed over accessing the boolean, should the boolean be 'public'
        """
        return self._hasStarted

    # Methods intended to be called from ''outside code''
    def __getitem__(self, this_id):
        """ Returns the Ganga Object with the given id.
            Raise RegistryKeyError
        Args:
            this_id (int): This is the key of an object in the object dictionary
        """
        logger.debug("__getitem__")
        # Is this an Incomplete Object?
        if this_id in self._incomplete_objects:
            return IncompleteObject(self, this_id)

        # Nope, so try to find it and raise an exception if not
        try:
            return self._objects[this_id]
        except KeyError as err:
            logger.debug("Repo KeyError: %s" % err)
            logger.debug("Keys: %s id: %s" % (list(self._objects.keys()), this_id))
            raise RegistryKeyError("Could not find object #%s" % this_id)

    @synchronised_read_lock
    def __len__(self):
        """ Returns the current number of root objects """
        logger.debug("__len__")
        return len(self._objects)

    @synchronised_read_lock
    def __contains__(self, this_id):
        """ Returns True if the given ID is in the registry
        Args:
            this_id (int): This is the key of an object in the object dictionary
        """
        logger.debug("__contains__")
        return this_id in self._objects

    def updateLocksNow(self):
        """
        Update the registry locks now. This is explicitly called from a method which can cause Ganga to pause for a long time
        TODO: determine if this is still valid
        """
        logger.debug("updateLocksNow")
        self.repository.updateLocksNow()

    @synchronised_read_lock
    def ids(self):
        """ Returns the list of ids of this registry """
        logger.debug("ids")
        return sorted(self._objects.keys())

    @synchronised_read_lock
    def items(self):
        """ Return the items (ID,obj) in this registry. 
        Recommended access for iteration, since accessing by ID can fail if the ID iterator is old"""
        return sorted(self._objects.items())

    def iteritems(self):
        """ Return the items (ID,obj) in this registry."""
        logger.debug("iteritems")
        returnable = list(self.items())
        return returnable

    @synchronised_read_lock
    def keys(self):
        """ Returns the list of ids of this registry """
        logger.debug("keys")
        returnable = self.ids()
        return returnable

    @synchronised_read_lock
    def values(self):
        """ Return the objects in this registry, in order of ID.
        Besides items() this is also recommended for iteration."""
        logger.debug("values")
        returnable = [it[1] for it in self.items()]
        return returnable

    def __iter__(self):
        """ Return an iterator for the self.values list """
        logger.debug("__iter__")
        returnable = iter(list(self.values()))
        return returnable

    def find(self, obj):
        """Returns the id of the given object in this registry, or 
        Raise ObjectNotInRegistryError if the Object is not found
        Args:
            _obj (GangaObject): This is the object we want to match in the objects repo
        """
        try:
            return next(id_ for id_, o in self._objects.items() if o is obj)
        except StopIteration:
            raise ObjectNotInRegistryError("Object '%s' does not seem to be in this registry: %s !" % (getName(obj), self.name))

    @synchronised_complete_lock
    def clean(self, force=False):
        """Deletes all elements of the registry, if no other sessions are present.
        if force == True it removes them regardless of other sessions.
        Returns True on success, False on failure.
        Args:
            force (bool): This will force a clean action to be performed even if there are other active Ganga sessions, EXTREMELY UNFRIENDLY!!!
        """
        logger.debug("clean")
        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot clean a disconnected repository!")

        if not force:
            other_sessions = self.repository.get_other_sessions()
            if len(other_sessions) > 0:
                logger.error("The following other sessions are active and have blocked the clearing of the repository: \n * %s" % ("\n * ".join(other_sessions)))
                return False
        self.repository.reap_locks()
        self.repository.delete(list(self._objects.keys()))
        self.repository.clean()

    # Methods that can be called by derived classes or Ganga-internal classes like Job
    # if the dirty objects list is modified, the methods must be locked by self._lock
    # all accesses to the repository must also be locked!

    @synchronised_complete_lock
    def _add(self, obj, force_index=None):
        """ Add an object to the registry and assigns an ID to it. 
        use force_index to set the index (for example for metadata). This overwrites existing objects!
        Raises RepositoryError
        Args:
            _obj (GangaObject): This is th object which is to be added to this Registy/Repo
            force_index (int): This is the index which we will give the object, None = auto-assign
        """
        logger.debug("_add")

        if self.hasStarted() is not True:
            raise RepositoryError("Cannot add objects to a disconnected repository!")

        if force_index is None:
            ids = self.repository.add([obj])
        else:
            if len(self.repository.lock([force_index])) == 0:
                raise RegistryLockError("Could not lock '%s' id #%i for a new object!" % (self.name, force_index))
            ids = self.repository.add([obj], [force_index])

        obj._setRegistry(self)
        obj._registry_locked = True

        self.repository.flush(ids)

        return ids[0]

    @synchronised_complete_lock
    def _remove(self, obj, auto_removed=0):
        """ Private method removing the obj from the registry. This method always called.
        This method may be overriden in the subclass to trigger additional actions on the removal.
        'auto_removed' is set to true if this method is called in the context of obj.remove() method to avoid recursion.
        Only then the removal takes place. In the opposite case the obj.remove() is called first which eventually calls
        this method again with "auto_removed" set to true. This is done so that obj.remove() is ALWAYS called once independent
        on the removing context.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError
        Raise ObjectNotInRegistryError
        Args:
            _obj (GangaObject): The object which we want to remove from the Repo/Registry
            auto_removed (int, bool): True/False for if the object can be auto-removed by the base Repository method
        """
        logger.debug("_remove")

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot remove objects from a disconnected repository!")

        if not auto_removed and hasattr(obj, "remove"):
            obj.remove()
        else:
            this_id = self.find(obj)
            self._acquire_session_lock(obj)

            logger.debug('deleting the object %d from the registry %s', this_id, self.name)
            self.repository.delete([this_id])

    @synchronised_flush_lock
    def _flush(self, objs):
        """
        Flush a set of objects to the persistency layer immediately

        Only those objects passed in will be flushed and only if they are dirty.

        Args:
            objs (list): a list of objects to flush
        """
        # Too noisy
        #logger.debug("_flush")

        if not isType(objs, (list, tuple, GangaList)):
            objs = [objs]

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot flush to a disconnected repository!")

        for obj in objs:
            # check if the object is dirty, if not do nothing
            if not obj._dirty:
                continue

            if not self.has_loaded(obj):
                continue

            with obj.const_lock:
                # flush the object
                obj_id = self.find(obj)
                self.repository.flush([obj_id])
                obj._setFlushed()

    def flush_all(self):
        """
        This will attempt to flush all the jobs in the registry.
        It does this via ``_flush`` so the same conditions apply.
        """
        if self.hasStarted():
            for _obj in self.values():
                self._flush(_obj)

        if self.metadata and self.metadata.hasStarted():
            self.metadata.flush_all()

    def _load(self, obj):
        """
        Use this function to load an object from disk as it will check if the object is already loaded *outside*
         of the lock for when the actual load takes place in _locked_load
        """
        if not self.repository.isObjectLoaded(obj):
            self._locked_load(obj)

    @synchronised_complete_lock
    def _locked_load(self, obj):
        """
        Fully load an object from a Repo/disk into memory. Should only ever be called from _load!
        Args:
            obj (GangaObject): This is the object we want to fully load
        """
        logger.debug("_locked_load")

        # find the object ID
        obj_id = self.find(obj)

        try:
            self.repository.load([obj_id])
        except Exception as err:
            logger.error("Error Loading Job! '%s'" % obj_id)
            # Cleanup after ourselves if an error occured
            # Didn't load mark as clean so it's not flushed
            if obj_id in self._objects:
                self._objects[obj_id]._setFlushed(auto_load_deps=False)
            raise

    def _acquire_session_lock(self, obj):
        """Obtain write access on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError
        Raise ObjectNotInRegistryError (via self.find())
        Args:
            _obj (GangaObject): This is the object we want to get write access to and lock on disk
        """
        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot get write access to a disconnected repository!")

        if not hasattr(obj, '_registry_locked') or not obj._registry_locked:
            this_id = self.find(obj)
            if len(self.repository.lock([this_id])) == 0:
                errstr = "Could not lock '%s' object #%i!" % (self.name, this_id)
                errstr += " Object is locked by session '%s' " % self.repository.get_lock_session(this_id)
                raise RegistryLockError(errstr)

            obj._registry_locked = True

    def _release_session_lock_and_flush(self, obj):
        """Release the lock on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise ObjectNotInRegistryError
        Args:
            obj (GangaObject): This is the object we want to release the file lock for
        """
        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot manipulate locks of a disconnected repository!")
        logger.debug("Reg: %s _release_lock(%s)" % (self.name, self.find(obj)))
        if hasattr(obj, '_registry_locked') and obj._registry_locked:
            oid = self.find(obj)
            self.repository.flush([oid])
            obj._registry_locked = False
            self.repository.unlock([oid])

    def getIndexCache(self, obj):
        """Returns a dictionary to be put into obj._index_cache (is this valid)
        This can and should be overwritten by derived Registries to provide more index values."""
        return {}

    @synchronised_complete_lock
    def startup(self):
        """Connect the repository to the registry. Called from Repository_runtime.py"""
        try:
            self._hasStarted = True
            t0 = time.time()
            self.repository = makeRepository(self)
            self._objects = self.repository.objects
            self._incomplete_objects = self.repository.incomplete_objects

            if self._needs_metadata:
                t2 = time.time()
                if self.metadata is None:
                    self.metadata = Registry(self.name + ".metadata", "Metadata repository for %s" % self.name)
                    self.metadata.type = self.type
                    self.metadata.location = self.location
                    setattr(self.metadata, '_parent', self) ## rcurrie Registry has NO '_parent' Object so don't understand this is this used for JobTree?
                logger.debug("metadata startup")
                self.metadata.startup()
                t3 = time.time()
                logger.debug("Startup of %s.metadata took %s sec" % (self.name, t3-t2))

            logger.debug("repo startup")
            #self.hasStarted() = True
            self.repository.startup()
            t1 = time.time()
            logger.debug("Registry '%s' [%s] startup time: %s sec" % (self.name, self.type, t1 - t0))
        except Exception as err:
            logger.debug("Logging Repo startup Error: %s" % err)
            self._hasStarted = False
            raise
        #finally:
        #    pass

        # Now we check the repo for faults and for inconsistent objects
        reg_config = getConfig('Registry')

        if reg_config['DisableLoadCheck']:
            return
        else:
            self.check()

    def check(self):
        """ This stub allows for a Registry class to perform maintenance on startup after the repo has been initialized
        This is intended to be called after the startup proceedure has been finished """
        pass

    @synchronised_complete_lock
    def shutdown(self):
        """Flush and disconnect the repository. Called from Repository_runtime.py """
        from GangaCore.Utility.logging import getLogger
        logger = getLogger()
        logger.debug("Shutting Down Registry")
        logger.debug("shutdown")
        try:
            self._hasStarted = True
            # NB flush_all by definition relies on both the metadata repo and the repo to be fully initialized
            try:
                self.flush_all()
            except Exception as err:
                logger.error("Exception on flushing '%s' registry: %s", self.name, err)

            # Now we can safely shutdown the metadata repo if one is loaded
            try:
                if self.metadata is not None:
                    self.metadata.shutdown()
            except Exception as err:
                logger.debug("Exception on shutting down metadata repository '%s' registry: %s", self.name, err)
                raise

            # Now we can release locks on the objects we have
            for obj in list(self._objects.values()):
                # locks are not guaranteed to survive repository shutdown
                obj._registry_locked = False
            self.repository.shutdown()

            self.metadata = None

        finally:
            self._hasStarted = False

    def info(self, full=False):
        """Returns an informative string onFlush and disconnect the repository. Called from Repository_runtime.py """
        logger.debug("info")
        s = "registry '%s': %i objects" % (self.name, len(self._objects))
        if full:
            other_sessions = self.repository.get_other_sessions()
            if len(other_sessions) > 0:
                s += ", %i other concurrent sessions:\n * %s" % (len(other_sessions), "\n * ".join(other_sessions))
        return s

    def has_loaded(self, obj):
        """Returns True/False for if a given object has been fully loaded by the Registry.
        Returns False on the object not being in the Registry!
        This ONLY applies to master jobs as the Registry has no apriori knowledge of the subjob structure.
        Consult SubJobXMLList for a more fine grained loaded/not-loaded info/test.
        Args:
            obj (GangaObject): Object which we want to look for in this repo
        """
        try:
            index = self.find(obj)
        except ObjectNotInRegistryError:
            return False

        return self.repository.isObjectLoaded(obj)

