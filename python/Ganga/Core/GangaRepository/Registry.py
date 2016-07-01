from __future__ import division

import functools
from Ganga.Utility.logging import getLogger

from Ganga.Core import GangaException
from Ganga.Core.GangaRepository import InaccessibleObjectError, RepositoryError

import time
import threading

from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
from Ganga.GPIDev.Base.Proxy import stripProxy, isType, getName
from Ganga.Utility.Config import getConfig

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
        from Ganga.Core.GangaRepository.GangaRepositoryXML import GangaRepositoryLocal
        return GangaRepositoryLocal(registry)
    elif registry.type in ["SQLite"]:
        from Ganga.Core.GangaRepository.GangaRepositorySQLite import GangaRepositorySQLite
        return GangaRepositorySQLite(registry)
    elif registry.type in ["Transient"]:
        from Ganga.Core.GangaRepository.GangaRepository import GangaRepository
        return GangaRepository(registry)
    elif registry.type in ["ImmutableTransient"]:
        from Ganga.Core.GangaRepository.GangaRepositoryImmutableTransient import GangaRepositoryImmutableTransient
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
        self.registry._lock.acquire()
        try:

            if not self.has_loaded(self._registry._objects[self.id]):
                self.registry._load([self.id])
            logger.debug("Successfully reloaded '%s' object #%i!" % (self.registry.name, self.id))
            for d in self.registry.changed_ids.itervalues():
                d.add(self.id)
        finally:
            self.registry._lock.release()

    def remove(self):
        """
        This will trigger a delete of the the object itself from within the given Repository but not registry
        TODO work out if this is safe and still called
        """
        self.registry._lock.acquire()
        try:
            if len(self.registry.repository.lock([self.id])) == 0:
                errstr = "Could not lock '%s' object #%i!" % (self.registry.name, self.id)
                try:
                    errstr += " Object is locked by session '%s' " % self.registry.repository.get_lock_session(self.id)
                except Exception as err:
                    logger.debug("Remove Lock error: %s" % err)
                raise RegistryLockError(errstr)
            self.registry.repository.delete([self.id])
            for d in self.registry.changed_ids.itervalues():
                d.add(self.id)
        finally:
            self.registry._lock.release()

    def __repr__(self):
        """
        This returns a repr of the object in question as inaccessible
        """
        return "Incomplete object in '%s', ID %i. Try reload() or remove()." % (self.registry.name, self.id)


def synchronised(f):
    """
    This decorator must be attached to a method on a ``Registry``
    It uses the object's lock to make sure that the object is held for the duration of the decorated function
    Args:
        f (function): Function in question being wrapped
    """
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
        # This is the Registry RLock
        # It it safe in principle to re-enter the registry through other lockable methods in the active lock-holding thread
        # However, the registry will decide through the use of the transaction locks as to whether it should actually do any work as a result
        with self._lock:
            return f(self, *args, **kwargs)
    return decorated


class RegistryFlusher(threading.Thread):
    """
    This class is intended to be used by the registry to perfom
    automatic flushes on a fixed schedule so that information is not
    lost if Ganga is shut down abruptly.
    """
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
        self._stop = threading.Event()

    def stop(self):
        """
        Ask the thread to stop what it is doing and it will finish
        the next chance it gets.
        TODO, does this need to be exposed as a method if only used internally?
        """
        self._stop.set()

    @property
    def stopped(self):
        """
        Returns if the flusher has stopped as a boolean?
        """
        return self._stop.isSet()

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

    def __init__(self, name, doc, update_index_time=30):
        """Registry constructor, giving public name and documentation
        Args:
            name (str): Name of teh registry e.g. 'jobs', 'box', 'prep'
            doc (str): This is the doc string of the registry describing it's use and contents
            update_index_time (int): This is used to determine how long to wait between updating the index in the repo
        """
        self.name = name
        self.doc = doc
        self._hasStarted = False
        self.update_index_time = update_index_time
        self._update_index_timer = 0
        self._needs_metadata = False
        self.metadata = None
        self._lock = threading.RLock()
        self.hard_lock = {}
        self.changed_ids = {}

        self._parent = None

        self.repository = None
        self._objects = None
        self._incomplete_objects = None

        ## Id's id(obj) of objects undergoing a transaction such as flush, remove, add, etc.
        self._inprogressDict = {}

        self.flush_thread = None

    def hasStarted(self):
        """
        Wrapper function to return _hasStarted boolen
        TODO is this needed over accessing the boolean, should the boolean be 'public'
        """
        return self._hasStarted

    def lock_transaction(self, this_id, action):
        """
        This creates a threading Lock on the repo which allows the repo to ignore transient repo actions and pause repo actions which modify the object
        Args:
            this_id (int): This is the python id of a given object, i.e. id(object) which is having a repo transaction performed on it
            action (str): This is a string which represents the transaction (useful for debugging collisions)
        """
        while this_id in self._inprogressDict:
            logger.debug("Getting item being operated on: %s" % this_id)
            logger.debug("Currently in state: %s" % self._inprogressDict[this_id])
            #import traceback
            #traceback.print_stack()
            #import sys
            #sys.exit(-1)
            #time.sleep(0.05)
        self._inprogressDict[this_id] = action
        if this_id not in self.hard_lock:
            self.hard_lock[this_id] = threading.Lock()
        self.hard_lock[this_id].acquire()

    def unlock_transaction(self, this_id):
        """
        This releases the threading Lock in the repo lock_transaction which re-allows all repo actions on the object in question
        """
        self._inprogressDict[this_id] = False
        del self._inprogressDict[this_id]
        self.hard_lock[this_id].release()

    # Methods intended to be called from ''outside code''
    def __getitem__(self, this_id):
        """ Returns the Ganga Object with the given id.
            Raise RegistryKeyError
        Args:
            this_id (int): This is the key of an object in the object dictionary
        """
        logger.debug("__getitem__")
        try:
            return self._objects[this_id]
        except KeyError as err:
            logger.debug("Repo KeyError: %s" % err)
            logger.debug("Keys: %s id: %s" % (self._objects.keys(), this_id))
            if this_id in self._incomplete_objects:
                return IncompleteObject(self, this_id)
            raise RegistryKeyError("Could not find object #%s" % this_id)

    @synchronised
    def __len__(self):
        """ Returns the current number of root objects """
        logger.debug("__len__")
        return len(self._objects)

    @synchronised
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

    @synchronised
    def ids(self):
        """ Returns the list of ids of this registry """
        logger.debug("ids")
        if self.hasStarted() is True and\
                (time.time() > self._update_index_timer + self.update_index_time):
            try:
                changed_ids = self.repository.update_index()
                for this_d in self.changed_ids.itervalues():
                    this_d.update(changed_ids)
            except Exception as err:
                pass
            self._update_index_timer = time.time()

        return sorted(self._objects.keys())

    @synchronised
    def items(self):
        """ Return the items (ID,obj) in this registry. 
        Recommended access for iteration, since accessing by ID can fail if the ID iterator is old"""
        logger.debug("items")
        if self.hasStarted() is True and\
                (time.time() > self._update_index_timer + self.update_index_time):
            try:
                changed_ids = self.repository.update_index()
                for this_d in self.changed_ids.itervalues():
                    this_d.update(changed_ids)
            except Exception as err:
                pass

            self._update_index_timer = time.time()

        return sorted(self._objects.items())

    def iteritems(self):
        """ Return the items (ID,obj) in this registry."""
        logger.debug("iteritems")
        returnable = self.items()
        return returnable

    @synchronised
    def keys(self):
        """ Returns the list of ids of this registry """
        logger.debug("keys")
        returnable = self.ids()
        return returnable

    @synchronised
    def values(self):
        """ Return the objects in this registry, in order of ID.
        Besides items() this is also recommended for iteration."""
        logger.debug("values")
        returnable = [it[1] for it in self.items()]
        return returnable

    def __iter__(self):
        """ Return an iterator for the self.values list """
        logger.debug("__iter__")
        returnable = iter(self.values())
        return returnable

    def find(self, _obj):
        """Returns the id of the given object in this registry, or 
        Raise ObjectNotInRegistryError if the Object is not found
        Args:
            _obj (GangaObject): This is the object we want to match in the objects repo
        """
    
        obj = stripProxy(_obj)
        try:
            return next(id_ for id_, o in self._objects.items() if o is obj)
        except StopIteration:
            raise ObjectNotInRegistryError("Object '%s' does not seem to be in this registry: %s !" % (getName(obj), self.name))

    @synchronised
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
        try:
            if not force:
                other_sessions = self.repository.get_other_sessions()
                if len(other_sessions) > 0:
                    logger.error("The following other sessions are active and have blocked the clearing of the repository: \n * %s" % ("\n * ".join(other_sessions)))
                    return False
            self.repository.reap_locks()
            self.repository.delete(self._objects.keys())
            self.changed_ids = {}
            self.repository.clean()
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise
        except Exception as err:
            logger.debug("Clean Unknown Err: %s" % err)
            raise

    @synchronised
    def __safe_add(self, obj, force_index=None):
        """
        Method which calls the underlying add function in the Repo for a given object
        Args:
            obj  (GangaObject): This is th object which is to be added to this Registy/Repo
            force_index (int, None): This is the index which we will give the object, None = auto-assign
        """
        logger.debug("__safe_add")
        if force_index is None:
            ids = self.repository.add([obj])
        else:
            if len(self.repository.lock([force_index])) == 0:
                raise RegistryLockError("Could not lock '%s' id #%i for a new object!" % (self.name, force_index))
            ids = self.repository.add([obj], [force_index])

        obj._setRegistry(self)
        obj._registry_locked = True

        this_id = self.find(obj)
        try:
            self.lock_transaction(this_id, "_add")

            self.repository.flush(ids)
            for this_v in self.changed_ids.itervalues():
                this_v.update(ids)

            logger.debug("_add-ed as: %s" % ids)
        finally:
            self.unlock_transaction(this_id)
        return ids[0]

    # Methods that can be called by derived classes or Ganga-internal classes like Job
    # if the dirty objects list is modified, the methods must be locked by self._lock
    # all accesses to the repository must also be locked!

    @synchronised
    def _add(self, _obj, force_index=None):
        """ Add an object to the registry and assigns an ID to it. 
        use force_index to set the index (for example for metadata). This overwrites existing objects!
        Raises RepositoryError
        Args:
            _obj (GangaObject): This is th object which is to be added to this Registy/Repo
            force_index (int): This is the index which we will give the object, None = auto-assign
        """
        logger.debug("_add")
        obj = stripProxy(_obj)

        if self.hasStarted() is not True:
            raise RepositoryError("Cannot add objects to a disconnected repository!")

        try:
            returnable_id = self.__safe_add(obj, force_index)
        except RepositoryError as err:
            raise
        except Exception as err:
            logger.debug("Unknown Add Error: %s" % err)
            raise

        return returnable_id

    @synchronised
    def _remove(self, _obj, auto_removed=0):
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
        obj = stripProxy(_obj)
        try:
            self.__reg_remove(obj, auto_removed)
        except ObjectNotInRegistryError as err:
            try:
                ## Actually  make sure we've removed the object from the repo 
                if self.find(obj):
                    del self._objects[self.find(obj)]
            except Exception as err:
                pass
        except Exception as err:
            raise

    def __reg_remove(self, obj, auto_removed=0):
        """
        This Method Performs the actual removal of the object from the Repo
        Args:
            obj (GangaObject): The object which we want to remove from the Repo/Registry
            auto_removed (0/1): True/False for if the object can be auto-removed by the base Repository method
        """

        logger.debug("_reg_remove")

        obj_id = id(obj)

        try:
            self.lock_transaction(obj_id, "_remove")


            if self.hasStarted() is not True:
                raise RegistryAccessError("Cannot remove objects from a disconnected repository!")
            if not auto_removed and hasattr(obj, "remove"):
                obj.remove()
            else:
                this_id = self.find(obj)
                try:
                    self._write_access(obj)
                except RegistryKeyError as err:
                    logger.debug("Registry KeyError: %s" % err)
                    logger.warning("double delete: Object #%i is not present in registry '%s'!" % (this_id, self.name))
                    return
                logger.debug('deleting the object %d from the registry %s', this_id, self.name)
                try:
                    self.repository.delete([this_id])
                    del obj
                    for this_v in self.changed_ids.itervalues():
                        this_v.add(this_id)
                except (RepositoryError, RegistryAccessError, RegistryLockError) as err:
                    raise
                except Exception as err:
                    logger.debug("unknown Remove Error: %s" % err)
                    raise
        finally:

            self.unlock_transaction(obj_id)

    @synchronised
    def _flush(self, objs):
        """
        Flush a set of objects to the persistency layer immediately

        Only those objects passed in will be flushed and only if they are dirty.

        Args:
            objs (list): a list of objects to flush
        """
        logger.debug("_flush")

        if isType(objs, (list, tuple, GangaList)):
            objs = [stripProxy(_obj) for _obj in objs]
        else:
            objs = [stripProxy(objs)]

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot flush to a disconnected repository!")

        for obj in objs:
            # check if the object is dirty, if not do nothing
            if not obj._dirty:
                continue

            if not self.has_loaded(obj):
                continue

            with obj.const_lock:
                # flush the object. Need to call _getWriteAccess for consistency reasons
                # TODO: getWriteAccess should only 'get write access', as that's not needed should it be called here?
                obj._getWriteAccess()
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

    def _read_access(self, _obj, sub_obj=None):
        """Obtain read access on a given object.
        sub-obj is the object the read access is actually desired (ignored at the moment)
        Raise RegistryAccessError
        Raise RegistryKeyError
        Args:
            _obj (GangaObject): The object which we want to get read access to and lock on disk
            sub_obj (GangaObject):  Ignored under the current model
        """
        logger.debug("_read_access")
        obj_id = id(stripProxy(_obj))
        if obj_id in self._inprogressDict:
            return

        with _obj.const_lock:
            self.__safe_read_access(_obj, sub_obj)

    @synchronised
    def _load(self, obj_ids):
        """
        Fully load an object from a Repo/disk into memory
        Args:
            obj_ids (list): This is the list of id which we want to fully load objects for according to object dict
        """
        logger.debug("_load")
        these_ids = []
        for obj_id in obj_ids:
            this_id = id(self[obj_id])
            these_ids.append(this_id)
            self.lock_transaction(this_id, "_load")

        ## Record dirty status before flushing
        ## Just in case we've requested loading over the job in memory
        prior_status = {}
        for obj_id in obj_ids:
            if obj_id in self._objects:
                prior_status[obj_id] = self._objects[obj_id]._dirty

        try:
            for obj_id in obj_ids:
                self.repository.load([obj_id])
        except Exception as err:
            logger.error("Error Loading Jobs! '%s'" % obj_ids)
            ## Cleanup aftr ourselves if an error occured
            for obj_id in obj_ids:
                ## Didn't load mark as clean so it's not flushed
                if obj_id in self._objects:
                    self._objects[obj_id]._setFlushed()
            raise
        finally:
            for obj_id in these_ids:
                self.unlock_transaction(obj_id)

    def __safe_read_access(self, _obj, sub_obj):
        """
        This method will attempt to load an unloaded object from disk and acquire a file lock to prevent other Ganga sessions modifying it
        Args:
            _obj (GangaObject): The object which we want to get read access to and lock on disk
            sub_obj (str): Ignored under the current model
        """
        logger.debug("_safe_read_access")
        obj = stripProxy(_obj)
        if id(obj) in self._inprogressDict:
            return

        if self.hasStarted() is not True:
            raise RegistryAccessError("The object #%i in registry '%s' is not fully loaded and the registry is disconnected! Type 'reactivate()' if you want to reconnect." % (self.find(obj), self.name))

        if hasattr(obj, "_registry_refresh"):
            delattr(obj, "_registry_refresh")
        assert not hasattr(obj, "_registry_refresh")

        try:
            try:
                if not self.has_loaded(obj):
                    this_id = self.find(obj)
                    self._load([this_id])
            except KeyError as err:
                logger.error("_read_access KeyError %s" % err)
                raise RegistryKeyError("Read: The object #%i in registry '%s' was deleted!" % (this_id, self.name))
            except InaccessibleObjectError as err:
                raise
                raise RegistryKeyError("Read: The object #%i in registry '%s' could not be accessed - %s!" % (this_id, self.name, err))
            for this_d in self.changed_ids.itervalues():
                this_d.add(this_id)
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise
        except Exception as err:
            logger.debug("Unknown read access Error: %s" % err)
            raise

    def _write_access(self, _obj):
        """Obtain write access on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError
        Raise ObjectNotInRegistryError (via self.find())
        Args:
            _obj (GangaObject): This is the object we want to get write access to and lock on disk
        """
        logger.debug("_write_access")
        obj = stripProxy(_obj)
        obj_id = id(_obj)
        if obj_id in self._inprogressDict:
            return

        with obj.const_lock:
            self.__write_access(obj)

    def __write_access(self, _obj):
        """
        This actually performs the file locks and gets access to the object
        Args:
            _obj (GangaObject): This is the object we want to get write access to and lock on disk
        """
        logger.debug("__write_acess")
        obj = stripProxy(_obj)
        this_id = id(obj)
        if this_id in self._inprogressDict:
            for this_d in self.changed_ids.itervalues():
                this_d.add(self.find(obj))
            return

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot get write access to a disconnected repository!")
        if not hasattr(obj, '_registry_locked') or not obj._registry_locked:
            try:
                this_id = self.find(obj)
                try:
                    if len(self.repository.lock([this_id])) == 0:
                        errstr = "Could not lock '%s' object #%i!" % (self.name, this_id)
                        try:
                            errstr += " Object is locked by session '%s' " % self.repository.get_lock_session(this_id)
                        except RegistryLockError as err:
                            raise
                        except Exception as err:
                            logger.debug( "Unknown Locking Exception: %s" % err)
                            raise
                        raise RegistryLockError(errstr)
                except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
                    raise
                except Exception as err:
                    logger.debug("Unknown write access Error: %s" % err)
                    raise
                finally:  # try to load even if lock fails
                    try:
                        if not self.has_loaded(obj):
                            self._load([this_id])
                            if hasattr(obj, "_registry_refresh"):
                                delattr(obj, "_registry_refresh")
                    except KeyError, err:
                        logger.debug("_write_access KeyError %s" % err)
                        raise RegistryKeyError("Write: The object #%i in registry '%s' was deleted!" % (this_id, self.name))
                    except InaccessibleObjectError as err:
                        raise RegistryKeyError("Write: The object #%i in registry '%s' could not be accessed - %s!" % (this_id, self.name, err))
                    for this_d in self.changed_ids.itervalues():
                        this_d.add(this_id)
                obj._registry_locked = True
            except Exception as err:
                raise

        return True

    def _release_lock(self, obj):
        """Release the lock on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise ObjectNotInRegistryError
        Args:
            obj (GangaObject): This is the object we want to release the file lock for
        """

        try:
            self.__release_lock(obj)
        except ObjectNotInRegistryError as err:
            pass
        except Exception as err:
            logger.debug("Unknown exception %s" % err)
            raise

    def __release_lock(self, _obj):
        """
        Actually perform the release of the file and cleanup any file locks
        Args:
            _obj (GangaObject): This is the object we want to release the file lock for
        """
        logger.debug("_release_lock")
        obj = stripProxy(_obj)

        if id(obj) in self._inprogressDict:
            return

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot manipulate locks of a disconnected repository!")
        logger.debug("Reg: %s _release_lock(%s)" % (self.name, self.find(obj)))
        try:
            if hasattr(obj, '_registry_locked') and obj._registry_locked:
                oid = self.find(obj)
                self.repository.flush([oid])
                obj._registry_locked = False
                self.repository.unlock([oid])
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise
        except Exception as err:
            logger.debug("un-known registry release lock err!")
            logger.debug("Err: %s" % err)
            raise

    @synchronised
    def pollChangedJobs(self, name):
        """Returns a list of job ids that changed since the last call of this function.
        On first invocation returns a list of all ids.
        Args:
            name (str): should be a unique identifier of the user of this information."""
        logger.debug("pollChangedJobs")
        if self.hasStarted() is True and\
                (time.time() > self._update_index_timer + self.update_index_time):
            changed_ids = self.repository.update_index()
            for this_d in self.changed_ids.itervalues():
                this_d.update(changed_ids)
            self._update_index_timer = time.time()
        res = self.changed_ids.get(name, set(self.ids()))
        self.changed_ids[name] = set()
        return res

    def getIndexCache(self, obj):
        """Returns a dictionary to be put into obj._index_cache (is this valid)
        This can and should be overwritten by derived Registries to provide more index values."""
        return {}

    @synchronised
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
                    self.metadata = Registry(self.name + ".metadata", "Metadata repository for %s" % self.name, update_index_time=self.update_index_time)
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
            # All Ids could have changed
            self.changed_ids = {}
            t1 = time.time()
            logger.debug("Registry '%s' [%s] startup time: %s sec" % (self.name, self.type, t1 - t0))
        except Exception as err:
            logger.debug("Logging Repo startup Error: %s" % err)
            self._hasStarted = False
            raise
        #finally:
        #    pass

    @synchronised
    def shutdown(self):
        """Flush and disconnect the repository. Called from Repository_runtime.py """
        from Ganga.Utility.logging import getLogger
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
            for obj in self._objects.values():
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

