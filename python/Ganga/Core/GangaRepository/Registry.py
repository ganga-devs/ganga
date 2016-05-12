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

_reg_id_str = '_registry_id'
_id_str = 'id'

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
    """Factory that selects, imports and instantiates the correct GangaRepository"""
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
        super(IncompleteObject, self).__init__()
        self.registry = registry
        self.id = this_id

    def reload(self):
        self.registry._lock.acquire()
        try:

            if self.registry.checkShouldFlush():
                self.registry.repository.flush([self.registry._objects[self.id]])
                self.registry._load([self.id])
            if self.id not in self.registry_loaded_ids:
                self.registry._load([self.id])
            logger.debug("Successfully reloaded '%s' object #%i!" % (self.registry.name, self.id))
            for d in self.registry.changed_ids.itervalues():
                d.add(self.id)
        finally:
            self.registry._lock.release()

    def remove(self):
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
        return "Incomplete object in '%s', ID %i. Try reload() or remove()." % (self.registry.name, self.id)


def synchronised(f):
    """
    This decorator must be attached to a method on a ``Registry``
    It uses the object's lock to make sure that the object is held for the duration of the decorated function
    """
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
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
        super(RegistryFlusher, self).__init__(*args, **kwargs)
        self.registry = registry
        self._stop = threading.Event()

    def stop(self):
        """
        Ask the thread to stop what it is doing and it will finish
        the next chance it gets.
        """
        self._stop.set()

    @property
    def stopped(self):
        return self._stop.isSet()

    def join(self, *args, **kwargs):
        self.stop()
        super(RegistryFlusher, self).join(*args, **kwargs)

    def run(self):
        """
        This will run an indefinite loop which periodically checks
        whether it should stop. In between calls to ``flush_all`` it
        will wait for a fixed period of time.
        """
        sleeps_per_second = 10  # This changes the granularity of the sleep.
        while not self.stopped:
            sleep_period = getConfig('Registry')['AutoFlusherWaitTime']
            for i in range(sleep_period*sleeps_per_second):
                time.sleep(1/sleeps_per_second)
                if self.stopped:
                    return
            # We want this to be a non-blocking lock to avoid this
            # interfering with interactive work or monitoring. It
            # will try again in a while anyway.
            logger.debug('Auto-flushing: %s', self.registry.name)
            self.registry.flush_all()


class Registry(object):

    """Ganga Registry
    Base class providing a dict-like locked and lazy-loading interface to a Ganga repository
    """

    def __init__(self, name, doc, dirty_flush_counter=10, update_index_time=30, dirty_max_timeout=60, dirty_min_timeout=30):
        """Registry constructor, giving public name and documentation"""
        self.name = name
        self.doc = doc
        self._hasStarted = False
        self.dirty_flush_counter = dirty_flush_counter
        self.dirty_hits = 0
        self.update_index_time = update_index_time
        self._update_index_timer = 0
        self._needs_metadata = False
        self.metadata = None
        self._lock = threading.RLock()
        self.hard_lock = {}
        self.changed_ids = {}
        self._autoFlush = True

        self._loaded_ids = []

        self._parent = None

        self.repository = None
        self._objects = None
        self._incomplete_objects = None

        ## Record the last dirty and flush times to determine whether an idividual flush command should flush
        ## Logc to use these is implemented in checkShouldFlush()
        self._dirtyModTime = None
        self._flushLastTime = None
        self._dirty_max_timeout = dirty_max_timeout
        self._dirty_min_timeout = dirty_min_timeout

        ## Id's id(obj) of objects undergoing a transaction such as flush, remove, add, etc.
        self._inprogressDict = {}


        self.shouldReleaseRun = True

        self.flush_thread = None

#        self.releaseThread = threading.Thread(target=self.trackandRelease, args=())
#        self.releaseThread.daemon = True
#        self.releaseThread.start()

    def hasStarted(self):
        return self._hasStarted

    def lock_transaction(self, this_id, action):
        while this_id in self._inprogressDict.keys():
            logger.debug("Getting item being operated on: %s" % this_id)
            logger.debug("Currently in state: %s" % self._inprogressDict[this_id])
            #import traceback
            #traceback.print_stack()
            #import sys
            #sys.exit(-1)
            #time.sleep(0.05)
        self._inprogressDict[this_id] = action
        if this_id not in self.hard_lock.keys():
            self.hard_lock[this_id] = threading.Lock()
        self.hard_lock[this_id].acquire()

    def unlock_transaction(self, this_id):
        self._inprogressDict[this_id] = False
        del self._inprogressDict[this_id]
        self.hard_lock[this_id].release()

    # Methods intended to be called from ''outside code''
    def __getitem__(self, this_id):
        """ Returns the Ganga Object with the given id.
            Raise RegistryKeyError"""
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
        """ Returns True if the given ID is in the registry """
        logger.debug("__contains__")
        return this_id in self._objects

    def updateLocksNow(self):
        logger.debug("updateLocksNow")
        self.repository.updateLocksNow()

    def trackandRelease(self):

        while self.shouldReleaseRun is True:

            ## Needed import for shutdown
            import time
            timeNow = time.time()

            modTime = self._dirtyModTime
            if modTime is None:
                modTime = timeNow
            dirtyTime = self._flushLastTime
            if dirtyTime is None:
                dirtyTime = timeNow

            delta_1 = abs(timeNow - modTime)
            delta_2 = abs(timeNow - dirtyTime)

            if delta_1 > self._dirty_max_timeout and delta_2 > self._dirty_max_timeout:

                 flush_thread = threading.Thread(target=self._flush, args=())
                 flush_thread.run()
    
            time.sleep(0.5)

    def turnOffAutoFlushing(self):
        self._autoFlush = False

    def turnOnAutoFlushing(self):
        self._autoFlush = True

    def isAutoFlushEnabled(self):
        return self._autoFlush

    def checkDirtyFlushtimes(self, timeNow):
        self._dirtyModTime = timeNow
        if self._flushLastTime is None:
            self._flushLastTime = timeNow

    @synchronised
    def checkShouldFlush(self):
        logger.debug("checkShouldFlush")

        timeNow = time.time()

        self.checkDirtyFlushtimes(timeNow)

        timeDiff = (self._dirtyModTime - self._flushLastTime)

        if timeDiff > self._dirty_min_timeout:
            hasMinTimedOut = True
        else:
            hasMinTimedOut = False

        if timeDiff > self._dirty_max_timeout:
            hasMaxTimedOut = True
        else:
            hasMaxTimedOut = False

        if self.dirty_hits > self.dirty_flush_counter:
            countLimitReached = True
        else:
            countLimitReached = False

        # THIS IS THE MAIN DECISION ABOUT WHETHER TO FLUSH THE OBJECT TO DISK!!!
        # if the minimum amount of time has passed __AND__ we meet a sensible condition for wanting to flush to disk
        decision = hasMinTimedOut and (hasMaxTimedOut or countLimitReached)
       
        ## This gives us the ability to automatically turn off the automatic flushing externally if required
        decision = decision and self._autoFlush

        ## Can't autosave if a flush is in progress. Wait until next time.
        if len(self._inprogressDict.keys()) != 0:
            decision = False

        if decision is True:
            self._flushLastTime = timeNow
            self.dirty_hits = 0

        return decision

    def _getObjects(self):
        logger.debug("_getObjects")
        returnable = self._objects
        return returnable

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

    def _checkObjects(self):
        for key, _obj in self._objects.iteritems():
            summary = "found: "
            try:
                if hasattr(_obj, _id_str):
                    summary = summary + "%s = '%s'" % (_id_str, getattr(_obj, _id_str))
                    assert(getattr(_obj, _id_str) == key)
                if hasattr(_obj, _reg_id_str):
                    summary = summary + " %s = '%s'" % (_reg_id_str, getattr(_obj, _reg_id_str))
                    assert(getattr(_obj, _reg_id_str) == key)
            except:
                logger.error(summary)
                raise
        return

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
        logger.debug("__iter__")
        returnable = iter(self.values())
        return returnable

    def find(self, _obj):
        """Returns the id of the given object in this registry, or 
        Raise ObjectNotInRegistryError if the Object is not found"""
    
        obj = stripProxy(_obj)
        try:
            return next(id for id, o in self._objects.items() if o is obj)
        except StopIteration:
            raise ObjectNotInRegistryError("Object '%s' does not seem to be in this registry: %s !" % (getName(obj), self.name))

    @synchronised
    def clean(self, force=False):
        """Deletes all elements of the registry, if no other sessions are present.
        if force == True it removes them regardless of other sessions.
        Returns True on success, False on failure."""
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
            self.dirty_hits = 0
            self.changed_ids = {}
            self.repository.clean()
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise
        except Exception as err:
            logger.debug("Clean Unknown Err: %s" % err)
            raise

    @synchronised
    def __safe_add(self, obj, force_index=None):
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

            for _id in ids:
                if hasattr(self._objects[_id], _reg_id_str):
                    assert(getattr(self._objects[_id], _reg_id_str) == _id)
                if hasattr(self._objects[_id], _id_str):
                    assert(getattr(self._objects[_id], _id_str) == _id)

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
        Raises RepositoryError"""
        logger.debug("_add")
        obj = stripProxy(_obj)

        if self.hasStarted() is not True:
            raise RepositoryError("Cannot add objects to a disconnected repository!")

        this_id = None
        returnable_id = None

        try:
            returnable_id = self.__safe_add(obj, force_index)
            ## Add to list of loaded jobs in memory
            self._loaded_ids.append(returnable_id)
        except RepositoryError as err:
            raise
        except Exception as err:
            logger.debug("Unknown Add Error: %s" % err)
            raise

        self._updateIndexCache(obj)

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
        Raise ObjectNotInRegistryError"""
        logger.debug("_remove")
        obj = stripProxy(_obj)
        try:
            self.__reg_remove(obj, auto_removed)
        except ObjectNotInRegistryError as err:
            try:
                ## Actually  make sure we've removed the object from the repo 
                if hasattr(obj, _reg_id_str):
                    del self._objects[getattr(obj, _reg_id_str)]
            except Exception as err:
                pass
            pass
        except Exception as err:
            raise

    def __reg_remove(self, obj, auto_removed=0):

        logger.debug("_reg_remove")
        u_id = self.find(obj)

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
            objs: a list of objects to flush
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

            if hasattr(obj, _reg_id_str):
                obj_id = getattr(obj, _reg_id_str)
                if obj_id not in self._loaded_ids:
                    continue
            else:
                continue

            with obj.const_lock:
                # flush the object. Need to call _getWriteAccess for consistency reasons
                # TODO: getWriteAccess should only 'get write access', as that's not needed should it be called here?
                obj._getWriteAccess()
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
        Raise RegistryKeyError"""
        logger.debug("_read_access")
        obj_id = id(stripProxy(_obj))
        if obj_id in self._inprogressDict.keys():
            return

        with _obj.const_lock:
            self.__safe_read_access(_obj, sub_obj)

    @synchronised
    def _updateIndexCache(self, _obj):
        logger.debug("_updateIndexCache")
        obj = stripProxy(_obj)
        if id(obj) in self._inprogressDict.keys():
            return

        self.repository.updateIndexCache(obj)

    @synchronised
    def _load(self, obj_ids):
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
                ## Track the objects we've loaded into memory
                self._loaded_ids.append(obj_id)
        except Exception as err:
            logger.error("Error Loading Jobs! '%s'" % obj_ids)
            ## Cleanup aftr ourselves if an error occured
            for obj_id in obj_ids:
                if obj_id in self._loaded_ids:
                    del self._loaded_ids[obj_id]
                ## Didn't load mark as clean so it's not flushed
                if obj_id in self._objects:
                    self._objects[obj_id]._setFlushed()
            raise
        finally:
            for obj_id in these_ids:
                self.unlock_transaction(obj_id)

    def __safe_read_access(self,  _obj, sub_obj):
        logger.debug("_safe_read_access")
        obj = stripProxy(_obj)
        if id(obj) in self._inprogressDict.keys():
            return

        if self.hasStarted() is not True:
            raise RegistryAccessError("The object #%i in registry '%s' is not fully loaded and the registry is disconnected! Type 'reactivate()' if you want to reconnect." % (self.find(obj), self.name))

        if hasattr(obj, "_registry_refresh"):
            delattr(obj, "_registry_refresh")
        assert not hasattr(obj, "_registry_refresh")

        try:
            this_id = self.find(obj)
            try:
                if this_id not in self._loaded_ids:
                    self._load([this_id])
            except KeyError as err:
                logger.error("_read_access KeyError %s" % err)
                raise RegistryKeyError("Read: The object #%i in registry '%s' was deleted!" % (this_id, self.name))
            except InaccessibleObjectError as err:
                raise RegistryKeyError("Read: The object #%i in registry '%s' could not be accessed - %s!" % (this_id, self.name, err))
            #finally:
            #    pass
            for this_d in self.changed_ids.itervalues():
                this_d.add(this_id)
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise
        except Exception as err:
            logger.debug("Unknown read access Error: %s" % err)
            raise
        #finally:
        #    pass

    def _write_access(self, _obj):
        """Obtain write access on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError
        Raise ObjectNotInRegistryError (via self.find())"""
        logger.debug("_write_access")
        obj = stripProxy(_obj)
        obj_id = id(_obj)
        if obj_id in self._inprogressDict.keys():
            return

        with obj.const_lock:
            self.__write_access(obj)

    def __write_access(self, _obj):
        logger.debug("__write_acess")
        obj = stripProxy(_obj)
        this_id = id(obj)
        if this_id in self._inprogressDict.keys():
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
                        if this_id not in self._loaded_ids:
                            self._load([this_id])
                            if hasattr(obj, "_registry_refresh"):
                                delattr(obj, "_registry_refresh")
                    except KeyError, err:
                        logger.debug("_write_access KeyError %s" % err)
                        raise RegistryKeyError("Write: The object #%i in registry '%s' was deleted!" % (this_id, self.name))
                    except InaccessibleObjectError as err:
                        raise RegistryKeyError("Write: The object #%i in registry '%s' could not be accessed - %s!" % (this_id, self.name, err))
                    #finally:
                    #    pass
                    for this_d in self.changed_ids.itervalues():
                        this_d.add(this_id)
                obj._registry_locked = True
            except Exception as err:
                raise
            #finally:
            #    pass

        return True

    def _release_lock(self, obj):
        """Release the lock on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise ObjectNotInRegistryError"""

        try:
            self.__release_lock(obj)
        except ObjectNotInRegistryError as err:
            pass
        except Exception as err:
            logger.debug("Unknown exception %s" % err)
            raise

    def __release_lock(self, _obj):
        logger.debug("_release_lock")
        obj = stripProxy(_obj)

        if id(obj) in self._inprogressDict.keys():
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
        "name" should be a unique identifier of the user of this information."""
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
        """Returns a dictionary to be put into obj._index_cache
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
                    self.metadata = Registry(self.name + ".metadata", "Metadata repository for %s" % self.name, dirty_flush_counter=self.dirty_flush_counter, update_index_time=self.update_index_time)
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
#        self.shouldReleaseRun = False
#        self.releaseThread.stop()
        logger = getLogger()
        logger.debug("Shutting Down Registry")
        logger.debug("shutdown")
        try:
            self._hasStarted = True
            try:
                if not self.metadata is None:
                    try:
                        self.flush_all()
                    except Exception, err:
                        logger.debug("shutdown _flush Exception: %s" % err)
                    self.metadata.shutdown()
            except Exception as err:
                logger.debug("Exception on shutting down metadata repository '%s' registry: %s", self.name, err)
            #finally:
            #    pass
            try:
                self.flush_all()
            except Exception as err:
                logger.error("Exception on flushing '%s' registry: %s", self.name, err)
                #raise
            #finally:
            #    pass
            for obj in self._objects.values():
                # locks are not guaranteed to survive repository shutdown
                obj._registry_locked = False
            self.repository.shutdown()

            self._loaded_ids = []

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

    def print_other_sessions(self):
        other_sessions = self.repository.get_other_sessions()
        if len(other_sessions) > 0:
            logger.warning("%i other concurrent sessions:\n * %s" % (len(other_sessions), "\n * ".join(other_sessions)))

    def has_loaded(self, obj):
        """Returns True/False for if a given object has been fully loaded by the Registry.
        Returns False on the object not being in the Registry!
        This ONLY applies to master jobs as the Registry has no apriori knowledge of the subjob structure.
        Consult SubJobXMLList for a more fine grained loaded/not-loaded info/test."""
        try:
            index = self.find(obj)
        except ObjectNotInRegistryError:
            return False

        if index in self._loaded_ids:
            return True
        else:
            return False

