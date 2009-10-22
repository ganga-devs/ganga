import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Core import GangaException

import time, threading

class RegistryError(GangaException):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "RegistryError: %s"%self.what

class RegistryAccessError(RegistryError):
    """ This error is raised if the request is valid in principle, 
        but the Registry cannot be accessed at the moment."""        
    def __str__(self):
        return "RegistryAccessError: %s"%self.what

class RegistryLockError(RegistryError):
    """ This error is raised if the request is valid in principle,
        but the object is locked by another Ganga session"""
    def __str__(self):
        return "RegistryLockError: %s"%self.what

class ObjectNotInRegistryError(RegistryError):
    """ This error is raised if an object has been associated to this registry,
        but is not actually in the registry. This most probably indicates an internal Ganga error."""
    def __str__(self):
        return "ObjectNotInRegistryError: %s"%self.what

class RegistryKeyError(RegistryError,KeyError):
    """ This error is raised if the given id is not found in the registry """
    def __str__(self):
        return "RegistryKeyError: %s"%self.what

class RegistryIndexError(RegistryError,IndexError):
    """ This error is raised if the given id is not found in the registry """
    def __str__(self):
        return "RegistryIndexError: %s"%self.what

def makeRepository(registry):
    """Factory that selects, imports and instantiates the correct GangaRepository"""
    if registry.type in ["LocalXML","LocalPickle"]:
        from GangaRepositoryXML import GangaRepositoryLocal
        return GangaRepositoryLocal(registry)
    elif registry.type in ["SQLite"]:
        from GangaRepositorySQLite import GangaRepositorySQLite
        return GangaRepositorySQLite(registry)
    elif registry.type in ["Transient"]:
        return GangaRepository(registry)
    else:
        raise RegistryError("Repository %s: Unknown repository type %s" % (registry.name, registry.type))

class Registry(object):
    """Ganga Registry
    Base class providing a dict-like locked and lazy-loading interface to a Ganga repository
    """

    def __init__(self, name, doc, dirty_flush_counter=10, update_index_time = 30):
        """Registry constructor, giving public name and documentation"""
        self.name = name
        self.doc = doc
        self._objects = {}
        self._started = False
        self.dirty_flush_counter = dirty_flush_counter
        self.dirty_objs = {}
        self.dirty_hits = 0
        self.update_index_time = update_index_time 
        self._update_index_timer = 0
        self._needs_metadata = False
        self.metadata = None
        self._lock = threading.RLock()

# Methods intended to be called from ''outside code''
    def __getitem__(self,id):
        """ Returns the Ganga Object with the given id.
            Raise RegistryKeyError"""
        try:
            return self._objects[id]
        except KeyError:
            raise RegistryKeyError("Could not find object #%s" % id)
            
    def __len__(self):
        """ Returns the current number of root objects """
        return len(self._objects)

    def __contains__(self,id):
        """ Returns True if the given ID is in the registry """
        return id in self._objects

    def ids(self):
        """ Returns the list of ids of this registry """
        if time.time() > self._update_index_timer + self.update_index_time:
            self._lock.acquire()
            try:
                self.repository.update_index()
            finally:
                self._lock.release()
            self._update_index_timer = time.time()
        k = self._objects.keys()
        k.sort()
        return k

    def items(self):
        """ Return the items (ID,obj) in this registry. 
        Recommended access for iteration, since accessing by ID can fail if the ID iterator is old"""
        if time.time() > self._update_index_timer + self.update_index_time:
            self._lock.acquire()
            try:
                self.repository.update_index()
            finally:
                self._lock.release()
            self._update_index_timer = time.time()
        its = self._objects.items()
        its.sort()
        return its

    def iteritems(self):
        """ Return the items (ID,obj) in this registry."""
        return self.items()

    def keys(self):
        """ Returns the list of ids of this registry """
        return self.ids()

    def values(self):
        """ Return the objects in this registry, in order of ID.
        Besides items() this is also recommended for iteration."""
        return [it[1] for it in self.items()]
    
    def find(self, obj):
        """Returns the id of the given object in this registry, or 
        Raise ObjectNotInRegistryError if the Object is not found"""
        try:
            assert obj == self._objects[obj._registry_id]
            return obj._registry_id
        except AttributeError:
            raise ObjectNotInRegistryError("Object %s does not seem to be in any registry!" % obj)
        except AssertionError:
            raise ObjectNotInRegistryError("Object %s does not seem to be in this registry!" % obj)

    def clean(self):
        """Tries to delete all elements of the registry (that it can lock)"""
        for obj in self.values():
            try:
                self._remove(obj)
            except Exception, x:
                logger.error("Error while removing object #%i: %s" % (self.find(obj), x))

# Methods that can be called by derived classes or Ganga-internal classes like Job
# if the dirty objects list is modified, the methods must be locked by self._lock
# all accesses to the repository must also be locked!

    def _add(self,obj,force_index=None):
        """ Add an object to the registry and assigns an ID to it. 
        use force_index to set the index (for example for metadata). This overwrites existing objects!
        Raises RepositoryError"""
        self._lock.acquire()
        try:
            if force_index is None:
                ids = self.repository.add([obj])
            else:
                if not self.repository.lock([force_index]):
                    raise RegistryLockError("Could not lock '%s' id #%i for a new object!" % (self.name,force_index))
                ids = self.repository.add([obj],[force_index])
            obj._registry_locked = True
            self.repository.flush(ids)
        finally:
            self._lock.release()

    def _remove(self,obj,auto_removed=0):
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
        if not auto_removed and "remove" in obj.__dict__:
            obj.remove()
        else:
            id = self.find(obj)
            try:
                self._write_access(obj)
            except RegistryKeyError:
                logger.warning("Object #%i was already deleted from registry '%s'!"%(id,self.name))
            logger.debug('deleting the object %d from the registry %s',id,self.name)
            self._lock.acquire()
            try:
                if obj in self.dirty_objs:
                    del self.dirty_objs[obj]
                self.repository.delete([id])
                del obj
            finally:
                self._lock.release()
            

    def _dirty(self,obj):
        """ Mark an object as dirty.
        Trigger automatic flush after specified number of dirty hits
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError"""
        logger.debug("_dirty(%s)" % id(obj))
        self._write_access(obj)
        self._lock.acquire()
        try:
            self.dirty_objs[obj] = 1
            self.dirty_hits += 1
            if self.dirty_hits % self.dirty_flush_counter == 0:
                self._flush()
            self.dirty_objs[obj] = 1 # HACK for GangaList: there _dirty is called _before_ the object is modified
        finally:
            self._lock.release()

    def _flush(self, objs=[]):
        """Flush a set of objects to the persistency layer immediately
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError"""
        #print self.name, objs, self.dirty_objs
        logger.debug("_flush(%s)" % objs)
        if not self._started:
            raise RegistryAccessError("Cannot flush to a disconnected repository!")
        for obj in objs:
            self._write_access(obj)

        self._lock.acquire()
        try:
            for obj in objs:
                self.dirty_objs[obj] = 1
            ids = []
            for obj in self.dirty_objs.keys():
                try:
                    ids.append(self.find(obj))
                except ObjectNotInRegistryError, x:
                    logger.error(x.what)
            logger.debug("repository.flush(%s)" % ids)
            self.repository.flush(ids)
            self.dirty_objs = {}
        finally:
            self._lock.release()

    def _read_access(self, obj, sub_obj = None):
        """Obtain read access on a given object.
        sub-obj is the object the read access is actually desired (ignored at the moment)
        Raise RegistryAccessError
        Raise RegistryKeyError"""
        #logger.debug("_read_access(%s)" % obj)
        if not obj._data:
            if not self._started:
                raise RegistryAccessError("The object #%i in registry '%s' is not fully loaded and the registry is disconnected! Type 'reactivate()' if you want to reconnect."%(self.find(obj),self.name))
            try:
                self._lock.acquire()
                try:
                    self.repository.load([self.find(obj)])
                finally:
                    self._lock.release()
            except KeyError:
                raise RegistryKeyError("The object #%i in registry '%s' was deleted or cannot be loaded." % (self.find(obj),self.name))
        

    def _write_access(self, obj):
        """Obtain write access on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError
        Raise ObjectNotInRegistryError"""
        logger.debug("_write_access(%s)" % obj)
        if not self._started:
            raise RegistryAccessError("Cannot get write access to a disconnected repository!")
        if not obj._registry_locked:
            self._lock.acquire()
            try:
                try:
                    if not self.repository.lock([self.find(obj)]):
                        raise RegistryLockError("Could not lock '%s' object #%i!" % (self.name,self.find(obj)))
                finally: # try to load even if lock fails
                    try:
                        self.repository.load([self.find(obj)])
                    except KeyError:
                        raise RegistryKeyError("The object #%i in registry '%s' was deleted or cannot be loaded." % (self.find(obj),self.name))
                obj._registry_locked = True
            finally:
                self._lock.release()
        return True
    
    def _release_lock(self, obj):
        """Release the lock on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise ObjectNotInRegistryError"""
        if not self._started:
            raise RegistryAccessError("Cannot manipulate locks of a disconnected repository!")
        logger.debug("_release_lock(%s)" % obj)
        self._lock.acquire()
        try:
            if obj._registry_locked:
                id = self.find(obj)
                if obj in self.dirty_objs:
                    self.repository.flush([id])
                    del self.dirty_objs[obj]
                obj._registry_locked = False
                self.repository.unlock([id])
        finally:
            self._lock.release()

    def getIndexCache(self,obj):
        """Returns a dictionary to be put into obj._index_cache
        This can and should be overwritten by derived Registries to provide more index values."""
        return {}

    def startup(self):
        """Connect the repository to the registry. Called from Repository_runtime.py"""
        t0 = time.time()
        self.repository = makeRepository(self)
        self.repository._objects = self._objects
        self.repository.startup()
        t1 = time.time()
        logger.info("Registry '%s' [%s] startup time: %s sec" % (self.name, self.type, t1-t0))
        self._started = True

        if self._needs_metadata:
            if self.metadata is None:
                self.metadata = Registry(self.name+".metadata", "Metadata repository for %s"%self.name, dirty_flush_counter=self.dirty_flush_counter, update_index_time = self.update_index_time)
                self.metadata.type = self.type
                self.metadata.location = self.location
                self.metadata._parent = self
            self.metadata.startup()
        
    def shutdown(self):
        """Flush and disconnect the repository. Called from Repository_runtime.py """
        self._lock.acquire()
        try:
            try:
                if not self.metadata is None:
                    self.metadata.shutdown()
            except Exception, x:
                logger.error("Exception on shutting down metadata repository '%s' registry: %s", self.name, x)
            try:
                self._flush()
            except Exception, x:
                logger.error("Exception on flushing '%s' registry: %s", self.name, x)
            self._started = False
            for obj in self._objects:
                obj._registry_locked = False # locks are not guaranteed to survive repository shutdown
            self.repository.shutdown()
        finally:
            self._lock.release()

