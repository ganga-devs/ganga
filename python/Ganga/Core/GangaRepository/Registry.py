import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Core import GangaException

import time

class RegistryError(GangaException):
    pass

class RegistryAccessError(GangaException):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "RegistryAccessError: %s"%self.what

class RegistryKeyError(GangaException,KeyError):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "RegistryKeyError: %s"%self.what

class ObjectNotInRegistryError(GangaException,KeyError):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "ObjectNotInRegistryError: %s"%self.what

class RegistryLockError(GangaException):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "RegistryLockError: %s"%self.what

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
        raise RegistryError(msg = "Repository %s: Unknown repository type %s" % (registry.name, registry.type))

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

# Methods intended to be called from ''outside code''
    def __getitem__(self,id):
        """ Returns the Ganga Object with the given id.
            Raise RegistryKeyError"""
        try:
            return self._objects[id]
        except KeyError:
            raise RegistryKeyError("Could not find object #%i" % id)
            
    def __len__(self):
        """ Returns the current number of root objects """
        return len(self._objects)

    def __contains__(self,id):
        """ Returns True if the given ID is in the registry """
        return id in self._objects

    def ids(self):
        """ Returns the list of ids of this registry """
        if time.time() > self._update_index_timer + self.update_index_time:
            self.repository.update_index()
            self._update_index_timer = time.time()
        k = self._objects.keys()
        k.sort()
        return k

    def items(self):
        """ Return the items (ID,obj) in this registry. 
        Recommended access for iteration, since accessing by ID can fail if the ID iterator is old"""
        if time.time() > self._update_index_timer + self.update_index_time:
            self.repository.update_index()
            self._update_index_timer = time.time()
        its = self._objects.items()
        its.sort()
        return its

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
            assert obj._registry_id == 0 or obj == self._objects[obj._registry_id]
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
    def _add(self,obj):
        """ Add an object to the registry and assigns an ID to it. 
        Raises RepositoryError"""
        ids = self.repository.add([obj])
        obj._registry_locked = True
        self.repository.flush(ids)

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
        self._write_access(obj)
        id = self.find(obj)
        if not auto_removed and "remove" in obj.__dict__:
            obj.remove()
        else:
            logger.debug('deleting the object %d from the registry %s',id,self.name)
            self.repository.delete([id])
            del obj

    def _dirty(self,obj):
        """ Mark an object as dirty.
        Trigger automatic flush after specified number of dirty hits
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError"""
        logger.debug("_dirty(%s)" % obj)
        self._write_access(obj)
        self.dirty_objs[obj] = 1
        self.dirty_hits += 1
        if self.dirty_hits % self.dirty_flush_counter == 0:
            self._flush()
        self.dirty_objs[obj] = 1 # HACK for GangaList: there _dirty is called _before_ the object is modified

    def _flush(self, objs=[]):
        """Flush a set of objects to the persistency layer immediately
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError"""
        logger.debug("_flush(%s)" % objs)
        if not self._started:
            raise RegistryAccessError("Cannot flush to a disconnected repository!")
        for obj in objs:
            self._write_access(obj)
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

    def _read_access(self, obj, sub_obj = None):
        """Obtain read access on a given object.
        sub-obj is the object the read access is actually desired (ignored at the moment)
        Raise RepositoryError
        Raise ObjectNotInRegistryError"""
        #logger.debug("_read_access(%s)" % obj)
        if not obj._data:
            if not self._started:
                raise RegistryAccessError("The object #%i in registry '%s' is not fully loaded and the registry is disconnected! Type 'reactivate()' if you want to reconnect."%(self.find(obj),self.name))
            try:
                self.repository.load([self.find(obj)])
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
            if not self.repository.lock([self.find(obj)]):
                raise RegistryLockError("Could not lock '%s' object #%i!" % (self.name,self.find(obj)))
            try:
                self.repository.load([self.find(obj)])
            except KeyError:
                raise RegistryKeyError("The object #%i in registry '%s' was deleted or cannot be loaded." % (self.find(obj),self.name))
            obj._registry_locked = True

        return True
    
    def _release_lock(self, obj):
        """Release the lock on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise ObjectNotInRegistryError"""
        if not self._started:
            raise RegistryAccessError("Cannot manipulate locks of a disconnected repository!")
        logger.debug("_release_lock(%s)" % obj)
        if obj._registry_locked:
            id = self.find(obj)
            if obj in self.dirty_objs:
                self.repository.flush([id])
                del self.dirty_objs[obj]
            obj._registry_locked = False
            self.repository.unlock([id])

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
        print "Registry '%s' [%s] startup time: %s sec" % (self.name, self.type, t1-t0)
        self._started = True
        self._metadata = self.repository._getMetadataObject()
        if self._metadata is None:
            self._metadata = self._createMetadataObject()
            if self._metadata is not None:
                self.repository._setMetadataObject(self._metadata)
                self._metadata._registry_locked = True
                self._metadata = self.repository._getMetadataObject()
                self.repository.flush([0])

    def shutdown(self):
        """Flush and disconnect the repository. Called from Repository_runtime.py """
        try:
            self._flush()
        except Exception, x:
            logger.error("Exception on flushing '%s' registry: %s", self.name, x)
        
        self._started = False
        self.repository.shutdown()

    def _createMetadataObject(self):
        return None

