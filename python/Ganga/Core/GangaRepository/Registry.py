import types, time

import Ganga.Utility.Config
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Core import GangaException

from GangaRepositoryXML import GangaRepositoryLocal

from Ganga.Core.InternalServices.Coordinator import checkInternalServices

def makeRepository(registry):
    if registry.type in ["LocalXML","LocalPickle"]:
        reg = GangaRepositoryLocal(registry)
    else:
        raise RepositoryError(msg = "Repository %s: Unknown repository type %s" % (registry.name, registry.type))
    return reg  

class RegistryAccessError(GangaException):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "RegistryAccessError: %s"%self.what

class RegistryAccessIndexError(GangaException,IndexError):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "RegistryAccessIndexError: %s"%self.what

class Registry(object):
    """ Registry keeping a persistified list of ganga objects """
    def __init__(self, name, doc):
        self.name = name
        self.doc = doc

    def startup(self):
        t0 = time.time()
        self.repository = makeRepository(self)
        self.repository.startup()
        t1 = time.time()
        print "Registry '%s' [%s] startup time: %s sec" % (self.name, self.type, t1-t0)

    def shutdown(self):
        self.repository.shutdown()

    def clean(self):
        self.repository.delete(self.repository.ids())
        
    def _add(self,obj):
        """ Add an object to the registry. This is a private method which must be called for each newly constructed
        obj object. """
        ids = self.repository.add([obj])
        self.repository.flush(ids)
        
    def _remove(self,obj,auto_removed=0):
        """ Private method removing the obj from the registry. This method always called.
        This method may be overriden in the subclass to trigger additional actions on the removal.
        'auto_removed' is set to true if this method is called in the context of obj.remove() method to avoid recursion.
        Only then the removal takes place. In the opposite case the obj.remove() is called first which eventually calls
        this method again with "auto_removed" set to true. This is done so that obj.remove() is ALWAYS called once independent
        on the removing context."""
        id = self.repository.find(obj)
        if id > -1:
            if not auto_removed and "remove" in obj.__dict__:
                obj.remove()
            else:
                logger.debug('deleting the object %d from the registry %s',id,self.name)
                self.repository.delete([id])
                del obj
        else:
            s='Attempted to remove a obj which does not exist in this registry (ID %d not found)'%(id,)
            print s
            raise ValueError(s)

    def _flush(self, objects):
        self.repository.flush([self.repository.find(obj) for obj in objects])

    def acquireWriteLock(self,obj):
        return 1 == len(self.repository.acquireWriteLock([self.repository.find(obj)]))

    def releaseWriteLock(self,obj):
        return 1 == len(self.repository.releaseWriteLock([self.repository.find(obj)]))
