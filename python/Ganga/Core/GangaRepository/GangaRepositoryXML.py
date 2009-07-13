# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

from GangaRepository import *
from Ganga.Utility.Config import getConfig
import os, os.path, fcntl, time, errno

from SessionLock import SessionLockManager

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Core.GangaRepository.PickleStreamer import to_file as pickle_to_file
from Ganga.Core.GangaRepository.PickleStreamer import from_file as pickle_from_file

def safe_save(fn,obj,to_file):
    """Writes a file safely, raises IOError on error"""
    try:
        tmpfile = open(fn + ".new", "w")
        to_file(obj, tmpfile)
        # Important: Flush, then sync file before renaming!
        tmpfile.flush()
        os.fsync(tmpfile.fileno())
        tmpfile.close()
    except IOError, e:
        raise IOError("Could not write file %s.new (%s)" % (fn,e))
    # Try to make backup copy...
    try:
        os.rename(fn,fn+"~")
    except OSError, e:
        logger.debug("Error on moving file %s (%s) " % (fn,e))
    try:
        os.rename(fn+".new",fn)
    except OSError, e:
        raise IOError("Error on moving file %s.new (%s) " % (fn,e))

class GangaRepositoryLocal(GangaRepository):
    """GangaRepository Local"""

    def get_fn(self,id):
        """ Returns the file name where the data for this object id is saved"""
        return os.path.join(self.root,"%ixxx"%(id/1000), "%i"%id, "data")

    def startup(self):
        """ Starts an repository and reads in a directory structure."""
        self._load_timestamp = {}
        self.root = os.path.join(self.registry.location,"6.0",self.registry.name)
        self.sessionlock = SessionLockManager(self.root, self.registry.type+"."+self.registry.name)
        if "XML" in self.registry.type:
            from Ganga.Core.GangaRepository.VStreamer import to_file, from_file
            self.to_file = to_file
            self.from_file = from_file
        elif "Pickle" in self.registry.type:
            self.to_file = pickle_to_file
            self.from_file = pickle_from_file
        else:
            raise RepositoryException("Unknown Repository type: %s"%self.registry.type)
        self.update_index()

    def update_index(self,id = None):
        # First locate and load the index files
        idx = {}
        for d in os.listdir(self.root):
            if d.endswith("xxx.index"):
                try:
                    idx.update(pickle_from_file(os.path.join(self.root,d)))
                except Exception, x:
                    logger.warning("Failed to load index from %s! %s" % (d,x)) # Probably should be DEBUG
        # Now create objects from the index
        for id in idx:
            try:
                if not id in self._objects:
                    obj = self._make_empty_object_(id,idx[id][0],idx[id][1])
                else:
                    obj = self._objects[id]
                if not obj._data:
                    obj._index_cache = idx[id][2]
            except Exception, x:
                logger.warning("Error processing cache line %i: %s " % (id, x)) # Probably should be DEBUG
                if not id in self._objects:
                    try:
                        self.load([id])
                    except Exception:
                        logger.warning("Failed to load id %i!" % (id)) # Probably should be DEBUG
        # now try to load all objects that are not in the index
        for d in os.listdir(self.root):
            if d.endswith("xxx"):
                for sd in os.listdir(os.path.join(self.root,d)):
                    try:
                        id = int(sd)
                    except ValueError:
                        pass
                    if not id in idx and not id in self._objects:
                        try:
                            self.load([id])
                        except Exception:
                            logger.debug("Failed to load id %i!" % (id))

    def add(self, objs):
        ids = self.sessionlock.make_new_ids(len(objs))
        for i in range(0,len(objs)):
            fn = self.get_fn(ids[i])
            try:
                os.makedirs(os.path.dirname(fn))
            except IOError, e:
                if e.errno != errno.ENOENT: 
                    raise RepositoryError("IOError: " + str(e))
            self._internal_setitem__(ids[i], objs[i])
        return ids

    def flush(self, ids):
        indices = {}
        for id in ids:
            try:
                fn = self.get_fn(id)
                obj = self._objects[id]
                if obj._name != "Unknown":
                    obj._index_cache = self.registry.getIndexCache(obj)
                    safe_save(fn, obj, self.to_file)
                    indices[id/1000] = 1
            except OSError, x:
                raise RepositoryError("OSError: " + str(x))
            except IOError, x:
                raise RepositoryError("IOError: " + str(x))
        for index in indices.keys():
            fn = os.path.join(self.root,"%ixxx.index")
            # TODO: Write index file

    def load(self, ids):
        for id in ids:
            fn = self.get_fn(id)
            try:
                fobj = file(fn,"r")
            except IOError, x:
                if x.errno == errno.ENOENT: 
                    raise KeyError(id)
                else: 
                    raise RepositoryError("IOError: " + str(x))
            try:
                if id in self._objects:
                    if self._objects[id]._data is None or self._load_timestamp[id] != os.fstat(fobj.fileno()).st_ctime:
                        tmpobj = self.from_file(fobj)[0]
                        self._objects[id]._data = tmpobj._data
                else:
                    self._internal_setitem__(id, self.from_file(fobj)[0])
                self._load_timestamp[id] = os.fstat(fobj.fileno()).st_ctime
                self._objects[id]._index_cache = None 
            except Exception, x:
                logger.error("Could not load object #%i: %s %s", id, x.__class__.__name__, x)
                raise KeyError(id)
                #self._internal_setitem__(id, EmptyGangaObject())

    def delete(self, ids):
        for id in ids:
            obj = self._objects[id]
            fn = self.get_fn(id)
            os.unlink(fn)
            try:
                os.unlink(fn+"~")
            except OSError:
                pass
            try:
                os.unlink(fn+".new")
            except OSError:
                pass
            try:
                os.removedirs(os.path.dirname(fn))
            except OSError:
                pass
            self._internal_del__(id, obj)

    def lock(self,ids):
        locked_ids = self.sessionlock.lock_ids(ids)
        if len(locked_ids) < len(ids):
            return False
        return True

    def unlock(self,ids):
        released_ids = self.sessionlock.release_ids(ids)
        if len(released_ids) < len(ids):
            logger.error("The write locks of some objects could not be released!")




