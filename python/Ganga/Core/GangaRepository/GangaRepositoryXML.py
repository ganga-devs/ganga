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

from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaListByRef

def safe_save(fn,obj,to_file):
    """Writes a file safely, raises IOError on error"""
    try:
        file(fn)
    except IOError:
        # file does not exist, so make it fast!
        try:
            to_file(obj, file(fn,"w"))
            return
        except IOError, e:
            raise IOError("Could not write file %s (%s)" % (fn,e))
    try:
        tmpfile = open(fn + ".new", "w")
        to_file(obj, tmpfile)
        # Important: Flush, then sync file before renaming!
        tmpfile.flush()
        #os.fsync(tmpfile.fileno())
        tmpfile.close()
    except IOError, e:
        raise IOError("Could not write file %s.new (%s)" % (fn,e))
    # Try to make backup copy...
    try:
        os.unlink(fn+"~")
    except OSError, e:
        logger.debug("Error on removing file %s~ (%s) " % (fn,e))
    try:
        os.rename(fn,fn+"~")
    except OSError, e:
        logger.debug("Error on file backup %s (%s) " % (fn,e))
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
        self._metadata = None
        self._load_timestamp = {}
        self._cache_load_timestamp = {}
        self.sub_split = "subjobs"
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

    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry"""
        pass

    def update_index(self,id = None):
        # First locate and load the index files
        print "updating index..."
        obj_chunks = [d for d in os.listdir(self.root) if d.endswith("xxx") and d[:-3].isdigit()]
        loaded_obj = 0
        loaded_cache = 0
        reloaded_cache = 0
        for d in obj_chunks:
            dir = os.path.join(self.root,d)
            listing = os.listdir(dir)
            indices = [l for l in listing if l.endswith(".index") and l[:-6].isdigit()]
            ids = [int(l) for l in listing if l.isdigit()]
            for idx in indices:
                try:
                    id = int(idx[:-6])
                    try:
                        if id != 0 or self._metadata is None:
                            obj = self._objects[id]
                        else:
                            obj = self._metadata
                        if not obj._data:
                            fobj = file(os.path.join(dir,idx))
                            if (self._cache_load_timestamp[id] != os.fstat(fobj.fileno()).st_ctime):
                                logger.debug("Reloading index %i" % id)
                                cat,cls,cache = pickle_from_file(fobj)[0]
                                obj._index_cache = cache
                                self._cache_load_timestamp[id] = os.fstat(fobj.fileno()).st_ctime
                                reloaded_cache += 1
                    except KeyError:
                        fobj = file(os.path.join(dir,idx))
                        #logger.debug("Loading index %i" % id)
                        cat,cls,cache = pickle_from_file(fobj)[0]
                        obj = self._make_empty_object_(id,cat,cls)
                        obj._index_cache = cache
                        self._cache_load_timestamp[id] = os.fstat(fobj.fileno()).st_ctime
                        loaded_cache += 1
                except Exception, x:
                    logger.warning("Failed to load index from %s! %s: %s" % (d,x.__class__.__name__,x)) # Probably should be DEBUG
            for id in ids:
                if not id in self._objects or (id == 0 and self._metadata is None):
                    try:
                        self.load([id])
                        loaded_obj += 1
                    except KeyError:
                        pass # deleted job
                    except Exception:
                        logger.warning("Failed to load id %i!" % (id))
        logger.warning("Updated cache: Loaded %i objects, %i cached objects and refreshed %i objects from cache" % (loaded_obj,loaded_cache,reloaded_cache))
        print "updated index done"

    def add(self, objs):
        ids = self.sessionlock.make_new_ids(len(objs))
        for i in range(0,len(objs)):
            fn = self.get_fn(ids[i])
            try:
                os.makedirs(os.path.dirname(fn))
            except OSError, e:
                if e.errno != errno.EEXIST: 
                    raise RepositoryError(self,"OSError: " + str(e))
            self._internal_setitem__(ids[i], objs[i])
        return ids

    def flush(self, ids):
        for id in ids:
            try:
                fn = self.get_fn(id)
                if id != 0:
                    obj = self._objects[id]
                else:
                    obj = self._metadata
                if obj._name != "Unknown":
                    split_cache = None
                    if self.sub_split and self.sub_split in obj._data:
                        split_cache = obj._data[self.sub_split]
                        for i in range(len(split_cache)):
                            if not split_cache[i]._dirty:
                                continue
                            sfn = os.path.join(os.path.dirname(fn),str(i),"data")
                            try:
                                os.makedirs(os.path.dirname(sfn))
                            except OSError, e:
                                if e.errno != errno.EEXIST: 
                                    raise RepositoryError(self,"OSError: " + str(e))
                            safe_save(sfn, split_cache[i], self.to_file)
                        obj._data[self.sub_split] = []
                    safe_save(fn, obj, self.to_file)
                    if split_cache:
                        obj._data[self.sub_split] = split_cache
                    try:
                        ifn = os.path.dirname(fn) + ".index"
                        new_idx_cache = self.registry.getIndexCache(obj)
                        if new_idx_cache != obj._index_cache:
                            obj._index_cache = new_idx_cache
                            pickle_to_file((obj._category,obj._name,obj._index_cache),file(ifn,"w"))
                    except IOError, x:
                        logger.error("Index saving to '%s' failed: %s %s" % (ifn,x.__class__.__name__,x))
            except OSError, x:
                raise RepositoryError(self,"OSError: " + str(x))
            except IOError, x:
                raise RepositoryError(self,"IOError: " + str(x))

    def load(self, ids):
        for id in ids:
            fn = self.get_fn(id)
            try:
                fobj = file(fn,"r")
            except IOError, x:
                if x.errno == errno.ENOENT: 
                    raise KeyError(id)
                else: 
                    raise RepositoryError(self,"IOError: " + str(x))
            try:
                if id == 0:
                    must_load = self._metadata is None or self._metadata._data is None
                else:
                    must_load = (not id in self._objects) or (self._objects[id]._data is None)
                tmpobj = None
                if must_load or (self._load_timestamp[id] != os.fstat(fobj.fileno()).st_ctime):
                    tmpobj = self.from_file(fobj)[0]
                    if self.sub_split:
                        i = 0
                        ld = os.listdir(os.path.dirname(fn))
                        l = []
                        while str(i) in ld:
                            sfn = os.path.join(os.path.dirname(fn),str(i),"data")
                            try:
                                sfobj = file(sfn,"r")
                            except IOError, x:
                                raise RepositoryError(self,"IOError: " + str(x))
                            l.append(self.from_file(sfobj)[0])
                            i += 1
                        tmpobj._data[self.sub_split] = makeGangaListByRef(l)

                    if id in self._objects:
                        self._objects[id]._data = tmpobj._data
                        self._objects[id]._index_cache = None
                    elif id == 0 and not self._metadata is None:
                        self._metadata._data = tmpobj._data
                        self._metadata._index_cache = None
                    else:
                        self._internal_setitem__(id, tmpobj)
                    if self.sub_split:
                        for sobj in self._objects[id]._data[self.sub_split]:
                            sobj._setParent(self._objects[id])
                        self._objects[id]._data[self.sub_split]._setParent(self._objects[id])

                    self._load_timestamp[id] = os.fstat(fobj.fileno()).st_ctime
            except Exception, x:
                if not id == 0:
                    logger.warning("Could not load object #%i: %s %s", id, x.__class__.__name__, x)
                    raise KeyError(id)
                    #self._internal_setitem__(id, EmptyGangaObject())

    def delete(self, ids):
        for id in ids:
            self._internal_del__(id)
            fn = self.get_fn(id)
            try:
                os.unlink(os.path.dirname(fn)+".index")
            except OSError:
                pass
            def rmrf(name):
                if os.path.isdir(name):
                    for sfn in os.listdir(name):
                        rmrf(os.path.join(name,sfn))
                    try:
                        os.removedirs(name)
                    except OSError:
                        pass
                else:
                    try:
                        os.unlink(name)
                    except OSError:
                        pass
            rmrf(os.path.dirname(fn))

    def lock(self,ids):
        locked_ids = self.sessionlock.lock_ids(ids)
        if len(locked_ids) < len(ids):
            return False
        return True

    def unlock(self,ids):
        released_ids = self.sessionlock.release_ids(ids)
        if len(released_ids) < len(ids):
            logger.error("The write locks of some objects could not be released!")

    def _getMetadataObject(self):
        return self._metadata

    def _setMetadataObject(self, obj):
        try:
            os.makedirs(os.path.dirname(self.get_fn(0)))
        except OSError, e:
            if e.errno != errno.EEXIST: 
                raise RepositoryError(self,"OSError: " + str(e))
        self._internal_setitem__(0,obj)
