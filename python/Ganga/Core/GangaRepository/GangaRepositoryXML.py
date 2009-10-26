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
from Ganga.GPIDev.Base.Objects import Node

def safe_save(fn,obj,to_file,ignore_subs=''):
    """Writes a file safely, raises IOError on error"""
    if not os.path.exists(fn):
        # file does not exist, so make it fast!
        try:
            to_file(obj, file(fn,"w"), ignore_subs)
            return
        except IOError, e:
            raise IOError("Could not write file '%s' (%s)" % (fn,e))
    try:
        tmpfile = open(fn + ".new", "w")
        to_file(obj, tmpfile, ignore_subs)
        # Important: Flush, then sync file before renaming!
        #tmpfile.flush()
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

    def __init__(self, registry):
        super(GangaRepositoryLocal,self).__init__(registry)
        self.sub_split = "subjobs"
        self.root = os.path.join(self.registry.location,"6.0",self.registry.name)

    def startup(self):
        """ Starts an repository and reads in a directory structure."""
        self._load_timestamp = {}
        self._cache_load_timestamp = {}
        if "XML" in self.registry.type:
            from Ganga.Core.GangaRepository.VStreamer import to_file, from_file
            self.to_file = to_file
            self.from_file = from_file
        elif "Pickle" in self.registry.type:
            self.to_file = pickle_to_file
            self.from_file = pickle_from_file
        else:
            raise RepositoryError(self.repo, "Unknown Repository type: %s"%self.registry.type)
        self.sessionlock = SessionLockManager(self, self.root, self.registry.type+"."+self.registry.name)
        self.sessionlock.startup()
        self.update_index()

    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry"""
        self.sessionlock.shutdown()

    def get_fn(self,id):
        """ Returns the file name where the data for this object id is saved"""
        return os.path.join(self.root,"%ixxx"%(id/1000), "%i"%id, "data")

    def get_idxfn(self,id):
        """ Returns the file name where the data for this object id is saved"""
        return os.path.join(self.root,"%ixxx"%(id/1000), "%i.index"%id)

    def index_load(self,id): 
        """ load the index file for this object if necessary
            Loads if never loaded or timestamp changed. Creates object if necessary/
            Raise IOError on access or unpickling error"""
        logger.debug("Loading index %i" % id)
        fn = self.get_idxfn(id)
        if self._cache_load_timestamp.get(id,0) != os.stat(fn).st_ctime: # index timestamp changed
            fobj = file(fn)
            try:
                try:
                    cat,cls,cache = pickle_from_file(fobj)[0]
                except Exception, x:
                    raise IOError("Error on unpickling: %s %s" % (x.__class__.__name__, x))
                if id in self.objects:
                    obj = self.objects[id]
                    if obj._data:
                        obj._registry_refresh = True
                else:
                    obj = self._make_empty_object_(id,cat,cls)
                obj._index_cache = cache
            finally:
                fobj.close()
                self._cache_load_timestamp[id] = os.stat(fn).st_ctime

    def index_write(self,id):
        """ write an index file for this object (must be locked).
            Should not raise any Errors """
        obj = self.objects[id]
        try:
            ifn = self.get_idxfn(id)
            new_idx_cache = self.registry.getIndexCache(obj)
            if new_idx_cache != obj._index_cache or not os.path.exists(ifn):
                pickle_to_file((obj._category,obj._name,obj._index_cache),file(ifn,"w"))
                obj._index_cache = new_idx_cache
        except IOError, x:
            logger.error("Index saving to '%s' failed: %s %s" % (ifn,x.__class__.__name__,x))

    def data_load(self,id):
        """ (re)load the data for this object from the repository """
        pass

    def data_write(self,id):
        """ write (flush) given data to the repository. (must be locked) """
        pass

    def get_index_listing(self):
        try:
            obj_chunks = [d for d in os.listdir(self.root) if d.endswith("xxx") and d[:-3].isdigit()]
        except OSError:
            raise RepositoryError(self, "Could not list repository '%s'!" % (self.root))
        objs = {} # True means index is present, False means index not present
        for c in obj_chunks:
            try:
                listing = os.listdir(os.path.join(self.root,c))
            except OSError:
                raise RepositoryError(self, "Could not list repository '%s'!" % (os.path.join(self.root,c)))
            indices = [int(l[:-6]) for l in listing if l.endswith(".index") and l[:-6].isdigit()]
            objs.update([(int(l),False) for l in listing if l.isdigit()])
            for id in indices:
                if id in objs:
                    objs[id] = True
                else:
                    try:
                        os.unlink(self.get_idxfn(id))
                    except OSError:
                        pass
        return objs

    def update_index(self,id = None):
        # First locate and load the index files
        logger.info("updating index...")
        objs = self.get_index_listing()
        loaded_obj = 0
        loaded_cache = 0
        reloaded_cache = 0
        for id, idx in objs.iteritems():
            # Locked IDs can be ignored
            if id in self.sessionlock.locked:
                continue
            # Now we treat unlocked IDs
            try:
                self.index_load(id) # if this succeeds, all is well and we are done
                continue
            except IOError, x:
                logger.debug("Failed to load index %i: %s" % (id,x)) # Probably should be DEBUG
            except OSError, x:
                logger.debug("Failed to load index %i: %s" % (id,x)) # Probably should be DEBUG

            if not id in self.objects: # this is bad - no index but object not loaded yet! Try to load it!
                try:
                    self.load([id])
                except KeyError:
                    pass # deleted job
                except Exception:
                    logger.warning("Failed to load id %i!" % (id))

        #logger.info("Updated %s cache: Loaded %i objects, %i cached objects and refreshed %i objects from cache" % (self.registry.name,loaded_obj,loaded_cache,reloaded_cache))
        logger.info("updated index done")

    def add(self, objs, force_ids = None):
        if not force_ids is None: # assume the ids are already locked by Registry
            if not len(objs) == len(force_ids):
                raise RepositoryError(self, "Internal Error: add with different number of objects and force_ids!")
            ids = force_ids
        else:
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
                obj = self.objects[id]
                if obj._name != "EmptyGangaObject":
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
                    safe_save(fn, obj, self.to_file, self.sub_split)
                    self.index_write(id)
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
                    # remove index so we do not continue working with wrong information
                    try:
                        os.unlink(os.path.dirname(fn)+".index")
                    except OSError:
                        pass
                    raise KeyError(id)
                else: 
                    raise RepositoryError(self,"IOError: " + str(x))
            try:
                must_load = (not id in self.objects) or (self.objects[id]._data is None)
                tmpobj = None
                if must_load or (self._load_timestamp[id] != os.fstat(fobj.fileno()).st_ctime):
                    tmpobj, errs = self.from_file(fobj)
                    if self.sub_split:
                        i = 0
                        ld = os.listdir(os.path.dirname(fn))
                        l = []
                        while str(i) in ld:
                            sfn = os.path.join(os.path.dirname(fn),str(i),"data")
                            try:
                                sfobj = file(sfn,"r")
                            except IOError, x:
                                if x.errno == errno.ENOENT: 
                                    raise IOError("Subobject %i.%i not found: %s" % (id,i,x))
                                else:
                                    raise RepositoryError(self,"IOError on loading subobject %i.%i: %s" % (id,i,x))
                            ff = self.from_file(sfobj)
                            l.append(ff[0])
                            errs.extend(ff[1])
                            i += 1
                        tmpobj._data[self.sub_split] = makeGangaListByRef(l)
                    if len(errs) > 0 and "status" in tmpobj._data: # MAGIC "status" if incomplete
                        tmpobj._data["status"] = "incomplete"
                        logger.error("Registry '%s': Could not load parts of object #%i: %s" % (self.registry.name,id,map(str,errs)))
                    if id in self.objects:
                        obj = self.objects[id]
                        obj._data = tmpobj._data
                        # Fix parent for objects in _data (necessary!)
                        for n, v in obj._data.items():
                            if isinstance(v,Node):
                                v._setParent(obj)
                            if (isinstance(v,list) or v.__class__.__name__ == "GangaList"):
                                # set the parent of the list or dictionary (or other iterable) items
                                for i in v:
                                    if isinstance(i,Node):
                                        i._setParent(obj)
                        obj._index_cache = None
                    else:
                        self._internal_setitem__(id, tmpobj)
                    if self.sub_split:
                        for sobj in self.objects[id]._data[self.sub_split]:
                            sobj._setParent(self.objects[id])
                        self.objects[id]._data[self.sub_split]._setParent(self.objects[id])
                    self._load_timestamp[id] = os.fstat(fobj.fileno()).st_ctime
            except RepositoryError:
                raise
            except Exception, x:
                logger.warning("Could not load object #%i: %s %s", id, x.__class__.__name__, x)
                # remove index so we do not continue working with wrong information
                try:
                    os.unlink(os.path.dirname(fn)+".index")
                except OSError:
                    pass
                raise KeyError(id)
                #self._internal_setitem__(id, EmptyGangaObject())

    def delete(self, ids):
        for id in ids:
            # First remove the index, so that it is gone if we later have a KeyError
            fn = self.get_fn(id)
            try:
                os.unlink(os.path.dirname(fn)+".index")
            except OSError:
                pass
            self._internal_del__(id)

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

