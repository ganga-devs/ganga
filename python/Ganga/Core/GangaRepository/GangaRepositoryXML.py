# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

from GangaRepository import GangaRepository, PluginManagerError, EmptyGangaObject, RepositoryError, InaccessibleObjectError
from Ganga.Utility.Config import getConfig
import os, os.path, fcntl, time, errno

from SessionLock import SessionLockManager

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Core.GangaRepository.PickleStreamer import to_file as pickle_to_file
from Ganga.Core.GangaRepository.PickleStreamer import from_file as pickle_from_file
            
from Ganga.Core.GangaRepository.VStreamer import to_file as xml_to_file
from Ganga.Core.GangaRepository.VStreamer import from_file as xml_from_file

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

class GangaRepositoryLocal(GangaRepository):
    """GangaRepository Local"""

    def __init__(self, registry):
        super(GangaRepositoryLocal,self).__init__(registry)
        self.sub_split = "subjobs"
        self.root = os.path.join(self.registry.location,"6.0",self.registry.name)


    def startup(self):
        """ Starts an repository and reads in a directory structure.
        Raise RepositoryError"""
        self._load_timestamp = {}
        self._cache_load_timestamp = {}
        self.known_bad_ids = []
        if "XML" in self.registry.type:
            self.to_file = xml_to_file
            self.from_file = xml_from_file
        elif "Pickle" in self.registry.type:
            self.to_file = pickle_to_file
            self.from_file = pickle_from_file
        else:
            raise RepositoryError(self.repo, "Unknown Repository type: %s"%self.registry.type)
        self.sessionlock = SessionLockManager(self, self.root, self.registry.type+"."+self.registry.name)
        self.sessionlock.startup()
        # Load the list of files, this time be verbose and print out a summary of errors
        self.update_index(verbose = True)


    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry
        Raise RepositoryError"""
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
            Raise IOError on access or unpickling error 
            Raise OSError on stat error
            Raise PluginManagerError if the class name is not found"""
        logger.debug("Loading index %s" % id)
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
                        obj.__dict__["_registry_refresh"] = True
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
                obj._index_cache = new_idx_cache
                pickle_to_file((obj._category,obj._name,obj._index_cache),file(ifn,"w"))
        except IOError, x:
            logger.error("Index saving to '%s' failed: %s %s" % (ifn,x.__class__.__name__,x))

    def data_load(self,id):
        """ (re)load the data for this object from the repository """
        pass

    def data_write(self,id):
        """ write (flush) given data to the repository. (must be locked) """
        pass

    def get_index_listing(self):
        """Get dictionary of possible objects in the Repository: True means index is present,
            False if not present
        Raise RepositoryError"""
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
            objs.update(dict([(int(l),False) for l in listing if l.isdigit()]))
            for l in listing:
                if l.endswith(".index") and l[:-6].isdigit():
                    id = int(l[:-6])
                    if id in objs:
                        objs[id] = True
                    else:
                        try:
                            os.unlink(self.get_idxfn(id))
                        except OSError:
                            pass
        return objs

    def update_index(self,id = None,verbose=False):
        """ Update the list of available objects
        Raise RepositoryError"""
        # First locate and load the index files
        logger.debug("updating index...")
        objs = self.get_index_listing()
        summary = []
        for id, idx in objs.iteritems():
            # Locked IDs can be ignored
            if id in self.sessionlock.locked:
                continue
            # Now we treat unlocked IDs
            try:
                self.index_load(id) # if this succeeds, all is well and we are done
                continue
            except IOError, x:
                logger.debug("IOError: Failed to load index %i: %s" % (id,x))
            except OSError, x:
                logger.debug("OSError: Failed to load index %i: %s" % (id,x))
            except PluginManagerError, x:
                logger.debug("PluginManagerError: Failed to load index %i: %s" % (id,x)) # Probably should be DEBUG
                summary.append((id,x)) # This is a FATAL error - do not try to load the main file, it will fail as well
                continue
            if not id in self.objects: # this is bad - no or corrupted index but object not loaded yet! Try to load it!
                try:
                    self.load([id])
                except KeyError:
                    pass # deleted job
                except InaccessibleObjectError, x:
                    logger.debug("Failed to load id %i: %s %s" % (id, x.orig.__class__.__name__, x.orig))
                    summary.append((id,x.orig))
                # Write out a new index if the file can be locked
                if len(self.lock([id])) != 0:
                    self.index_write(id)
                    self.unlock([id])
        if len(summary) > 0:
            cnt = {}
            examples = {}
            for id,x in summary:
                if id in self.known_bad_ids:
                    continue
                cnt[x.__class__.__name__] = cnt.get(x.__class__.__name__,[]) + [str(id)]
                examples[x.__class__.__name__] = str(x)
                self.known_bad_ids.append(id)
            for exc,ids in cnt.items():
                logger.error("Registry '%s': Failed to load %i jobs (IDs: %s) due to '%s' (first error: %s)" % (self.registry.name, len(ids), ",".join(ids), exc, examples[exc]))
        logger.debug("updated index done")

    def add(self, objs, force_ids = None):
        """ Add the given objects to the repository, forcing the IDs if told to.
        Raise RepositoryError"""
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
                    raise RepositoryError(self,"OSError on mkdir: %s" % (str(e)))
            self._internal_setitem__(ids[i], objs[i])
            # Set subjobs dirty - they will not be flushed if they are not.
            if self.sub_split and self.sub_split in objs[i]._data:
                for j in range(len(objs[i]._data[self.sub_split])):
                    objs[i]._data[self.sub_split][j]._dirty = True
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
                raise RepositoryError(self,"OSError on flushing id '%i': %s" % (id,str(x)))
            except IOError, x:
                raise RepositoryError(self,"IOError on flushing id '%i': %s" % (id,str(x)))

    def load(self, ids):
        for id in ids:
            fn = self.get_fn(id)
            try:
                fobj = file(fn,"r")
            except IOError, x:
                if x.errno == errno.ENOENT: 
                    # remove index so we do not continue working with wrong information
                    try:
                        self._internal_del__(id) # remove internal representation
                        os.unlink(os.path.dirname(fn)+".index")
                    except OSError:
                        pass
                    raise KeyError(id)
                else: 
                    raise RepositoryError(self,"IOError: " + str(x))
            try:
                must_load = (not id in self.objects) or (self.objects[id]._data is None)
                tmpobj = None
                if must_load or (self._load_timestamp.get(id,0) != os.fstat(fobj.fileno()).st_ctime):
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
                    if len(errs) > 0:
                        raise errs[0]
                    #if len(errs) > 0 and "status" in tmpobj._data: # MAGIC "status" if incomplete
                    #    tmpobj._data["status"] = "incomplete"
                    #    logger.error("Registry '%s': Could not load parts of object #%i: %s" % (self.registry.name,id,map(str,errs)))
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
                        # Check if index cache; if loaded; was valid:
                        if obj._index_cache:
                            new_idx_cache = self.registry.getIndexCache(obj)
                            if new_idx_cache != obj._index_cache:
                                # index is wrong! Try to get read access - then we can fix this 
                                if len(self.lock([id])) != 0:
                                    self.index_write(id)
                                    self.unlock([id])
                                    logger.warning("Incorrect index cache of '%s' object #%s was corrected!" % (self.registry.name, id))
                                # if we cannot lock this, the inconsistency is most likely the result of another ganga process modifying the repo
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
                logger.debug("Could not load object #%i: %s %s", id, x.__class__.__name__, x)
                # remove index so we do not continue working with wrong information
                try:
                    os.unlink(os.path.dirname(fn)+".index")
                except OSError:
                    pass
                #self._internal_setitem__(id, EmptyGangaObject()) // NO NO NO! BREAKS EVERYTHING HORRIBLY!
                raise InaccessibleObjectError(self,id,x)

    def delete(self, ids):
        for id in ids:
            # First remove the index, so that it is gone if we later have a KeyError
            fn = self.get_fn(id)
            try:
                os.unlink(os.path.dirname(fn)+".index")
            except OSError:
                pass
            self._internal_del__(id)


            rmrf(os.path.dirname(fn))

    def lock(self,ids):
        return self.sessionlock.lock_ids(ids)

    def unlock(self,ids):
        released_ids = self.sessionlock.release_ids(ids)
        if len(released_ids) < len(ids):
            logger.error("The write locks of some objects could not be released!")

    def get_lock_session(self,id): 
        """get_lock_session(id)
        Tries to determine the session that holds the lock on id for information purposes, and return an informative string.
        Returns None on failure
        """
        return self.sessionlock.get_lock_session(id)

    def get_other_sessions(self): 
        """get_session_list()
        Tries to determine the other sessions that are active and returns an informative string for each of them.
        """
        return self.sessionlock.get_other_sessions()

    def reap_locks(self):
        """reap_locks() --> True/False
        Remotely clear all foreign locks from the session.
        WARNING: This is not nice.
        Returns True on success, False on error."""
        return self.sessionlock.reap_locks()

    def clean(self):
        """clean() --> True/False
        Clear EVERYTHING in this repository, counter, all jobs, etc.
        WARNING: This is not nice."""
        self.shutdown()
        rmrf(self.root)
        self.startup()
        
