# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

from GangaRepository import *
from Ganga.Utility.Config import getConfig
import os, os.path, fcntl, time, errno
from Ganga.Core.GangaRepository.VStreamer import from_file, to_file
from SessionLock import SessionLockManager

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


def safe_save_xml(fn,obj):
    """Writes an XML file safely, raises IOError on error"""
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


class GangaRepositoryXML(GangaRepository):
    """GangaRepository XML"""

    def get_fn(self,id):
        """ Returns the file name where the data for this object id is saved"""
        return os.path.join(self.root,"%ixxx"%(id/1000), "%i"%id, "data")

    def startup(self):
        """ Starts an XML repository and reads in a directory structure."""
        self._load_timestamp = {}
        self.root = os.path.join(self.location,"6.0",self.name)
        self.sessionlock = SessionLockManager(self.root, "LocalXML."+self.name)
        ids = []
        # Obtain candidate list of ids by scanning directories
        for d in os.listdir(self.root):
            if not d.endswith("xxx"): continue
            for sd in os.listdir(os.path.join(self.root,d)):
                try:
                    id = int(sd)
                except ValueError:
                    pass
                ids.append(id)
        # Try to load all candidate IDs
        for id in ids:
            try:
                self.load([id]) # TODO: Here insert index read
            except KeyError:
                pass

    def add(self, objs):
        """add(self, objs) --> list of unique ids
        Raise RepositoryError
        objs -- list of ganga objects to register
        Returns a list of unique ids for the items in order.
        Does not check if objs are already registered.
        """
        ids = self.sessionlock.make_new_ids(len(objs))
        print "Made IDS: ", ids
        for i in range(0,len(objs)):
            fn = self.get_fn(ids[i])
            try:
                os.makedirs(os.path.dirname(fn))
            except IOError, e:
                if e.errno != errno.ENOENT: raise
            self._internal_setitem__(ids[i], objs[i])
        return ids

    def flush(self, ids):
        locked_ids = self.sessionlock.lock_ids(ids)
        for id in locked_ids:
            fn = self.get_fn(id)
            obj = self._objects[id]
            if obj._name != "Unknown":
                safe_save_xml(fn, self._objects[id])

    def load(self, ids):
        for id in ids:
            fn = self.get_fn(id)
            try:
                fobj = file(fn,"r")
            except IOError, x:
                if x.errno != errno.ENOENT: raise
                else: raise KeyError(id)                
            try:
                if id in self._objects:
                    if self._objects[id]._data is None or self._load_timestamp[id] != os.fstat(fobj.fileno()).st_ctime:
                        tmpobj = from_file(fobj)[0]
                        self._objects[id]._data = tmpobj._data
                else:
                    self._internal_setitem__(id, from_file(fobj)[0])
                self._load_timestamp[id] = os.fstat(fobj.fileno()).st_ctime
            except Exception, x:
                print x
                self._internal_setitem__(id, EmptyGangaObject())
        return [self._objects[id] for id in ids]

    def delete(self, ids):
        locked_ids = self.sessionlock.lock_ids(ids)
        for id in locked_ids:
            obj = self._objects[id]
            obj._setRegistry(None)
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
            del self._object_id_map[obj]
            if obj in self.dirty_objs:
                del self.dirty_objs[obj]
            del self._objects[id]

    def acquireWriteLock(self,ids):
        locked_ids = self.sessionlock.lock_ids(ids)
        self.load(ids)
        return locked_ids

    def releaseWriteLock(self,ids):
        released_ids = self.sessionlock.release_ids(ids)
        self.flush(released_ids)


