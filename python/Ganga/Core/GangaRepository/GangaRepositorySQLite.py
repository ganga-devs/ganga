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

import sqlite

try:
         import cPickle as pickle
except:
         import pickle

from Ganga.Utility.Plugin import PluginManagerError, allPlugins

class GangaRepositorySQLite(GangaRepository):
    """GangaRepository XML"""

    def startup(self):
        """ Starts an repository and reads in a directory structure."""
        self.root = os.path.join(self.location,"0.1",self.name)
        try:
            os.makedirs(self.root)
        except OSError, x:
            pass
        self.con = sqlite.connect(os.path.join(self.root,"database.db"))
        print "Connected to ", os.path.join(self.root,"database.db")
        self.cur = self.con.cursor()
        tables = self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' and name LIKE 'objects'")
        if len(self.cur.fetchall()) == 0:
            self.cur.execute("CREATE TABLE objects (id INTEGER PRIMARY KEY, classname VARCHAR(30), category VARCHAR(20), idx VARCHAR(100), data VARCHAR(1000))")
        self.con.commit()
        self.load_index()

    def load_index(self,ids = None):
        if ids is None:
            self.cur.execute("SELECT id,classname,category,idx FROM objects")
        else:
            self.cur.execute("SELECT id,classname,category,idx FROM objects WHERE id IN (%s)" % (",".join(map(str,ids))))
        for e in self.cur:
            id = int(e[0])
            if e[1] is None: # deleted object
                continue
            #print "load_index: ",e
            if not id in self._objects:
                try:
                    cls = allPlugins.find(e[2],e[1])
                except PluginManagerError,e:
                    print e
                else:
                    obj  = super(cls, cls).__new__(cls)
                    obj._proxyObject = None
                    obj._data = None
                    self._internal_setitem__(int(e[0]),obj)
            else:
                obj = self._objects[id]
            if self._objects[id]._data is None:
                obj._index_cache = pickle.loads(e[3])
        
    def load(self,ids = None):
        if ids is None:
            self.cur.execute("SELECT id,classname,category,data FROM objects")
        else:
            self.cur.execute("SELECT id,classname,category,data FROM objects WHERE id IN (%s)" % (",".join(map(str,ids))))
        for e in self.cur:
            #print "load: ",e
            id = int(e[0])
            if e[1] is None: # deleted object
                continue
            if not id in self._objects:
                try:
                    cls = allPlugins.find(e[2],e[1])
                except PluginManagerError,e:
                    print e
                else:
                    obj  = super(cls, cls).__new__(cls)
                    obj._proxyObject = None
                    self._internal_setitem__(int(e[0]),obj)
            else:
                obj = self._objects[id]
            if obj._data is None:
                obj._data = pickle.loads(e[3])
                obj.__setstate__(obj.__dict__)
        return [self._objects[id] for id in ids]

    def shutdown(self):
        """shutdown(self) ---> None
        Releases all the locks and flushes the repository to persistent storage
        This is called by an atexit handler registered by the registry runtime.
        Raise RepositoryError
        """
        self.flush([self.find(o) for o in self.dirty_objs.keys()])
        self.con.commit()

    def add(self, objs):
        """add(self, objs) --> list of unique ids
        Raise RepositoryError
        objs -- list of ganga objects to register
        Returns a list of unique ids for the items in order.
        Does not check if objs are already registered.
        """
        ids = []
        for i in range(0,len(objs)):
            cls = objs[i]._name
            cat = objs[i]._category
            objs[i]._index_cache = make_index_cache(objs[i])
            data = pickle.dumps(objs[i]._data).replace("'","''")
            idx = pickle.dumps(objs[i]._index_cache).replace("'","''")
            self.cur.execute("INSERT INTO objects (id,classname,category,idx,data) VALUES (NULL,'%s','%s','%s','%s')" % (cls,cat,idx,data))
            ids.append(self.cur.lastrowid)
            self._internal_setitem__(ids[i], objs[i])
        self.con.commit()
        return ids

    def flush(self, ids):
        self.acquireWriteLock(ids) # makes sure _data is filled
        for id in ids:
            obj = self._objects[id] 
            if self.dirty_objs.has_key(obj):
                del self.dirty_objs[obj]
            if obj._name != "Unknown":
                obj._index_cache = make_index_cache(obj)
                data = pickle.dumps(obj._data).replace("'","''")
                idx = pickle.dumps(obj._index_cache).replace("'","''")
                self.cur.execute("UPDATE objects SET idx='%s',data='%s' WHERE id=%s" % (idx, data, id))
                #print "flushing id ", id, " backend ", obj.backend._name
        self.con.commit()

    def delete(self, ids):
        for id in ids:
            obj = self._objects[id]
            obj._setRegistry(None)
            #self.cur.execute("DELETE FROM objects WHERE id='%s'" % id)
            self.cur.execute("UPDATE objects SET classname=NULL,category=NULL,idx=NULL,data=NULL WHERE id=%s" % (id))
            del self._object_id_map[obj]
            if obj in self.dirty_objs:
                del self.dirty_objs[obj]
            del self._objects[id]
        self.con.commit()

    def acquireWriteLock(self,ids):
        new_ids = [id for id in ids if not id in self._objects or self._objects[id]._data is None]
        if len(new_ids) > 0:
            self.load(ids)
        return ids

    def releaseWriteLock(self,ids):
        return ids


