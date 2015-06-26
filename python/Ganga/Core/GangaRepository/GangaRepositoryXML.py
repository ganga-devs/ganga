from __future__ import absolute_import
# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

from .GangaRepository import GangaRepository, RepositoryError, InaccessibleObjectError
from Ganga.Utility.Plugin import PluginManagerError
import os
import os.path
import time
import errno

from .SessionLock import SessionLockManager

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Core.GangaRepository.PickleStreamer import to_file as pickle_to_file
from Ganga.Core.GangaRepository.PickleStreamer import from_file as pickle_from_file

from Ganga.Core.GangaRepository.VStreamer import to_file as xml_to_file
from Ganga.Core.GangaRepository.VStreamer import from_file as xml_from_file

from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaListByRef
from Ganga.GPIDev.Base.Objects import Node

printed_explanation = False


def safe_save(fn, obj, to_file, ignore_subs=''):
    """Writes a file safely, raises IOError on error"""
    if hasattr(obj, 'application') and hasattr(obj.application, 'hash') and obj.application.hash is not None:
        if not obj.application.calc_hash(verify=True):
            try:
                logger.warning('Protected attribute(s) of %s application (associated with %s #%s) changed!'
                               % (obj.application._name, obj._name, obj._registry_id))
            except:
                logger.warning('Protected attribute(s) of %s application (associated with %s) changed!!!!'
                               % (obj.application._name, obj._name))

            logger.warning(
                'If you knowingly circumvented the protection, ignore this message (and, optionally,')
            logger.warning(
                're-prepare() the application). Otherwise, please file a bug report at:')
            # logger.warning('http://savannah.cern.ch/projects/ganga/')
            logger.warning('https://its.cern.ch/jira/browse/GANGA')
    elif hasattr(obj, 'analysis') and hasattr(obj.analysis, 'application') and \
            hasattr(obj.analysis.application, 'hash') and obj.analysis.application.hash is not None:
        if not obj.analysis.application.calc_hash(verify=True):
            try:
                logger.warning('Protected attribute(s) of %s application (associated with %s #%s) changed!'
                               % (obj.analysis.application._name, obj._name, obj._registry_id))
            except:
                logger.warning('Protected attribute(s) of %s application (associated with %s) changed!!!!'
                               % (obj.analysis.application._name, obj._name))
            logger.warning(
                'If you knowingly circumvented the protection, ignore this message (and, optionally,')
            logger.warning(
                're-prepare() the application). Otherwise, please file a bug report at:')
            # logger.warning('http://savannah.cern.ch/projects/ganga/')
            logger.warning('https://its.cern.ch/jira/browse/GANGA')

    if not os.path.exists(fn):
        # file does not exist, so make it fast!
        try:
            with open(fn, "w") as this_file:
                to_file(obj, this_file, ignore_subs)
            return
        except IOError as e:
            raise IOError("Could not write file '%s' (%s)" % (fn, e))
    try:
        with open(fn + ".new", "w") as tmpfile:
            to_file(obj, tmpfile, ignore_subs)
            # Important: Flush, then sync file before renaming!
            # tmpfile.flush()
            # os.fsync(tmpfile.fileno())
    except IOError as e:
        raise IOError("Could not write file %s.new (%s)" % (fn, e))
    # Try to make backup copy...
    try:
        os.unlink(fn + "~")
    except OSError as e:
        logger.debug("Error on removing file %s~ (%s) " % (fn, e))
    try:
        os.rename(fn, fn + "~")
    except OSError as e:
        logger.debug("Error on file backup %s (%s) " % (fn, e))
    try:
        os.rename(fn + ".new", fn)
    except OSError as e:
        raise IOError("Error on moving file %s.new (%s) " % (fn, e))


def rmrf(name):
    if os.path.isdir(name):
        for sfn in os.listdir(name):
            rmrf(os.path.join(name, sfn))
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
        super(GangaRepositoryLocal, self).__init__(registry)
        self.sub_split = "subjobs"
        self.root = os.path.join(
            self.registry.location, "6.0", self.registry.name)
        self.lockroot = os.path.join(self.registry.location, "6.0")
        self.saved_paths = {}
        self.saved_idxpaths = {}

    def startup(self):
        """ Starts a repository and reads in a directory structure.
        Raise RepositoryError"""
        self._load_timestamp = {}

        # New Master index to speed up loading of many, MANY files
        self._cache_load_timestamp = {}
        self._cached_cat = {}
        self._cached_cls = {}
        self._cached_obj = {}
        self._master_index_timestamp = 0

        self.known_bad_ids = []
        if "XML" in self.registry.type:
            self.to_file = xml_to_file
            self.from_file = xml_from_file
        elif "Pickle" in self.registry.type:
            self.to_file = pickle_to_file
            self.from_file = pickle_from_file
        else:
            raise RepositoryError(
                self.repo, "Unknown Repository type: %s" % self.registry.type)
        self.sessionlock = SessionLockManager(
            self, self.lockroot, self.registry.name)
        self.sessionlock.startup()
        # Load the list of files, this time be verbose and print out a summary
        # of errors
        self.update_index(verbose=True, firstRun=True)
        logger.debug("GangaRepositoryLocal Finished Startup")

    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry
        Raise RepositoryError"""
        logger.debug("Shutting Down GangaRepositoryLocal")
        self._write_master_cache()
        self.sessionlock.shutdown()

    def get_fn(self, id):
        """ Returns the file name where the data for this object id is saved"""
        if id not in self.saved_paths:
            self.saved_paths[id] = os.path.join(
                self.root, "%ixxx" % int(id * 0.001), "%i" % id, "data")
        return self.saved_paths[id]

    def get_idxfn(self, id):
        """ Returns the file name where the data for this object id is saved"""
        if id not in self.saved_idxpaths:
            self.saved_idxpaths[id] = os.path.join(
                self.root, "%ixxx" % int(id * 0.001), "%i.index" % id)
        return self.saved_idxpaths[id]

    def index_load(self, id):
        """ load the index file for this object if necessary
            Loads if never loaded or timestamp changed. Creates object if necessary
            Returns True if this object has been changed, False if not
            Raise IOError on access or unpickling error 
            Raise OSError on stat error
            Raise PluginManagerError if the class name is not found"""
        #logger.debug("Loading index %s" % id)
        fn = self.get_idxfn(id)
        # index timestamp changed
        if self._cache_load_timestamp.get(id, 0) != os.stat(fn).st_ctime:
            try:
                with open(fn, 'r') as fobj:
                    cat, cls, cache = pickle_from_file(fobj)[0]
            except Exception as x:
                raise IOError("Error on unpickling: %s %s" %
                              (x.__class__.__name__, x))
            if id in self.objects:
                obj = self.objects[id]
                if obj._data:
                    obj.__dict__["_registry_refresh"] = True
            else:
                obj = self._make_empty_object_(id, cat, cls)
            obj._index_cache = cache
            self._cache_load_timestamp[id] = os.stat(fn).st_ctime
            self._cached_cat[id] = cat
            self._cached_cls[id] = cls
            self._cached_obj[id] = cache
            return True
        elif id not in self.objects:
            self.objects[id] = self._make_empty_object_(
                id, self._cached_cat[id], self._cached_cls[id])
            self.objects[id]._index_cache = self._cached_obj[id]
            return True
        return False

    def index_write(self, id):
        """ write an index file for this object (must be locked).
            Should not raise any Errors """
        obj = self.objects[id]
        try:
            ifn = self.get_idxfn(id)
            new_idx_cache = self.registry.getIndexCache(obj)
            if new_idx_cache != obj._index_cache or not os.path.exists(ifn):
                obj._index_cache = new_idx_cache
                with open(ifn, "w") as this_file:
                    pickle_to_file(
                        (obj._category, obj._name, obj._index_cache), this_file)
        except IOError as x:
            logger.error("Index saving to '%s' failed: %s %s" %
                         (ifn, x.__class__.__name__, x))

    def get_index_listing(self):
        """Get dictionary of possible objects in the Repository: True means index is present,
            False if not present
        Raise RepositoryError"""
        try:
            obj_chunks = [
                d for d in os.listdir(self.root) if d.endswith("xxx") and d[:-3].isdigit()]
        except OSError:
            raise RepositoryError(
                self, "Could not list repository '%s'!" % (self.root))
        objs = {}  # True means index is present, False means index not present
        for c in obj_chunks:
            try:
                listing = os.listdir(os.path.join(self.root, c))
            except OSError:
                raise RepositoryError(
                    self, "Could not list repository '%s'!" % (os.path.join(self.root, c)))
            objs.update(dict([(int(l), False)
                              for l in listing if l.isdigit()]))
            for l in listing:
                if l.endswith(".index") and l[:-6].isdigit():
                    id = int(l[:-6])
                    if id in objs:
                        objs[id] = True
                    else:
                        try:
                            os.unlink(self.get_idxfn(id))
                            logger.warning(
                                "Deleted index file without data file: %s" % self.get_idxfn(id))
                        except OSError:
                            pass
        return objs

    def _read_master_cache(self):
        try:
            import os.path
            _master_idx = os.path.join(self.root, 'master.idx')
            if os.path.isfile(_master_idx):
                logger.debug("Reading Master index")
                import os
                self._master_index_timestamp = os.stat(_master_idx).st_ctime
                with open(_master_idx, 'r') as input_f:
                    this_master_cache = pickle_from_file(input_f)[0]
                for this_cache in this_master_cache:
                    this_id = this_cache[0]
                    self._cache_load_timestamp[this_id] = this_cache[1]
                    self._cached_cat[this_id] = this_cache[2]
                    self._cached_cls[this_id] = this_cache[3]
                    self._cached_obj[this_id] = this_cache[4]
            else:
                logger.debug("Not Reading Master Index")
        except:
            logger.debug("Master Index corrupt, ignoring it")
            for k, v in self._cache_load_timestamp.iteritems():
                self._cache_load_timestamp.pop(k)
            for k, v in self._cached_cat.iteritems():
                self._cached_cat.pop(k)
            for k, v in self._cached_cls.iteritems():
                self._cached_cls.pop(k)
            for k, v in self._cached_obj.iteritems():
                self._cached_obj.pop(k)
        return

    def _write_master_cache(self, shutdown=False):
        logger.debug("Updating master index")
        try:
            import os.path
            _master_idx = os.path.join(self.root, 'master.idx')
            this_master_cache = []
            if os.path.isfile(_master_idx) and not shutdown:
                import os
                if abs(self._master_index_timestamp - os.stat(_master_idx).st_ctime) < 300:
                    return
            items_to_save = self.objects.iteritems()
            for k, v in items_to_save:
                try:
                    # Check and write index first
                    obj = self.objects[k]
                    new_index = None
                    if obj:
                        new_index = self.registry.getIndexCache(obj)
                    if new_index and new_index != obj._index_cache:
                        if len(self.lock([k])) != 0:
                            self.index_write(k)
                            self.unlock([k])
                except Exception as x:
                    logger.debug(
                        "Failed to update index: %s on shutdown" % str(k))
                    logger.debug("%s" % str(x))
                    pass
            cached_list = []
            iterables = self._cache_load_timestamp.iteritems()
            for k, v in iterables:
                cached_list.append(k)
                try:
                    fn = self.get_idxfn(k)
                    time = os.stat(fn).st_ctime
                except:
                    time = 0
                cached_list.append(time)
                cached_list.append(self._cached_cat[k])
                cached_list.append(self._cached_cls[k])
                cached_list.append(self._cached_obj[k])
                this_master_cache.append(cached_list)

            try:
                with open(_master_idx, 'w') as of:
                    pickle_to_file(this_master_cache, of)
            except:
                try:
                    import os
                    os.unlink(os.path.join(self.root, 'master.idx'))
                except:
                    pass
        except:
            pass

        return

    def updateLocksNow(self):
        self.sessionlock.updateNow()

    def update_index(self, id=None, verbose=False, firstRun=False):
        """ Update the list of available objects
        Raise RepositoryError"""
        # First locate and load the index files
        logger.debug("updating index...")
        objs = self.get_index_listing()
        changed_ids = []
        deleted_ids = set(self.objects.keys())
        summary = []
        if firstRun:
            self._read_master_cache()
        for id, idx in objs.iteritems():
            deleted_ids.discard(id)
            # Make sure we do not overwrite older jobs if someone deleted the
            # count file
            if id > self.sessionlock.count:
                self.sessionlock.count = id + 1
            # Locked IDs can be ignored
            if id in self.sessionlock.locked:
                continue
            # Now we treat unlocked IDs
            try:
                # if this succeeds, all is well and we are done
                if self.index_load(id):
                    changed_ids.append(id)
                continue
            except IOError as x:
                logger.debug("IOError: Failed to load index %i: %s" % (id, x))
            except OSError as x:
                logger.debug("OSError: Failed to load index %i: %s" % (id, x))
            except PluginManagerError as x:
                # Probably should be DEBUG
                logger.debug(
                    "PluginManagerError: Failed to load index %i: %s" % (id, x))
                # This is a FATAL error - do not try to load the main file, it
                # will fail as well
                summary.append((id, x))
                continue

            # print id
            # print self.objects

            # this is bad - no or corrupted index but object not loaded yet!
            # Try to load it!
            if not id in self.objects:
                try:
                    self.load([id])
                    changed_ids.append(id)
                    # Write out a new index if the file can be locked
                    if len(self.lock([id])) != 0:
                        self.index_write(id)
                        self.unlock([id])
                except KeyError:
                    # deleted job
                    if id in self.objects:
                        self._internal_del__(id)
                        changed_ids.append(id)
                except InaccessibleObjectError as x:
                    logger.debug("Failed to load id %i: %s %s" %
                                 (id, x.orig.__class__.__name__, x.orig))
                    summary.append((id, x.orig))

        # Check deleted files:
        for id in deleted_ids:
            self._internal_del__(id)
            changed_ids.append(id)
        if len(deleted_ids) > 0:
            logger.warning("Registry '%s': Job %s externally deleted." % (
                self.registry.name, ",".join(map(str, list(deleted_ids)))))

        if len(summary) > 0:
            cnt = {}
            examples = {}
            for id, x in summary:
                if id in self.known_bad_ids:
                    continue
                cnt[x.__class__.__name__] = cnt.get(
                    x.__class__.__name__, []) + [str(id)]
                examples[x.__class__.__name__] = str(x)
                self.known_bad_ids.append(id)
                # add object to incomplete_objects
                if not id in self.incomplete_objects:
                    self.incomplete_objects.append(id)
            global printed_explanation
            for exc, ids in cnt.items():
                if examples[exc].find('comments') > 0:
                    printed_explanation = True
                    from Ganga.Utility.repairJobRepository import repairJobRepository
                    for jobid in ids:
                        repairJobRepository(int(jobid))
                else:
                    logger.error("Registry '%s': Failed to load %i jobs (IDs: %s) due to '%s' (first error: %s)" % (
                        self.registry.name, len(ids), ",".join(ids), exc, examples[exc]))
            if not printed_explanation:
                logger.error("If you want to delete the incomplete objects, you can type 'for i in %s.incomplete_ids(): %s(i).remove()' (press 'Enter' twice)" % (
                    self.registry.name, self.registry.name))
                logger.error(
                    "WARNING!!! This will result in corrupt jobs being completely deleted!!!")
                printed_explanation = True
        logger.debug("updated index done")

        if len(changed_ids) != 0:
            self._write_master_cache(shutdown=True)

        return changed_ids

    def add(self, objs, force_ids=None):
        """ Add the given objects to the repository, forcing the IDs if told to.
        Raise RepositoryError"""
        if not force_ids is None:  # assume the ids are already locked by Registry
            if not len(objs) == len(force_ids):
                raise RepositoryError(
                    self, "Internal Error: add with different number of objects and force_ids!")
            ids = force_ids
        else:
            ids = self.sessionlock.make_new_ids(len(objs))
        for i in range(0, len(objs)):
            fn = self.get_fn(ids[i])
            try:
                os.makedirs(os.path.dirname(fn))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise RepositoryError(
                        self, "OSError on mkdir: %s" % (str(e)))
            self._internal_setitem__(ids[i], objs[i])
            # Set subjobs dirty - they will not be flushed if they are not.
            if self.sub_split and self.sub_split in objs[i]._data:
                try:
                    for j in range(len(objs[i]._data[self.sub_split])):
                        objs[i]._data[self.sub_split][j]._dirty = True
                except AttributeError:
                    pass  # this is not a list of Ganga objects
        return ids

    def flush(self, ids):
        logger.debug("Flushing: %s" % ids)
        #import traceback
        # traceback.print_stack()
        for id in ids:
            try:
                fn = self.get_fn(id)
                obj = self.objects[id]
                if obj._name != "EmptyGangaObject":
                    split_cache = None
                    do_sub_split = (not self.sub_split is None) and (self.sub_split in obj._data) and len(
                        obj._data[self.sub_split]) > 0 and hasattr(obj._data[self.sub_split][0], "_dirty")
                    if do_sub_split:
                        split_cache = obj._data[self.sub_split]
                        for i in range(len(split_cache)):
                            if not split_cache[i]._dirty:
                                continue
                            sfn = os.path.join(
                                os.path.dirname(fn), str(i), "data")
                            try:
                                os.makedirs(os.path.dirname(sfn))
                            except OSError as e:
                                if e.errno != errno.EEXIST:
                                    raise RepositoryError(
                                        self, "OSError: " + str(e))
                            safe_save(sfn, split_cache[i], self.to_file)
                            split_cache[i]._setFlushed()
                        safe_save(fn, obj, self.to_file, self.sub_split)
                        # clean files not in subjobs anymore... (bug 64041)
                        for idn in os.listdir(os.path.dirname(fn)):
                            if idn.isdigit() and int(idn) >= len(split_cache):
                                rmrf(os.path.join(os.path.dirname(fn), idn))
                    else:
                        safe_save(fn, obj, self.to_file, "")
                        # clean files leftover from sub_split
                        for idn in os.listdir(os.path.dirname(fn)):
                            if idn.isdigit():
                                rmrf(os.path.join(os.path.dirname(fn), idn))
                    self.index_write(id)
                    obj._setFlushed()
            except OSError as x:
                raise RepositoryError(
                    self, "OSError on flushing id '%i': %s" % (id, str(x)))
            except IOError as x:
                raise RepositoryError(
                    self, "IOError on flushing id '%i': %s" % (id, str(x)))

    def is_loaded(self, id):

        return (id in self.objects) and (self.objects[id]._data is not None)

    def count_nodes(self, id):

        node_count = 0
        fn = self.get_fn(id)

        ld = os.listdir(os.path.dirname(fn))
        i = 0
        while str(i) in ld:
            sfn = os.path.join(os.path.dirname(fn), str(i), "data")
            if os.path.exists(sfn):
                node_count = node_count + 1
            i += 1

        return node_count

    def load(self, ids, load_backup=False):

        # print "load: %s " % str(ids)
        #import traceback
        # traceback.print_stack()

        logger.debug("Loading Repo object(s): %s" % str(ids))

        for id in ids:
            fn = self.get_fn(id)
            if load_backup:
                fn = fn + "~"
            try:
                fobj = open(fn, "r")
            except IOError as x:
                if x.errno == errno.ENOENT:
                    # remove index so we do not continue working with wrong
                    # information
                    try:
                        # remove internal representation
                        self._internal_del__(id)
                        os.unlink(os.path.dirname(fn) + ".index")
                    except OSError:
                        pass
                    raise KeyError(id)
                else:
                    raise RepositoryError(self, "IOError: " + str(x))
            finally:
                try:
                    if os.path.isdir(os.path.dirname(fn)):
                        ld = os.listdir(os.path.dirname(fn))
                        if len(ld) == 0:
                            os.rmdir(os.path.dirname(fn))
                            logger.debug(
                                "No job index or data found, removing empty directory: %s" % os.path.dirname(fn))
                except:
                    pass
            try:
                must_load = (not id in self.objects) or (
                    self.objects[id]._data is None)
                tmpobj = None
                if must_load or (self._load_timestamp.get(id, 0) != os.fstat(fobj.fileno()).st_ctime):
                    tmpobj, errs = self.from_file(fobj)
                    do_sub_split = (not self.sub_split is None) and (
                        self.sub_split in tmpobj._data) and len(tmpobj._data[self.sub_split]) == 0
                    if do_sub_split:
                        i = 0
                        ld = os.listdir(os.path.dirname(fn))
                        if len(ld) == 0:
                            os.rmdir(os.path.dirname(fn))
                            raise IOError(
                                "No job index or data found, removing empty directory: %s" % os.path.dirname(fn))
                        l = []
                        logger.debug(
                            "About to load about %s subjobs" % str(len(ld)))
                        while str(i) in ld:
                            sfn = os.path.join(
                                os.path.dirname(fn), str(i), "data")
                            if load_backup:
                                sfn = sfn + "~"
                            try:
                                #logger.debug( "Loading subjob at: %s" % sfn )
                                sfobj = open(sfn, "r")
                            except IOError as x:
                                if x.errno == errno.ENOENT:
                                    raise IOError(
                                        "Subobject %i.%i not found: %s" % (id, i, x))
                                else:
                                    raise RepositoryError(
                                        self, "IOError on loading subobject %i.%i: %s" % (id, i, x))
                            ff = self.from_file(sfobj)
                            l.append(ff[0])
                            errs.extend(ff[1])
                            i += 1
                            sfobj.close()
                        tmpobj._data[self.sub_split] = makeGangaListByRef(l)
                    if len(errs) > 0:
                        raise errs[0]
                    # if len(errs) > 0 and "status" in tmpobj._data: # MAGIC "status" if incomplete
                    #    tmpobj._data["status"] = "incomplete"
                    # logger.error("Registry '%s': Could not load parts of
                    # object #%i: %s" % (self.registry.name,id,map(str,errs)))
                    if id in self.objects:
                        obj = self.objects[id]
                        obj._data = tmpobj._data
                        # Fix parent for objects in _data (necessary!)
                        for n, v in obj._data.items():
                            if isinstance(v, Node):
                                v._setParent(obj)
                            if (isinstance(v, list) or v.__class__.__name__ == "GangaList"):
                                # set the parent of the list or dictionary (or
                                # other iterable) items
                                for i in v:
                                    if isinstance(i, Node):
                                        i._setParent(obj)

                        # Check if index cache; if loaded; was valid:
                        if obj._index_cache:
                            new_idx_cache = self.registry.getIndexCache(obj)
                            if new_idx_cache != obj._index_cache:
                                # index is wrong! Try to get read access - then
                                # we can fix this
                                if len(self.lock([id])) != 0:
                                    self.index_write(id)
                                    # self.unlock([id])

                                    old_idx_subset = all((k in new_idx_cache and new_idx_cache[
                                                         k] == v) for k, v in obj._index_cache.iteritems())
                                    if not old_idx_subset:
                                        # Old index cache isn't subset of new
                                        # index cache
                                        new_idx_subset = all((k in obj._index_cache and obj._index_cache[
                                                             k] == v) for k, v in new_idx_cache.iteritems())
                                    else:
                                        # Old index cache is subset of new
                                        # index cache so no need to check
                                        new_idx_subset = True

                                    if not old_idx_subset and not new_idx_subset:
                                        logger.warning("Incorrect index cache of '%s' object #%s was corrected!" % (
                                            self.registry.name, id))
                                        logger.debug(
                                            "old cache: %s\t\tnew cache: %s" % (str(obj._index_cache), str(new_idx_cache)))
                                        self.unlock([id])
                                # if we cannot lock this, the inconsistency is
                                # most likely the result of another ganga
                                # process modifying the repo
                                obj._index_cache = None
                    else:
                        self._internal_setitem__(id, tmpobj)
                    if do_sub_split:
                        try:
                            for sobj in self.objects[id]._data[self.sub_split]:
                                sobj._setParent(self.objects[id])
                        except AttributeError:
                            # not actually Ganga objects in the sub-split field
                            pass
                        self.objects[id]._data[
                            self.sub_split]._setParent(self.objects[id])

                    self._load_timestamp[id] = os.fstat(fobj.fileno()).st_ctime
            except RepositoryError:
                raise
            except Exception as x:
                if load_backup:
                    logger.debug(
                        "Could not load backup object #%i: %s %s", id, x.__class__.__name__, x)
                    raise InaccessibleObjectError(self, id, x)

                logger.debug(
                    "Could not load object #%i: %s %s", id, x.__class__.__name__, x)
                # try loading backup
                try:
                    self.load([id], load_backup=True)
                    logger.warning(
                        "Object '%s' #%i loaded from backup file - the last changes may be lost.", self.registry.name, id)
                    continue
                except Exception:
                    pass
                # add object to incomplete_objects
                if not id in self.incomplete_objects:
                    self.incomplete_objects.append(id)
                # remove index so we do not continue working with wrong
                # information
                try:
                    os.unlink(os.path.dirname(fn) + ".index")
                except OSError:
                    pass
                raise InaccessibleObjectError(self, id, x)
            finally:
                fobj.close()

    def delete(self, ids):
        for id in ids:
            # First remove the index, so that it is gone if we later have a
            # KeyError
            fn = self.get_fn(id)
            try:
                os.unlink(os.path.dirname(fn) + ".index")
            except OSError:
                pass
            self._internal_del__(id)
            rmrf(os.path.dirname(fn))

    def lock(self, ids):
        return self.sessionlock.lock_ids(ids)

    def unlock(self, ids):
        released_ids = self.sessionlock.release_ids(ids)
        if len(released_ids) < len(ids):
            logger.error(
                "The write locks of some objects could not be released!")

    def get_lock_session(self, id):
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
