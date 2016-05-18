# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

from Ganga.Core.GangaRepository import GangaRepository, RepositoryError, InaccessibleObjectError
from Ganga.Utility.Plugin import PluginManagerError
import os
import os.path
import time
import errno
import copy
import threading

from Ganga.Core.GangaRepository.SessionLock import SessionLockManager

import Ganga.Utility.logging

from Ganga.Core.GangaRepository.PickleStreamer import to_file as pickle_to_file
from Ganga.Core.GangaRepository.PickleStreamer import from_file as pickle_from_file

from Ganga.Core.GangaRepository.VStreamer import to_file as xml_to_file
from Ganga.Core.GangaRepository.VStreamer import from_file as xml_from_file
from Ganga.Core.GangaRepository.VStreamer import XMLFileError

from Ganga.GPIDev.Base.Objects import Node
from Ganga.Core.GangaRepository import SubJobXMLList

from Ganga.GPIDev.Base.Proxy import isType, stripProxy, getName

logger = Ganga.Utility.logging.getLogger()

save_all_history = False

def check_app_hash(obj):
    """Writes a file safely, raises IOError on error"""

    isVerifiableApp = False
    isVerifiableAna = False

    if hasattr(obj, 'application'):
        if hasattr(obj.application, 'hash'):
            if obj.application.hash is not None:
                isVerifiableApp = True
    elif hasattr(obj, 'analysis'):
        if hasattr(obj.analysis, 'application'):
            if hasattr(obj.analysis.application, 'hash'):
                if obj.analysis.application.hash is not None:
                    isVerifiableAna = True

    if isVerifiableApp is True:
        hashable_app = stripProxy(obj.application)
    elif isVerifiableAna is True:
        hashable_app = stripProxy(obj.analysis.application)
    else:
        hashable_app = None

    if hashable_app is not None:
        if not hashable_app.calc_hash(verify=True):
            try:
                logger.warning("%s" % hashable_app)
                logger.warning('Protected attribute(s) of %s application (associated with %s #%s) changed!' % (getName(hashable_app), getName(obj), obj._registry_id))
            except AttributeError as err:
                logger.warning('Protected attribute(s) of %s application (associated with %s) changed!!!!' % (getName(hashable_app), getName(obj)))
                logger.warning("%s" % err)
            jobObj = stripProxy(hashable_app).getJobObject()
            if jobObj is not None:
                logger.warning('Job: %s is now possibly corrupt!' % jobObj.getFQID('.'))
            logger.warning('If you knowingly circumvented the protection, ignore this message (and, optionally,')
            logger.warning('re-prepare() the application). Otherwise, please file a bug report at:')
            logger.warning('https://github.com/ganga-devs/ganga/issues/')

def safe_save(fn, _obj, to_file, ignore_subs=''):
    """Try to save the XML for this object in as safe a way as possible"""

    # Add a global lock to make absolutely sure we don't have multiple threads writing files
    # See Github Issue 185
    with safe_save.lock:

        obj = stripProxy(_obj)
        check_app_hash(obj)

        # Create the dirs
        dirname = os.path.dirname(fn)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # Prepare new data file
        new_name = fn + '.new'
        with open(new_name, "w") as tmpfile:
            to_file(obj, tmpfile, ignore_subs)

        # everything ready so create new data file and backup old one
        if os.path.exists(new_name):

            # Do we have an old one to backup?
            if os.path.exists(fn):
                os.rename(fn, fn + "~")

            os.rename(new_name, fn)

# Global lock for above function - See issue #185
safe_save.lock = threading.Lock()

def rmrf(name, count=0):
    """
    Safely recursively remove a file/folder from disk by first moving it then removing it
    calls self and will only attempt to move/remove a file 3 times before giving up
    """

    if count != 0:
        logger.debug("Trying again to remove: %s" % name)
        if count == 3:
            logger.error("Tried 3 times to remove file/folder: %s" % name)
            from Ganga.Core.exceptions import GangaException
            raise GangaException("Failed to remove file/folder: %s" % name)

    if os.path.isdir(name):

        try:
            remove_name = name + "_" + str(time.time()) + '__to_be_deleted_'
            os.rename(name, remove_name)
            #logger.debug("Move completed")
        except OSError as err:
            if err.errno != errno.ENOENT:
                logger.debug("rmrf Err: %s" % err)
                logger.debug("name: %s" % name)
                remove_name = name
                raise
            return

        for sfn in os.listdir(remove_name):
            try:
                rmrf(os.path.join(remove_name, sfn), count)
            except OSError as err:
                if err.errno == errno.EBUSY:
                    logger.debug("rmrf Remove err: %s" % err)
                    logger.debug("name: %s" % remove_name)
                    ## Sleep 2 sec and try again
                    time.sleep(2.)
                    rmrf(os.path.join(remove_name, sfn), count+1)
        try:
            os.removedirs(remove_name)
        except OSError as err:
            if err.errno == errno.ENOTEMPTY:
                rmrf(remove_name, count+1)
            elif err.errno != errno.ENOENT:
                logger.debug("%s" % err)
                raise
            return
    else:
        try:
            remove_name = name + "_" + str(time.time()) + '__to_be_deleted_'
            os.rename(name, remove_name)
        except OSError as err:
            if err.errno not in [errno.ENOENT, errno.EBUSY]:
                raise
            logger.debug("rmrf Move err: %s" % err)
            logger.debug("name: %s" % name)
            if err.errno == errno.EBUSY:
                rmrf(name, count+1)
            return

        try:
            os.remove(remove_name)
        except OSError as err:
            if err.errno != errno.ENOENT:
                logger.debug("%s" % err)
                logger.debug("name: %s" % remove_name)
                raise
            return


class GangaRepositoryLocal(GangaRepository):

    """GangaRepository Local"""

    def __init__(self, registry):
        """
        Initialize a Repository from within a Registry and keep a reference to the Registry which 'owns' it
        """
        super(GangaRepositoryLocal, self).__init__(registry)
        self.dataFileName = "data"
        self.sub_split = "subjobs"
        self.root = os.path.join(self.registry.location, "6.0", self.registry.name)
        self.lockroot = os.path.join(self.registry.location, "6.0")
        self.saved_paths = {}
        self.saved_idxpaths = {}
        self._cache_load_timestamp = {}
        self.printed_explanation = False
        self._fully_loaded = {}

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
            raise RepositoryError(self.repo, "Unknown Repository type: %s" % self.registry.type)
        self.sessionlock = SessionLockManager(self, self.lockroot, self.registry.name)
        self.sessionlock.startup()
        # Load the list of files, this time be verbose and print out a summary
        # of errors
        self.update_index(verbose=True, firstRun=True)
        logger.debug("GangaRepositoryLocal Finished Startup")

    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry
        Raise RepositoryError
        Write an index file for all new objects in memory and master index file of indexes"""
        from Ganga.Utility.logging import getLogger
        logger = getLogger()
        logger.debug("Shutting Down GangaRepositoryLocal: %s" % self.registry.name)
        for k in self._fully_loaded.keys():
            self.index_write(k, shutdown=True)
        self._write_master_cache(True)
        self.sessionlock.shutdown()

    def get_fn(self, this_id):
        """ Returns the file name where the data for this object id is saved"""
        if this_id not in self.saved_paths:
            self.saved_paths[this_id] = os.path.join(self.root, "%ixxx" % int(this_id * 0.001), "%i" % this_id, self.dataFileName)
        return self.saved_paths[this_id]

    def get_idxfn(self, this_id):
        """ Returns the file name where the data for this object id is saved"""
        if this_id not in self.saved_idxpaths:
            self.saved_idxpaths[this_id] = os.path.join(self.root, "%ixxx" % int(this_id * 0.001), "%i.index" % this_id)
        return self.saved_idxpaths[this_id]

    def index_load(self, this_id):
        """ load the index file for this object if necessary
            Loads if never loaded or timestamp changed. Creates object if necessary
            Returns True if this object has been changed, False if not
            Raise IOError on access or unpickling error 
            Raise OSError on stat error
            Raise PluginManagerError if the class name is not found"""
        #logger.debug("Loading index %s" % this_id)
        fn = self.get_idxfn(this_id)
        # index timestamp changed
        if self._cache_load_timestamp.get(this_id, 0) != os.stat(fn).st_ctime:
            logger.debug("%s != %s" % (self._cache_load_timestamp.get(this_id, 0), os.stat(fn).st_ctime))
            try:
                with open(fn, 'r') as fobj:
                    cat, cls, cache = pickle_from_file(fobj)[0]
            except Exception as x:
                logger.debug("index_load Exception: %s" % x)
                raise IOError("Error on unpickling: %s %s" %(getName(x), x))
            if this_id in self.objects:
                obj = self.objects[this_id]
                setattr(obj, "_registry_refresh", True)
            else:
                try:
                    obj = self._make_empty_object_(this_id, cat, cls)
                except Exception as err:
                    raise IOError('Failed to Parse information in Index file: %s. Err: %s' % (fn, err))
            this_cache = obj._index_cache
            this_data = this_cache if this_cache else {}
            for k, v in cache.iteritems():
                this_data[k] = v
            #obj.setNodeData(this_data)
            obj._index_cache = cache
            self._cache_load_timestamp[this_id] = os.stat(fn).st_ctime
            self._cached_cat[this_id] = cat
            self._cached_cls[this_id] = cls
            self._cached_obj[this_id] = cache
            return True
        elif this_id not in self.objects:
            self.objects[this_id] = self._make_empty_object_(this_id, self._cached_cat[this_id], self._cached_cls[this_id])
            self.objects[this_id]._index_cache = self._cached_obj[this_id]
            setattr(self.objects[this_id], '_registry_refresh', True)
            return True
        else:
            logger.debug("Doubly loading of object with ID: %s" % this_id)
            logger.debug("Just silently continuing")
        return False

    def index_write(self, this_id, shutdown=False):
        """ write an index file for this object (must be locked).
            Should not raise any Errors,
            shutdown=True causes this to always be written regardless of any checks"""
        logger.debug("Writing index: %s" % this_id)
        obj = self.objects[this_id]
        try:
            ifn = self.get_idxfn(this_id)
            new_idx_cache = self.registry.getIndexCache(stripProxy(obj))
            if not os.path.exists(ifn) or shutdown:
                new_cache = new_idx_cache
                with open(ifn, "w") as this_file:
                    new_index = (obj._category, getName(obj), new_cache)
                    logger.debug("Writing: %s" % str(new_index))
                    pickle_to_file(new_index, this_file)
                self._cached_obj[this_id] = new_cache
                obj._index_cache = {}
            self._cached_obj[this_id] = new_idx_cache
        except IOError as err:
            logger.error("Index saving to '%s' failed: %s %s" % (ifn, getName(err), err))

    def get_index_listing(self):
        """Get dictionary of possible objects in the Repository: True means index is present,
            False if not present
        Raise RepositoryError"""
        try:
            obj_chunks = [d for d in os.listdir(self.root) if d.endswith("xxx") and d[:-3].isdigit()]
        except OSError as err:
            logger.debug("get_index_listing Exception: %s" % err)
            raise RepositoryError(self, "Could not list repository '%s'!" % (self.root))
        objs = {}  # True means index is present, False means index not present
        for c in obj_chunks:
            try:
                listing = os.listdir(os.path.join(self.root, c))
            except OSError as err:
                logger.debug("get_index_listing Exception: %s")
                raise RepositoryError(self, "Could not list repository '%s'!" % (os.path.join(self.root, c)))
            objs.update(dict([(int(l), False) for l in listing if l.isdigit()]))
            for l in listing:
                if l.endswith(".index") and l[:-6].isdigit():
                    this_id = int(l[:-6])
                    if this_id in objs:
                        objs[this_id] = True
                    else:
                        try:
                            rmrf(self.get_idxfn(this_id))
                            logger.warning("Deleted index file without data file: %s" % self.get_idxfn(this_id))
                        except OSError as err:
                            logger.debug("get_index_listing delete Exception: %s" % err)
        return objs

    def _read_master_cache(self):
        """
        read in the master cache to reduce significant I/O over many indexes separately on startup
        """
        try:
            _master_idx = os.path.join(self.root, 'master.idx')
            if os.path.isfile(_master_idx):
                logger.debug("Reading Master index")
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
        except Exception as err:
            Ganga.Utility.logging.log_unknown_exception()
            logger.debug("Master Index corrupt, ignoring it")
            logger.debug("Exception: %s" % err)
            self._clear_stored_cache()

    def _clear_stored_cache(self):
        """
        clear the master cache(s) which have been stored in memory
        """
        for k, v in self._cache_load_timestamp.iteritems():
            self._cache_load_timestamp.pop(k)
        for k, v in self._cached_cat.iteritems():
            self._cached_cat.pop(k)
        for k, v in self._cached_cls.iteritems():
            self._cached_cls.pop(k)
        for k, v in self._cached_obj.iteritems():
            self._cached_obj.pop(k)

    def _write_master_cache(self, shutdown=False):
        """
        write a master index cache once per 300sec
        shutdown=True causes this to be written now
        """
        #logger.info("Updating master index: %s" % self.registry.name)
        try:
            _master_idx = os.path.join(self.root, 'master.idx')
            this_master_cache = []
            if os.path.isfile(_master_idx) and not shutdown:
                if abs(self._master_index_timestamp - os.stat(_master_idx).st_ctime) < 300:
                    return

            items_to_save = self.objects.iteritems()
            #logger.info("Updating Items: %s" % self.objects.keys())
            for k, v in items_to_save:
                try:
                    #logger.info("Examining: %s" % k)
                    if k in self._fully_loaded.keys():
                        # Check and write index first
                        obj = v#self.objects[k]
                        new_index = None
                        if obj is not None:
                            new_index = self.registry.getIndexCache(stripProxy(obj))

                        if new_index is not None:
                            #logger.debug("k: %s" % k)
                            arr_k = [k]
                            if len(self.lock(arr_k)) != 0:
                                self.index_write(k)
                                self.unlock(arr_k)
                                self._cached_obj[k] = new_index

                except Exception as err:
                    logger.debug("Failed to update index: %s on startup/shutdown" % k)
                    logger.debug("Reason: %s" % err)

            #cached_list = []
            iterables = self._cache_load_timestamp.iteritems()
            for k, v in iterables:
                cached_list = []
                cached_list.append(k)
                try:
                    fn = self.get_idxfn(k)
                    time = os.stat(fn).st_ctime
                except OSError as err:
                    logger.debug("_write_master_cache: %s" % err)
                    logger.debug("_cache_load_timestamp: %s" % self._cache_load_timestamp)
                    import errno
                    if err.errno == errno.ENOENT:  # If file is not found
                        time = -1
                    else:
                        raise
                if time > 0:
                    cached_list.append(time)
                    cached_list.append(self._cached_cat[k])
                    cached_list.append(self._cached_cls[k])
                    cached_list.append(self._cached_obj[k])
                    this_master_cache.append(cached_list)

            try:
                with open(_master_idx, 'w') as of:
                    pickle_to_file(this_master_cache, of)
            except IOError as err:
                logger.debug("write_master: %s" % err)
                try:
                    os.remove(os.path.join(self.root, 'master.idx'))
                except OSError as x:
                    Ganga.Utility.logging.log_user_exception(debug=True)
        except Exception as err:
            logger.debug("write_error2: %s" % err)
            Ganga.Utility.logging.log_unknown_exception()

        return

    def updateLocksNow(self):
        """
        Trigger the session locks to all be updated now
        This is useful when the SessionLock is updating either too slowly or has gone to sleep when there are multiple sessions
        """
        self.sessionlock.updateNow()

    def update_index(self, this_id=None, verbose=False, firstRun=False):
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
        logger.debug("Iterating over Items")

        locked_ids = self.sessionlock.locked

        for this_id in objs.keys():
            deleted_ids.discard(this_id)
            # Make sure we do not overwrite older jobs if someone deleted the
            # count file
            if this_id > self.sessionlock.count:
                self.sessionlock.count = this_id + 1
            # Locked IDs can be ignored
            if this_id in locked_ids:
                continue
            # Skip corrupt IDs
            if this_id in self.incomplete_objects:
                continue
            # Now we treat unlocked IDs
            try:
                # if this succeeds, all is well and we are done
                if self.index_load(this_id):
                    changed_ids.append(this_id)
                continue
            except IOError as err:
                logger.debug("IOError: Failed to load index %i: %s" % (this_id, err))
            except OSError as err:
                logger.debug("OSError: Failed to load index %i: %s" % (this_id, err))
            except PluginManagerError as err:
                # Probably should be DEBUG
                logger.debug("PluginManagerError: Failed to load index %i: %s" % (this_id, err))
                # This is a FATAL error - do not try to load the main file, it
                # will fail as well
                summary.append((this_id, err))
                continue

            # this is bad - no or corrupted index but object not loaded yet!
            # Try to load it!
            if not this_id in self.objects:
                try:
                    self.load([this_id])
                    changed_ids.append(this_id)
                    # Write out a new index if the file can be locked
                    if len(self.lock([this_id])) != 0:
                        self.index_write(this_id)
                        self.unlock([this_id])
                except KeyError as err:
                    logger.debug("update Error: %s" % err)
                    # deleted job
                    if this_id in self.objects:
                        self._internal_del__(this_id)
                        changed_ids.append(this_id)
                except Exception as x:
                    ## WE DO NOT CARE what type of error occured here and it can be
                    ## due to corruption so could be one of MANY exception types
                    ## If the job is not accessible this should NOT cause the loading of ganga to fail!
                    ## we can't reasonably write all possible exceptions here!
                    logger.debug("update_index: Failed to load id %i: %s" % (this_id, x))
                    summary.append((this_id, x))
                    
                #finally:
                #    pass

        logger.debug("Iterated over Items")

        # Check deleted files:
        for this_id in deleted_ids:
            self._internal_del__(this_id)
            changed_ids.append(this_id)
        if len(deleted_ids) > 0:
            logger.warning("Registry '%s': Job %s externally deleted." % (self.registry.name, ",".join(map(str, list(deleted_ids)))))

        if len(summary) > 0:
            cnt = {}
            examples = {}
            for this_id, x in summary:
                if this_id in self.known_bad_ids:
                    continue
                cnt[getName(x)] = cnt.get(getName(x), []) + [str(this_id)]
                examples[getName(x)] = str(x)
                self.known_bad_ids.append(this_id)
                # add object to incomplete_objects
                if not this_id in self.incomplete_objects:
                    logger.error("Adding: %s to Incomplete Objects to avoid loading it again in future" % this_id)
                    self.incomplete_objects.append(this_id)

            for exc, ids in cnt.items():
                logger.error("Registry '%s': Failed to load %i jobs (IDs: %s) due to '%s' (first error: %s)" % (self.registry.name, len(ids), ",".join(ids), exc, examples[exc]))

            if self.printed_explanation is False:
                logger.error("If you want to delete the incomplete objects, you can type:\n")
                logger.error("'for i in %s.incomplete_ids(): %s(i).remove()'\n (then press 'Enter' twice)" % (self.registry.name, self.registry.name))
                logger.error("WARNING!!! This will result in corrupt jobs being completely deleted!!!")
                self.printed_explanation = True
        logger.debug("updated index done")

        if len(changed_ids) != 0:
            isShutdown = not firstRun
            self._write_master_cache(shutdown=isShutdown)

        return changed_ids

    def add(self, objs, force_ids=None):
        """ Add the given objects to the repository, forcing the IDs if told to.
        Raise RepositoryError"""

        logger.debug("add")

        if force_ids not in [None, []]:  # assume the ids are already locked by Registry
            if not len(objs) == len(force_ids):
                raise RepositoryError(self, "Internal Error: add with different number of objects and force_ids!")
            ids = force_ids
        else:
            ids = self.sessionlock.make_new_ids(len(objs))

        logger.debug("made ids")

        for i in range(0, len(objs)):
            fn = self.get_fn(ids[i])
            try:
                os.makedirs(os.path.dirname(fn))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise RepositoryError( self, "OSError on mkdir: %s" % (e))
            self._internal_setitem__(ids[i], objs[i])

            # Set subjobs dirty - they will not be flushed if they are not.
            if self.sub_split and self.sub_split in objs[i]._data:
                try:
                    sj_len = len(getattr(objs[i], self.sub_split))
                    if sj_len > 0:
                        for j in range(sj_len):
                            getattr(objs[i], self.sub_split)[j]._dirty = True
                except AttributeError as err:
                    logger.debug("RepoXML add Exception: %s" % err)

        logger.debug("Added")

        return ids

    def _safe_flush_xml(self, this_id):
        """
        Flush XML to disk whilst checking for relavent SubJobXMLList which handles subjobs now
        flush for "this_id" in the self.objects list
        """

        fn = self.get_fn(this_id)
        obj = self.objects[this_id]
        from Ganga.Core.GangaRepository.VStreamer import EmptyGangaObject
        if not isType(obj, EmptyGangaObject):
            split_cache = None

            has_children = (not self.sub_split is None) and\
                    (self.sub_split in obj._data) and hasattr(obj, self.sub_split) and len(getattr(obj, self.sub_split)) > 0

            if has_children:

                logger.debug("has_children")

                if hasattr(getattr(obj, self.sub_split), 'flush'):
                    # I've been read from disk in the new SubJobXMLList format I know how to flush
                    getattr(obj, self.sub_split).flush()
                else:
                    # I have been constructed in this session, I don't know how to flush!
                    if hasattr(getattr(obj, self.sub_split)[0], "_dirty"):
                        split_cache = getattr(obj, self.sub_split)
                        for i in range(len(split_cache)):
                            if not split_cache[i]._dirty:
                                continue
                            sfn = os.path.join(os.path.dirname(fn), str(i), self.dataFileName)
                            if not os.path.exists(os.path.dirname(sfn)):
                                logger.debug("Constructing Folder: %s" % os.path.dirname(sfn))
                                os.makedirs(os.path.dirname(sfn))
                            else:
                                logger.debug("Using Folder: %s" % os.path.dirname(sfn))
                            safe_save(sfn, split_cache[i], self.to_file)
                            split_cache[i]._setFlushed()
                    from Ganga.Core.GangaRepository.SubJobXMLList import SubJobXMLList
                    # Now generate an index file to take advantage of future non-loading goodness
                    tempSubJList = SubJobXMLList(os.path.dirname(fn), self.registry, self.dataFileName, False, parent=obj)
                    ## equivalent to for sj in job.subjobs
                    tempSubJList._setParent(obj)
                    job_dict = {}
                    for sj in getattr(obj, self.sub_split):
                        job_dict[sj.id] = stripProxy(sj)
                    tempSubJList._reset_cachedJobs(job_dict)
                    tempSubJList.flush()
                    del tempSubJList

                safe_save(fn, obj, self.to_file, self.sub_split)
                # clean files not in subjobs anymore... (bug 64041)
                for idn in os.listdir(os.path.dirname(fn)):
                    split_cache = getattr(obj, self.sub_split)
                    if idn.isdigit() and int(idn) >= len(split_cache):
                        rmrf(os.path.join(os.path.dirname(fn), idn))
            else:

                logger.debug("not has_children")

                safe_save(fn, obj, self.to_file, "")
                # clean files leftover from sub_split
                for idn in os.listdir(os.path.dirname(fn)):
                    if idn.isdigit():
                        rmrf(os.path.join(os.path.dirname(fn), idn))
            self.index_write(this_id)
            obj._setFlushed()
        else:
            raise RepositoryError(self, "Cannot flush an Empty object for ID: %s" % this_id)

        if this_id not in self._fully_loaded.keys():
            self._fully_loaded[this_id] = obj

    def flush(self, ids):
        """
        flush the set of "ids" to disk and write the XML representing said objects in self.objects
        """
        logger.debug("Flushing: %s" % ids)

        #import traceback
        #traceback.print_stack()
        for this_id in ids:
            try:
                logger.debug("safe_flush")
                self._safe_flush_xml(this_id)

                self._cache_load_timestamp[this_id] = time.time()
                self._cached_cls[this_id] = getName(self.objects[this_id])
                self._cached_cat[this_id] = self.objects[this_id]._category
                self._cached_obj[this_id] = self.objects[this_id]._index_cache

                try:
                    self.index_write(this_id)
                except:
                    logger.debug("Index write failed")
                    pass

                if this_id not in self._fully_loaded.keys():
                    self._fully_loaded[this_id] = self.objects[this_id]

            except (OSError, IOError, XMLFileError) as x:
                raise RepositoryError(self, "Error of type: %s on flushing id '%s': %s" % (type(x), this_id, x))

    def is_loaded(self, this_id):
        """
        Has "this_id" been loaded from disk already
        Check self.objects for the ._data sttribute
        """
        return (this_id in self.objects) and (self.objects[this_id]._data is not None)

    def count_nodes(self, this_id):
        """
        Count the number of nodes in "this_id" of self.objects"
        Is this used any more?
        """
        node_count = 0
        fn = self.get_fn(this_id)

        ld = os.listdir(os.path.dirname(fn))
        i = 0
        while str(i) in ld:
            sfn = os.path.join(os.path.dirname(fn), str(i), self.dataFileName)
            if os.path.exists(sfn):
                node_count = node_count + 1
            i += 1

        return node_count

    def _actually_loaded(self, this_id):
        """
        Return if "this_id" is in the _full_loaded dictionary of objects loaded from XML by this class
        This is different to is_loaded as it checks for actual read operations
        """
        return this_id in self._fully_loaded.keys()

    def _check_index_cache(self, obj, this_id):
        """
        Checks the index cache of "this_id" against the index cache generated from the "obj"ect
        If there is a problem then the object is unloaded from memory but will not do anything if everything agrees here
        """
        new_idx_cache = self.registry.getIndexCache(stripProxy(obj))
        if new_idx_cache != obj._index_cache:
            logger.debug("NEW: %s" % new_idx_cache)
            logger.debug("OLD: %s" % obj._index_cache)
            # index is wrong! Try to get read access - then we can fix this
            if len(self.lock([this_id])) != 0:
                self.index_write(this_id)
                # self.unlock([this_id])

                old_idx_subset = all((k in new_idx_cache and new_idx_cache[k] == v) for k, v in obj._index_cache.iteritems())
                if not old_idx_subset:
                    # Old index cache isn't subset of new index cache
                    new_idx_subset = all((k in obj._index_cache and obj._index_cache[k] == v) for k, v in new_idx_cache.iteritems())
                else:
                    # Old index cache is subset of new index cache so no need to check
                    new_idx_subset = True

                if not old_idx_subset and not new_idx_subset:
                    logger.warning("Incorrect index cache of '%s' object #%s was corrected!" % (self.registry.name, this_id))
                    logger.debug("old cache: %s\t\tnew cache: %s" % (obj._index_cache, new_idx_cache))
                    self.unlock([this_id])
            else:
                pass
                # if we cannot lock this, the inconsistency is
                # most likely the result of another ganga
                # process modifying the repo

    def _must_actually_load_xml(self, fn, this_id, load_backup, has_children, tmpobj):
        """
        If we must actually load the object from disk then we end up here.
        This replaces the attrs of "objects[this_id]" with the attrs from tmpobj
        If there are children then a SubJobXMLList is created to manage them.
        The fn of the job is passed to the SubbJobXMLList and there is some knowledge of if we should be loading the backup passed as well
        """
        obj = self.objects[this_id]
        for key, val in tmpobj._data.items():
            obj.setSchemaAttribute(key, val)
        for attr_name, attr_val in obj._schema.allItems():
            if attr_name not in tmpobj._data:
                obj.setSchemaAttribute(attr_name, obj._schema.getDefaultValue(attr_name))

        if has_children:
        #    logger.info("Adding children")
            obj.setSchemaAttribute(self.sub_split, SubJobXMLList.SubJobXMLList(os.path.dirname(fn), self.registry, self.dataFileName, load_backup, parent=obj))
        else:
            obj.setSchemaAttribute(self.sub_split, None)

        from Ganga.GPIDev.Base.Objects import do_not_copy
        for node_key, node_val in obj._data.items():
            if isType(node_val, Node):
                if node_key not in do_not_copy:
                    node_val._setParent(obj)

        # Check if index cache; if loaded; was valid:
        if obj._index_cache not in [{}]:
            self._check_index_cache(obj, this_id)

        obj._index_cache = {}

        if this_id not in self._fully_loaded.keys():
            self._fully_loaded[this_id] = obj

    def _actually_load_xml(self, fobj, fn, this_id, load_backup):
        """
        This is the method which will load the job from fn using the fobj using the self.from_file method and _must_actually_load_xml is called to replace the
        self.objects[this_id] with the correct attributes. We also preseve knowledge of if we're being asked to load a backup or not
        """
        #print("ACTUALLY LOAD")

        tmpobj = None

        if (self._load_timestamp.get(this_id, 0) != os.fstat(fobj.fileno()).st_ctime):

            import time
            b4=time.time()
            tmpobj, errs = self.from_file(fobj)
            a4=time.time()
            logger.debug("Loading XML file for ID: %s took %s sec" % (this_id, a4-b4))

            if len(errs) > 0:
                logger.error("#%s Error(s) Loading File: %s" % (len(errs), fobj.name))
                raise InaccessibleObjectError(self, this_id, errs[0])

            has_children = (self.sub_split is not None) and hasattr(tmpobj, self.sub_split) and len(getattr(tmpobj, self.sub_split)) == 0

            if this_id in self.objects:
                self._must_actually_load_xml(fn, this_id, load_backup, has_children, tmpobj)

            else:
                self._internal_setitem__(this_id, tmpobj)

            if hasattr(self.objects[this_id], self.sub_split):
                sub_attr = getattr(self.objects[this_id], self.sub_split)
                if sub_attr is not None and hasattr(sub_attr, '_setParent'):
                    sub_attr._setParent(self.objects[this_id])

            self._load_timestamp[this_id] = os.fstat(fobj.fileno()).st_ctime

        else:
            logger.debug("Didn't Load Job ID: %s" % this_id)

        logger.debug("Finished Loading XML")

    def _open_xml_file(self, fn, this_id, _copy_backup=False):
        """
        This loads the XML for the job "this_id" in self.objects using the file "fn" and knowing whether we want the file or the backup by _copy_backup
        """
        fobj = None

        has_loaded_backup = False

        try:
            if not os.path.isfile(fn) and _copy_backup:
                if os.path.isfile(fn + '~'):
                    logger.warning("XML File: %s missing, recovering from backup, recent changes may have been lost!" % fn)
                    has_loaded_backup = True
                    try:
                        from shutil import copyfile
                        copyfile(fn+'~', fn)
                    except:
                        logger.warning("Error Recovering the backup file! loading of Job may Fail!")
            fobj = open(fn, "r")
        except IOError as x:
            if x.errno == errno.ENOENT:
                # remove index so we do not continue working with wrong information
                try:
                    # remove internal representation
                    self._internal_del__(this_id)
                    rmrf(os.path.dirname(fn) + ".index")
                except OSError as err:
                    logger.debug("load unlink Error: %s" % err)
                    pass
                raise KeyError(this_id)
            else:
                raise RepositoryError(self, "IOError: %s" % x)
        finally:
            try:
                if os.path.isdir(os.path.dirname(fn)):
                    ld = os.listdir(os.path.dirname(fn))
                    if len(ld) == 0:
                        os.rmdir(os.path.dirname(fn))
                        logger.warning("No job index or data found, removing empty directory: %s" % os.path.dirname(fn))
            except Exception as err:
                logger.debug("load error %s" % err)
                pass

        return fobj, has_loaded_backup

    def load(self, ids, load_backup=False):
        """
        Load the following "ids" from disk
        If we want to load the backup files for these ids then use _copy_backup
        Correctly loaded objects are dirty, Objects loaded from backups for whatever reason are marked dirty
        """
        #print("load: %s " % ids)
        #import traceback
        #traceback.print_stack()
        #print("\n")

        logger.debug("Loading Repo object(s): %s" % ids)

        for this_id in ids:

            if this_id in self.incomplete_objects:
                raise RepositoryError(self.repo, "Trying to re-load a corrupt repository id: %s" % this_id)

            fn = self.get_fn(this_id)
            if load_backup:
                has_loaded_backup = True
                fn = fn + "~"
            else:
                has_loaded_backup = False

            fobj = None

            try:
                fobj, has_loaded_backup2 = self._open_xml_file(fn, this_id, _copy_backup=True)
                if has_loaded_backup2:
                    has_loaded_backup = has_loaded_backup2
            except Exception as err:
                logger.debug("XML load: Failed to load XML file: %s" % fn)
                logger.debug("Error was:\n%s" % err)
                logger.error("Adding id: %s to Corrupt IDs will not attempt to re-load this session" % this_id)
                self.incomplete_objects.append(this_id)
                raise

            try:
                self._actually_load_xml(fobj, fn, this_id, load_backup)
            except RepositoryError as err:
                logger.debug("Repo Exception: %s" % err)
                logger.error("Adding id: %s to Corrupt IDs will not attempt to re-load this session" % this_id)
                self.incomplete_objects.append(this_id)
                raise

            except Exception as err:

                should_continue = self._handle_load_exception(err, fn, this_id, load_backup)

                if should_continue is True:
                    has_loaded_backup = True
                    continue
                else:
                    logger.error("Adding id: %s to Corrupt IDs will not attempt to re-load this session" % this_id)
                    self.incomplete_objects.append(this_id)
                    raise

            finally:
                fobj.close()

            if has_loaded_backup:
                self.objects[this_id]._setDirty()
            else:
                self.objects[this_id]._setFlushed()

        logger.debug("Finished 'load'-ing of: %s" % ids)


    def _handle_load_exception(self, err, fn, this_id, load_backup):
        """
        This method does a lot of the handling of an exception thrown from the load method
        We will return True/False here, True if the error can be correctly caught and False if this is terminal and we couldn't load the object
        """
        if isType(err, XMLFileError):
            logger.error("XML File failed to load for Job id: %s" % this_id)
            logger.error("Actual Error was:\n%s" % err)

        if load_backup:
            logger.debug("Could not load backup object #%s: %s" % (this_id, err))
            raise InaccessibleObjectError(self, this_id, err)

        logger.debug("Could not load object #%s: %s" % (this_id, err))

        # try loading backup
        try:
            self.load([this_id], load_backup=True)
            logger.warning("Object '%s' #%s loaded from backup file - recent changes may be lost." % (self.registry.name, this_id))
            return True
        except Exception as err2:
            logger.debug("Exception when loading backup: %s" % err2 )
        #finally:
        #    pass

        logger.error("XML File failed to load for Job id: %s" % this_id)
        logger.error("Actual Error was:\n%s" % err2)

        # add object to incomplete_objects
        if not this_id in self.incomplete_objects:
            logger.error("Loading: %s into incomplete_objects to avoid loading it again in future" % this_id)
            self.incomplete_objects.append(this_id)
            # remove index so we do not continue working with wrong
            # information
            rmrf(os.path.dirname(fn) + ".index")
            raise InaccessibleObjectError(self, this_id, err)

        return False

    def delete(self, ids):
        """
        This is the method to 'delete' an object from disk, it's written in python and starts with the indexes first
        """
        for this_id in ids:
            # First remove the index, so that it is gone if we later have a
            # KeyError
            fn = self.get_fn(this_id)
            try:
                rmrf(os.path.dirname(fn) + ".index")
            except OSError as err:
                logger.debug("Delete Error: %s" % err)
            self._internal_del__(this_id)
            rmrf(os.path.dirname(fn))
            if this_id in self._fully_loaded.keys():
                del self._fully_loaded[this_id]
            if this_id in self.objects:
                del self.objects[this_id]

    def lock(self, ids):
        """
        Request a session lock for the following ids
        """
        return self.sessionlock.lock_ids(ids)

    def unlock(self, ids):
        """
        Unlock (release file locks of) the following ids
        """
        released_ids = self.sessionlock.release_ids(ids)
        if len(released_ids) < len(ids):
            logger.error("The write locks of some objects could not be released!")

    def get_lock_session(self, this_id):
        """get_lock_session(id)
        Tries to determine the session that holds the lock on id for information purposes, and return an informative string.
        Returns None on failure
        """
        return self.sessionlock.get_lock_session(this_id)

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

    def updateIndexCache(self, obj):
        """
        This method should be obsoleted fairly soon as it's no longer needed. We now handle caches correctly
        """
        pass

