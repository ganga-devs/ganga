# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

from GangaCore.Core.GangaRepository import GangaRepository, RepositoryError, InaccessibleObjectError
from GangaCore.Utility.Plugin import PluginManagerError
import os
import os.path
import time
import errno
import copy
import threading

from GangaCore import GANGA_SWAN_INTEGRATION

from GangaCore.Core.GangaRepository.SessionLock import SessionLockManager, dry_run_unix_locks
from GangaCore.Core.GangaRepository.FixedLock import FixedLockManager

import GangaCore.Utility.logging

from GangaCore.Core.GangaRepository.PickleStreamer import to_file as pickle_to_file
from GangaCore.Core.GangaRepository.PickleStreamer import from_file as pickle_from_file

from GangaCore.Core.GangaRepository.VStreamer import to_file as xml_to_file
from GangaCore.Core.GangaRepository.VStreamer import from_file as xml_from_file
from GangaCore.Core.GangaRepository.VStreamer import XMLFileError

from GangaCore.GPIDev.Base.Objects import Node
from GangaCore.Core.GangaRepository.SubJobXMLList import SubJobXMLList

from GangaCore.GPIDev.Base.Proxy import isType, stripProxy, getName

from GangaCore.Utility.Config import getConfig

logger = GangaCore.Utility.logging.getLogger()

save_all_history = False

def check_app_hash(obj):
    """Writes a file safely, raises IOError on error
    Args:
        obj (GangaObject): This is an object which has a prepared application
    """

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
        if not hashable_app.calc_hash(True):
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
    """Try to save the XML for this object in as safe a way as possible
    Args:
        fn (str): This is the name of the file we are to save the object to
        _obj (GangaObject): This is the object which we want to save to the file
        to_file (str): This is the method we want to use to save the to the file
        ignore_subs (str): This is the name(s) of the attribute of _obj we want to ignore in writing to disk
    """

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
    Args:
        count (int): This function calls itself recursively 3 times then gives up, this increments on each call
    """

    if count != 0:
        logger.debug("Trying again to remove: %s" % name)
        if count == 3:
            logger.error("Tried 3 times to remove file/folder: %s" % name)
            from GangaCore.Core.exceptions import GangaException
            raise GangaException("Failed to remove file/folder: %s" % name)

    if os.path.isdir(name):

        try:
            remove_name = name
            if not remove_name.endswith('__to_be_deleted'):
                remove_name += '_%s__to_be_deleted_' % time.time()
                os.rename(name, remove_name)
                #logger.debug("Move completed")
        except OSError as err:
            if err.errno != errno.ENOENT:
                logger.debug("rmrf Err: %s" % err)
                logger.debug("name: %s" % name)
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
        Args:
            Registry (Registry): This is the registry which manages this Repo
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
            raise RepositoryError(self, "Unknown Repository type: %s" % self.registry.type)
        if getConfig('Configuration')['lockingStrategy'] == "UNIX":
            # First test the UNIX locks are working as expected
            try:
                dry_run_unix_locks(self.lockroot)
            except Exception as err:
                # Locking has not worked, lets raise an error
                logger.error("Error: %s" % err)
                msg="\n\nUnable to launch due to underlying filesystem not working with unix locks."
                msg+="Please try launching again with [Configuration]lockingStrategy=FIXED to start Ganga without multiple session support."
                raise RepositoryError(self, msg)

            # Locks passed test so lets continue
            self.sessionlock = SessionLockManager(self, self.lockroot, self.registry.name)
        elif getConfig('Configuration')['lockingStrategy'] == "FIXED":
            self.sessionlock = FixedLockManager(self, self.lockroot, self.registry.name)
        else:
            raise RepositoryError(self, "Unable to launch due to unknown file-locking Strategy: \"%s\"" % getConfig('Configuration')['lockingStrategy'])
        self.sessionlock.startup()
        # Load the list of files, this time be verbose and print out a summary
        # of errors
        self.update_index(True, True)
        logger.debug("GangaRepositoryLocal Finished Startup")

    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry
        Raise RepositoryError
        Write an index file for all new objects in memory and master index file of indexes"""
        from GangaCore.Utility.logging import getLogger
        logger = getLogger()
        logger.debug("Shutting Down GangaRepositoryLocal: %s" % self.registry.name)
        for k in self._fully_loaded:
            try:
                self.index_write(k, True)
            except Exception as err:
                logger.error("Warning: problem writing index object with id %s" % k)
        try:
            self._write_master_cache(True)
        except Exception as err:
            logger.warning("Warning: Failed to write master index due to: %s" % err)
        self.sessionlock.shutdown()

    def get_fn(self, this_id):
        """ Returns the file name where the data for this object id is saved
        Args:
            this_id (int): This is the object id we want the XML filename for
        """
        if this_id not in self.saved_paths:
            self.saved_paths[this_id] = os.path.join(self.root, "%ixxx" % int(this_id * 0.001), "%i" % this_id, self.dataFileName)
        return self.saved_paths[this_id]

    def get_idxfn(self, this_id):
        """ Returns the file name where the data for this object id is saved
        Args:
            this_id (int): This is the object id we want the index filename for
        """
        if this_id not in self.saved_idxpaths:
            self.saved_idxpaths[this_id] = os.path.join(self.root, "%ixxx" % int(this_id * 0.001), "%i.index" % this_id)
        return self.saved_idxpaths[this_id]

    def index_load(self, this_id):
        """ load the index file for this object if necessary
            Loads if never loaded or timestamp changed. Creates object if necessary
            Returns True if this object has been changed, False if not
            Raise IOError on access or unpickling error 
            Raise OSError on stat error
            Raise PluginManagerError if the class name is not found
        Args:
            this_id (int): This is the id for which we want to load the index file from disk
        """
        #logger.debug("Loading index %s" % this_id)
        fn = self.get_idxfn(this_id)
        # index timestamp changed
        fn_ctime = os.stat(fn).st_ctime
        cache_time = self._cache_load_timestamp.get(this_id, 0)
        if cache_time != fn_ctime:
            logger.debug("%s != %s" % (cache_time, fn_ctime))
            try:
                with open(fn, 'rb') as fobj:
                    cat, cls, cache = pickle_from_file(fobj)[0]
            except EOFError:
                pass
            except Exception as x:
                logger.warning("index_load Exception: %s" % x)
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
            for k, v in cache.items():
                this_data[k] = v
            #obj.setNodeData(this_data)
            obj._index_cache = cache
            self._cache_load_timestamp[this_id] = fn_ctime
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
        Args:
            this_id (int): This is the index for which we want to write the index to disk
            shutdown (bool): True causes this to always be written regardless of any checks"""
        if this_id in self.incomplete_objects:
            return
        logger.debug("Writing index: %s" % this_id)
        obj = self.objects[this_id]
        try:
            ifn = self.get_idxfn(this_id)
            new_idx_cache = self.registry.getIndexCache(stripProxy(obj))
            if not os.path.exists(ifn) or shutdown:
                new_cache = new_idx_cache
                with open(ifn, "wb") as this_file:
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
            if not os.path.exists(self.root):
                os.makedirs(self.root)
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
                with open(_master_idx, 'rb') as input_f:
                    this_master_cache = pickle_from_file(input_f)[0]
                for this_cache in this_master_cache:
                    if this_cache[1] >= 0:
                        this_id = this_cache[0]
                        self._cache_load_timestamp[this_id] = this_cache[1]
                        self._cached_cat[this_id] = this_cache[2]
                        self._cached_cls[this_id] = this_cache[3]
                        self._cached_obj[this_id] = this_cache[4]
            else:
                logger.debug("Not Reading Master Index")
        except Exception as err:
            GangaCore.Utility.logging.log_unknown_exception()
            logger.debug("Master Index corrupt, ignoring it")
            logger.debug("Exception: %s" % err)
            self._clear_stored_cache()
        finally:
            rmrf(os.path.join(self.root, 'master.idx'))

    def _clear_stored_cache(self):
        """
        clear the master cache(s) which have been stored in memory
        """
        for k, v in self._cache_load_timestamp.items():
            self._cache_load_timestamp.pop(k)
        for k, v in self._cached_cat.items():
            self._cached_cat.pop(k)
        for k, v in self._cached_cls.items():
            self._cached_cls.pop(k)
        for k, v in self._cached_obj.items():
            self._cached_obj.pop(k)

    def _write_master_cache(self, shutdown=False):
        """
        write a master index cache once per 300sec
        Args:
            shutdown (boool): True causes this to be written now
        """
        try:
            _master_idx = os.path.join(self.root, 'master.idx')
            this_master_cache = []
            if os.path.isfile(_master_idx) and not shutdown:
                if abs(self._master_index_timestamp - os.stat(_master_idx).st_ctime) < 300:
                    return

            items_to_save = iter(self.objects.items())
            for k, v in items_to_save:
                if k in self.incomplete_objects:
                    continue
                try:
                    if k in self._fully_loaded:
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

            iterables = iter(self._cache_load_timestamp.items())
            for k, v in iterables:
                if k in self.incomplete_objects:
                    continue
                cached_list = []
                cached_list.append(k)
                try:
                    fn = self.get_idxfn(k)
                    if os.path.isfile(fn):
                        time = os.stat(fn).st_ctime
                    else:
                        time = -1
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
                with open(_master_idx, 'wb') as of:
                    pickle_to_file(this_master_cache, of)
            except IOError as err:
                logger.debug("write_master: %s" % err)
                try:
                    os.remove(os.path.join(self.root, 'master.idx'))
                except OSError as x:
                    GangaCore.Utility.logging.log_user_exception(True)
        except Exception as err:
            logger.debug("write_error2: %s" % err)
            GangaCore.Utility.logging.log_unknown_exception()

        return

    def updateLocksNow(self):
        """
        Trigger the session locks to all be updated now
        This is useful when the SessionLock is updating either too slowly or has gone to sleep when there are multiple sessions
        """
        self.sessionlock.updateNow()

    def update_index(self, this_id=None, verbose=False, firstRun=False):
        """ Update the list of available objects
        Raise RepositoryError
        TODO avoid updating objects which haven't changed as this causes un-needed I/O
        Args:
            this_id (int): This is the id we want to explicitly check the index on disk for
            verbose (bool): Should we be verbose
            firstRun (bool): If this is the call from the Repo startup then load the master index for perfomance boost
        """
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

        for this_id in objs:
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
                    logger.debug("Loading disk based Object: %s from %s as indexes were missing" % (this_id, self.registry.name))
                    self.load([this_id])
                    changed_ids.append(this_id)
                    # Write out a new index if the file can be locked
                    if len(self.lock([this_id])) != 0:
                        if this_id not in self.incomplete_objects:
                            # If object is loaded mark it dirty so next flush will regenerate XML,
                            # otherwise just go about fixing it
                            if not self.isObjectLoaded(self.objects[this_id]):
                                self.index_write(this_id)
                            else:
                                self.objects[this_id]._setDirty()
                        #self.unlock([this_id])
                except KeyError as err:
                    logger.debug("update Error: %s" % err)
                    # deleted job
                    if this_id in self.objects:
                        self._internal_del__(this_id)
                        changed_ids.append(this_id)
                except (InaccessibleObjectError, ) as x:
                    logger.debug("update_index: Failed to load id %i: %s" % (this_id, x))
                    summary.append((this_id, x))

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
            self._write_master_cache(isShutdown)

        return changed_ids

    def add(self, objs, force_ids=None):
        """ Add the given objects to the repository, forcing the IDs if told to.
        Raise RepositoryError
        Args:
            objs (list): GangaObject-s which we want to add to the Repo
            force_ids (list, None): IDs to assign to object, None for auto-assign
        """

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
            if self.sub_split and hasattr(objs[i], self.sub_split):
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
        Args:
            this_id (int): This is the id of the object we want to flush to disk
        """

        fn = self.get_fn(this_id)
        obj = self.objects[this_id]
        from GangaCore.Core.GangaRepository.VStreamer import EmptyGangaObject
        if not isType(obj, EmptyGangaObject):
            split_cache = None

            has_children = getattr(obj, self.sub_split, False)

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
                    # Now generate an index file to take advantage of future non-loading goodness
                    tempSubJList = SubJobXMLList(os.path.dirname(fn), self.registry, self.dataFileName, False, obj)
                    ## equivalent to for sj in job.subjobs
                    tempSubJList._setParent(obj)
                    job_dict = {}
                    for sj in getattr(obj, self.sub_split):
                        job_dict[sj.id] = stripProxy(sj)
                    tempSubJList._reset_cachedJobs(job_dict)
                    tempSubJList.flush(ignore_disk=True)
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
            if this_id not in self.incomplete_objects:
                self.index_write(this_id)
        else:
            raise RepositoryError(self, "Cannot flush an Empty object for ID: %s" % this_id)

        if this_id not in self._fully_loaded:
            self._fully_loaded[this_id] = obj

    def flush(self, ids):
        """
        flush the set of "ids" to disk and write the XML representing said objects in self.objects
        NB: This adds the given objects corresponding to ids to the _fully_loaded dict
        Args:
            ids (list): List of integers, used as keys to objects in the self.objects dict
        """
        logger.debug("Flushing: %s" % ids)

        #import traceback
        #traceback.print_stack()
        for this_id in ids:
            if this_id in self.incomplete_objects:
                logger.debug("Should NEVER re-flush an incomplete object, it's now 'bad' respect this!")
                continue
            try:
                logger.debug("safe_flush: %s" % this_id)
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

                if this_id not in self._fully_loaded:
                    self._fully_loaded[this_id] = self.objects[this_id]

                subobj_attr = getattr(self.objects[this_id], self.sub_split, None)
                sub_attr_dirty = getattr(subobj_attr, '_dirty', False)
                if sub_attr_dirty:
                    if hasattr(subobj_attr, 'flush'):
                        subobj_attr.flush()

                self.objects[this_id]._setFlushed()

            except (OSError, IOError, XMLFileError) as x:
                raise RepositoryError(self, "Error of type: %s on flushing id '%s': %s" % (type(x), this_id, x))

    def _check_index_cache(self, obj, this_id):
        """
        Checks the index cache of "this_id" against the index cache generated from the "obj"ect
        If there is a problem then the object is unloaded from memory but will not do anything if everything agrees here
        TODO CHECK IF THIS IS VALID GIVEN WE DYNAMICALLY GENERATE INDEX FOR LOADED OBJECTS
        Args:
            obj (GangaObject): This is the object which we've loaded from disk
            this_id (int): This is the object id which is the objects key in the objects dict
        """
        new_idx_cache = self.registry.getIndexCache(stripProxy(obj))
        if new_idx_cache != obj._index_cache:
            logger.debug("NEW: %s" % new_idx_cache)
            logger.debug("OLD: %s" % obj._index_cache)
            # index is wrong! Try to get read access - then we can fix this
            if len(self.lock([this_id])) != 0:
                if this_id not in self.incomplete_objects:
                    # Mark as dirty if loaded, otherwise load and fix
                    if not self.isObjectLoaded(self.objects[this_id]):
                        self.index_write(this_id)
                    else:
                        self.objects[this_id]._setDirty()
                # self.unlock([this_id])

                old_idx_subset = all((k in new_idx_cache and new_idx_cache[k] == v) for k, v in obj._index_cache.items())
                if not old_idx_subset:
                    # Old index cache isn't subset of new index cache
                    new_idx_subset = all((k in obj._index_cache and obj._index_cache[k] == v) for k, v in new_idx_cache.items())
                else:
                    # Old index cache is subset of new index cache so no need to check
                    new_idx_subset = True

                if not old_idx_subset and not new_idx_subset:
                    if not GANGA_SWAN_INTEGRATION:
                        logger.warning("Incorrect index cache of '%s' object #%s was corrected!" % (self.registry.name, this_id))
                    logger.debug("old cache: %s\t\tnew cache: %s" % (obj._index_cache, new_idx_cache))
                    self.unlock([this_id])
            else:
                pass
                # if we cannot lock this, the inconsistency is
                # most likely the result of another ganga
                # process modifying the repo

    def _parse_xml(self, fn, this_id, load_backup, has_children, tmpobj):
        """
        If we must actually load the object from disk then we end up here.
        This replaces the attrs of "objects[this_id]" with the attrs from tmpobj
        If there are children then a SubJobXMLList is created to manage them.
        The fn of the job is passed to the SubbJobXMLList and there is some knowledge of if we should be loading the backup passed as well
        Args:
            fn (str): This is the path to the data file for this object in the XML
            this_id (int): This is the integer key of the object in the self.objects dict
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' XML file
            has_children (bool): This contains the result of the decision as to whether this object actually has children
            tmpobj (GangaObject): This contains the object which has been read in from the fn file
        """

        # If this_id is not in the objects add the object we got from reading the XML
        need_to_copy = True
        if this_id not in self.objects:
            self.objects[this_id] = tmpobj
            need_to_copy = False

        obj = self.objects[this_id]

        # If the object was already in the objects (i.e. cache object, replace the schema content wilst avoiding R/O checks and such
        # The end goal is to keep the object at this_id the same object in memory but to make it closer to tmpobj.
        # TODO investigate changing this to copyFrom
        # The temp object is from disk so all contents have correctly passed through sanitising via setattr at least once by now so this is safe
        if need_to_copy:
            for key, val in tmpobj._data.items():
                obj.setSchemaAttribute(key, val)
            for attr_name, attr_val in obj._schema.allItems():
                if attr_name not in tmpobj._data:
                    obj.setSchemaAttribute(attr_name, obj._schema.getDefaultValue(attr_name))

        if has_children:
            logger.debug("Adding children")
            # NB Keep be a SetSchemaAttribute to bypass the list manipulation which will put this into a list in some cases 
            obj.setSchemaAttribute(self.sub_split, SubJobXMLList(os.path.dirname(fn), self.registry, self.dataFileName, load_backup, obj))
        else:
            if obj._schema.hasAttribute(self.sub_split):
                # Infinite loop if we use setattr btw
                def_val = obj._schema.getDefaultValue(self.sub_split)
                if def_val == []:
                    from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
                    def_val = GangaList()
                obj.setSchemaAttribute(self.sub_split, def_val)

        from GangaCore.GPIDev.Base.Objects import do_not_copy
        for node_key, node_val in obj._data.items():
            if isType(node_val, Node):
                if node_key not in do_not_copy:
                    node_val._setParent(obj)

        # Check if index cache; if loaded; was valid:
        if obj._index_cache not in [{}]:
            self._check_index_cache(obj, this_id)

        obj._index_cache = {}

        if this_id not in self._fully_loaded:
            self._fully_loaded[this_id] = obj

    def _load_xml_from_obj(self, fobj, fn, this_id, load_backup):
        """
        This is the method which will load the job from fn using the fobj using the self.from_file method and _parse_xml is called to replace the
        self.objects[this_id] with the correct attributes. We also preseve knowledge of if we're being asked to load a backup or not
        Args:
            fobj (file handler): This is the file handler for the fn
            fn (str): fn This is the name of the file which contains the XML data
            this_id (int): This is the key of the object in the objects dict where the output will be stored
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' XML file
        """

        b4=time.time()
        tmpobj, errs = self.from_file(fobj)
        a4=time.time()
        logger.debug("Loading XML file for ID: %s took %s sec" % (this_id, a4-b4))

        if len(errs) > 0:
            logger.error("#%s Error(s) Loading File: %s" % (len(errs), fobj.name))
            for err in errs:
                logger.error("err: %s" % err)
            raise InaccessibleObjectError(self, this_id, errs[0])

        logger.debug("Checking children: %s" % str(this_id))
	#logger.debug("Checking in: %s" % os.path.dirname(fn))
	#logger.debug("found: %s" % os.listdir(os.path.dirname(fn)))

        has_children = SubJobXMLList.checkJobHasChildren(os.path.dirname(fn), self.dataFileName)

        logger.debug("Found children: %s" % str(has_children))

        self._parse_xml(fn, this_id, load_backup, has_children, tmpobj)

        if hasattr(self.objects[this_id], self.sub_split):
            sub_attr = getattr(self.objects[this_id], self.sub_split)
            if sub_attr is not None and hasattr(sub_attr, '_setParent'):
                sub_attr._setParent(self.objects[this_id])

        self._load_timestamp[this_id] = os.fstat(fobj.fileno()).st_ctime

        logger.debug("Finished Loading XML")

    def _open_xml_file(self, fn, this_id, _copy_backup=False):
        """
        This loads the XML for the job "this_id" in self.objects using the file "fn" and knowing whether we want the file or the backup by _copy_backup
        Args:
            fn (str): This is the full XML filename for the given id
            this_id (int): This is the key for the object in the objects dict
            _copy_backup (bool): Should we use the backup file 'data~' (True) or the 'data' file (False)
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
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' XML file
        """
        #print("load: %s " % ids)
        #import traceback
        #traceback.print_stack()
        #print("\n")

        logger.debug("Loading Repo object(s): %s" % ids)

        for this_id in ids:

            if this_id in self.incomplete_objects:
                raise RepositoryError(self, "Trying to re-load a corrupt repository id: %s" % this_id)

            fn = self.get_fn(this_id)
            if load_backup:
                has_loaded_backup = True
                fn = fn + "~"
            else:
                has_loaded_backup = False

            try:
                fobj, has_loaded_backup2 = self._open_xml_file(fn, this_id, True)
                if has_loaded_backup2:
                    has_loaded_backup = has_loaded_backup2
            except Exception as err:
                logger.debug("XML load: Failed to load XML file: %s" % fn)
                logger.debug("Error was:\n%s" % err)
                logger.error("Adding id: %s to Corrupt IDs will not attempt to re-load this session" % this_id)
                self.incomplete_objects.append(this_id)
                raise

            try:
                self._load_xml_from_obj(fobj, fn, this_id, load_backup)
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

            subobj_attr = getattr(self.objects[this_id], self.sub_split, None)
            sub_attr_dirty = getattr(subobj_attr, '_dirty', False)

            if has_loaded_backup:
                self.objects[this_id]._setDirty()
            else:
                self.objects[this_id]._setFlushed()

            if sub_attr_dirty:
                getattr(self.objects[this_id], self.sub_split)._setDirty()

        logger.debug("Finished 'load'-ing of: %s" % ids)


    def _handle_load_exception(self, err, fn, this_id, load_backup):
        """
        This method does a lot of the handling of an exception thrown from the load method
        We will return True/False here, True if the error can be correctly caught and False if this is terminal and we couldn't load the object
        Args:
            err (exception): This is the original exception loading the XML data from disk
            fn (str): This is the filename which was used to load the file from disk
            this_id (int): This is the key of the object in the objects dict
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' XML file
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
            self.load([this_id], True)
            logger.warning("Object '%s' #%s loaded from backup file - recent changes may be lost." % (self.registry.name, this_id))
            return True
        except Exception as err2:
            logger.debug("Exception when loading backup: %s" % err2 )

        logger.error("XML File failed to load for Job id: %s" % this_id)
        logger.error("Actual Error was:\n%s" % err)

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
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
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
            if this_id in self._fully_loaded:
                del self._fully_loaded[this_id]
            if this_id in self.objects:
                del self.objects[this_id]

    def lock(self, ids):
        """
        Request a session lock for the following ids
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        return self.sessionlock.lock_ids(ids)

    def unlock(self, ids):
        """
        Unlock (release file locks of) the following ids
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        released_ids = self.sessionlock.release_ids(ids)
        if len(released_ids) < len(ids):
            logger.error("The write locks of some objects could not be released!")

    def get_lock_session(self, this_id):
        """get_lock_session(id)
        Tries to determine the session that holds the lock on id for information purposes, and return an informative string.
        Returns None on failure
        Args:
            this_id (int): Get the id of the session which has a lock on the object with this id
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
        try:
            rmrf(self.root)
        except Exception as err:
           logger.error("Failed to correctly clean repository due to: %s" % err)
        self.startup()

    def isObjectLoaded(self, obj):
        """
        This will return a true false if an object has been fully loaded into memory
        Args:
            obj (GangaObject): The object we want to know if it was loaded into memory
        """
        try:
            _id = next(id_ for id_, o in self._fully_loaded.items() if o is obj)
            return True
        except StopIteration:
            return False

