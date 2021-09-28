# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

import os
import copy
import json
import time
import docker
import pymongo

from GangaCore.Utility import logging
from GangaCore import GANGA_SWAN_INTEGRATION
from GangaCore.GPIDev.Base.Objects import Node
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Plugin import PluginManagerError, allPlugins
from GangaCore.GPIDev.Base.Proxy import getName, isType, stripProxy
from GangaCore.Core.GangaRepository.SessionLock import SessionLockManager, dry_run_unix_locks
from GangaCore.Core.GangaRepository.FixedLock import FixedLockManager
from GangaCore.Core.GangaRepository.SubJobJsonList import SubJobJsonList
from GangaCore.Core.GangaRepository.container_controllers import (
    native_handler,
    docker_handler,
    udocker_handler,
    singularity_handler,
    get_database_config
)

from GangaCore.Core.GangaRepository import (
    GangaRepository,
    RepositoryError,
    InaccessibleObjectError,
)

from GangaCore.Core.GangaRepository.DStreamer import (
    EmptyGangaObject,
    object_to_database,
    object_from_database,
    index_to_database,
    index_from_database,
    DatabaseError,
)

from GangaCore.Utility.Decorators import repeat_while_none

# Simple Patch to avoid SubJobJsonList not found in internal error
allPlugins.add(SubJobJsonList, "internal", "SubJobJsonList")

logger = logging.getLogger()

save_all_history = False

controller_map = {
    "native": native_handler,
    "docker": docker_handler,
    "udocker": udocker_handler,
    "singularity": singularity_handler,
}


def check_app_hash(obj):
    """Writes a file safely, raises IOError on error
    Args:
        obj (GangaObject): This is an object which has a prepared application
    """

    isVerifiableApp = False
    isVerifiableAna = False

    if hasattr(obj, "application"):
        if hasattr(obj.application, "hash"):
            if obj.application.hash is not None:
                isVerifiableApp = True
    elif hasattr(obj, "analysis"):
        if hasattr(obj.analysis, "application"):
            if hasattr(obj.analysis.application, "hash"):
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
                logger.warning(
                    "Protected attribute(s) of %s application (associated with %s #%s) changed!"
                    % (getName(hashable_app), getName(obj), obj._registry_id)
                )
            except AttributeError as err:
                logger.warning(
                    "Protected attribute(s) of %s application (associated with %s) changed!!!!"
                    % (getName(hashable_app), getName(obj))
                )
                logger.warning("%s" % err)
            jobObj = stripProxy(hashable_app).getJobObject()
            if jobObj is not None:
                logger.warning("Job: %s is now possibly corrupt!" %
                               jobObj.getFQID("."))
            logger.warning(
                "If you knowingly circumvented the protection, ignore this message (and, optionally,"
            )
            logger.warning(
                "re-prepare() the application). Otherwise, please file a bug report at:"
            )
            logger.warning("https://github.com/ganga-devs/ganga/issues/")


# TODO: If ServerSelectionTimeoutError: forecully close ganga
def safe_save(_object, conn, master, ignore_subs=[]):
    """Try to save the Json for this object in as safe a way as possible
    Args:
        _object (GangaObject): Object to be stored in database
        conn (pymongo): Connection to the doc where the object will be stored
        master (int): Index Id of parent/master of _object
        ignore_subs (list): Attrs of object to be ignored
    """
    obj = stripProxy(_object)
    check_app_hash(obj)
    confirmation = object_to_database(
        j=obj, document=conn, master=master, ignore_subs=ignore_subs
    )
    if confirmation is None:
        raise RepositoryError(
            f"The object with obj_id {id} could not be saved into the database"
        )


class GangaRepositoryLocal(GangaRepository):

    """GangaRepository Local"""

    def __init__(self, registry):
        """
        Initialize a Repository from within a Registry and keep a reference to the Registry which 'owns' it
        Args:
            Registry (Registry): This is the registry which manages this Repo
        """
        super(GangaRepositoryLocal, self).__init__(registry)
        self._fully_loaded = {}
        self.gangadir = os.path.expanduser(getConfig("Configuration")["gangadir"])
        self.dataFileName = "data"
        self.sub_split = "subjobs"
        self._cache_load_timestamp = {}
        self.printed_explanation = False
        self.lockroot = os.path.join(self.gangadir, "sessions")


    def startup(self):
        """ Starts a repository and reads in a directory structure.
        Raise RepositoryError"""

        self._cached_obj = {}
        self.known_bad_ids = []
        self._load_timestamp = {}
        self.container_controller = None
        self.to_database = object_to_database
        self.from_database = object_from_database
        # self.database_config = getConfig("DatabaseConfiguration")
        self.database_config = get_database_config(self.gangadir)
        self.db_name = self.database_config["dbname"]

        if getConfig('Configuration')['lockingStrategy'] == "UNIX":
            # First test the UNIX locks are working as expected
            try:
                dry_run_unix_locks(self.lockroot)
            except Exception as err:
                # Locking has not worked, lets raise an error
                logger.error("Error: %s" % err)
                msg = "\n\nUnable to launch due to underlying filesystem not working with unix locks."
                msg += "Please try launching again with [Configuration]lockingStrategy=FIXED to start Ganga without multiple session support."
                raise RepositoryError(self, msg)

            # Locks passed test so lets continue
            self.sessionlock = SessionLockManager(
                self, self.lockroot, self.registry.name)
        elif getConfig('Configuration')['lockingStrategy'] == "FIXED":
            self.sessionlock = FixedLockManager(
                self, self.lockroot, self.registry.name)
        else:
            raise RepositoryError(self, "Unable to launch due to unknown file-locking Strategy: \"%s\"" %
                                  getConfig('Configuration')['lockingStrategy'])

        try:
            self.start_database()
        except Exception as err:
            # database is not responsive, lets raise an error
            logger.error("Error: %s" % err)
            msg = "Unable to reach the database server."
            msg += "Please contanct the developers"
            raise Exception(err, msg)
        self.sessionlock.startup()

        # FIXME: Add index updating here
        self.update_index(True, True, True)
        logger.debug("GangaRepositoryLocal Finished Startup")

    # TODO: catch pymongo.errors.ServerSelectionTimeoutError
    # This shows that the port is already used
    def start_database(self):
        """Start the mongodb with the prefered back_end
        """
        PORT = self.database_config["port"]
        HOST = self.database_config["host"]
        connection_string = f"mongodb://{HOST}:{PORT}/"
        client = pymongo.MongoClient(
        connection_string, serverSelectionTimeoutMS=10000)
        self.connection = client[self.db_name]

        self.container_controller = controller_map[self.database_config["controller"]]
        self.container_controller(
            database_config=self.database_config, action="start", gangadir=self.gangadir)

    def shutdown(self, kill=False):
        """Shutdown the repository. Flushing is done by the Registry
        Raise RepositoryError
        Write an index file for all new objects in memory and master index file of indexes"""
        logger.debug("Shutting Down GangaRepositoryLocal: %s" %
                     self.registry.name)
        try:
            self._write_master_cache()
        except Exception as err:
            logger.warning(
                "Warning: Failed to write master index due to: %s" % err)
        other_sessions = self.get_other_sessions()
        if kill and not len(other_sessions):
            self.kill_database()
        self.sessionlock.shutdown()

    def kill_database(self):
        """Kill the mongo db instance in a docker container
        """
        # if the database is naitve, we skip shutting it down
        self.container_controller(
            database_config=self.database_config, action="quit", gangadir=self.gangadir)
        logger.debug(f"mongo stopped from: {self.registry.name}")

    def _write_master_cache(self):
        """
        write a master index cache once per 300sec
        Args:
            shutdown (boool): True causes this to be written now
        """
        items_to_save = iter(self.objects.items())
        all_indexes = []
        for k, v in items_to_save:
            if k in self.incomplete_objects:
                continue
            try:
                if k in self._fully_loaded:
                    # Check and write index first
                    obj = v  # self.objects[k]
                    new_index = None
                    if obj is not None:
                        new_index = self.registry.getIndexCache(
                            stripProxy(obj))

                    if new_index is not None:
                        new_index["classname"] = getName(obj)
                        new_index["category"] = obj._category
                        new_index["modified_time"] = time.time()
                        if hasattr(obj, "master") and obj._category == "jobs" and obj.master:
                            new_index["master"] = obj.master
                        else:
                            new_index["master"] = -1
                        all_indexes.append(new_index)

            except Exception as err:
                logger.debug(
                    "Failed to update index: %s on startup/shutdown" % k)
                logger.debug("Reason: %s" % err)

        # bulk saving the indexes, if there is anything to save
        if all_indexes:
            for temp in all_indexes:
                index_to_database(data=temp, document=self.connection.index)
            # self.connection.index.insert_many(documents=all_indexes)

        return

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
                raise RepositoryError(
                    self,
                    "Internal Error: add with different number of objects and force_ids!",
                )
            ids = force_ids
        else:
            ids = [i + len(self.objects) for i in range(len(objs))]

        logger.debug("made ids")

        for obj_id, obj in zip(ids, objs):
            self._internal_setitem__(obj_id, obj)

            # Set subjobs dirty - they will not be flushed if they are not.
            if self.sub_split and hasattr(obj, self.sub_split):
                try:
                    sj_len = len(getattr(obj, self.sub_split))
                    if sj_len > 0:
                        for j in range(sj_len):
                            getattr(obj, self.sub_split)[j]._dirty = True
                except AttributeError as err:
                    logger.debug("RepoXML add Exception: %s" % err)

        logger.debug("Added")

        return ids

    # FIXME: Force override of `ignore_subs` to include `master` information for subjobs
    def _flush(self, this_id):
        """
        Flush Json to disk whilst checking for relavent SubJobJsonList which handles subjobs now
        flush for "this_id" in the self.objects list
        Args:
            this_id (int): This is the id of the object we want to flush to disk
        """
        obj = self.objects[this_id]

        if not isType(obj, EmptyGangaObject):
            split_cache = None

            has_children = getattr(obj, self.sub_split, False)

            if has_children:
                logger.debug("has_children")

                if hasattr(getattr(obj, self.sub_split), "flush"):
                    # I've been read from disk in the new SubJobJsonList format I know how to flush
                    getattr(obj, self.sub_split).flush()
                else:
                    # I have been constructed in this session, I don't know how to flush!
                    if hasattr(getattr(obj, self.sub_split)[0], "_dirty"):
                        split_cache = getattr(obj, self.sub_split)
                        for i in range(len(split_cache)):
                            if not split_cache[i]._dirty:
                                continue
                            safe_save(
                                master=this_id,
                                ignore_subs=[],
                                _object=split_cache[i],
                                conn=self.connection[self.registry.name],
                            )
                            split_cache[i]._setFlushed()
                    # # Now generate an index file to take advantage of future non-loading goodness
                    tempSubJList = SubJobJsonList(
                        registry=self.registry, connection=self.connection, parent=obj
                    )
                    # # equivalent to for sj in job.subjobs
                    tempSubJList._setParent(obj)
                    job_dict = {}
                    for sj in getattr(obj, self.sub_split):
                        job_dict[sj.id] = stripProxy(sj)
                    tempSubJList._reset_cachedJobs(job_dict)
                    tempSubJList.flush()
                    del tempSubJList

                # Saving the parent object
                safe_save(
                    _object=obj,
                    conn=self.connection[self.registry.name],
                    ignore_subs=[self.sub_split],
                    master=-1,
                )

            else:

                logger.debug("not has_children")
                safe_save(
                    _object=obj,
                    conn=self.connection[self.registry.name],
                    ignore_subs=[],
                    master=-1,
                )

            if this_id not in self.incomplete_objects:
                self.index_write(this_id)
        else:
            raise RepositoryError(
                self, "Cannot flush an Empty object for ID: %s" % this_id
            )

        if this_id not in self._fully_loaded:
            self._fully_loaded[this_id] = obj

    def flush(self, ids):
        """
        flush the set of "ids" to database and write the json representing said objects in self.objects
        NB: This adds the given objects corresponding to ids to the _fully_loaded dict
        Args:
            ids (list): List of integers, used as keys to objects in the self.objects dict
        """
        logger.debug("Flushing: {ids}".format(ids=str(ids)))

        # import traceback
        # traceback.print_stack()
        for this_id in ids:
            if this_id in self.incomplete_objects:
                logger.debug(
                    "Should NEVER re-flush an incomplete object, it's now 'bad' respect this!"
                )
                continue
            try:
                logger.debug("safe_flush: %s" % this_id)
                self._flush(this_id)

                self._cache_load_timestamp[this_id] = time.time()
                self._cached_obj[this_id] = self.objects[this_id]._index_cache
                self.index_write(this_id)

                if this_id not in self._fully_loaded:
                    self._fully_loaded[this_id] = self.objects[this_id]

                subobj_attr = getattr(
                    self.objects[this_id], self.sub_split, None)
                sub_attr_dirty = getattr(subobj_attr, "_dirty", False)
                if sub_attr_dirty:
                    if hasattr(subobj_attr, "flush"):
                        subobj_attr.flush()

                self.objects[this_id]._setFlushed()

            except (OSError, IOError, DatabaseError) as x:
                raise RepositoryError(
                    self,
                    "Error of type: %s on flushing id '%s': %s" % (
                        type(x), this_id, x),
                )

    def index_write(self, this_id=None, shutdown=False):
        """
        Save index information of objects into the master index
        Args:
            this_id (int): Id of object whose index is to be stored
            shutdown (bool): Flag to save all information
        """
        if shutdown:
            for id in self.objects:
                self.index_write(this_id=id, shutdown=False)
        else:
            logger.debug("Adding index of {id}".format(id=this_id))
            obj = self.objects[this_id]
            temp = self.registry.getIndexCache(stripProxy(obj))
            self._cached_obj[this_id] = temp
            self._cache_load_timestamp[this_id] = time.time()
            if temp:
                temp["classname"] = getName(obj)
                temp["category"] = obj._category
                if getattr(obj, "master"):
                    temp["master"] = obj.master
                else:
                    temp["master"] = -1

            index_to_database(data=temp, document=self.connection.index)
            # TODO: Instead of replacing everything, replace only the changed
            # if the repository is shutting down, save everything again
            if shutdown:
                raise NotImplementedError(
                    "Call function to save master index here.")

    @repeat_while_none(max=10, message='Waiting for Mongo DB to reply')
    def _read_master_cache(self):
        """Reads the index document from the database
        """
        logger.debug("Reading the MasterCache")
        try:
            master_cache = self.connection.index.find(
                filter={"category": self.registry.name, "master": -1}
            )  # loading masters so.
            if master_cache:
                for cache in master_cache:
                    self.index_load(this_id=cache["id"], item=cache)
                return dict([(_["id"], True) for _ in master_cache])
            else:
                logger.debug(
                    "No master index information exists, new/blank repository startup is assumed"
                )
                return {}
        except pymongo.errors.ServerSelectionTimeoutError as e:
            return None

    def _clear_stored_cache(self):
        """
        clear the master cache(s) which have been stored in memory
        """
        for k in self._cache_load_timestamp.keys():
            self._cache_load_timestamp.pop(k)
        for k in self._cached_obj.keys():
            self._cached_obj.pop(k)

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
        if firstRun:
            objs = self._read_master_cache()
        else:
            objs = set(self.objects.keys())
        changed_ids = []
        summary = []
        logger.debug("Iterating over Items")

        for this_id in objs:
            if this_id in self.incomplete_objects:
                continue

            if self.index_load(this_id):
                changed_ids.append(this_id)

            # this is bad - no or corrupted index but object not loaded yet!
            # Try to load it!
            if not this_id in self.objects:
                try:
                    logger.debug(
                        "Loading database based Object: %s from %s as indexes were missing"
                        % (this_id, self.registry.name)
                    )
                    self.load([this_id])
                    changed_ids.append(this_id)
                    if this_id not in self.incomplete_objects:
                        # If object is loaded mark it dirty so next flush will regenerate XML,
                        # otherwise just go about fixing it
                        if not self.isObjectLoaded(self.objects[this_id]):
                            self.index_write(this_id)
                        else:
                            self.objects[this_id]._setDirty()
                    # self.unlock([this_id])
                except KeyError as err:
                    logger.debug("update Error: %s" % err)
                    # deleted job
                    if this_id in self.objects:
                        self._internal_del__(this_id)
                        changed_ids.append(this_id)
                except (InaccessibleObjectError,) as x:
                    logger.debug(
                        "update_index: Failed to load id %i: %s" % (this_id, x)
                    )
                    summary.append((this_id, x))

        logger.debug("Iterated over Items")

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
                    logger.error(
                        "Adding: %s to Incomplete Objects to avoid loading it again in future"
                        % this_id
                    )
                    self.incomplete_objects.append(this_id)

            for exc, ids in cnt.items():
                logger.error(
                    "Registry '%s': Failed to load %i jobs (IDs: %s) due to '%s' (first error: %s)"
                    % (self.registry.name, len(ids), ",".join(ids), exc, examples[exc])
                )

            if self.printed_explanation is False:
                logger.error(
                    "If you want to delete the incomplete objects, you can type:\n"
                )
                logger.error(
                    "'for i in %s.incomplete_ids(): %s(i).remove()'\n (then press 'Enter' twice)"
                    % (self.registry.name, self.registry.name)
                )
                logger.error(
                    "WARNING!!! This will result in corrupt jobs being completely deleted!!!"
                )
                self.printed_explanation = True
        logger.debug("updated index done")

        # if len(changed_ids) != 0:
        #     isShutdown = not firstRun
        #     self._write_master_cache(isShutdown)

        return changed_ids

    def index_load(self, this_id, item=None):
        """
        Will load index file from the database, so we know what objects exist in the database
        raise NotImplementedError("Load all the information at once")

        """
        if item is None:
            item = index_from_database(
                _filter={"id": this_id, "master": -1},  # loading master jobs
                document=self.connection.index,
            )
        if item and item["modified_time"] != self._cache_load_timestamp.get(this_id, 0):
            if this_id in self.objects:
                obj = self.objects[this_id]
                setattr(obj, "_registry_refresh", True)
            else:
                try:
                    obj = self._make_empty_object_(
                        this_id, item["category"], item["classname"]
                    )
                except Exception as e:
                    raise Exception(
                        "{e} Failed to create empty ganga object for {this_id}".format(
                            e=e, this_id=this_id
                        )
                    )
            obj._index_cache = item
            self._cached_obj[this_id] = item
            self._cache_load_timestamp[this_id] = item["modified_time"]
            return True

        elif this_id not in self.objects:
            self.objects[this_id] = self._make_empty_object_(
                this_id,
                self._cached_obj[this_id]["category"],
                self._cached_obj[this_id]["classname"],
            )
            self.objects[this_id]._index_cache = self._cached_obj[this_id]
            setattr(self.objects[this_id], "_registry_refresh", True)
            return True

        else:
            logger.debug("Doubly loading of object with ID: %s" % this_id)
            logger.debug("Just silently continuing")
        return False

    def save_index(self):
        """Save the index information of this registry into the database
        """
        # all the indexes are saved in the same files
        confirmation = index_to_database(
            document=self.connection.index,
            index={"name": self.regitry.name, "items": self._cached_obj},
        )
        if not confirmation:
            raise NotImplementedError("Should the repository close now?")

    def _parse_json(self, this_id, has_children, tmpobj):
        """
        If we must actually load the object from database then we end up here.
        This replaces the attrs of "objects[this_id]" with the attrs from tmpobj
        If there are children then a SubJobJsonList is created to manage them.
        The fn of the job is passed to the SubbJobXMLList and there is some knowledge of if we should be loading the backup passed as well
        Args:
            this_id (int): This is the integer key of the object in the self.objects dict
            has_children (bool): This contains the result of the decision as to whether this object actually has children
            tmpobj (GangaObject): This contains the object which has been read in from the fn file
        """

        # If this_id is not in the objects add the object we got from reading the Json
        # logger.info(f"tmpobj does have ?: {tmpobj._getRegistry()}")
        # logger.info(f"XXXXtmpobj does have ?: {self.objects[this_id]}")

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
                    obj.setSchemaAttribute(
                        attr_name, obj._schema.getDefaultValue(attr_name)
                    )

        if has_children:
            logger.debug("Adding children")
            obj.setSchemaAttribute(
                self.sub_split,
                SubJobJsonList(
                    registry=self.registry, connection=self.connection, parent=obj
                ),
            )
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
        # if obj._index_cache not in [{}]:
        #     self._check_index_cache(obj, this_id)

        obj._index_cache = {}

        if this_id not in self._fully_loaded:
            self._fully_loaded[this_id] = obj

    def _load_json_from_obj(self, document, this_id):
        """
        This is the method which will load the job from fn using the fobj using the self.from_file method and _parse_json is called to replace the
        self.objects[this_id] with the correct attributes. We also preseve knowledge of if we're being asked to load a backup or not
        Args:
            fn (str): fn This is the name of the file which contains the JSon data
            this_id (int): This is the key of the object in the objects dict where the output will be stored
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' JSon file
        """

        b4 = time.time()
        tmpobj, errs = self.from_database(
            _filter={"id": this_id}, document=document)
        a4 = time.time()
        logger.debug("Loading Json file for ID: %s took %s sec" %
                     (this_id, a4 - b4))

        if len(errs) > 0:
            logger.error("#%s Error(s) Loading File: %s" %
                         (len(errs), document.name))
            for err in errs:
                logger.error("err: %s" % err)
            raise InaccessibleObjectError(self, this_id, errs[0])

        logger.debug("Checking children: %s" % str(this_id))

        # we dont check for `children` in the demo
        has_children = SubJobJsonList.checkJobHasChildren(
            master_id=tmpobj.id, document=self.connection[self.registry.name]
        )

        # logger.debug("Found children: %s" % str(has_children))

        self._parse_json(this_id, has_children=has_children, tmpobj=tmpobj)

        if hasattr(self.objects[this_id], self.sub_split):
            sub_attr = getattr(self.objects[this_id], self.sub_split)
            if sub_attr is not None and hasattr(sub_attr, "_setParent"):
                sub_attr._setParent(self.objects[this_id])

        # implement the time reader
        # self._load_timestamp[this_id] = self._cached_obj[this_id]["modified_time"]
        # self._load_timestamp[this_id] = os.fstat(fobj.fileno()).st_ctime

        logger.debug("Finished Loading Json")

    # FIXME: Allow bulk_reads when many ids are read
    def load(self, ids):
        """
        Load the following "ids" from disk
        If we want to load the backup files for these ids then use _copy_backup
        Correctly loaded objects are dirty, Objects loaded from backups for whatever reason are marked dirty
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' Json file
        """
        logger.debug("Loading Repo object(s): %s" % ids)

        for this_id in ids:

            if this_id in self.incomplete_objects:
                raise RepositoryError(
                    self,
                    "Trying to re-load a corrupt repository id: {this_id}".format(
                        this_id=this_id
                    ),
                )

            try:
                self._load_json_from_obj(
                    this_id=this_id, document=self.connection[self.registry.name]
                )
            except RepositoryError as err:
                logger.debug(f"Repo Exception: {err}")
                logger.error(
                    f"Adding id: {this_id} to Corrupt IDs will not attempt to re-load this session"
                )
                self.incomplete_objects.append(this_id)
                raise

            subobj_attr = getattr(self.objects[this_id], self.sub_split, None)
            sub_attr_dirty = getattr(subobj_attr, "_dirty", False)

            self.objects[this_id]._setFlushed()

            if sub_attr_dirty:
                getattr(self.objects[this_id], self.sub_split)._setDirty()

        logger.debug(f"Finished 'load'-ing of: {ids}")

    def delete(self, ids):
        """
        This is the method to 'delete' an object from disk, it's written in python and starts with the indexes first
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        for this_id in ids:
            # removing the master object
            self.connection.jobs.delete_one({"id": this_id, "master": -1})
            self.connection.jobs.delete_many({"master": this_id})

            # remove the index from the database
            self.connection.index.delete_one({"id": this_id, "master": -1})
            self.connection.index.delete_many({"master": this_id})

            self._internal_del__(this_id)
            if this_id in self._fully_loaded:
                del self._fully_loaded[this_id]
            if this_id in self.objects:
                del self.objects[this_id]

    # RatPass: This will be not implemented, kept for compatibility
    def lock(self, ids):
        """
        Request a session lock for the following ids
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    # RatPass: This will be not implemented, kept for compatibility
    def unlock(self, ids):
        """
        Unlock (release file locks of) the following ids
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    # RatPass: This will be implemented latter
    def get_other_sessions(self):
        """get_session_list()
        Tries to determine the other sessions that are active and returns an informative string for each of them.
        """
        return self.sessionlock.get_other_sessions()


    def clean(self):
        """clean() --> True/False
        Clear EVERYTHING in this repository, counter, all jobs, etc.
        WARNING: This is not nice."""
        self.shutdown()
        try:
            # rmrf(self.root)
            _ = pymongo.MongoClient()
            _.drop_database(self.db_name)

        except Exception as err:
            logger.error(
                "Failed to correctly clean repository due to: %s" % err)
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
