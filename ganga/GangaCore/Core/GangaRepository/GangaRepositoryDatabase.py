# Note: Following stuff must be considered in a GangaRepository:
#
# * lazy loading
# * locking

import os
import copy
import json
import time
import errno
import docker
import pymongo
import GangaCore.Utility.logging

from GangaCore import GANGA_SWAN_INTEGRATION
from GangaCore.GPIDev.Base.Objects import Node
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Plugin import PluginManagerError
from GangaCore.Core.GangaRepository.JStreamer import JsonFileError
from GangaCore.GPIDev.Base.Proxy import getName, isType, stripProxy
from GangaCore.Core.GangaRepository.JStreamer import EmptyGangaObject


from GangaCore.Core.GangaRepository import (
    GangaRepository,
    RepositoryError,
    InaccessibleObjectError,
)
from GangaCore.Core.GangaRepository.JStreamer import (
    to_database,
    from_database,
    index_to_database,
    index_from_database
)

# from GangaCore.Core.GangaRepository.SubJobXMLList import SubJobXMLList
from GangaCore.Core.GangaRepository.SubJobJSONList import (
    SubJobJsonList as SubJobXMLList,
)

logger = GangaCore.Utility.logging.getLogger()

save_all_history = False

# RatPass: oldfunction no change
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
                logger.warning("Job: %s is now possibly corrupt!" % jobObj.getFQID("."))
            logger.warning(
                "If you knowingly circumvented the protection, ignore this message (and, optionally,"
            )
            logger.warning(
                "re-prepare() the application). Otherwise, please file a bug report at:"
            )
            logger.warning("https://github.com/ganga-devs/ganga/issues/")


# RatPass: Find a better alternative for this, seems unnecessary
def safe_save(_object, connection, master=None, ignore_subs=[]):
    """Try to save the Json for this object in as safe a way as possible
    Args:
        _object (GangaObject): This is the object which we want to save to the file
        connection (pymongo): Connection to the document/table where the object will be stored
        master (int): Index Id of parent/master of _object
        ignore_subs (list): This is the names of the attribute of _obj we want to ignore in writing to the database
    """
    obj = stripProxy(_object)
    check_app_hash(obj)
    confirmation = to_database(
        j=obj, document=connection, master=master, ignore_subs=ignore_subs
    )
    if confirmation is None:
        raise RepositoryError(
            "The object with obj_id {id} could not be saved into the database".format(
                id=str(obj.id)
            )
        )

# similar to getting the filename for the objects and indexes
def search_database(filter_keys, connection, document):
    """Search the database for objects with the given keys
    keys (list of tuples): List of (key, value) pairs that are to be searched
    """
    result = connection[document].find_one(filter=filter_keys)
    return result


class GangaRepositoryLocal(GangaRepository):

    """GangaRepository Local"""

    def __init__(self, registry):
        """
        Initialize a Repository from within a Registry and keep a reference to the Registry which 'owns' it
        Args:
            Registry (Registry): This is the registry which manages this Repo
        """
        #! The registry has a reference to its repository, why does repository need registry ref?
        super(GangaRepositoryLocal, self).__init__(registry)
        self.dataFileName = "data"
        self.sub_split = "subjobs"
        # self.root_document = self.registry.name
        self.root_document = "GangaDatabase"
        self.saved_paths = {}
        self.saved_idxpaths = {}
        self._cache_load_timestamp = {}
        self.printed_explanation = False
        self._fully_loaded = {}


    def startup(self):
        """ Starts a repository and reads in a directory structure.
        Raise RepositoryError"""
        self._load_timestamp = {}

        # databased based initialization
        _ = pymongo.MongoClient()
        self.db_name = "dumbmachine"
        self.connection = _[self.db_name]

        # New Master index to speed up loading of many, MANY files
        self._cache_load_timestamp = {}
        # self._cached_cat = {}
        # self._cached_cls = {}
        self._cached_obj = {}
        self._cached_obj_timestamps = {} #track time for updating values in the object cache
        self._master_index_timestamp = 0

        self.known_bad_ids = []

        self.to_database = to_database
        self.from_database = from_database

        if getConfig("DatabaseConfigurations")["database"] == "MONGODB":
            try:
                # check_database_responsive(self.connection)
                # start the database instance
                self.start_mongomon()
            except Exception as err:
                # database is not responsive, lets raise an error
                logger.error("Error: %s" % err)
                msg = "\n\nUnable to reach the database server."
                msg += (
                    "Please contanct the developers"  # I dont think this should happen
                )
                # raise RepositoryError(self, msg)
                raise Exception(err, msg)

        # FIXME: Add index updating here

        logger.debug("GangaRepositoryLocal Finished Startup")

    # TODO: Add options to add custom option information for the database
    def start_mongomon(self, options=None, backend="docker"):
        """Start the mongodb with the prefered back_end
        """

        if backend is not "docker":
            raise NotImplementedError("This feature has not been implemented yet.")

        # Will not need a container client if the database is natively installed
        self.container_client = docker.from_env()
        database_name = getConfig("DatabaseConfigurations")["containerName"]
        try:
            container = self.container_client.containers.get(database_name)
            if container.status != "running":
                container.restart()

        except docker.errors.NotFound:
            logger.info("Pulling a copy of container")
            container = self.container_client.containers.run(
                detach=True,
                name=database_name,
                image="mongo:latest",
                ports={"27017/tcp": 27017},
                volumes={"/data/db": {"bind": "/mongomon_data", "mode": "rw"}},
            )
        except Exception as e:
            logger.error(e)
            logger.info("Quiting ganga as the mongo backend could not start")
            # TODO: Handle gracefull quiting of ganga

        logger.info("mongomon has started")

    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry
        Raise RepositoryError
        Write an index file for all new objects in memory and master index file of indexes"""
        logger.debug("Shutting Down GangaRepositoryDatabase")
        self.kill_mongomon()

        # FIXME: Add index saving information here

        # for k in self._fully_loaded:
        #     try:
        #         self.index_write(k, True)
        #     except Exception as err:
        #         logger.error("Warning: problem writing index object with id %s" % k)
        # try:
        #     self._write_master_cache(True)
        # except Exception as err:
        #     logger.warning("Warning: Failed to write master index due to: %s" % err)
        # self.sessionlock.shutdown()

    def kill_mongomon(self):
        """Kill the mongo db instance in a docker container
        """
        # check if the docker container already exists
        database_name = getConfig("DatabaseConfigurations")["containerName"]
        try:
            container = self.container_client.containers.get(database_name)
            container.kill()
        except docker.errors.APIError as e:
            if e.response.status_code == 409:
                logger.debug(
                    "database container was already killed by another registry"
                )
            else:
                raise e
            logger.info("mongo stopped")

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
            # TODO: Implement these
            # ids = self.sessionlock.make_new_ids(len(objs))
            # raise NotImplementedError
            ids = [i + len(self.objects) for i in range(len(objs))]
            logger.info(f"made custom ids : {ids}")

        logger.debug("made ids")

        logger.info(f"This is how we roll: {ids}")
        for obj_id, obj in zip(ids, objs):
            logger.info(f"obj_id: {obj_id}")
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
        Flush Json to disk whilst checking for relavent SubJobXMLList which handles subjobs now
        flush for "this_id" in the self.objects list
        Args:
            this_id (int): This is the id of the object we want to flush to disk
        """
        obj = self.objects[this_id]

        if not isType(obj, EmptyGangaObject):
            split_cache = None

            has_children = getattr(obj, self.sub_split, False)
            # FIXME: Check the files implementation for objects with children
            if has_children:
                raise NotImplementedError("Childrens feature is not implemented yet")
                logger.debug("has_children")

                if hasattr(getattr(obj, self.sub_split), "flush"):
                    # I've been read from disk in the new SubJobXMLList format I know how to flush
                    getattr(obj, self.sub_split).flush()
                else:
                    # I have been constructed in this session, I don't know how to flush!
                    if hasattr(getattr(obj, self.sub_split)[0], "_dirty"):
                        split_cache = getattr(obj, self.sub_split)
                        for i in range(len(split_cache)):
                            if not split_cache[i]._dirty:
                                continue
                            safe_save(
                                master=None,
                                ignore_subs=[],
                                _object=split_cache[i],
                                connection=self.connection[self.registry.name],
                            )
                            split_cache[i]._setFlushed()
                    # Now generate an index file to take advantage of future non-loading goodness
                    tempSubJList = SubJobXMLList(
                        os.path.dirname(fn),
                        self.registry,
                        self.dataFileName,
                        False,
                        obj,
                    )
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
                safe_save(
                    _object=obj,
                    connection=self.connection[self.registry.name],
                    ignore_subs=[],
                    master=None,
                )

            if this_id not in self.incomplete_objects:
                self._index_write(this_id)
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
                # self._cached_cls[this_id] = getName(self.objects[this_id])

                # Should remove try inside of try instead, have the index raise a error
                # try:
                #     self.index_write(this_id)
                # except:
                #     logger.debug("Index write failed")
                #     pass

                if this_id not in self._fully_loaded:
                    self._fully_loaded[this_id] = self.objects[this_id]

                subobj_attr = getattr(self.objects[this_id], self.sub_split, None)
                sub_attr_dirty = getattr(subobj_attr, "_dirty", False)
                if sub_attr_dirty:
                    if hasattr(subobj_attr, "flush"):
                        subobj_attr.flush()

                self.objects[this_id]._setFlushed()

            except (OSError, IOError, JsonFileError) as x:
                raise RepositoryError(
                    self,
                    "Error of type: %s on flushing id '%s': %s" % (type(x), this_id, x),
                )

    def _index_write(self, this_id=None, shutdown=False):
        """
        Save index information of this_id's object into the master index
        Args:
            this_id (int): This is the index for which we want to write the index to disk
            shutdown (bool): True causes this to always be written regardless of any checks
        """
        logger.debug("Adding index of {id}".format(id=this_id))

        obj = self.objects[this_id]
        temp = self.registry.getIndexCache(stripProxy(obj))
        self._cached_obj[this_id] = temp
        self._cache_load_timestamp[this_id] = time.time()
        if temp:
            temp["category"] = obj._category
            temp["classname"] = getName(obj)

        index_to_database(
            data=temp,
            document=self.connection.index
        )
        # TODO: Instead of replacing everything, replace only the changed
        # if the repository is shutting down, save everything again
        if shutdown:
            raise NotImplementedError("Call function to save master index here.")


    def _index_load(self, this_id, force=False):
        """
        raise NotImplementedError("Load all the information at once")

        """
        item = index_from_database(
            filter={"_id": this_id},
            document=self.connection.index
        )
        if item and item["modified_time"] != self._cache_load_timestamp.get(this_id, 0):
            if this_id in self.objects:
                obj = self.objects[this_id]
                setattr(obj, "_registry_refresh", True)
            else:
                try:
                    obj = self._make_empty_object(this_id, item["category"], item["classname"])
                except Exception as e:
                    raise Exception("{e} Failed to create empty ganga object for {this_id}".format(e=e,this_id=this_id))
            obj._index_cache = item
            self._cached_cat[this_id] = item["category"]
            self._cached_cls[this_id] = item["classname"]
            self._cached_obj[this_id] = item
            self._cache_load_timestamp[this_id] = item["modified_time"]
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

    def update_index(self, this_id=None, verbose=False, firstRun=False):
        """ Update the list of available objects
        Raise RepositoryError
        TODO avoid updating objects which haven't changed as this causes un-needed I/O
        Args:
            this_id (int): This is the id we want to explicitly check the index on disk for
            verbose (bool): Should we be verbose
            firstRun (bool): If this is the call from the Repo startup then load the master index for perfomance boost
        """
        logger.debug("updating index...")
        logger.debug(f"{str(this_id)}-{str(firstRun)}")


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
        If there are children then a SubJobXMLList is created to manage them.
        The fn of the job is passed to the SubbJobXMLList and there is some knowledge of if we should be loading the backup passed as well
        Args:
            this_id (int): This is the integer key of the object in the self.objects dict
            has_children (bool): This contains the result of the decision as to whether this object actually has children
            tmpobj (GangaObject): This contains the object which has been read in from the fn file
        """

        # If this_id is not in the objects add the object we got from reading the Json
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
            raise NotImplementedError
            # NB Keep be a SetSchemaAttribute to bypass the list manipulation which will put this into a list in some cases
            # obj.setSchemaAttribute(self.sub_split, SubJobXMLList(os.path.dirname(fn), self.registry, self.dataFileName, load_backup, obj))
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

    def _load_json_from_obj(self, document, this_id, load_backup):
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
            document=document, attribute="_id", value=this_id
        )
        logger.debug(
            f"The erros found while loading object from {document.name} are {errs} and the object is {tmpobj}"
        )
        a4 = time.time()
        logger.debug("Loading Json file for ID: %s took %s sec" % (this_id, a4 - b4))

        if len(errs) > 0:
            logger.error("#%s Error(s) Loading File: %s" % (len(errs), document.name))
            for err in errs:
                logger.error("err: %s" % err)
            raise InaccessibleObjectError(self, this_id, errs[0])

        logger.debug("Checking children: %s" % str(this_id))

        # we dont check for `children` in the demo
        # has_children = SubJobXMLList.checkJobHasChildren(
        #     os.path.dirname(fn), self.dataFileName
        # )

        # logger.debug("Found children: %s" % str(has_children))

        self._parse_json(this_id, has_children=False, tmpobj=tmpobj)

        if hasattr(self.objects[this_id], self.sub_split):
            sub_attr = getattr(self.objects[this_id], self.sub_split)
            if sub_attr is not None and hasattr(sub_attr, "_setParent"):
                sub_attr._setParent(self.objects[this_id])

        # implement the time reader
        self._load_timestamp[this_id] = tmpobj["modified_time"]
        # self._load_timestamp[this_id] = os.fstat(fobj.fileno()).st_ctime

        logger.debug("Finished Loading Json")

    def load(self, ids, load_backup=False):
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
                    self, "Trying to re-load a corrupt repository id: %s" % this_id
                )

            fn = self.get_fn(this_id)
            try:
                fobj, has_loaded_backup2 = self._open_json_file(fn, this_id, True)
                if has_loaded_backup2:
                    has_loaded_backup = has_loaded_backup2
            except Exception as err:
                logger.debug("json load: Failed to load Json file: %s" % fn)
                logger.debug("Error was:\n%s" % err)
                logger.error(
                    "Adding id: %s to Corrupt IDs will not attempt to re-load this session"
                    % this_id
                )
                self.incomplete_objects.append(this_id)
                raise

            try:
                self._load_json_from_obj(fobj, fn, this_id, load_backup)
            except RepositoryError as err:
                logger.debug("Repo Exception: %s" % err)
                logger.error(
                    "Adding id: %s to Corrupt IDs will not attempt to re-load this session"
                    % this_id
                )
                self.incomplete_objects.append(this_id)
                raise

            except Exception as err:

                should_continue = self._handle_load_exception(
                    err, fn, this_id, load_backup
                )

                if should_continue is True:
                    has_loaded_backup = True
                    continue
                else:
                    logger.error(
                        "Adding id: %s to Corrupt IDs will not attempt to re-load this session"
                        % this_id
                    )
                    self.incomplete_objects.append(this_id)
                    raise

            finally:
                fobj.close()

            subobj_attr = getattr(self.objects[this_id], self.sub_split, None)
            sub_attr_dirty = getattr(subobj_attr, "_dirty", False)

            if has_loaded_backup:
                self.objects[this_id]._setDirty()
            else:
                self.objects[this_id]._setFlushed()

            if sub_attr_dirty:
                getattr(self.objects[this_id], self.sub_split)._setDirty()

        logger.debug("Finished 'load'-ing of: %s" % ids)

    def delete(self, ids):
        """
        This is the method to 'delete' an object from disk, it's written in python and starts with the indexes first
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        for this_id in ids:
            # First remove the index, so that it is gone if we later have a
            # KeyError
            # fn = self.get_fn(this_id)
            self.connection.jobs.remove({"_id": this_id})
            self._internal_del__(this_id)
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
        pass

    def unlock(self, ids):
        """
        Unlock (release file locks of) the following ids
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        pass

    def get_other_sessions(self):
        """get_session_list()
        Tries to determine the other sessions that are active and returns an informative string for each of them.
        """
        return []
        # return self.sessionlock.get_other_sessions()

    def clean(self):
        """clean() --> True/False
        Clear EVERYTHING in this repository, counter, all jobs, etc.
        WARNING: This is not nice."""
        self.shutdown()
        try:
            # rmrf(self.root)
            _ = pymongo.MongoClient()
            _.drop_database(self.db_name)

            raise NotImplementedError(
                "Cleaning of the database document is not implemented yet"
            )
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

    # ShameZome: All the useless functions and notimplemented functions live here.

    def _handle_load_exception(self, err, fn, this_id, load_backup):
        raise NotImplementedError

    # dumbmachineComment: index_load: index file inside of the jobs folder for each distinct job
    def index_load(self, this_id):
        raise NotImplementedError

    def index_write(self, this_id, shutdown=False):
        raise NotImplementedError

    def get_index_listing(self):
        raise NotImplementedError

    def _read_master_cache(self):
        raise NotImplementedError

    def _clear_stored_cache(self):
        raise NotImplementedError

    def _write_master_cache(self, shutdown=False):
        raise NotImplementedError

    def update_index(self, this_id=None, verbose=False, firstRun=False):
        raise NotImplementedError

    def _check_index_cache(self, obj, this_id):
        raise NotImplementedError
