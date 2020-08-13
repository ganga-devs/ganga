"""
Same as Database but uses Central Database:
TODO:
- Save each job with owners information
"""
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
from GangaCore.GPIDev.Base.Proxy import getName, isType, stripProxy
from GangaCore.Core.GangaRepository.SubJobJsonList import SubJobJsonList


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
)


logger = GangaCore.Utility.logging.getLogger()

save_all_history = False


def check_app_hash(obj):
    """Writes a file safely, raises IOError on error
    Args:
        obj (GangaObject): This is an object which has a prepared application
    """
    raise NotImplementedError


def safe_save(_object, connection, master=None, ignore_subs=[]):
    """Try to save the Json for this object in as safe a way as possible
    Args:
        _object (GangaObject): This is the object which we want to save to the file
        connection (pymongo): Connection to the document/table where the object will be stored
        master (int): Index Id of parent/master of _object
        ignore_subs (list): This is the names of the attribute of _obj we want to ignore in writing to the database
    """
    raise NotImplementedError


class GangaRepositoryLocal(GangaRepository):

    """GangaRepository Local"""

    def __init__(self, registry):
        """
        Initialize a Repository from within a Registry and keep a reference to the Registry which 'owns' it
        Args:
            Registry (Registry): This is the registry which manages this Repo
        """
        raise NotImplementedError

    def startup(self):
        """ Starts a repository and reads in a directory structure.
        Raise RepositoryError"""
        raise NotImplementedError

    def start_mongomon(self, options=None, backend="docker"):
        """Start the mongodb with the prefered back_end
        """
        raise NotImplementedError

    def shutdown(self):
        """Shutdown the repository. Flushing is done by the Registry
        Raise RepositoryError
        Write an index file for all new objects in memory and master index file of indexes
        """
        raise NotImplementedError

    def kill_mongomon(self):
        """Kill the mongo db instance in a docker container
        """
        raise NotImplementedError

    def add(self, objs, force_ids=None):
        """ Add the given objects to the repository, forcing the IDs if told to.
        Raise RepositoryError
        Args:
            objs (list): GangaObject-s which we want to add to the Repo
            force_ids (list, None): IDs to assign to object, None for auto-assign
        """
        raise NotImplementedError

    def _flush(self, this_id):
        """
        Flush Json to disk whilst checking for relavent SubJobJsonList which handles subjobs now
        flush for "this_id" in the self.objects list
        Args:
            this_id (int): This is the id of the object we want to flush to disk
        """
        raise NotImplementedError

    def flush(self, ids):
        """
        flush the set of "ids" to database and write the json representing said objects in self.objects
        NB: This adds the given objects corresponding to ids to the _fully_loaded dict
        Args:
            ids (list): List of integers, used as keys to objects in the self.objects dict
        """
        raise NotImplementedError

    def index_write(self, this_id=None, shutdown=False):
        """
        Save index information of this_id's object into the master index
        Args:
            this_id (int): This is the index for which we want to write the index to disk
            shutdown (bool): True causes this to always be written regardless of any checks
        """
        raise NotImplementedError

    def read_master_cache(self):
        """Reads the index document from the database
        """
        raise NotImplementedError

    def _clear_stored_cache(self):
        """
        clear the master cache(s) which have been stored in memory
        """
        raise NotImplementedError

    def index_load(self, this_id, startup=False):
        """
        Will load index file from the database, so we know what objects exist in the database
        """
        raise NotImplementedError

    def save_index(self):
        """Save the index information of this registry into the database
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def _load_json_from_obj(self, document, this_id):
        """
        This is the method which will load the job from fn using the fobj using the self.from_file method and _parse_json is called to replace the
        self.objects[this_id] with the correct attributes. We also preseve knowledge of if we're being asked to load a backup or not
        Args:
            fn (str): fn This is the name of the file which contains the JSon data
            this_id (int): This is the key of the object in the objects dict where the output will be stored
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' JSon file
        """
        raise NotImplementedError

    def load(self, ids, load_backup=False):
        """
        Load the following "ids" from disk
        If we want to load the backup files for these ids then use _copy_backup
        Correctly loaded objects are dirty, Objects loaded from backups for whatever reason are marked dirty
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
            load_backup (bool): This reflects whether we are loading the backup 'data~' or normal 'data' Json file
        """
        raise NotImplementedError

    def delete(self, ids):
        """
        This is the method to 'delete' an object from disk, it's written in python and starts with the indexes first
        Args:
            ids (list): The object keys which we want to iterate over from the objects dict
        """
        raise NotImplementedError

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
        raise NotImplementedError

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

    def _handle_load_exception(self, err, fn, this_id, load_backup):
        raise NotImplementedError

    def get_index_listing(self):
        raise NotImplementedError

    def _read_master_cache(self):
        raise NotImplementedError

    def _clear_stored_cache(self):
        raise NotImplementedError

    def _write_master_cache(self, shutdown=False):
        raise NotImplementedError

    def _check_index_cache(self, obj, this_id):
        raise NotImplementedError
