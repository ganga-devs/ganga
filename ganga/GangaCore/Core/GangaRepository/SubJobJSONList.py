import copy
import errno
import shutil
from os import listdir, path, stat

from GangaCore.Core.exceptions import GangaException
from GangaCore.Core.GangaRepository.GangaRepository import RepositoryError
from GangaCore.Core.GangaRepository.GangaRepositoryJson import safe_save
from GangaCore.Core.GangaRepository.JStreamer import JsonFileError
from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.GPIDev.Schema.Schema import Schema, SimpleItem, Version
from GangaCore.Utility.logging import getLogger

logger = getLogger()

print("Subjobs are using json backend")

##FIXME There has to be a better way of doing this?
class SJXLIterator(object):
    """Class for iterating over SJJsonList, potentially very unstable, dangerous and only supports looping forwards ever"""

    __slots__ = ("_myCount", "_mySubJobs")

    def __init__(self, theseSubJobs):
        """ Iterator constructor
        Args:
            theseSubJobs (list): sequential list of subjobs we're to iterate over
        """
        self._mySubJobs = theseSubJobs
        self._myCount = 0

    ## NB becomes __next__ in Python 3.x don't know if Python 2.7 has a wrapper here
    def __next__(self):
        if self._myCount < len(self._mySubJobs):
            returnable = self._mySubJobs[self._myCount]
            self._myCount += 1
            return returnable
        else:
            raise StopIteration


class SubJobJsonList(GangaObject):
    """
        SUBJOBJsonLIST class for managing the subjobs so they're loaded only when needed
    """

    _category = "internal"
    _exportmethods = [
        "__getitem__",
        "__len__",
        "__iter__",
        "getAllCachedData",
        "values",
    ]
    _hidden = True
    _name = "SubJobJsonList"

    _schema = Schema(Version(1, 0), {})

    def __init__(self, registry=None, parent=None):
        """ Constructor for SubjobJsonList
        Args:
            jobDirectory (str): dir on disk which contains subjob folders
            registry (Registry): the registry managing me,
            dataFileName (str): incase it ever changes, normally 'data'
            load_backup (bool): are we using the backpus only/first? This used to be set like this btw
            paret (Job): parent of self after constuction
        """
        super(SubJobJsonList, self).__init__()

        self._registry = registry
        self._cachedJobs = {}

        self._definedParent = None

        # FIXME: We do not handle indexes yet
        # self._subjob_master_index_name = "subjobs.idx"

        if registry is None:
            return

        # self._subjobIndexData = {}
        if parent:
            self._setParent(parent)
        # self.load_subJobIndex()

        self._cached_filenames = {}
        self._stored_len = []
        # For caching a large list of integers, the key is the length of the list
        self._storedKeys = {}


    ## THIS CLASS MAKES USE OF THE INTERNAL CLASS DICTIONARY ONLY!!!
    ## THIS CLASS DOES NOT MAKE USE OF THE SCHEMA TO STORE INFORMATION AS TRANSIENT OR UNCOPYABLE
    ## THIS CLASS CONTAINS A LOT OF OBJECT REFERENCES WHICH SHOULD NOT BE DEEPCOPIED!!!
    def __deepcopy__(self, memo=None):
        obj = super(SubJobJsonList, self).__deepcopy__(memo)

        obj._subjobIndexData = copy.deepcopy(self._subjobIndexData, memo)
        obj._jobDirectory = copy.deepcopy(self._jobDirectory, memo)
        obj._registry = self._registry
        obj._dataFileName = copy.deepcopy(self._dataFileName, memo)
        obj._load_backup = copy.deepcopy(self._load_backup, memo)
        obj._cached_filenames = copy.deepcopy(self._cached_filenames, memo)
        obj._stored_len = copy.deepcopy(self._stored_len, memo)

        ## Manually define unsafe/uncopyable objects
        obj._definedParent = None
        obj._cachedJobs = {}
        return obj

    def _reset_cachedJobs(self, obj):
        """Hard reset function. Not really to be used externally without great care
        Args:
            obj (dict): This is the new dictonary of subjob Job objects with sequential integer keys
        """
        self._cachedJobs = obj

    def isLoaded(self, subjob_id):
        """Has the subjob been loaded? True/False
        Args:
            subjob_id (int): This is the id of the job that we're interested in
        """
        return subjob_id in self._cachedJobs

    def __iter__(self):
        """Return iterator for this class"""
        return SJXLIterator(self)

    def __data(self, index, force_backup=False):
        """Get the filename for this file (with out without backup '~'. Store already determine combinations in _cached_filenames for speed
        Args:
            index (int): This is the index of the subjob we're interested in
            force_backup (bool): Should we force the loading from the backup Json
        """
        """
        Search for the subjob and return information
        """
        raise NotImplementedError

    def __len__(self):
        """ return length or lookup the last modified time compare against self._stored_len[0] and if nothings changed return self._stored_len[1]"""
        return len(self._definedParent.subjobs)

    def keys(self):
        """Return keys to access subjobs"""
        myLen = len(self)
        if myLen not in self._storedKeys:
            self._storedKeys[myLen] = [i for i in range(len(self))]
        return self._storedKeys[myLen]

    def values(self):
        """Return the actual subjobs"""
        return [self[i] for i in range(0, len(self))]

    def getSafeJob(self):
        """ Return the job object or None, no faffing around with throwing exceptions """
        try:
            job_obj = self.getJobObject()
        except Exception as err:
            logger.debug("Error: %s" % err)
            try:
                job_obj = self._getParent()
            except Exception as err:
                job_obj = None
        return job_obj

    def getMasterID(self):
        """ Return a string corresponding to the parent job ID """
        job_obj = self.getSafeJob()
        if job_obj is not None:
            try:
                fqid = job_obj.getFQID(".")
            except Exception as err:
                try:
                    fqid = job_obj.id
                except:
                    fqid = "unknown"
        else:
            fqid = "unknown"
        return fqid

    def _loadSubJobFromDisk(self, subjob_data):
        """Load the subjob file 'subjob_data' from disk. No Parsing
        Args:
            subjob_data (str): filename for the subjob 'data' file we're interested in
        """
        # For debugging where this was called from to try and push it to as high a level as possible at runtime
        # print("SJJson Load")
        # import traceback
        # traceback.print_stack()
        # print("\n\n\n")
        # import sys
        # sys.exit(-1)
        job_obj = self.getSafeJob()
        if job_obj is not None:
            fqid = self.getMasterID()
            logger.debug("Loading subjob at: %s for job %s" % (subjob_data, fqid))
        else:
            logger.debug("Loading subjob at: %s" % subjob_data)
        sj_file = open(subjob_data, "r")
        return sj_file

    def __call__(self, index):
        """Same as getitem
        Args:
            index (int): The index corresponding to the subjob object we want
        """
        return self.__getitem__(index)

    def __getitem__(self, index):
        """Return a subjob based upon index
        Args:
            index (int): The index corresponding to the subjob object we want
        """
        try:
            return self._getItem(index)
        except (GangaException, IOError, JsonFileError) as err:
            logger.error("CANNOT LOAD SUBJOB INDEX: %s. Reason: %s" % (index, err))
            raise

    def _getItem(self, index):
        """Actual meat of loading the subjob from disk is required, parsing and storing a copy in memory
        (_cached_subjobs) for future use
        Args:
            index (int): The index corresponding to the subjob object we want
        """
        logger.debug("Requesting subjob: #%s" % index)

        if index not in self._cachedJobs:

            logger.debug("Attempting to load subjob: #%s from database" % index)

            # obtain a lock to make sure multiple loads of the same object don't happen
            with self._load_lock:

                # just make sure we haven't loaded this object already while waiting on the lock
                if index in self._cachedJobs:
                    return self._cachedJobs[index]

                has_loaded_backup = False

                # Now try to load the subjob
                if len(self) < index:
                    raise GangaException("Subjob: %s does NOT exist" % index)
                subjob_data = self.__get_dataFile(str(index))
                try:
                    sj_file = self._loadSubJobFromDisk(subjob_data)
                except (JsonFileError, IOError) as x:
                    logger.warning("Error loading Json file: %s" % x)
                    try:
                        logger.debug(
                            "Loading subjob #%s for job #%s from disk, recent changes may be lost"
                            % (index, self.getMasterID())
                        )
                        subjob_data = self.__get_dataFile(str(index), True)
                        sj_file = self._loadSubJobFromDisk(subjob_data)
                        has_loaded_backup = True
                    except (IOError, JsonFileError) as err:
                        logger.debug("Error loading subjob Json:\n%s" % err)

                        if isinstance(x, IOError) and x.errno == errno.ENOENT:
                            raise IOError("Subobject %s not found: %s" % (index, x))
                        else:
                            raise RepositoryError(
                                self, "IOError on loading subobject %s: %s" % (index, x)
                            )

                from GangaCore.Core.GangaRepository.JStreamer import from_file

                # load the subobject into a temporary object
                try:
                    loaded_sj = from_file(sj_file)[0]
                except (IOError, JsonFileError) as err:

                    try:
                        logger.warning(
                            "Loading subjob #%s for job #%s from backup, recent changes may be lost"
                            % (index, self.getMasterID())
                        )
                        subjob_data = self.__get_dataFile(str(index), True)
                        sj_file = self._loadSubJobFromDisk(subjob_data)
                        loaded_sj = from_file(sj_file)[0]
                        has_loaded_backup = True
                    except (IOError, JsonFileError) as err:
                        logger.debug(
                            "Failed to Load Json for job: %s using: %s"
                            % (index, subjob_data)
                        )
                        logger.debug("Err:\n%s" % err)
                        raise

                loaded_sj._setParent(self._definedParent)
                if has_loaded_backup:
                    loaded_sj._setDirty()
                else:
                    loaded_sj._setFlushed()
                self._cachedJobs[index] = loaded_sj

        return self._cachedJobs[index]

    def _setParent(self, parentObj):
        """Set the parent of self and any objects in memory we control
        Args:
            parentObj (Job): This is the master job we're hanging these children off in the tree
        """

        if parentObj is not None and hasattr(parentObj, "getFQID"):
            parent_name = "Job: %s" % parentObj.getFQID(".")
        elif parentObj is not None and hasattr(parentObj, "id"):
            parent_name = "Job: %s" % parentObj.id
        else:
            parent_name = "None"
        logger.debug("Setting Parent: %s" % parent_name)

        super(SubJobJsonList, self)._setParent(parentObj)

        if self._definedParent is not parentObj:
            self._definedParent = parentObj

        if not hasattr(self, "_cachedJobs"):
            return
        for k in self._cachedJobs:
            if self._cachedJobs[k]._getParent() is not self._definedParent:
                self._cachedJobs[k]._setParent(parentObj)

    def getAllSJStatus(self):
        """
        Returns the cached statuses of the subjobs whilst respecting the Lazy loading
        """
        sj_statuses = []
        if len(self._subjobIndexData) == len(self):
            for i in range(len(self)):
                if self.isLoaded(i):
                    sj_statuses.append(self.__getitem__(i).status)
                else:
                    sj_statuses.append(self._subjobIndexData[i]["status"])
        else:
            for i in range(len(self)):
                sj_statuses.append(self.__getitem__(i).status)
        return sj_statuses

    def flush(self, ignore_disk=False):
        """
        Flush all subjobs to disk using Json methods
        """
        range_limit = list(range(len(self)))

        for index in range_limit:
            if index in self._cachedJobs:
                ## If it ain't dirty skip it
                if not self._cachedJobs[index]._dirty:
                    continue

                subjob_data = self.__get_dataFile(str(index))
                subjob_obj = self._cachedJobs[index]

                if subjob_obj is subjob_obj._getRoot():
                    raise GangaException(
                        self, "Subjob parent not set correctly in flush."
                    )

                safe_save(subjob_data, subjob_obj, to_file)

        # self.write_subJobIndex(ignore_disk)

    def _setFlushed(self):
        """ Like Node only descend into objects which aren't in the Schema"""
        for index in self._cachedJobs:
            self._cachedJobs[index]._setFlushed()
        super(SubJobJsonList, self)._setFlushed()


    def getCachedData(self, index):
        """Get the cached data from the index for one of the subjobs
        Args:
            index (int): index for the subjob we're interested in
        """
        if index > len(self) or index < 0:
            return None

        if index in self._subjobIndexData:
            if self.isLoaded(index):
                return self._registry.getIndexCache(self.__getitem__(index))
            else:
                return self._subjobIndexData[index]
        else:
            return self._registry.getIndexCache(self.__getitem__(index))

        return None

    def getAllCachedData(self):
        """Get the cached data from the index for all subjobs"""
        cached_data = []
        # logger.debug("Cache: %s" % self._subjobIndexData)
        if len(self._subjobIndexData) == len(self):
            for i in range(len(self)):
                if self.isLoaded(i):
                    cached_data.append(
                        self._registry.getIndexCache(self.__getitem__(i))
                    )
                else:
                    cached_data.append(self._subjobIndexData[i])
        else:
            for i in range(len(self)):
                cached_data.append(self._registry.getIndexCache(self.__getitem__(i)))

        return cached_data


    def _private_display(self, reg_slice, this_format, default_width, markup):
        """ This is a private display method which makes use of the display slice as well as knowlegde of the wanted format, default_width and markup to be used
        Given it's  display method this returns a displayable string. Given it's tied into the RegistrySlice it's similar to that
        Args:
            reg_slice (RegistrySlice): This is the registry slice which is the context in which this is called
            this_format (str): This is the format used in the registry slice for the formatting of the table
            defult_width (int): default width for a colum as defined in registry slice
            markup (str): This is the markup function used to format the text in the table from registry slice
        """
        ds = ""
        for obj_i in self.keys():

            cached_data = self.getCachedData(obj_i)
            colour = reg_slice._getColour(cached_data)

            vals = []
            for item in reg_slice._display_columns:
                display_str = "display:" + str(item)
                # logger.debug("Looking for : %s" % display_str)
                width = reg_slice._display_columns_width.get(item, default_width)
                try:
                    if item == "fqid":
                        vals.append(str(cached_data[display_str]))
                    else:
                        vals.append(str(cached_data[display_str])[0:width])
                except KeyError as err:
                    logger.debug("_private_display KeyError: %s" % err)
                    vals.append("Unknown")

            ds += markup(this_format % tuple(vals), colour)

        return ds

    def load_subJobIndex(self):
        pass

    def write_subJobIndex(self, ignore_disk=False):
        pass

    def __really_writeIndex(self, ignore_disk=False):
        pass
