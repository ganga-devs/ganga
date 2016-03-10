from Ganga.GPIDev.Schema.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.Utility.logging import getLogger
from Ganga.Core.GangaRepository.GangaRepository import RepositoryError
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Base.Proxy import stripProxy
import errno
import copy
logger = getLogger()

##FIXME There has to be a better way of doing this?
class SJXLIterator(object):
    """Class for iterating over SJXMLList, potentially very unstable, dangerous and only supports looping forwards ever"""
    def __init__(self, theseSubJobs):

        self._mySubJobs = theseSubJobs
        self._myCount = 0

    ## NB becomes __next__ in Python 3.x don't know if Python 2.7 has a wrapper here
    def next(self):
        if self._myCount < len(self._mySubJobs):
            returnable = self._mySubJobs[self._myCount]
            self._myCount += 1
            return returnable
        else:
            raise StopIteration


class SubJobXMLList(GangaObject):
    """
        SUBJOBXMLLIST class for managing the subjobs so they're loaded only when needed
    """

    _category = 'internal'
    _exportmethods = ['__getitem__', '__len__', '__iter__', 'getAllCachedData', 'values']
    _hidden = True
    _name = 'SubJobXMLList'

    _schema = Schema(Version(1, 0), {}) 

    def __init__(self, jobDirectory='', registry=None, dataFileName='data', load_backup=False, parent=None ):
        """jobDirectory: dir on disk which contains subjob folders
        registry: the registry managing me,
        dataFileName: incase it ever changes,
        load_backup: are we using the backpus only/first? This used to be set like this btw
        paret: parent of self after constuction"""
        super(SubJobXMLList, self).__init__()

        self._jobDirectory = jobDirectory
        self._registry = registry
        self._cachedJobs = {}

        self._dataFileName = dataFileName
        self._load_backup = load_backup

        self._definedParent = None

        self._subjob_master_index_name = "subjobs.idx"

        if jobDirectory == '' and registry is None:
            return

        self._subjobIndexData = {}
        if parent is not None:
            self._setParent(parent)
        self.load_subJobIndex()

        self._cached_filenames = {}
        self._stored_len = []

    def __construct__(self, args):
        super(SubJobXMLList, self).__construct__(args)
        self._definedParent = None

        self._subjobIndexData = {}
        self._subjob_master_index_name = "subjobs.idx"
        self._jobDirectory = None
        self._registry = None
        self._dataFileName = None
        self._load_backup = None
        self._cachedJobs = {}
        self._cached_filenames = {}
        self._stored_len =[]

    ## THIS CLASS MAKES USE OF THE INTERNAL CLASS DICTIONARY ONLY!!!
    ## THIS CLASS DOES NOT MAKE USE OF THE SCHEMA TO STORE INFORMATION AS TRANSIENT OR UNCOPYABLE
    ## THIS CLASS CONTAINS A LOT OF OBJECT REFERENCES WHICH SHOULD NOT BE DEEPCOPIED!!!
    def __deepcopy__(self, memo=None):
        if not isType(self, SubJobXMLList):
            logger.error("CANNOT COPY A SUBJOBXMLLIST FROM ANOTHER CLASS TYPE!!!")
            return
        cls = self.__class__
        obj = cls()
        obj.__init__()
        new_dict = {}
        for dict_key, dict_value in self.__dict__.iteritems():

            ## Copy objects where it's sane to
            if dict_key not in ['_cachedJobs', '_definedParent', '_registry', '_parent']:
                new_dict[dict_key] = deepcopy(dict_value)

            ## Assign by reference objects where it's sane to
            elif dict_key in ['_registry']:
                new_dict[dict_key] = dict_value

            else:
                new_dict[dict_key] = dict_value

        ## Manually define unsafe/uncopyable objects
        new_dict['_definedParent'] = None
        new_dict['_cachedJobs'] = {}
        obj.__dict__ = new_dict
        return obj

    def _reset_cachedJobs(self, obj):
        """Hard reset function. Not really to be used externally without great care"""
        self._cachedJobs = obj

    def isLoaded(self, subjob_id):
        """Has the subjob been loaded? True/False"""
        return subjob_id in self._cachedJobs.keys()

    def load_subJobIndex(self):
        """Load the index from all sujobs ynto _subjobIndexData or empty it is an error occurs"""
        import os
        index_file = os.path.join(self._jobDirectory, self._subjob_master_index_name )
        if os.path.isfile( index_file ):
            index_file_obj = None
            try:
                from Ganga.Core.GangaRepository.PickleStreamer import from_file
                try:
                    index_file_obj = open( index_file, "r" )
                    self._subjobIndexData = from_file( index_file_obj )[0]
                except IOError, err:
                    self._subjobIndexData = None

                if self._subjobIndexData is None:
                    self._subjobIndexData = {}
                else:
                    for subjob_id in self._subjobIndexData.keys():
                        index_data = self._subjobIndexData.get(subjob_id)
                        ## CANNOT PERFORM REASONABLE DISK CHECKING ON AFS
                        ## SLOW FILE ACCESS WRITE AND METADATA MEANS FILE DATA DOES NOT MATCH MOD TIME
                        #if index_data is not None and 'modified' in index_data:
                        #    mod_time = index_data['modified']
                        #    disk_location = self.__get_dataFile(str(subjob_id))
                        #    disk_time = os.stat(disk_location).st_ctime
                        #    diff = disk_time - mod_time
                        #    if disk_time > mod_time and (diff*diff < 9.):
                        #        logger.warning("objs: %s" %str(self._cachedJobs.keys()))
                        #        logger.warning("%s != %s" % (str(mod_time), str(disk_time)))
                        #        logger.warning("SubJob: %s has been modified, re-loading" % (subjob_id))
                        #        new_data = self._registry.getIndexCache( self.__getitem__(subjob_id) )
                        #        self._subjobIndexData[subjob_id] = new_data
                        #        break
                        #else:
                        if index_data is None:
                            logger.warning("Cannot find subjob index %s, rebuilding" % str(subjob_id))
                            new_data = self._registry.getIndexCache( self.__getitem__(subjob_id) )
                            self._subjobIndexData[subjob_id] = new_data
                            continue
                            #self._subjobIndexData = {}
            except Exception, err:
                logger.error( "Subjob Index file open, error: %s" % str(err) )
                self._subjobIndexData = {}
            finally:
                if index_file_obj is not None:
                    index_file_obj.close()
                if self._subjobIndexData is None:
                    self._subjobIndexData = {}
        return

    def write_subJobIndex(self):
        """interface for writing the index which captures errors and alerts the user vs throwing uncaught exception"""
        try:
            self.__really_writeIndex()
        except Exception as err:
            logger.debug("Can't write Index. Moving on as this is not essential to functioning it's a performance bug")
            logger.debug("Error: %s" % str(err))

    def __really_writeIndex(self):
        """Do the actual work of writing the index for all subjobs"""
        import os

        all_caches = {}
        for sj_id in range(len(self)):
            if sj_id in self._cachedJobs.keys():
                this_cache = self._registry.getIndexCache( self.__getitem__(sj_id) )
                all_caches[sj_id] = this_cache
                disk_location = self.__get_dataFile(sj_id)
                all_caches[sj_id]['modified'] = os.stat(disk_location).st_ctime
            else:
                if sj_id in self._subjobIndexData.keys():
                    all_caches[sj_id] = self._subjobIndexData[sj_id]
                else:
                    this_cache = self._registry.getIndexCache( self.__getitem__(sj_id) )
                    all_caches[sj_id] = this_cache
                    disk_location = self.__get_dataFile(sj_id)
                    all_caches[sj_id]['modified'] = os.stat(disk_location).st_ctime

        try:
            from Ganga.Core.GangaRepository.PickleStreamer import to_file
            index_file = os.path.join(self._jobDirectory, self._subjob_master_index_name )
            index_file_obj = open( index_file, "w" )
            to_file( all_caches, index_file_obj )
            index_file_obj.close()
        except Exception, err:
            logger.debug( "cache write error: %s" % str(err) )

    def __iter__(self):
        """Return iterator for this class"""
        return SJXLIterator(self)

    def __get_dataFile(self, index, force_backup=False):
        """Get the filename for this file (with out without backup '~'. Store already determine combinations in _cached_filenames for speed"""

        backup_decision = self._load_backup is True or force_backup is True

        index_str = str(index)+"_"+str(backup_decision)

        if index_str in self._cached_filenames:
            return self._cached_filenames[index_str]

        import os.path
        subjob_data = os.path.join(self._jobDirectory, str(index), self._dataFileName)
        if backup_decision is True:
            subjob_data = subjob_data + '~'

        self._cached_filenames[index_str] = subjob_data
        return subjob_data

    def __len__(self):
        """ return length or lookup the last modified time compare against self._stored_len[0] and if nothings changed return self._stored_len[1]"""
        import os
        try:
            this_time = os.stat(self._jobDirectory).st_ctime
        except OSError:
            return 0

        if len(self._stored_len) == 2:
            last_time = self._stored_len[0]
            if this_time == last_time:
                return self._stored_len[1]

        subjob_count = 0
        from os import listdir, path
        if not path.isdir( self._jobDirectory ):
            return 0

        jobDirectoryList = listdir( self._jobDirectory )

        import os.path
        subjob_count=0
        while True:
            if str(subjob_count) in jobDirectoryList:
                expected_folder = os.path.join(self._jobDirectory, str(subjob_count))
                if os.path.isdir(expected_folder):
                    subjob_count=subjob_count+1
                    continue
                else:
                    break
            else:
                break
            #subjob_data = self.__get_dataFile(str(i))
            #import os.path
            #if os.path.isfile(subjob_data)):
            #    subjob_count = subjob_count + 1
            #i += 1

        if len(self._stored_len) != 2:
            self._stored_len = []
            self._stored_len.append(this_time)
            self._stored_len.append(subjob_count)
        else:
            self._stored_len[0] = this_time
            self._stored_len[1] = subjob_count

        return subjob_count

    def keys(self):
        """Return keys to access subjobs"""
        return [i for i in range(len(self))]

    def values(self):
        """Return the actual subjobs"""
        return [self[i] for i in range(0, len(self))]

    def _loadSubJobFromDisk(self, subjob_data):
        """Load the subjob file 'subjob_data' from disk. No Parsing"""
        # For debugging where this was called from to try and push it to as high a level as possible at runtime
        #print("SJXML Load")
        #import traceback
        #traceback.print_stack()
        #print("\n\n\n")
        #import sys
        #sys.exit(-1)
        try:
            job_obj = self.getJobObject()
        except Exception, err:
            logger.debug( "Error: %s" % str(err) )
            try:
                job_obj = self._getParent()
            except Exception as err:
                job_obj = None
            job_obj = None
        finally:
            pass
        if job_obj is not None:
            try:
                fqid = job_obj.getFQID('.')
                logger.debug( "Loading subjob at: %s for job %s" % (subjob_data, fqid) )
            except Exception as err:
                try:
                    _id = job_obj.id
                except:
                    _id = "unknown"
                logger.debug("Loading subjob at: $s for job %s" % (subjob_data, _id))
        else:
            logger.debug( "Loading subjob at: %s" % subjob_data )
        sj_file = open(subjob_data, "r")
        return sj_file

    def __call__(self, index):
        """Same as getitem"""
        return self.__getitem__(index)

    def __getitem__(self, index):
        """Return a subjob based upon index"""
        return self._getItem(index)
        try:
            return self._getItem(index)
        except Exception as err:
            logger.error("CANNOT LOAD SUBJOB INDEX: %s" % str(index))
            return None

    def _getItem(self, index):
        """Actual meat of loading the subjob from disk is required, parsing and storing a copy in memory (_cached_subjobs) for future use"""
        logger.debug("Requesting: %s" % str(index))

        #if index == 0:
        #import traceback
        #traceback.print_stack()

        subjob_data = None
        if not index in self._cachedJobs.keys():
            if len(self) < index:
                raise GangaException("Subjob: %s does NOT exist" % str(index))
            subjob_data = self.__get_dataFile(str(index))
            try:
                sj_file = self._loadSubJobFromDisk(subjob_data)
            except IOError as x:
                if x.errno == errno.ENOENT:
                    raise IOError("Subobject %s not found: %s" % (index, x))
                else:
                    raise RepositoryError(self,"IOError on loading subobject %s: %s" % (index, x))

            from Ganga.Core.GangaRepository.VStreamer import from_file

            try:
                self._cachedJobs[index] = from_file(sj_file)[0]
            except Exception as err:

                try:
                    subjob_data = self.__get_dataFile(str(index), True)
                    self._cachedJobs[index] = from_file(sj_file)[0]
                    if self._definedParent is not None:
                        self._cachedJobs[index]._setParent( self._definedParent )
                except Exception as err:
                    logger.debug("Failed to Load XML for job: %s using: %s" % (str(index), str(subjob_data)))
                    logger.debug("Err:\n%s" % str(err))
                    raise err

        if self._definedParent is not None and self._cachedJobs[index]._getParent() is not self._definedParent:
            self._cachedJobs[index]._setParent( self._definedParent )
        return self._cachedJobs[index]

    def _setParent(self, parentObj):
        """Set the parent of self and any objects in memory we control"""

        if parentObj is not None and hasattr(parentObj, 'getFQID'):
            parent_name = "Job: %s" % parentObj.getFQID('.')
        elif parentObj is not None and hasattr(parentObj, 'id'):
            parent_name = "Job: %s" % parentObj.id
        else:
            parent_name = "None"
        logger.debug('Setting Parent: %s' % parent_name)

        super(SubJobXMLList, self)._setParent( parentObj )

        if self._definedParent is not parentObj:
            self._definedParent = parentObj

        if not hasattr(self, '_cachedJobs'):
            return
        for k in self._cachedJobs.keys():
            if self._cachedJobs[k]._getParent() is not self._definedParent:
                self._cachedJobs[k]._setParent( parentObj )

    def getCachedData(self, index):
        """Get the cached data from the index for one of the subjobs"""
        if index > len(self) or index < 0:
            return None

        if index in self._subjobIndexData.keys():
            if self.isLoaded(index):
                return self._registry.getIndexCache( self.__getitem__(index) )
            else:
                return self._subjobIndexData[index]
        else:
            return self._registry.getIndexCache( self.__getitem__(index) )

        return None

    def getAllCachedData(self):
        """Get the cached data from the index for all subjobs"""
        cached_data = []
        #logger.debug( "Cache: %s" % str(self._subjobIndexData.keys()) )
        if len(self._subjobIndexData.keys()) == len(self):
            for i in range(len(self)):
                if self.isLoaded(i):
                    cached_data.append( self._registry.getIndexCache( self.__getitem__(i) ) )
                else:
                    cached_data.append( self._subjobIndexData[i] )
        else:
            for i in range(len(self)):
                cached_data.append(self._registry.getIndexCache( self.__getitem__(i) ) )

        return cached_data

    def flush(self):
        """Flush all subjobs to disk using XML methods"""
        from Ganga.Core.GangaRepository.GangaRepositoryXML import safe_save

        from Ganga.Core.GangaRepository.VStreamer import to_file

        for index in range(len(self)):
            if index in self._cachedJobs.keys():
                subjob_data = self.__get_dataFile(str(index))
                subjob_obj = self._cachedJobs[index]

                safe_save( subjob_data, subjob_obj, to_file )

        self.write_subJobIndex()
        return

