# This, although inheriting from GangaList should be here as the class has to know about on-disk structure of the XML repo

from Ganga.GPIDev.Schema.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.Utility.logging import getLogger
from Ganga.Core.GangaRepository.VStreamer import from_file, to_file
from Ganga.Core.GangaRepository.GangaRepository import RepositoryError
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Base.Proxy import stripProxy
import errno
import copy
logger = getLogger()

##FIXME There has to be a better way of doing this?
class SJXLIterator(object):

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
        jobDirectory: Directory of parent job containing subjobs
        from_file: Helper function to read object from disk
    """

    _category = 'internal'
    _exportmethods = ['__getitem__', '__len__', '__iter__', 'getAllCachedData', 'values']
    _hidden = True
    _name = 'SubJobXMLList'

    _schema = Schema(Version(1, 0), {}) 

    def __init__(self, jobDirectory='', registry=None, dataFileName='data', load_backup=False ):

        super(SubJobXMLList, self).__init__()

        self._jobDirectory = jobDirectory
        self._registry = registry
        self._cachedJobs = {}

        self._to_file = to_file
        self._from_file = from_file
        self._dataFileName = dataFileName
        self._load_backup = load_backup

        self._definedParent = None

        self._subjob_master_index_name = "subjobs.idx"

        if jobDirectory == '' and registry is None:
            return

        self._subjobIndexData = {}
        self.load_subJobIndex()

    def __construct__(self, args):
        super(SubJobXMLList, self).__construct__(args)
        self._definedParent = None

        self._subjobIndexData = {}
        self._subjob_master_index_name = "subjobs.idx"
        self._jobDirectory = None
        self._registry = None
        self._to_file = None
        self._from_file = None
        self._dataFileName = None
        self._load_backup = None
        self._cachedJobs = {}

    def __deepcopy__(self, memo=None):
        cls = type(self)
        obj = super(cls, cls).__new__(cls)
        #this_dict = copy.deepcopy(self.__dict__, memo)
        new_dict = {}
        for dict_key, dict_value in self.__dict__.iteritems():

            ## Copy objects where it's sane to
            if dict_key not in ['_cachedJobs', '_definedParent', '_to_file', '_from_file', '_registry']:
                new_dict[dict_key] = copy.deepcopy(dict_value)

            ## Assign by reference objects where it's sane to
            elif dict_key in ['_to_file', '_from_file', '_registry']:
                new_dict[dict_key] = dict_value

        ## Manually define unsafe/uncopyable objects
        new_dict['_definedParent'] = None
        new_dict['_cachedJobs'] = {}
        obj.__dict__ = new_dict
        return obj

    def _reset_cachedJobs(self, obj):
        stripProxy(self)._cachedJobs = obj

    def isLoaded(self, subjob_id):
        return subjob_id in stripProxy(self)._cachedJobs.keys()

    def load_subJobIndex(self):

        raw_self = stripProxy(self)

        import os.path
        index_file = os.path.join(raw_self._jobDirectory, raw_self._subjob_master_index_name )
        if os.path.isfile( index_file ):
            index_file_obj = None
            try:
                from Ganga.Core.GangaRepository.PickleStreamer import from_file
                try:
                    index_file_obj = open( index_file, "r" )
                    raw_self._subjobIndexData = from_file( index_file_obj )[0]
                except IOError, err:
                    raw_self._subjobIndexData = None

                if raw_self._subjobIndexData is None:
                    raw_self._subjobIndexData = {}
                else:
                    for subjob in raw_self._subjobIndexData.keys():
                        index_data = raw_self._subjobIndexData.get(subjob)
                        if index_data is not None and 'modified' in index_data:
                            mod_time = index_data['modified']
                            disk_location = raw_self.__get_dataFile(str(subjob))
                            import os
                            disk_time = os.stat(disk_location).st_ctime
                            if mod_time != disk_time:
                                raw_self._subjobIndexData = {}
                                break
                        else:
                            raw_self._subjobIndexData = {}
            except Exception, err:
                logger.error( "Subjob Index file open, error: %s" % str(err) )
                raw_self._subjobIndexData = {}
            finally:
                if index_file_obj is not None:
                    index_file_obj.close()
                if raw_self._subjobIndexData is None:
                    raw_self._subjobIndexData = {}
        return

    def write_subJobIndex(self):
        try:
            self.__really_writeIndex()
        except Exception as err:
            logger.debug("Can't write Index. Moving on as this is not essential to functioning it's a performance bug")
            logger.debug("Error: %s" % str(err))

    def __really_writeIndex(self):

        raw_self = stripProxy(self)

        all_caches = {}
        for i in range(len(raw_self)):
            this_cache = raw_self._registry.getIndexCache( raw_self.__getitem__(i) )
            all_caches[i] = this_cache
            disk_location = raw_self.__get_dataFile(i)
            import os
            all_caches[i]['modified'] = os.stat(disk_location).st_ctime

        import os.path
        try:
            from Ganga.Core.GangaRepository.PickleStreamer import to_file
            index_file = os.path.join(raw_self._jobDirectory, raw_self._subjob_master_index_name )
            index_file_obj = open( index_file, "w" )
            to_file( all_caches, index_file_obj )
            index_file_obj.close()
        except Exception, err:
            logger.debug( "cache write error: %s" % str(err) )

    def __iter__(self):
        raw_self = stripProxy(self)
        return SJXLIterator(raw_self)

    def __get_dataFile(self, index, force_backup=False):
        import os.path
        raw_self = stripProxy(self)
        subjob_data = os.path.join(raw_self._jobDirectory, str(index), raw_self._dataFileName)
        if raw_self._load_backup is True or force_backup is True:
            subjob_data = subjob_data + '~'
        return subjob_data

    def __len__(self):
        raw_self = stripProxy(self)
        subjob_count = 0
        from os import listdir, path
        if not path.isdir( raw_self._jobDirectory ):
            return 0

        jobDirectoryList = listdir( raw_self._jobDirectory )

        i=0
        while str(i) in jobDirectoryList:
            subjob_data = raw_self.__get_dataFile(str(i))
            import os.path
            if os.path.isfile( subjob_data ):
                subjob_count = subjob_count + 1
            i += 1

        return subjob_count

    def values(self):
        raw_self = stripProxy(self)
        return [raw_self[i] for i in range(0, len(raw_self))]

    def _loadSubJobFromDisk(self, subjob_data):
        # For debugging where this was called from to try and push it to as high a level as possible at runtime
        #import traceback
        #traceback.print_stack()
        #import sys
        #sys.exit(-1)
        try:
            job_obj = stripProxy(self).getJobObject()
        except Exception, err:
            logger.debug( "Error: %s" % str(err) )
            try:
                job_obj = stripProxy(self)._getParent()
            except Exception as err:
                job_obj = None
            job_obj = None
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
        return stripProxy(self).__getitem__(index)

    def __getitem__(self, index):

        return self._getItem(index)
        try:
            return self._getItem(index)
        except Exception as err:
            logger.error("CANNOT LOAD SUBJOB INDEX: %s" % str(index))
            return None

    def _getItem(self, index):

        raw_self = stripProxy(self)
        logger.debug("Requesting: %s" % str(index))

        #if index == 0:
        #import traceback
        #traceback.print_stack()

        subjob_data = None
        if not index in raw_self._cachedJobs.keys():
            if len(raw_self) < index:
                raise GangaException("Subjob: %s does NOT exist" % str(index))
            subjob_data = raw_self.__get_dataFile(str(index))
            try:
                sj_file = raw_self._loadSubJobFromDisk(subjob_data)
            except IOError as x:
                if x.errno == errno.ENOENT:
                    raise IOError("Subobject %s not found: %s" % (index, x))
                else:
                    raise RepositoryError(raw_self,"IOError on loading subobject %s: %s" % (index, x))

            try:
                raw_self._cachedJobs[index] = raw_self._from_file(sj_file)[0]
            except Exception as err:

                try:
                    subjob_data = raw_self.__get_dataFile(str(index), True)
                    raw_self._cachedJobs[index] = raw_self._from_file(sj_file)[0]
                    raw_self._cachedJobs[index] = raw_self._from_file(sj_file)[0]
                except Exception as err:
                    logger.debug("Failed to Load XML for job: %s using: %s" % (str(index), str(subjob_data)))
                    logger.debug("Err:\n%s" % str(err))
                    raise err

        if raw_self._definedParent is not None:
            if hasattr(raw_self._definedParent, 'getFQID'):
                parent_name = "Job: %s" % raw_self._definedParent.getFQID('.')
            elif hasattr(raw_self._definedParent, 'id'):
                parent_name = "Job: %s" % raw_self._definedParent.id
            else:
                parent_name = "Job: unknown"
        else:
            parent_name = "None"
        logger.debug('Setting Parent [%s]: %s' % (str(index), (parent_name)))
        if raw_self._definedParent is not None:
            raw_self._cachedJobs[index]._setParent( raw_self._definedParent )
        return raw_self._cachedJobs[index]

    def _setParent(self, parentObj):
        
        raw_self = stripProxy(self)
        if parentObj is not None and hasattr(parentObj, 'getFQID'):
            parent_name = "Job: %s" % parentObj.getFQID('.')
        elif parentObj is not None and hasattr(parentObj, 'id'):
            parent_name = "Job: %s" % parentObj.id
        else:
            parent_name = "None"
        logger.debug('Setting Parent: %s' % parent_name)

        super(SubJobXMLList, raw_self)._setParent( parentObj )

        raw_self._definedParent = parentObj

        if not hasattr(raw_self, '_cachedJobs'):
            return
        for k in raw_self._cachedJobs.keys():
            raw_self._cachedJobs[k]._setParent( parentObj )

    def getCachedData(self, index):

        return

    def getAllCachedData(self):

        raw_self = stripProxy(self)
        cached_data = []
        logger.debug( "Cache: %s" % str(raw_self._subjobIndexData.keys()) )
        if len(raw_self._subjobIndexData.keys()) == len(raw_self):
            for i in range(len(raw_self)):
                cached_data.append( raw_self._subjobIndexData[i] )
        else:
            for i in range(len(raw_self)):
                cached_data.append( raw_self._registry.getIndexCache( raw_self.__getitem__(i) ) )

        return cached_data

    def flush(self):
        from Ganga.Core.GangaRepository.GangaRepositoryXML import safe_save

        raw_self = stripProxy(self)
        for index in range(len(raw_self)):
            if index in raw_self._cachedJobs.keys():
                subjob_data = raw_self.__get_dataFile(str(index))
                subjob_obj = raw_self._cachedJobs[index]

                safe_save( subjob_data, subjob_obj, raw_self._to_file )

        raw_self.write_subJobIndex()
        return

