# This, although inheriting from GangaList should be here as the class has to know about on-disk structure of the XML repo

from Ganga.GPIDev.Schema.Schema import Schema, SimpleItem, Version
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.Utility.logging import getLogger
from Ganga.Core.GangaRepository.VStreamer import from_file, to_file
from Ganga.Core.exceptions import GangaException
import errno
logger = getLogger()

##FIXME There has to be a better way of doing this?
class SJXLIterator(object):

    def __init__(self, theseSubJobs):

        self._mySubJobs = theseSubJobs
        self._myCount = 0

    ## NB becomes __next__ in Python 3.x don't know if Python 2.7 has a wrapper here
    def next(self):
        if self._myCount < len(self._mySubJobs):
            self._myCount += 1
            return self._mySubJobs[self._myCount-1]
        else:
            raise StopIteration


class SubJobXMLList(GangaObject):
    """
        jobDirectory: Directory of parent job containing subjobs
        from_file: Helper function to read object from disk
    """

    _category = 'internal'
    _exportmethods = ['__getitem__', '__len__', '__iter__', 'getAllCachedData']
    _hidden = True
    _name = 'SubJobXMLList'

    #_schema = GangaList.GangaList._schema.inherit_copy()
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
        self._storedList = []

        self._subjob_master_index_name = "subjobs.idx"

        if jobDirectory == '' and registry is None:
            return

        self._subjobIndexData = {}
        self.load_subJobIndex()

    def __deepcopy__(self, memo=None):
        cls = type(self)
        obj = super(cls, cls).__new__(cls)
        dict = self.__dict__.__deepcopy__(memo)
        obj.__dict__ = dict
        return obj

    def load_subJobIndex(self):

        import os.path
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
                    for subjob in self._subjobIndexData.keys():
                        index_data = self._subjobIndexData.get(subjob)
                        if index_data is not None and 'modified' in index_data:
                            mod_time = index_data['modified']
                            disk_location = self.__get_dataFile(str(subjob))
                            import os
                            disk_time = os.stat(disk_location).st_ctime
                            if mod_time != disk_time:
                                self._subjobIndexData = {}
                                break
                        else:
                            self._subjobIndexData = {}
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

        all_caches = {}
        for i in range(len(self)):
            this_cache = self._registry.getIndexCache( self.__getitem__(i) )
            all_caches[i] = this_cache
            disk_location = self.__get_dataFile(i)
            import os
            all_caches[i]['modified'] = os.stat(disk_location).st_ctime

        import os.path
        try:
            from Ganga.Core.GangaRepository.PickleStreamer import to_file
            index_file = os.path.join(self._jobDirectory, self._subjob_master_index_name )
            index_file_obj = open( index_file, "w" )
            to_file( all_caches, index_file_obj )
            index_file_obj.close()
        except Exception, err:
            logger.debug( "cache write error: %s" % str(err) )

    #def _attribute_filter__get__(self, name ):

    #    if name == "_list":
    #        if len(self._cachedJobs.keys()) != len(self):
    #            if self._storedList != []:
    #                self._storedList = []
    #            i=0
    #            for i in range( len(self) ):
    #                self._storedList.append( self.__getitem__(i) )
    #                i+=1
    #        return self._storedList
    #    else:
    #        self.__getattribute__(self, name )

    def __iter__(self):
        return SJXLIterator(self)

    def __get_dataFile(self, index):
        import os.path
        subjob_data = os.path.join(self._jobDirectory, str(index), self._dataFileName)
        if self._load_backup:
            subjob_data = subjob_data + '~'
        return subjob_data

    def __len__(self):
        subjob_count = 0
        from os import listdir, path
        if not path.isdir( self._jobDirectory ):
            return 0

        jobDirectoryList = listdir( self._jobDirectory )

        i=0
        while str(i) in jobDirectoryList:
            subjob_data = self.__get_dataFile(str(i))
            import os.path
            if os.path.isfile( subjob_data ):
                subjob_count = subjob_count + 1
            i += 1

        return subjob_count

    def _loadSubJobFromDisk(self, subjob_data):
        # For debugging where this was called from to try and push it to as high a level as possible at runtime
        #import traceback
        #traceback.print_stack()
        #import sys
        #sys.exit(-1)
        try:
            job_obj = self.getJobObject()
        except Exception, err:
            logger.debug( "Error: %s" % str(err) )
            job_obj = None
        if job_obj is not None:
            fqid = job_obj.getFQID('.')
            logger.debug( "Loading subjob at: %s for job %s" % (subjob_data, fqid) )
        else:
            logger.debug( "Loading subjob at: %s" % subjob_data )
        sj_file = open(subjob_data, "r")
        return sj_file

    def __getitem__(self, index):

        logger.debug("Requesting: %s" % str(index))

        subjob_data = None
        if not index in self._cachedJobs.keys():
            if len(self) < index:
                raise GangaException("Subjob: %s does NOT exist" % str(index))
            subjob_data = self.__get_dataFile(str(index))
            try:
                sj_file = self._loadSubJobFromDisk(subjob_data)
            except IOError as x:
                if x.errno == errno.ENOENT:
                    raise IOError("Subobject %s not found: %s" % (fqid, x))
                else:
                    raise RepositoryError(self,"IOError on loading subobject %s: %s" % (fqid, x))

            try:
                self._cachedJobs[index] = self._from_file(sj_file)[0]
            except Exception as err:
                logger.debug("Failed to Load XML for job: %s using: %s" % (str(index), str(subjob_data)))
                logger.debug("Err:\n%s" % str(err))
                raise err

        if self._definedParent is not None:
            parent_name = "Job: %s" % self._definedParent.getFQID('.')
        else:
            parent_name = "None"
        logger.debug('Setting Parent: %s' % parent_name)
        if self._definedParent:
            self._cachedJobs[index]._setParent( self._definedParent )
        return self._cachedJobs[index]

    def _setParent(self, parentObj):
        
        if parentObj is not None:
            parent_name = "Job: %s" % parentObj.getFQID('.')
        else:
            parent_name = "None"
        logger.debug('Setting Parent: %s' % parent_name)

        super(SubJobXMLList, self)._setParent( parentObj )
        if not hasattr(self, '_cachedJobs'):
            return
        for k in self._cachedJobs.keys():
            self._cachedJobs[k]._setParent( parentObj )
        self._definedParent = parentObj

    def getCachedData(self, index):

        return

    def getAllCachedData(self):

        cached_data = []
        logger.debug( "Cache: %s" % str(self._subjobIndexData.keys()) )
        if len(self._subjobIndexData.keys()) == len(self):
            for i in range(len(self)):
                cached_data.append( self._subjobIndexData[i] )
        else:
            for i in range(len(self)):
                cached_data.append( self._registry.getIndexCache( self.__getitem__(i) ) )

        return cached_data

    def flush(self):
        from Ganga.Core.GangaRepository.GangaRepositoryXML import safe_save

        for index in range(len(self)):
            if index in self._cachedJobs.keys():
                subjob_data = self.__get_dataFile(str(index))
                subjob_obj = self._cachedJobs[index]

                safe_save( subjob_data, subjob_obj, self._to_file )

        self.write_subJobIndex()
        return

