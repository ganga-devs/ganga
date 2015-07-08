# This, although inheriting from GangaList should be here as the class has
# to know about on-disk structure of the XML repo

from Ganga.GPIDev.Lib.GangaList import GangaList
from Ganga.Utility.logging import getLogger
logger = getLogger()


class SubJobXMLList(GangaList):

    """
        jobDirectory: Directory of parent job containing subjobs
        from_file: Helper function to read object from disk
    """

    _category = 'internal'
    _exportmethods = ['__getitem__', '__len__', '__iter__', 'getAllCachedData']
    _hidden = 1
    _enable_plugin = 1
    _name = 'SubJobXMLList'

    _schema = GangaList._schema.inherit_copy()
    _enable_config = 1

    def __init__(self, jobDirectory, registry, dataFileName, load_backup):

        self.jobDirectory = jobDirectory
        from Ganga.Core.GangaRepository.VStreamer import from_file, to_file
        self.to_file = to_file
        self.from_file = from_file
        self.dataFileName = dataFileName
        self.load_backup = load_backup

        self._cachedJobs = {}
        self._definedParent = None
        self._storedList = []

        self.registry = registry

        super(SubJobXMLList, self).__init__()

        self.subjob_master_index_name = "subjobs.idx"

        self.subjobIndexData = {}
        self.load_subJobIndex()

    def load_subJobIndex(self):

        import os.path
        index_file = os.path.join(self.jobDirectory, self.subjob_master_index_name)
        if os.path.isfile(index_file):
            try:
                from Ganga.Core.GangaRepository.PickleStreamer import from_file
                index_file_obj = open(index_file, "r")
                self.subjobIndexData = from_file(index_file_obj)[0]
                index_file_obj.close()
                for subjob in self.subjobIndexData.keys():
                    mod_time = self.subjobIndexData.get(subjob)['modified']
                    disk_location = self.__get_dataFile(subjob)
                    import os
                    disk_time = os.stat(disk_location).st_ctime
                    if mod_time != disk_time:
                        self.subjobIndexData = {}
                        break
            except Exception, err:
                logger.error("Subjob Index file open, error: %s" % str(err))
                self.subjobIndexData = {}
        return

    def write_subJobIndex(self):

        all_caches = {}
        for i in range(len(self)):
            this_cache = self.registry.getIndexCache(self.__getitem__(i))
            all_caches[i] = this_cache
            disk_location = self.__get_dataFile(i)
            import os
            all_caches[i]['modified'] = os.stat(disk_location).st_ctime

        import os.path
        try:
            from Ganga.Core.GangaRepository.PickleStreamer import to_file
            index_file = os.path.join(
                self.jobDirectory, self.subjob_master_index_name)
            index_file_obj = open(index_file, "w")
            to_file(all_caches, index_file_obj)
            index_file_obj.close()
        except Exception, err:
            logger.debug("cache write error: %s" % str(err))
            pass

    def _attribute_filter__get__(self, name):

        if name == "_list":
            if len(self._cachedJobs.keys()) != len(self):
                if self._storedList != []:
                    self._storedList = []
                i = 0
                for i in range(len(self)):
                    self._storedList.append(self.__getitem__(i))
                    i += 1
            return self._storedList
        else:
            self.__getattribute__(self, name)

    def __iter__(self):
        if self._storedList == []:
            i = 0
            for i in range(len(self)):
                self._storedList.append(self.__getitem__(i))
                i += 1
        for subjob in self._storedList:
            yield subjob

    def __get_dataFile(self, index):
        import os.path
        subjob_data = os.path.join(
            self.jobDirectory, str(index), self.dataFileName)
        if self.load_backup:
            subjob_data.append('~')
        return subjob_data

    def __len__(self):
        subjob_count = 0
        from os import listdir
        jobDirectoryList = listdir(self.jobDirectory)

        i = 0
        while str(i) in jobDirectoryList:
            subjob_data = self.__get_dataFile(str(i))
            import os.path
            if os.path.isfile(subjob_data):
                subjob_count = subjob_count + 1
            i += 1

        return subjob_count

    def __getitem__(self, index):

        if not index in self._cachedJobs.keys():
            subjob_data = self.__get_dataFile(str(index))
            try:
                # For debugging where this was called from to try and push it to as high a level as possible at runtime
                #import traceback
                # traceback.print_stack()
                #import sys
                # sys.exit(-1)
                try:
                    job_obj = self.getJobObject()
                except Exception, err:
                    logger.debug("Error: %s" % str(err))
                    job_obj = None
                if job_obj:
                    fqid = job_obj.getFQID('.')
                    logger.debug(
                        "Loading subjob at: %s for job %s" % (subjob_data, str(fqid)))
                else:
                    logger.debug("Loading subjob at: %s" % subjob_data)
                sj_file = open(subjob_data, "r")
            except IOError, x:
                if x.errno == errno.ENOENT:
                    raise IOError("Subobject %i.%i not found: %s" % (id, i, x))
                else:
                    raise RepositoryError(
                        self, "IOError on loading subobject %i.%i: %s" % (id, i, x))
            self._cachedJobs[index] = self.from_file(sj_file)[0]
            if self._definedParent:
                self._cachedJobs[index]._setParent(self._definedParent)
        return self._cachedJobs[index]

    def _setParent(self, parentObj):
        for k in self._cachedJobs.keys():
            self._cachedJobs[k]._setParent(parentObj)
        self._definedParent = parentObj
        super(SubJobXMLList, self)._setParent(parentObj)

    def getCachedData(self, index):

        return

    def getAllCachedData(self):

        cached_data = []
        logger.debug("Cache: %s" % str(self.subjobIndexData.keys()))
        if len(self.subjobIndexData.keys()) == len(self):
            for i in range(len(self)):
                cached_data.append(self.subjobIndexData[i])
        else:
            for i in range(len(self)):
                cached_data.append(
                    self.registry.getIndexCache(self.__getitem__(i)))

        return cached_data

    def flush(self):
        from Ganga.Core.GangaRepository.GangaRepositoryXML import safe_save

        for index in range(len(self)):
            if index in self._cachedJobs.keys():
                subjob_data = self.__get_dataFile(str(index))
                subjob_obj = self._cachedJobs[index]

                safe_save(subjob_data, subjob_obj, self.to_file)

        self.write_subJobIndex()
        return
