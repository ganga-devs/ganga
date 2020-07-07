#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
from copy import deepcopy
import tempfile
import fnmatch
from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Lib.Dataset import GangaDataset
from GangaCore.GPIDev.Schema import GangaFileItem, SimpleItem, Schema, Version, ComponentItem
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.Utility.Config import getConfig, ConfigError
from GangaDirac.Lib.Files.DiracFile import DiracFile
import GangaCore.Utility.logging
import GangaLHCb.Lib.LHCbDataset
from .LHCbDatasetUtils import isLFN, isPFN, isDiracFile, strToDataFile, getDataFile
from GangaCore.GPIDev.Base.Proxy import isType, stripProxy, getName
from GangaCore.GPIDev.Lib.Job.Job import Job, JobTemplate
from GangaDirac.Lib.Backends.DiracUtils import get_result
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaListByRef
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
logger = GangaCore.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbCompressedFileSet(GangaObject):
    '''A class for handling sets of files. This stores the common start of the lfns,
    a list of all the non-common ends and a list of the metadata for each file.

    The metadata is stored as a list of tuples of numbers. If it is filled the order is
    (Luminosity, EvtStat, RunNo, TCK)
    '''

    schema = {}
    schema['lfn_prefix'] = SimpleItem(defvalue = None, typelist = ['str', None], doc = 'The common starting path of the LFN')
    schema['suffixes'] = SimpleItem(defvalue = [], typelist = [GangaList, 'str'], sequence=1, doc = 'The individual end of each LFN')
    _schema = Schema(Version(3, 0), schema)
    _category = 'gangafiles'
    _exportmethods = ['__len__', 'getLFNs', 'getLFN']
    def __init__(self, files=None, lfn_prefix=None):
        super(LHCbCompressedFileSet, self).__init__()
        if lfn_prefix:
            self.lfn_prefix = lfn_prefix
            self.suffixes = [(_f) for _f in files]
        elif files:
            self.lfn_prefix = ''
            if not isType(files, [str, list, tuple, GangaList]):
                raise GangaException("Incorrect type %s passed to LHCbCompressedFileSet" % type(files))
            if isType(files, [list, tuple, GangaList]):
                commonpath = os.path.commonpath(files)
                if commonpath == '/':
                    commonpath = ''
                suffixes = [_lfn.replace(commonpath, '', 1) for _lfn in files]
                self.lfn_prefix = commonpath
                self.suffixes = suffixes
            else:
                self.files.append(files)

    def __len__(self):
        return len(self.suffixes)

    def getLFNs(self):
        '''Return a list of all the LFNs contained in this set'''
        lfns = [self.lfn_prefix + _suffix for _suffix in self.suffixes]
        return lfns

    def getLFN(self, i):
        '''Get a siingle LFN from the set'''
        new_lfn = self.lfn_prefix + self.suffixes[i]
        return new_lfn


class LHCbCompressedDataset(GangaDataset):

    '''Class for handling LHCb data sets (i.e. inputdata for LHCb jobs).
    This is a version of LHCbDataset that should use less disk space.
    It should only be used with DiracFile objects, and is best constructed
    from a BKQuery.

    All of the usual methods for datasets can be used here (extend, union, difference etc).
    These also work if the other dataset is a regular LHCbDataset.

    The LHCbCompressedDataset furthermore offers the ability to store some metadata about
    the files in it, i.e. Luminosity, EvtStat, Run no, and TCK

    For this dataset everything revolves around the LFN rather than individual file objects
    '''
    schema = {}
    docstr = 'List of DiracFile objects'
    schema['files'] = SimpleItem(defvalue=[], typelist=[LHCbCompressedFileSet], sequence=1, doc='A list of lists of the file suffixes')
    schema['XMLCatalogueSlice'] = GangaFileItem(defvalue=None, doc='Use contents of file rather than generating catalog.')
    schema['persistency'] = SimpleItem(defvalue=None, typelist=['str', 'type(None)'], doc='Specify the dataset persistency technology')
    schema['credential_requirements'] = ComponentItem('CredentialRequirement', defvalue=None)
    schema['depth'] = SimpleItem(defvalue = 0, doc='Depth')
    _schema = Schema(Version(3, 0), schema)
    _category = 'datasets'
    _name = "LHCbCompressedDataset"
    _exportmethods = ['getReplicas', '__len__', '__getitem__', '__iter__', '__next__', 'replicate',
                      'append', 'extend', 'getCatalog', 'optionsString', 'getFileNames', 'getFilenameList',
                      'getLFNs', 'getFullFileNames', 'getFullDataset', 'hasLFNs',
                      'difference', 'isSubset', 'isSuperset', 'intersection',
                      'symmetricDifference', 'union', 'bkMetadata', 'getMetadata',
                      'getLuminosity', 'getEvtStat', 'getRunNumbers', 'isEmpty', 'getPFNs'] 

    def __init__(self, files=None, metadata = None, persistency=None, depth=0, fromRef=False):
        super(LHCbCompressedDataset, self).__init__()
        self.files = []
        #if files is an LHCbDataset

        if files and isType(files, GangaLHCb.Lib.LHCbDataset.LHCbDataset):
            newset = LHCbCompressedFileSet(files.getLFNs())
            self.files.append(newset)
        #if files is an LHCbCompressedDataset
        if files and isType(files, LHCbCompressedDataset):
            self.files.extend(files.files)
        #if files is just a string
        if files and isType(files, str):
            newset = LHCbCompressedFileSet(files)
            self.files.append(newset)
        #if files is a single DiracFile
        if files and isType(files, DiracFile):
            newset = LHCbCompressedFileSet(files.lfn)
            self.files.append(newset)
        #if files is a single LHCbCompressedFileSet
        if files and isType(files, LHCbCompressedFileSet):
            self.files.append(files)
        #if files is a list
        if files and isType(files, [list, GangaList]):
            #Is it a list of strings? Then it may have been produced from the BKQuery so pass along the metadata as well
            if isType(files[0], str):
                newset = LHCbCompressedFileSet(files)
                self.files.append(newset)
            #Is it a list of DiracFiles?
            if isType(files[0], DiracFile):
                lfns = []
                for _df in files:
                    lfns.append(_df.lfn)
                newset = LHCbCompressedFileSet(lfns)
                self.files.append(newset)
            #Is it a list of file sets?
            if isType(files[0], LHCbCompressedFileSet):
                    self.files.extend(files)

        self.files._setParent(self)
        self.persistency = persistency
        self.current = 0
        logger.debug("Dataset Created")


    def _location(self, i):
        '''Figure out where a file of index i is. Returns the subset no and the location within that subset'''
        setNo = 0
        fileTotal = 0
        while fileTotal < i+1 and setNo < len(self.files):
            fileTotal += len(self.files[setNo])
            setNo += 1
        if fileTotal < i:
            return -1, -1
        setNo = setNo - 1
        fileTotal = fileTotal - len(self.files[setNo])
        setLocation = i - fileTotal
        return setNo, setLocation

    def _totalNFiles(self):
        '''Return the total no. of files in the dataset'''
        total = 0
        for _set in self.files:
            total += len(_set)
        return total

    def __len__(self):
        '''Redefine the __len__ function'''
        return self._totalNFiles()

    def __getitem__(self, i):
        '''Proivdes scripting (e.g. ds[2] returns the 3rd file) '''
        if type(i) == type(slice(0)):
            #We construct a list of all LFNs first. Not the most efficient but it allows us to use the standard slice machinery
            newLFNs = self.getLFNs()[i]
            #We define these here for future speed
            indexLen = len(newLFNs)
            #Now pull out the prefixes/suffixes
            setNo = 0
            step = 1
            #If we are going backwards start at the end
            if i.step and i.step < 0:
                step = -1
                setNo = len(self.files)-1
            currentPrefix = None
            #Iterate over the LFNs and find out where it came from
            ds = LHCbCompressedDataset()
            tempList = []
            j = 0
            while j < len(newLFNs):
                if newLFNs[j] in self.files[setNo].getLFNs():
                    tempList.append(newLFNs[j])
                    j += 1
                else:
                    if len(tempList) > 0:
                        ds.addSet(LHCbCompressedFileSet(tempList))
                    setNo += step
                    tempList = []
            ds.addSet(LHCbCompressedFileSet(tempList))
        else:
            #Figure out where the file lies
            setNo, setLocation = self._location(i)
            if setNo < 0 or i >= self._totalNFiles():
                logger.error("Unable to retrieve file %s. It is larger than the dataset size" % i)
                return None
            ds = DiracFile(lfn = self.files[setNo].getLFN(setLocation), credential_requirements = self.credential_requirements)
        return ds

    def __iter__(self):
        '''Fix the iterator'''
        self.current = 0
        return self

    def __next__(self):
        '''Fix the iterator'''
        if self.current == self._totalNFiles():
            raise StopIteration
        else:
            self.current += 1
            return self[self.current-1]

    def addSet(self, newSet):
        '''Add a new FileSet to the dataset'''
        self.files.append(newSet)

    def getFileNames(self):
        'Returns a list of the names of all files stored in the dataset'
        names = []
        for _lfn in self.getLFNs():
            names.append(_lfn)
        return names

    def getFilenameList(self):
        'Return a list of filenames to be created as input.txt on the WN. These will be the PFNs'
        #We know these are DiracFiles so collate the LFNs and get the accessURLs together
        from GangaDirac.Lib.Backends.DiracUtils import getAccessURLs
        fileList = getAccessURLs(self.getLFNs())
        return fileList

    def getReplicas(self):
        'Returns the replicas for all files in the dataset.'
        lfns = self.getLFNs()
        cmd = 'getReplicas(%s)' % str(lfns)
        result = get_result(cmd, 'LFC query error. Could not get replicas.')
        return result['Successful']

    def extend(self, other, unique=False):
        '''Extend the dataset. If unique, then only add files which are not
        already in the dataset. You may extend with another LHCbCompressedDataset,
        LHCbDataset, DiracFile or a list of string of LFNs'''

        if isType(other, LHCbCompressedDataset):
            self.files.extend(other.files)
        elif isType(other, GangaLHCb.Lib.LHCbDataset.LHCbDataset):
            lfns = other.getLFNs()
            self.files.append(LHCbCompressedFileSet(lfns))
        elif isType(other, DiracFile):
            self.files.append(LHCbCompressedFileSet(other.lfn))
        elif isType(other, [list, tuple, GangaList]):
            self.files.append(LHCbCompressedFileSet(other))
        else:
            logger.error("Cannot add object of type %s to an LHCbCompressedDataset" % type(other))

    def getLFNs(self):
        '''Returns a list of all LFNs (by name) stored in the dataset.'''
        lfns = []
        if not self:
            return lfns
        for fileset in self.files:
            lfns.extend(fileset.getLFNs())
        logger.debug("Returning #%s LFNS" % str(len(lfns)))
        return lfns

    def getMetadata(self):
        '''Returns a list of all the metadata'''
        from GangaLHCb.Lib.Backends.Dirac import getLFNMetadata
        return getLFNMetadata(self.getLFNs())

    def getLuminosity(self):
        '''Returns the total luminosity'''
        lumi = 0
        mets = self.getMetadata()
        for _f in mets.keys():
            lumi += mets[_f]['Luminosity']
        return lumi

    def getEvtStat(self):
        '''Returns the total events'''
        evtStat = 0
        mets = self.getMetadata()
        for _f in mets.keys():
            evtStat += mets[_f]['EventStat']
        return evtStat

    def getRunNumbers(self):
        '''Returns a list of the run numbers'''
        runNos = []
        mets = self.getMetadata()
        for _f in mets.keys():
            runNos.append(mets[_f]['RunNumber'])
        return runNos

    def isEmpty(self):
        '''Does this contain files'''
        return not len(self)>0

    def hasLFNs(self):
        '''Does it contain LFNs'''
        return not self.isEmpty()

    def getPFNs(self):
        'Returns a list of all PFNs (by name) stored in the dataset.'
        return self.getFilenameList()

    def getFullFileNames(self):
        'Returns all file names with LFN prepended.'
        names = []
        for _lfn in self.getLFNs():
                names.append('LFN:%s' % _lfn)
        return names

    def getFullDataset(self):
        '''Returns an LHCb dataset'''
        ds = GangaLHCb.Lib.LHCbDataset.LHCbDataset(persistency = self.persistency)
        lfns = self.getLFNs()
        for _lfn in lfns:
            ds.extend(DiracFile(lfn = _lfn))
        return ds

    def getCatalog(self, site=''):
        '''Generates an XML catalog from the dataset (returns the XML string).
        Note: site defaults to config.LHCb.LocalSite
        Note: If the XMLCatalogueSlice attribute is set, then it returns
              what is written there.'''
        if hasattr(self.XMLCatalogueSlice, 'name'):
            if self.XMLCatalogueSlice.name:
                f = open(self.XMLCatalogueSlice.name)
                xml_catalog = f.read()
                f.close()
                return xml_catalog
        if not site:
            site = getConfig('LHCb')['LocalSite']
        lfns = self.getLFNs()
        depth = self.depth
        tmp_xml = tempfile.NamedTemporaryFile(suffix='.xml')
        cmd = 'getLHCbInputDataCatalog(%s,%d,"%s","%s")' \
              % (str(lfns), depth, site, tmp_xml.name)
        result = get_result(cmd, 'LFN->PFN error. XML catalog error.')
        xml_catalog = tmp_xml.read()
        tmp_xml.close()
        return xml_catalog

    def optionsString(self, file=None):
        'Returns the Gaudi-style options string for the dataset (if a filename' \
            ' is given, the file is created and output is written there).'
        if not self or len(self) == 0:
            return ''
        snew = ''
        if self.persistency == 'ROOT':
            snew = '\n#new method\nfrom GaudiConf import IOExtension\nIOExtension(\"%s\").inputFiles([' % self.persistency
        elif self.persistency == 'POOL':
            snew = '\ntry:\n    #new method\n    from GaudiConf import IOExtension\n    IOExtension(\"%s\").inputFiles([' % self.persistency
        elif self.persistency is None:
            snew = '\ntry:\n    #new method\n    from GaudiConf import IOExtension\n    IOExtension().inputFiles(['
        else:
            logger.warning(
                "Unknown LHCbCompressedDataset persistency technology... reverting to None")
            snew = '\ntry:\n    #new method\n    from GaudiConf import IOExtension\n    IOExtension().inputFiles(['

        sold = '\nexcept ImportError:\n    #Use previous method\n    from Gaudi.Configuration import EventSelector\n    EventSelector().Input=['
        sdatasetsnew = ''
        sdatasetsold = ''

        dtype_str_default = getConfig('LHCb')['datatype_string_default']
        dtype_str_patterns = getConfig('LHCb')['datatype_string_patterns']
        for _f in self.getLFNs():
            dtype_str = dtype_str_default
            for this_str in dtype_str_patterns:
                matched = False
                for pat in dtype_str_patterns[this_str]:
                    if fnmatch.fnmatch(_f, pat):
                        dtype_str = this_str
                        matched = True
                        break
                if matched:
                    break
            sdatasetsnew += '\n        '
            sdatasetsold += '\n        '
            #f is always a DiracFile
            sdatasetsnew += """ \"LFN:%s\",""" % _f
            sdatasetsold += """ \"DATAFILE='LFN:%s' %s\",""" % (_f, dtype_str)

        if sdatasetsold.endswith(","):
            if self.persistency == 'ROOT':
                sdatasetsnew = sdatasetsnew[:-1] + """\n], clear=True)"""
            else:
                sdatasetsnew = sdatasetsnew[:-1] + """\n    ], clear=True)"""
            sdatasetsold = sdatasetsold[:-1]
            sdatasetsold += """\n    ]"""
        if(file):
            f = open(file, 'w')
            if self.persistency == 'ROOT':
                f.write(snew)
                f.write(sdatasetsnew)
            else:
                f.write(snew)
                f.write(sdatasetsnew)
                f.write(sold)
                f.write(sdatasetsold)
            f.close()
        else:
            if self.persistency == 'ROOT':
                return snew + sdatasetsnew
            else:
                return snew + sdatasetsnew + sold + sdatasetsold

    def _checkOtherFiles(self, other ):
        if isType(other, GangaList) or isType(other, []):
            other_files = other
        elif isType(other, LHCbCompressedDataset):
            other_files = other.getLFNs()
        elif isType(other, GangaLHCb.Lib.LHCbDataset.LHCbDataset):
            other_files = other.getLFNs()
        else:
            raise GangaException("Unknown type for difference")
        return other_files

    def difference(self, other):
        '''Returns a new data set w/ files in this that are not in other.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getLFNs()).difference(other_files)
        data = LHCbCompressedDataset(list(files))
        return data

    def isSubset(self, other):
        '''Is every file in this data set in other?'''
        other_files = self._checkOtherFiles(other)
        return set(self.getLFNs()).issubset(other_files)

    def isSuperset(self, other):
        '''Is every file in other in this data set?'''
        other_files = self._checkOtherFiles(other)
        return set(self.getLFNs()).issuperset(other_files)

    def symmetricDifference(self, other):
        '''Returns a new data set w/ files in either this or other but not
        both.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getLFNs()).symmetric_difference(other_files)
        data = LHCbCompressedDataset(list(files))
        return data

    def intersection(self, other):
        '''Returns a new data set w/ files common to this and other.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getLFNs()).intersection(other_files)
        data = LHCbCompressedDataset(list(files))
        return data

    def union(self, other):
        '''Returns a new data set w/ files from this and other.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getLFNs()).union(other_files)
        data = LHCbCompressedDataset(list(files))
        return data

    def bkMetadata(self):
        'Returns the bookkeeping metadata for all LFNs. '
        logger.info("Using BKQuery(bkpath).getDatasetMetadata() with bkpath=the bookkeeping path, will yeild more metadata such as 'TCK' info...")
        cmd = 'bkMetaData(%s)' % self.getLFNs()
        b = get_result(cmd, 'Error removing getting metadata.')
        return b

