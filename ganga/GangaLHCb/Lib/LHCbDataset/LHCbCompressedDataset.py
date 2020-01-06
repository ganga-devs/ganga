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
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from .LHCbDatasetUtils import isLFN, isPFN, isDiracFile, strToDataFile, getDataFile
from GangaCore.GPIDev.Base.Proxy import isType, stripProxy, getName
from GangaCore.GPIDev.Lib.Job.Job import Job, JobTemplate
from GangaDirac.Lib.Backends.DiracUtils import get_result
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaListByRef
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
logger = GangaCore.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbCompressedFileSet(GangaObject):
    '''A class for handling sets of files'''

    schema = {}
    schema['lfn_prefix'] = SimpleItem(defvalue = None, typelist = ['str', None], doc = 'The common starting path of the LFN')
    schema['suffixes'] = SimpleItem(defvalue = [], typelist = [GangaList, 'str'], sequence=1, doc = 'The individual end of each LFN')
    schema['metadata'] = SimpleItem(defvalue = [], typelist = [tuple], sequence=1, doc = 'A list of tuples containing the metadata for each file')
    _schema = Schema(Version(3, 0), schema)
    def __init__(self, files=None, lfn_prefix=None, metadata = None):
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
                suffixes = [_lfn.replace(commonpath, '') for _lfn in files]
                self.lfn_prefix = commonpath
                self.suffixes = suffixes
            else:
                self.files.append(files)
        if metadata:
            self.metadata.extend(metadata)
    def __len__(self):
        return len(self.suffixes)

    def getLFNs(self):
        lfns = [self.lfn_prefix + _suffix for _suffix in self.suffixes]
        return lfns

    def getMetadata(self):
        return self.metadata

    def getLFN(self, i):
        new_lfn = self.lfn_prefix + self.suffixes[i]
        return new_lfn

class LHCbCompressedDataset(GangaDataset):

    '''Class for handling LHCb data sets (i.e. inputdata for LHCb jobs).

    Example Usage:
    ds = LHCbCompressedDataset(["lfn:/some/lfn.file","pfn:/some/pfn.file"])
    ds[0] # DiracFile("/some/lfn.file") - see DiracFile docs for usage
    ds[1] # PhysicalFile("/some/pfn.file")- see PhysicalFile docs for usage
    len(ds) # 2 (number of files)
    ds.getReplicas() # returns replicas for *all* files in the data set
    ds.replicate("CERN-USER") # replicate *all* LFNs to "CERN-USER" SE
    ds.getCatalog() # returns XML catalog slice
    ds.optionsString() # returns Gaudi-sytle options 
    [...etc...]
    '''
    schema = {}
    docstr = 'List of PhysicalFile and DiracFile objects'
    schema['files'] = SimpleItem(defvalue=[], typelist=[LHCbCompressedFileSet], sequence=1, doc='A list of lists of the file suffixes')
    schema['XMLCatalogueSlice'] = GangaFileItem(defvalue=None, doc='Use contents of file rather than generating catalog.')
    schema['persistency'] = SimpleItem(defvalue=None, typelist=['str', 'type(None)'], doc='Specify the dataset persistency technology')
    schema['credential_requirements'] = ComponentItem('CredentialRequirement', defvalue=None)
    schema['treat_as_inputfiles'] = SimpleItem(defvalue=False, doc="Treat the inputdata as inputfiles, i.e. copy the inputdata to the WN")

    _schema = Schema(Version(3, 0), schema)
    _category = 'datasets'
    _name = "LHCbCompressedDataset"
    _exportmethods = ['getReplicas', '__len__', '__getitem__', 'replicate',
                      'append', 'extend', 'getCatalog', 'optionsString',
                      'getLFNs', 'getFullFileNames', 'getFullDataset',
                      'difference', 'isSubset', 'isSuperset', 'intersection',
                      'symmetricDifference', 'union', 'bkMetadata', 'getAllMetadata',
                      'getLuminosity', 'getEvtStat', 'isEmpty', 'getPFNs'] 

    def __init__(self, files=None, metadata = None, persistency=None, depth=0, fromRef=False):
        super(LHCbCompressedDataset, self).__init__()
        self.files = []
        #if files is an LHCbDataset
        if files and isType(files, LHCbDataset):
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
                newset = LHCbCompressedFileSet(files, metadata = metadata)
                self.files.append(newset)
            #Is it a list of DiracFiles?
            if isType(files[0], DiracFile):
                lfns = []
                for _df in files:
                    lfns.append(df.lfn)
                newset = LHCbCompressedFileSet(lfns)
                self.files.append(newset)
            #Is it a list of file sets?
            if isType(files[0], LHCbCompressedFileSet):
                    self.files.extend(files)

        self.files._setParent(self)
        self.persistency = persistency
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
            newMetadata = self.getAllMetadata()[i]
            #We define these here for future speed
            indexLen = len(newLFNs)
            metLen = len(newMetadata)
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
            tempMetadata = []
            for j in range(0, len(newLFNs)):
                if newLFNs[j] in self.files[setNo].getLFNs():
                    tempList.append(newLFNs[j])
                    if metLen == indexLen:
                        tempMetadata.append(newMetadata[j])
                else:
                    if len(tempList) > 0:
                        ds.addSet(LHCbCompressedFileSet(tempList, metadata = tempMetadata))
                    setNo += step
                    tempList = []
            ds.addSet(LHCbCompressedFileSet(tempList, metadata = tempMetadata))
        else:
            #Figure out where the file lies
            setNo, setLocation = self._location(i)
            if setNo < 0 or i >= self._totalNFiles():
                logger.error("Unable to retrieve file %s. It is larger than the dataset size" % i)
                return
            ds = DiracFile(lfn = self.files[setNo].getLFN(setLocation), credential_requirements = self.credential_requirements)
        return ds

    def getLuminosity(self, i):
        '''Returns the luminosity of the given file index. If a slice is given, the total luminosity of the slice is returned'''
        if type(i) == type(slice(0)):
            tempMetadata = self.getAllMetadata()

    def addSet(self, newSet):
        '''Add a new FileSet to the dataset'''
        self.files.append(newSet)

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
        elif isType(other, LHCbDataset):
            lfns = other.getLFNs()
            self.files.append(LHCbCompressedFileSet(lfns))
        elif isType(other, DiracFile):
            self.files.append(LHCbCompressedFileSet(other.lfn))
        elif isType(other, [list, tuple, GangaList]):
            self.files.append(LHCbCompressedFileSet(other))
        else:
            logger.error("Cannot add object of type %s to an LHCbCompressedDataset" % type(other))
                

    def getLFNs(self):
        'Returns a list of all LFNs (by name) stored in the dataset.'
        lfns = []
        if not self:
            return lfns
        for fileset in self.files:
            lfns.extend(fileset.getLFNs())
        logger.debug("Returning #%s LFNS" % str(len(lfns)))
        return lfns

    def getAllMetadata(self):
        '''Returns a list of all the metadata'''
        mets = []
        for fileset in self.files:
            if not len(fileset.metadata) == len(fileset):
                mets.extend((0 for i in range(0, len(fileset))))
            else:
                mets.extend(fileset.getMetadata())
        return mets

    def getLuminosity(self):
        '''Returns the total luminosity'''
        lumi = 0
        mets = self.getAllMetadata()
        for _f in mets:
            if len(_f) == 4:
                lumi += _f[0]
        return lumi

    def getEvtStat(self):
        '''Returns the total events'''
        evtStat = 0
        mets = self.getAllMetadata()
        for _f in mets:
            if len(_f) == 4:
                evtStat += _f[1]
        return evtStat

    def getPFNs(self):
        'Returns a list of all PFNs (by name) stored in the dataset.'
        pfns = []
        if not self:
            return pfns
        for f in self.files:
            if isPFN(f):
                pfns.append(f.namePattern)
        return pfns

    def getFullFileNames(self):
        'Returns all file names w/ PFN or LFN prepended.'
        names = []
        from GangaDirac.Lib.Files.DiracFile import DiracFile
        for f in self.files:
            if isType(f, DiracFile):
                names.append('LFN:%s' % f.lfn)
            else:
                try:
                    names.append('PFN:%s' % f.namePattern)
                except:
                    logger.warning("Cannot determine filename for: %s " % f)
                    raise GangaException("Cannot Get File Name")
        return names

    def getFullDataset(self):
        '''Returns an LHCb dataset'''
        ds = LHCbDataset.LHCbDataset(persistency = self.persistency)
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
        for f in self.files:
            dtype_str = dtype_str_default
            for this_str in dtype_str_patterns:
                matched = False
                for pat in dtype_str_patterns[this_str]:
                    if fnmatch.fnmatch(f.namePattern, pat):
                        dtype_str = this_str
                        matched = True
                        break
                if matched:
                    break
            sdatasetsnew += '\n        '
            sdatasetsold += '\n        '
            if isDiracFile(f):
                sdatasetsnew += """ \"LFN:%s\",""" % f.lfn
                sdatasetsold += """ \"DATAFILE='LFN:%s' %s\",""" % (f.lfn, dtype_str)
            else:
                sdatasetsnew += """ \"%s\",""" % f.accessURL()[0]
                sdatasetsold += """ \"DATAFILE='%s' %s\",""" % (f.accessURL()[0], dtype_str)
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
        elif isType(other, LHCbDataset):
            other_files = other.getLFNs()
        else:
            raise GangaException("Unknown type for difference")
        return other_files

    def difference(self, other):
        '''Returns a new data set w/ files in this that are not in other.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getLFNs()).difference(other_files)
        data = LHCbCompressedDataset(files)
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
        other_files = other._checkOtherFiles(other)
        files = set(self.getLFNs()).symmetric_difference(other_files)
        data = LHCbCompressedDataset(files)
        return data

    def intersection(self, other):
        '''Returns a new data set w/ files common to this and other.'''
        other_files = other._checkOtherFiles(other)
        files = set(self.getLFNs()).intersection(other_files)
        data = LHCbCompressedDataset(files)
        return data

    def union(self, other):
        '''Returns a new data set w/ files from this and other.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getLFNs()).union(other_files)
        data = LHCbCompressedDataset(files)
        return data

    def bkMetadata(self):
        'Returns the bookkeeping metadata for all LFNs. '
        logger.info("Using BKQuery(bkpath).getDatasetMetadata() with bkpath=the bookkeeping path, will yeild more metadata such as 'TCK' info...")
        cmd = 'bkMetaData(%s)' % self.getLFNs()
        b = get_result(cmd, 'Error removing getting metadata.')
        return b



from GangaCore.GPIDev.Base.Filters import allComponentFilters
"""

def string_datafile_shortcut_lhcb(name, item):

    # Overload the LHCb instance if the Core beet us to it
    mainFileOutput = None
    try:
        mainFileOutput = GangaCore.GPIDev.Lib.File.string_file_shortcut(name, item)
    except Exception as x:
        logger.debug("Failed to Construct a default file type: %s" % str(name))
        pass

    #   We can do some 'magic' with strings so lets do that here
    if (mainFileOutput is not None):
        #logger.debug( "Core Found: %s" % str( mainFileOutput ) )
        if (type(name) is not str):
            return mainFileOutput

    if type(name) is not str:
        return None
    if item is None and name is None:
        return None  # used to be c'tor, but shouldn't happen now
    else:  # something else...require pfn: or lfn:
        try:
            this_file = strToDataFile(name, True)
            if this_file is None:
                if not mainFileOutput is None:
                    return mainFileOutput
                else:
                    raise GangaException("Failed to find filetype for: %s" % str(name))
            return this_file
        except Exception as x:
            # if the Core can make a file object from a string then use that,
            # else raise an error
            if not mainFileOutput is None:
                return mainFileOutput
            else:
                raise x
    return None

allComponentFilters['gangafiles'] = string_datafile_shortcut_lhcb
"""
# Name of this method set in the GPIComponentFilters section of the
# Core... either overload this default or leave it
"""
def string_dataset_shortcut(files, item):
    from GangaLHCb.Lib.Tasks.LHCbTransform import LHCbTransform
    from GangaCore.GPIDev.Base.Objects import ObjectMetaclass
    # This clever change mirrors that in IPostprocessor (see there)
    # essentially allows for dynamic extensions to JobTemplate
    # such as LHCbJobTemplate etc.
    from GangaCore.GPIDev.Base.Proxy import getProxyInterface
    inputdataList = [stripProxy(i)._schema.datadict['inputdata'] for i in getProxyInterface().__dict__.values()
                     if isinstance(stripProxy(i), ObjectMetaclass)
                     and (issubclass(stripProxy(i), Job) or issubclass(stripProxy(i), LHCbTransform))
                     and 'inputdata' in stripProxy(i)._schema.datadict]
    if type(files) not in [list, tuple, GangaList]:
        return None
    if item in inputdataList:
        ds = LHCbCompressedDataset()
        ds.extend(files)
        return ds
    else:
        return None  # used to be c'tors, but shouldn't happen now

allComponentFilters['datasets'] = string_dataset_shortcut
"""
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

