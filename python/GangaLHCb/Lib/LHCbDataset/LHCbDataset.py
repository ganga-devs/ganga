#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from copy import deepcopy
import tempfile
import fnmatch
from Ganga.Core import GangaException
from Ganga.GPIDev.Lib.Dataset import GangaDataset
from Ganga.GPIDev.Schema import GangaFileItem, SimpleItem, Schema, Version
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from LHCbDatasetUtils import isLFN, isPFN, isDiracFile, strToDataFile, getDataFile
from OutputData import OutputData
from Ganga.GPIDev.Base.Proxy import isType, stripProxy, getName
from Ganga.GPIDev.Lib.Job.Job import Job, JobTemplate
from GangaDirac.Lib.Backends.DiracUtils import get_result
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaListByRef
from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class LHCbDataset(GangaDataset):

    '''Class for handling LHCb data sets (i.e. inputdata for LHCb jobs).

    Example Usage:
    ds = LHCbDataset(["lfn:/some/lfn.file","pfn:/some/pfn.file"])
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
    schema['files'] = GangaFileItem(defvalue=[], typelist=['str', 'Ganga.GPIDev.Adapters.IGangaFile.IGangaFile'], sequence=1, doc=docstr)
    docstr = 'Ancestor depth to be queried from the Bookkeeping'
    schema['depth'] = SimpleItem(defvalue=0, doc=docstr)
    docstr = 'Use contents of file rather than generating catalog.'
    schema['XMLCatalogueSlice'] = GangaFileItem(defvalue=None, doc=docstr)
    docstr = 'Specify the dataset persistency technology'
    schema['persistency'] = SimpleItem(
        defvalue=None, typelist=['str', 'type(None)'], doc=docstr)
    schema['treat_as_inputfiles'] = SimpleItem(defvalue=False, doc="Treat the inputdata as inputfiles, i.e. copy the inputdata to the WN")

    _schema = Schema(Version(3, 0), schema)
    _category = 'datasets'
    _name = "LHCbDataset"
    _exportmethods = ['getReplicas', '__len__', '__getitem__', 'replicate',
                      'hasLFNs', 'append', 'extend', 'getCatalog', 'optionsString',
                      'getLFNs', 'getFileNames', 'getFullFileNames',
                      'difference', 'isSubset', 'isSuperset', 'intersection',
                      'symmetricDifference', 'union', 'bkMetadata',
                      'isEmpty', 'hasPFNs', 'getPFNs']  # ,'pop']

    def __init__(self, files=None, persistency=None, depth=0):
        super(LHCbDataset, self).__init__()
        if files is None:
            files = []
        new_files = GangaList()
        if isType(files, LHCbDataset):
            for this_file in files:
                new_files.append(deepcopy(this_file))
        elif isType(files, IGangaFile):
            new_files.append(deepcopy(this_file))
        elif isType(files, (list, tuple, GangaList)):
            new_list = []
            for this_file in files:
                if type(this_file) is str:
                    new_file = string_datafile_shortcut_lhcb(this_file, None)
                elif isType(this_file, IGangaFile):
                    new_file = this_file
                else:
                    new_file = strToDataFile(this_file)
                new_list.append(stripProxy(new_file))
            stripProxy(new_files)._list = new_list
        elif type(files) is str:
            new_files.append(string_datafile_shortcut_lhcb(this_file, None), False)
        else:
            raise GangaException("Unknown object passed to LHCbDataset constructor!")
        new_files._setParent(self)

        logger.debug("Processed inputs, assigning files")

        # Feel free to turn this on again for debugging but it's potentially quite expensive
        #logger.debug( "Creating dataset with:\n%s" % files )
        self.files = new_files
        
        logger.debug("Assigned files")

        self.persistency = persistency
        self.depth = depth
        logger.debug("Dataset Created")

    def __construct__(self, args):
        logger.debug("__construct__")
        self.files = []
        if (len(args) != 1):
            super(LHCbDataset, self).__construct__(args[1:])

        #logger.debug("__construct__: %s" % str(args))

        if len(args) == 0:
            return

        self.files = []
        if type(args[0]) is str:
            this_file = string_datafile_shortcut_lhcb(args[0], None)
            self.files.append(args[0])
        else:
            for file_arg in args[0]:
                if type(file_arg) is str:
                    this_file = string_datafile_shortcut_lhcb(file_arg, None)
                else:
                    this_file = file_arg
                self.files.append(file_arg)
        # Equally as expensive
        #logger.debug( "Constructing dataset len: %s\n%s" % (str(len(self.files)), str(self.files) ) )
        logger.debug("Constructing dataset len: %s" % str(len(self.files)))

    def __getitem__(self, i):
        '''Proivdes scripting (e.g. ds[2] returns the 3rd file) '''
        #this_file = self.files[i]
        # print type(this_file)
        # return this_file
        # return this_file
        # return this_file
        if type(i) == type(slice(0)):
            ds = LHCbDataset(files=self.files[i])
            ds.depth = self.depth
            #ds.XMLCatalogueSlice = self.XMLCatalogueSlice
            return ds
        else:
            return self.files[i]

    def getReplicas(self):
        'Returns the replicas for all files in the dataset.'
        lfns = self.getLFNs()
        cmd = 'getReplicas(%s)' % str(lfns)
        result = get_result(cmd, 'LFC query error', 'Could not get replicas.')
        return result['Value']['Successful']

    def hasLFNs(self):
        'Returns True is the dataset has LFNs and False otherwise.'
        for f in self.files:
            if isDiracFile(f):
                return True
        return False

    def hasPFNs(self):
        'Returns True is the dataset has PFNs and False otherwise.'
        for f in self.files:
            if not isDiracFile(f):
                return True
        return False

    def replicate(self, destSE=''):
        '''Replicate all LFNs to destSE.  For a list of valid SE\'s, type
        ds.replicate().'''

        if not destSE:
            from GangaDirac.Lib.Files.DiracFile import DiracFile
            DiracFile().replicate('')
            return
        if not self.hasLFNs():
            raise GangaException('Cannot replicate dataset w/ no LFNs.')

        retry_files = []

        for f in self.files:
            if not isDiracFile(f):
                continue
            try:
                result = f.replicate( destSE=destSE )
            except Exception as err:
                msg = 'Replication error for file %s (will retry in a bit).' % f.lfn
                logger.warning(msg)
                logger.warning("Error: %s" % str(err))
                retry_files.append(f)

        for f in retry_files:
            try:
                result = f.replicate( destSE=destSE )
            except Exception as err:
                msg = '2nd replication attempt failed for file %s. (will not retry)' % f.lfn
                logger.warning(msg)
                logger.warning(str(err))

    def extend(self, files, unique=False):
        '''Extend the dataset. If unique, then only add files which are not
        already in the dataset.'''
        from Ganga.GPIDev.Base import ReadOnlyObjectError

        if self._parent is not None and self._parent._readonly():
            raise ReadOnlyObjectError('object Job#%s  is read-only and attribute "%s/inputdata" cannot be modified now' % (self._parent.id, getName(self)))

        _external_files = []

        if type(files) is str or isType(files, IGangaFile):
            _external_files = [files]
        elif type(files) in [list, tuple]:
            _external_files = files
        elif isType(files, LHCbDataset):
            _external_files = files.files
        else:
            if not hasattr(files, "__getitem__") or not hasattr(files, '__iter__'):
                _external_files = [files]

        # just in case they extend w/ self
        _to_remove = []
        for this_file in _external_files:
            if hasattr(this_file, 'subfiles'):
                if len(this_file.subfiles) > 0:
                    _external_files = makeGangaListByRef(this_file.subfiles)
                    _to_remove.append(this_file)
            if type(this_file) is str:
                _external_files.append(string_datafile_shortcut_lhcb(this_file, None))
                _to_remove.append(this_file)

        for _this_file in _to_remove:
            _external_files.pop(_external_files.index(_this_file))

        for this_f in _external_files:
            _file = getDataFile(this_f)
            if _file is None:
                _file = this_f
            myName = _file.namePattern
            from GangaDirac.Lib.Files.DiracFile import DiracFile
            if isType(_file, DiracFile):
                myName = _file.lfn
            if unique and myName in self.getFileNames():
                continue
            self.files.append(stripProxy(_file))

    def removeFile(self, input_file):
        try:
            self.files.remove(input_file)
        except:
            raise GangaException('Dataset has no file named %s' % input_file.namePattern)

    def getLFNs(self):
        'Returns a list of all LFNs (by name) stored in the dataset.'
        lfns = []
        if not self:
            return lfns
        for f in self.files:
            if isDiracFile(f):
                subfiles = f.getSubFiles()
                if len(subfiles) == 0:
                    lfns.append(f.lfn)
                else:
                    for file in subfiles:
                        lfns.append(file.lfn)

        #logger.debug( "Returning LFNS:\n%s" % str(lfns) )
        logger.debug("Returning #%s LFNS" % str(len(lfns)))
        return lfns

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
        result = get_result(cmd, 'LFN->PFN error', 'XML catalog error.')
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
        elif self.persistency == None:
            snew = '\ntry:\n    #new method\n    from GaudiConf import IOExtension\n    IOExtension().inputFiles(['
        else:
            logger.warning(
                "Unknown LHCbDataset persistency technology... reverting to None")
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
                sdatasetsnew += """ \"PFN:%s\",""" % f.namePattern
                sdatasetsold += """ \"DATAFILE='PFN:%s' %s\",""" % (f.namePattern, dtype_str)
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
            other_files = LHCbDataset(other).getFullFileNames()
        elif isType(other, LHCbDataset):
            other_files = other.getFullFileNames()
        else:
            raise GangaException("Unknown type for difference")
        return other_files

    def difference(self, other):
        '''Returns a new data set w/ files in this that are not in other.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getFullFileNames()).difference(other_files)
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return data

    def isSubset(self, other):
        '''Is every file in this data set in other?'''
        other_files = self._checkOtherFiles(other)
        return set(self.getFileNames()).issubset(other_files)

    def isSuperset(self, other):
        '''Is every file in other in this data set?'''
        other_files = self._checkOtherFiles(other)
        return set(self.getFileNames()).issuperset(other_files)

    def symmetricDifference(self, other):
        '''Returns a new data set w/ files in either this or other but not
        both.'''
        other_files = other.checkOtherFiles(other)
        files = set(self.getFullFileNames()).symmetric_difference(other_files)
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return data

    def intersection(self, other):
        '''Returns a new data set w/ files common to this and other.'''
        other_files = other._checkOtherFiles(other)
        files = set(self.getFullFileNames()).intersection(other_files)
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return data

    def union(self, other):
        '''Returns a new data set w/ files from this and other.'''
        other_files = self._checkOtherFiles(other)
        files = set(self.getFullFileNames()).union(other_files)
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return data

    def bkMetadata(self):
        'Returns the bookkeeping metadata for all LFNs. '
        logger.info("Using BKQuery(bkpath).getDatasetMetadata() with bkpath=the bookkeeping path, will yeild more metadata such as 'TCK' info...")
        cmd = 'bkMetaData(%s)' % self.getLFNs()
        b = get_result(cmd, 'Error removing replica', 'Replica rm error.')
        return b

    # def pop(self,file):
    #    if type(file) is str: file = strToDataFile(file,False)
    #    try: job = self.getJobObject()
    #    except: job = None
    #    if job:
    #        if job.status != 'new' and job.status != 'failed':
    #            msg = 'Cannot pop file b/c the job status is "%s". '\
    #                  'Job must be either "new" or "failed".' % job.status
    #            raise GangaException(msg)
    #        master = job.master
    #        if job.subjobs:
    #            self.removeFile(file)
    #            for sj in job.subjobs:
    #                try: sj.inputdata.removeFile(file)
    #                except: pass
    #        elif master:
    #            master.inputdata.removeFile(file)
    #            self.removeFile(file)
    #        else: self.removeFile(file)
    #    else:
    #        self.removeFile(file)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base.Filters import allComponentFilters


def string_datafile_shortcut_lhcb(name, item):

    # Overload the LHCb instance if the Core beet us to it
    mainFileOutput = None
    try:
        mainFileOutput = Ganga.GPIDev.Lib.File.string_file_shortcut(name, item)
    except Exception, x:
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
        except Exception, x:
            # if the Core can make a file object from a string then use that,
            # else raise an error
            if not mainFileOutput is None:
                return mainFileOutput
            else:
                raise x
    return None

allComponentFilters['gangafiles'] = string_datafile_shortcut_lhcb

# Name of this method set in the GPIComponentFilters section of the
# Core... either overload this default or leave it

def string_dataset_shortcut(files, item):
    from GangaLHCb.Lib.Tasks.LHCbTransform import LHCbTransform
    from Ganga.GPIDev.Base.Objects import ObjectMetaclass
    # This clever change mirrors that in IPostprocessor (see there)
    # essentially allows for dynamic extensions to JobTemplate
    # such as LHCbJobTemplate etc.
    from Ganga.GPIDev.Base.Proxy import getProxyInterface
    inputdataList = [stripProxy(i)._schema.datadict['inputdata'] for i in getProxyInterface().__dict__.values()
                     if isinstance(stripProxy(i), ObjectMetaclass)
                     and (issubclass(stripProxy(i), Job) or issubclass(stripProxy(i), LHCbTransform))
                     and 'inputdata' in stripProxy(i)._schema.datadict]
    if type(files) not in [list, tuple]:
        return None
    if item in inputdataList:
        ds = LHCbDataset()
        ds.__construct__([files])
        return ds
    else:
        return None  # used to be c'tors, but shouldn't happen now

allComponentFilters['datasets'] = string_dataset_shortcut

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

