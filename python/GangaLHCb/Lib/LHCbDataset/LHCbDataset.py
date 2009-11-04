#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import tempfile
import fnmatch
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from LHCbDatasetUtils import *
from PhysicalFile import *
from LogicalFile import *
from OutputData import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Lib.Job.Job import Job

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbDataset(Dataset):
    '''Class for handling LHCb data sets (i.e. inputdata for LHCb jobs).

    Example Usage:
    ds = LHCbDataset(["lfn:/some/lfn.file","pfn:/some/pfn.file"])
    ds[0] # LogicalFile("/some/lfn.file") - see LogicalFile docs for usage
    ds[1] # PhysicalFile("/some/pfn.file")- see PhysicalFile docs for usage
    len(ds) # 2 (number of files)
    ds.getReplicas() # returns replicas for *all* files in the data set
    ds.replicate("CERN-USER") # replicate *all* LFNs to "CERN-USER" SE
    ds.getCatalog() # returns XML catalog slice
    ds.optionsString() # returns Gaudi-sytle options 
    [...etc...]
    '''
    schema = {}
    docstr = 'List of PhysicalFile and LogicalFile objects'
    schema['files'] = ComponentItem(category='datafiles',defvalue=[],
                                    sequence=1,doc=docstr)
    docstr = 'Ancestor depth to be queried from the Bookkeeping'
    schema['depth'] = SimpleItem(defvalue=0 ,doc=docstr)
    docstr = 'Not fully implemented yet'
    schema['XMLCatalogueSlice']= FileItem(defvalue=None,doc=docstr)

    _schema = Schema(Version(3,0), schema)
    _category = 'datasets'
    _name = "LHCbDataset"
    _exportmethods = ['getReplicas','__len__','__getitem__','replicate',
                      'hasLFNs','extend','getCatalog','optionsString']

    def __init__(self, files=[]):
        super(LHCbDataset, self).__init__()
        self.files = files

    def _auto__init__(self):
        for f in self.files:
            if f.name.find('OUTPUTDATA:/') >= 0:
                msg = 'Can only convert strings that begin w/ PFN: or LFN: ' \
                      'to data files.'
                raise GangaException(msg)
            #f._auto__init__()

    def _attribute_filter__set__(self,n,v):
        if n == 'files':
            for f in v: f._auto__init__()
            return v
        else: return v

    def __len__(self):
        """The number of files in the dataset."""
        result = 0
        if self.files: result = len(self.files)
        return result

    def __nonzero__(self):
        """This is always True, as with an object."""
        return True

    def __getitem__(self,i):
        '''Proivdes scripting (e.g. ds[2] returns the 3rd file) '''
        if type(i) == type(slice(0)):
            ds = LHCbDataset(files=self.files[i])
            ds.depth = self.depth
            ds.XMLCatalogueSlice = self.XMLCatalogueSlice
            return GPIProxyObjectFactory(ds)
        else:
            return GPIProxyObjectFactory(self.files[i])

    def isEmpty(self): return not bool(self.files)
    
    def getReplicas(self):
        'Returns the replicas for all files in the dataset.'
        lfns = self.getLFNs()
        cmd = 'result = DiracCommands.getReplicas(%s)' % str(lfns)
        result = get_result(cmd,'LFC query error','Could not get replicas.')
        return result['Value']['Successful']

    def hasLFNs(self):
        'Returns True is the dataset has LFNs and False otherwise.'
        for f in self.files:
            if isLFN(f): return True
        return False

    def replicate(self,destSE='',srcSE='',locCache=''):
        '''Replicate all LFNs to destSE.  For a list of valid SE\'s, type
        ds.replicate().'''
        if not destSE:
            LogicalFile().replicate('')
            return
        if not self.hasLFNs():
            raise GangaException('Cannot replicate dataset w/ no LFNs.')
        for f in self.files:
            if not isLFN(f): continue
            f.replicate(destSE,srcSE,locCache)

    def extend(self,files,unique=False):
        '''Extend the dataset. If unique, then only add files which are not
        already in the dataset.'''
        if not hasattr(files,"__getitem__"):
            raise GangaException('Argument "files" must be a iterable.')
        names = self.getFileNames()
        for f in files:
            f = getDataFile(f)
            if unique and f.name in names: continue
            self.files.append(f)

    def getLFNs(self):
        'Returns a list of all LFNs (by name) stored in the dataset.'
        lfns = []
        if not self: return lfns
        for f in self.files:
            if isLFN(f): lfns.append(f.name)
        return lfns

    def getPFNs(self): 
        'Returns a list of all PFNs (by name) stored in the dataset.'
        pfns = []
        if not self: return pfns
        for f in self.files:
            if isPFN(f): pfns.append(f.name)
        return pfns

    def getFileNames(self):
        'Returns a list of the names of all files stored in the dataset.'
        return [f.name for f in self.files]

    def getCatalog(self,site=''):
        '''Generates an XML catalog from the dataset (returns the XML string).
        Note: site defaults to config.LHCb.LocalSite'''
        if not site: site = getConfig('LHCb')['LocalSite']
        lfns = self.getLFNs()
        depth = self.depth
        tmp_xml = tempfile.NamedTemporaryFile(suffix='.xml')
        cmd = 'result = DiracCommands.getInputDataCatalog(%s,%d,"%s","%s")' \
              % (str(lfns),depth,site,tmp_xml.name)
        result = get_result(cmd,'LFN->PFN error','XML catalog error.')
        xml_catalog = tmp_xml.read()
        tmp_xml.close()
        return xml_catalog

    def optionsString(self):
        'Returns the Gaudi-style options string for the dataset.'
        if not self: return ''
        s = 'EventSelector.Input = {'
        dtype_str_default = getConfig('LHCb')['datatype_string_default']
        dtype_str_patterns = getConfig('LHCb')['datatype_string_patterns']
        for f in self.files:
            dtype_str = dtype_str_default
            for str in dtype_str_patterns:
                matched = False
                for pat in dtype_str_patterns[str]:
                    if fnmatch.fnmatch(f.name,pat):
                        dtype_str = str
                        matched = True
                        break
                if matched: break
            s += '\n'
            if isLFN(f):
                s += """ \"DATAFILE='LFN:%s' %s\",""" % (f.name, dtype_str)
            else:
                s += """ \"DATAFILE='PFN:%s' %s\",""" % (f.name, dtype_str)
        if s.endswith(","):
            s = s[:-1]
            s += """\n};"""
        return s

    # schema migration stuff (v 5.4.0)
    class LHCbDatasetSchemaMigration50400(Dataset):
        schema = {}
        schema['files'] = ComponentItem(category='datafiles',defvalue=[],
                                        sequence=1)
        docstr = 'The date the last full cache update was run.'
        schema['cache_date'] = SimpleItem(defvalue='', doc=docstr)
        docstr = 'True when the cache has never been updated before'
        schema['new_cache'] = SimpleItem(defvalue=True, doc=docstr , hidden=1)
        defvaluestr = """TYP='POOL_ROOTTREE' OPT='READ'"""
        docstr = 'The string that is added after the filename in the options '\
                 'to tell Gaudi how to read the data. If reading raw data ' \
                 '(mdf files) it should be set to "SVC=\'LHCb::MDFSelector\'"'
        schema['datatype_string'] = SimpleItem(defvalue=defvaluestr,doc=docstr)
        docstr = 'Ancestor depth to be queried from the Bookkeeping system.'
        schema['depth'] = SimpleItem(defvalue=1,doc=docstr)
        docstr = 'Select an optional XMLCatalogueSlice to the dataset'
        schema['XMLCatalogueSlice']= FileItem(defvalue=None,doc=docstr)

        _schema = Schema(Version(2,4), schema)
        _category = 'application_converters'
        _name = 'LHCbDatasetSchemaMigration50400'

    def getMigrationClass(cls,version):
        return cls.LHCbDatasetSchemaMigration50400
    getMigrationClass = classmethod(getMigrationClass)

    def getMigrationObject(cls,obj):
        version = obj._schema.version
        old_cls = cls.getMigrationClass(version)
        if not old_cls: return None #currently, this shouldn't ever happen
        # determine whether to make LHCbDataset or OutputData
        has_lfns_or_pfns = False
        has_output = False
        for f in obj.files:
            if strToDataFile(f.name): has_lfns_or_pfns = True
            else: has_output = True
        if has_lfns_or_pfns and has_output: return None # don't know what to do
        if has_lfns_or_pfns:
            lhcbdataset = LHCbDataset()
            lhcbdataset.depth = obj.depth
            lhcbdataset.XMLCatalogueSlice = obj.XMLCatalogueSlice
            for f in obj.files: lhcbdataset.files.append(strToDataFile(f.name))
            return lhcbdataset
        else:
            outputdata = OutputData()
            for f in obj.files: outputdata.files.append(f.name)
            return outputdata
    getMigrationObject = classmethod(getMigrationObject)
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base.Filters import allComponentFilters

def string_datafile_shortcut(name,item):
    if type(name) is not type(''): return None
    if item is None: # constructor
        class Dummy:
            _category = 'datafiles'
            _schema = Schema(Version(0,0),{'name':SimpleItem(None)})
            def __init__(self,name=''): self.name = name
        return Dummy(name=name)
    else: # something else...require pfn: or lfn:
        file = strToDataFile(name)
        if file is None:
            msg = 'Can only convert strings that begin w/ PFN: or LFN: to '\
                  'data files.'
            raise GangaException(msg)
        return file
    return None

allComponentFilters['datafiles'] = string_datafile_shortcut

def string_dataset_shortcut(files,item):
    if type(files) is not type([]): return None
    if item == Job._schema['inputdata']:
        ds = LHCbDataset()        
        for f in files:
            if type(f) is type(''):
                file = strToDataFile(f)
                if file is None:
                    msg = 'Can only convert strings that begin w/ PFN: or '\
                          'LFN: to data files.'
                    raise GangaException(msg)
                ds.files.append(file)
            else:
                ds.files.append(f)
        ds._auto__init__()
        return ds               
    elif item == Job._schema['outputdata']:
        return OutputData(files=files)
    else:
        l = []        
        for f in files:
            if type(f) is type(''):
                file = strToDataFile(f)
                if file is None:                    
                    l.append(strToDataFile('PFN:OUTPUTDATA:/'+f))
                else:
                    l.append(file)
            else: l.append(f)
        ds = LHCbDataset()
        ds.files = l[:]
        return ds 

allComponentFilters['datasets'] = string_dataset_shortcut

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
