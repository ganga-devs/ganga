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
from Ganga.GPIDev.Lib.Job.Job import Job,JobTemplate

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
    docstr = 'Use contents of file rather than generating catalog.'
    schema['XMLCatalogueSlice']= FileItem(defvalue=None,doc=docstr)
    docstr = 'Specify the dataset persistency technology'
    schema['persistency'] = SimpleItem(defvalue=None,typelist=['str','type(None)'] ,doc=docstr)
    
    _schema = Schema(Version(3,0), schema)
    _category = 'datasets'
    _name = "LHCbDataset"
    _exportmethods = ['getReplicas','__len__','__getitem__','replicate',
                      'hasLFNs','extend','getCatalog','optionsString',
                      'getFileNames','getFullFileNames',
                      'difference','isSubset','isSuperset','intersection',
                      'symmetricDifference','union','bkMetadata']#,'pop']

    def __init__(self, files=[], persistency=None, depth=0):
        super(LHCbDataset, self).__init__()
        self.files = files
        self.persistency=persistency
        self.depth = depth
        
    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type([])):
            super(LHCbDataset,self).__construct__(args)
        else:
            self.files = []
            for f in args[0]:
                if type(f) is type(''):
                    file = strToDataFile(f,False)
                    self.files.append(file)
                else:
                    self.files.append(f)
                    
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
        cmd = 'getReplicas(%s)' % str(lfns)
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
        retry_files = []
        for f in self.files:
            if not isLFN(f): continue
            try:
                result = f.replicate(destSE,srcSE,locCache)
            except:
                msg = 'Replication error for file %s (will retry in a bit).'\
                      % f.name
                logger.warning(msg)
                retry_files.append(f)
        for f in retry_files:
            try:
                result = f.replicate(destSE,srcSE,locCache)
            except:
                msg = '2nd replication attempt failed for file %s.' \
                      ' (will not retry)' % f.name
                logger.warning(msg)
                logger.warning(str(result))

    def extend(self,files,unique=False):
        '''Extend the dataset. If unique, then only add files which are not
        already in the dataset.'''
        if not hasattr(files,"__getitem__"):
            raise GangaException('Argument "files" must be a iterable.')
        
        names = self.getFileNames()
        files = [f for f in files] # just in case they extend w/ self
        for f in files:
            file = getDataFile(f)
            if file is None: file = f
            if unique and file.name in names: continue
            self.files.append(file)

    def removeFile(self,file):
        try:
            self.files.remove(file)
        except:
            raise GangaException('Dataset has no file named %s' % file.name)
        

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

    def getFullFileNames(self):
        'Returns all file names w/ PFN or LFN prepended.'
        names = []
        for f in self.files:
            if type(f) is LogicalFile: names.append('LFN:%s' % f.name)
            else: names.append('PFN:%s' % f.name)
        return names

    def getCatalog(self,site=''):
        '''Generates an XML catalog from the dataset (returns the XML string).
        Note: site defaults to config.LHCb.LocalSite
        Note: If the XMLCatalogueSlice attribute is set, then it returns
              what is written there.'''
        if self.XMLCatalogueSlice.name:
            f = open(self.XMLCatalogueSlice.name)
            xml_catalog = f.read()
            f.close()
            return xml_catalog
        if not site: site = getConfig('LHCb')['LocalSite']
        lfns = self.getLFNs()
        depth = self.depth
        tmp_xml = tempfile.NamedTemporaryFile(suffix='.xml')
        cmd = 'getInputDataCatalog(%s,%d,"%s","%s")' \
              % (str(lfns),depth,site,tmp_xml.name)
        result = get_result(cmd,'LFN->PFN error','XML catalog error.')
        xml_catalog = tmp_xml.read()
        tmp_xml.close()
        return xml_catalog

    def optionsString(self,file=None):
        'Returns the Gaudi-style options string for the dataset (if a filename' \
        ' is given, the file is created and output is written there).'
        if not self or len(self) == 0: return ''
        snew=''
        if self.persistency=='ROOT':
            snew='\n#new method\nfrom GaudiConf import IOExtension\nIOExtension(\"%s\").inputFiles([' % self.persistency
        elif self.persistency=='POOL': 
            snew='\ntry:\n    #new method\n    from GaudiConf import IOExtension\n    IOExtension(\"%s\").inputFiles([' % self.persistency
        elif self.persistency==None:
            snew='\ntry:\n    #new method\n    from GaudiConf import IOExtension\n    IOExtension().inputFiles(['
        else:
            logger.warning("Unknown LHCbDataset persistency technology... reverting to None")
            snew='\ntry:\n    #new method\n    from GaudiConf import IOExtension\n    IOExtension().inputFiles(['
            
        sold='\nexcept ImportError:\n    #Use previous method\n    from Gaudi.Configuration import EventSelector\n    EventSelector().Input=['
        sdatasetsnew=''
        sdatasetsold=''
        
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
            sdatasetsnew += '\n        '
            sdatasetsold += '\n        '
            if isLFN(f):
                sdatasetsnew += """ \"LFN:%s\",""" % f.name
                sdatasetsold += """ \"DATAFILE='LFN:%s' %s\",""" % (f.name, dtype_str)
            else:
                sdatasetsnew += """ \"PFN:%s\",""" % f.name
                sdatasetsold += """ \"DATAFILE='PFN:%s' %s\",""" % (f.name, dtype_str)
        if sdatasetsold.endswith(","):
            if self.persistency=='ROOT': sdatasetsnew = sdatasetsnew[:-1] + """\n], clear=True)"""
            else: sdatasetsnew = sdatasetsnew[:-1] + """\n    ], clear=True)"""
            sdatasetsold = sdatasetsold[:-1]
            sdatasetsold += """\n    ]"""
        if(file):
            f = open(file,'w')
            if self.persistency=='ROOT':
                f.write(snew)
                f.write(sdatasetsnew)
            else:
                f.write(snew)
                f.write(sdatasetsnew)
                f.write(sold)
                f.write(sdatasetsold)
            f.close()
        else:
            if self.persistency=='ROOT':
                return snew+sdatasetsnew
            else:
                return snew+sdatasetsnew+sold+sdatasetsold

    def difference(self,other):
        '''Returns a new data set w/ files in this that are not in other.'''
        other_files = other.getFullFileNames()
        files = set(self.getFullFileNames()).difference(other_files)
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return GPIProxyObjectFactory(data)

    def isSubset(self,other):
        '''Is every file in this data set in other?'''
        return set(self.getFileNames()).issubset(other.getFileNames())

    def isSuperset(self,other):
        '''Is every file in other in this data set?'''
        return set(self.getFileNames()).issuperset(other.getFileNames())

    def symmetricDifference(self,other):
        '''Returns a new data set w/ files in either this or other but not
        both.'''
        other_files = other.getFullFileNames()
        files = set(self.getFullFileNames()).symmetric_difference(other_files)
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return GPIProxyObjectFactory(data)

    def intersection(self,other):
        '''Returns a new data set w/ files common to this and other.'''
        other_files = other.getFullFileNames()
        files = set(self.getFullFileNames()).intersection(other_files)
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return GPIProxyObjectFactory(data)

    def union(self,other):
        '''Returns a new data set w/ files from this and other.'''
        files = set(self.getFullFileNames()).union(other.getFullFileNames())
        data = LHCbDataset()
        data.__construct__([list(files)])
        data.depth = self.depth
        return GPIProxyObjectFactory(data)

    def bkMetadata(self):
        'Returns the bookkeeping metadata for all LFNs. '        
        logger.info("Using BKQuery(bkpath).getDatasetMetadata() with bkpath=the bookkeeping path, will yeild more metadata such as 'TCK' info...")
        cmd = 'bkMetaData(%s)' % self.getLFNs()
        b =  get_result(cmd,'Error removing replica','Replica rm error.')
        return b
        

    #def pop(self,file):
    #    if type(file) is type(''): file = strToDataFile(file,False)
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

def string_datafile_shortcut(name,item):
    from GangaLHCb.Lib.Backends.Dirac import Dirac
    if type(name) is not type(''): return None
    if item is None: return None # used to be c'tor, but shouldn't happen now
    else: # something else...require pfn: or lfn:
        file = strToDataFile(name,False)
        if item is Dirac._schema['inputSandboxLFNs']:
            if type(file) is PhysicalFile:
                msg = 'Only LFNs can be placed in Dirac.inputSandboxLFNs!'
                raise GangaException(msg)
        return file
    return None

allComponentFilters['datafiles'] = string_datafile_shortcut

def string_dataset_shortcut(files,item):
    from GangaLHCb.Lib.Tasks.LHCbTransform import LHCbTransform
    inputdataList  = [Job._schema['inputdata'],
                      LHCbTransform._schema['inputdata'],
                      JobTemplate._schema['inputdata']
                      ]
    outputdataList = [Job._schema['outputdata'],
                      LHCbTransform._schema['outputdata'],
                      JobTemplate._schema['outputdata']
                      ]
    if type(files) is not type([]): return None
    if item in inputdataList:
        ds = LHCbDataset()
        ds.__construct__([files])
        return ds               
    elif item in outputdataList:
        return OutputData(files=files)
    else:
        return None # used to be c'tors, but shouldn't happen now

allComponentFilters['datasets'] = string_dataset_shortcut

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
