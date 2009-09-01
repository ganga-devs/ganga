#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from LHCbDataFile import LHCbDataFile
from LHCbDatasetUtils import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# LHCbDataset is a simple list of files (LFNs)
class LHCbDataset(Dataset):

    schema = {}
    schema['files'] = ComponentItem(category='datafiles',defvalue=[],
                                    sequence=1)
    docstr = 'The date the last full cache update was run.'
    schema['cache_date'] = SimpleItem(defvalue='', doc=docstr)
    docstr = 'True when the cache has never been updated before'
    schema['new_cache'] = SimpleItem(defvalue=True, doc=docstr , hidden=1)
    defvaluestr = """TYP='POOL_ROOTTREE' OPT='READ'"""
    docstr = 'The string that is added after the filename in the options ' + \
             'to tell Gaudi how to read the data. If reading raw data ' + \
             '(mdf files) it should be set to "SVC=\'LHCb::MDFSelector\'"'
    schema['datatype_string'] = SimpleItem(defvalue=defvaluestr, doc=docstr)
    docstr = 'Ancestor depth to be queried from the Bookkeeping system.'
    schema['depth'] = SimpleItem(defvalue = 1 , doc=docstr)
    docstr = 'Select an optional XMLCatalogueSlice to the dataset'
    schema['XMLCatalogueSlice']= FileItem(defvalue=None,doc=docstr)

    _schema = Schema(Version(2,4), schema)
    _category = 'datasets'
    _name = "LHCbDataset"
    _exportmethods = ['updateReplicaCache','__len__','cacheOutOfDate',
                      'hasLFNs','replicate','__getitem__','extend']

    def __init__(self, files=[]):
        super(LHCbDataset, self).__init__()
        self.files = files

    def __len__(self):
        """The number of files in the dataset."""
        result = 0
        if self.files:
            result = len(self.files)
        return result

    def __nonzero__(self):
        """Returns the logical value of a dataset. This is always True, as with
        an object."""
        return True

    def __getitem__(self,i):
        if type(i) == type(slice(0)):
            ds = LHCbDataset(files=self.files[i])
            ds.datatype_string = self.datatype_string
            ds.depth = self.depth
            ds.cache_date = self.cache_date
            ds.XMLCatalogueSlice = self.XMLCatalogueSlice
            return GPIProxyObjectFactory(ds)
        else:
            return GPIProxyObjectFactory(self.files[i])

    def _getFileNames(self):
        names = []
        for f in self.files: names.append(f.name)
        return names
        
    def cacheOutOfDate(self, maximum_cache_age=None):
        """Checks that the Dataset was updated less than maximum_cache_age
        minutes ago.

        If cache_expiry is None, then the value is taken from the LHCb section
        of the Ganga config. Returns True if the cache is invalid, and False
        otherwise. Zero or negative values of maximum_age_cache will cause the
        cache to be invalid."""

        result = True
        if not self.cache_date:
            return result

        import time
        cache_time = time.mktime(time.strptime(self.cache_date))
        time_now = time.time()
        #cache time should be in the past
        time_diff = time_now - cache_time

        if time_diff < 0:
            logger.warning('The cache_date is in the future and will be '\
                           'treated as unreliable.')
            return result

        if maximum_cache_age == None:
            #get from config
            maximum_cache_age = getCacheAge()

        if maximum_cache_age <= 0:
            msg = 'Invalid maximum_cache_age set - %d. This value must ' + \
                  'be greater than 0'
            logger.warning(msg, maximum_cache_age)
            return result
        
        if ((time_diff//60) - maximum_cache_age) < 0: result = False

        return result

    def isEmpty(self):
        return not bool(self.files)
    
    def updateReplicaCache(self, forceUpdate = False):
        """Updates the cache of replicas

        If forceUpdate is True then all lfns are updated,
        otherwise only those that don't contain replica info.

        If we are doing a full update then the cache_date will
        be updated.
        """

        #Savannah 40219
        if not forceUpdate and not self.cacheOutOfDate():
            return

        lfns=[]
        for f in self.files:
            if f.isLFN():
                #update only if none cached or force
                if not f.replicas or forceUpdate:
                    lfns.append(f._stripFileName())
        
        result = replicaCache(str(lfns))
        replicas = result['Value']['Successful']
        
        logger.debug('Replica information received is: ' + repr(replicas))
        for f in self.files:
            f_name = f._stripFileName()
            if replicas.has_key(f_name):
                f.replicas = replicas[f_name].keys()

        if forceUpdate or self.new_cache:
            #allows the cache to be invalidated
            import time
            self.cache_date = time.asctime()
            self.new_cache = False

    def hasLFNs(self):
        for f in self.files:
            if f.isLFN(): return True
        return False

    def replicate(self,destSE='',srcSE='',locCache=''):
        '''Replicate all LFNs to destSE.  For a list of valid SE\'s, type
        ds.replicate().'''
        if not destSE:
            self.files[0].replicate('')
            return
        if not self.hasLFNs():
            raise GangaException('Cannot replicate dataset w/ no LFNs.')
        for f in self.files:
            if not f.isLFN(): continue
            f.replicate(destSE,srcSE,locCache)

    def extend(self,files,unique=False):
        '''Extend the dataset. If unique, then only add files which are not
        already in the dataset.'''        
        if not hasattr(files,"__getitem__"):
            raise GangaException('Argument "files" must be a iterable.')
        names = self._getFileNames()
        for f in files:
            if type(f) is type(''):
                if unique and f in names: continue
                self.files.append(LHCbDataFile(name=f))
            else:
                if unique and f.name in names: continue
                fcopy = LHCbDataFile(name=f.name)
                fcopy.replicas = f.replicas
                self.files.append(fcopy)
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base.Filters import allComponentFilters

def string_dataset_shortcut(v,item):
    if type(v) is type([]):
        l=[]
        
        for i in v:
            if type(i) is type(''):
                f=LHCbDataFile()
                f.name=i
                l.append(f)
        ds=LHCbDataset()
        ds.files=l[:]
        return ds       
    return None

allComponentFilters['datasets']=string_dataset_shortcut

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
