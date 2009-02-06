from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


class MockDataFile(GangaObject):
    _schema = Schema(Version(1,0),{'name':SimpleItem(defvalue='',doc='''name of a data file. A string'''),
                                   'replicas':SimpleItem(defvalue=[],sequence=1,doc='''Cached replicas of the datafile''',typelist= ['str'])})
    _category='datafiles'
    _name='MockDataFile'
    _exportmethods = ['updateReplicaCache']

    def __init__(self, name = '',replicas = []):
        super(MockDataFile,self).__init__()
        self.name = name
        self.replicas = replicas
        
    def updateReplicaCache(self):
        """Updates the cache of replicas"""
        pass
        
    def isLFN(self):
        if self.name.startswith('PFN:'):
            return False
        else:
            return True

class MockDataset(Dataset):
    _schema = Schema(Version(1,0), {
        'files':ComponentItem(category='datafiles',defvalue=[],sequence=1),
        'cache_date': SimpleItem(defvalue = '', doc = 'The date the last full cache update was run.'),
        'new_cache' : SimpleItem(defvalue = True, doc = 'True when the cache has never been updated before',hidden = 1),
        'depth' : SimpleItem(defvalue = 1)
        })
    _category = 'datasets'
    _name = "MockDataset"
    _exportmethods = ['updateReplicaCache','cacheOutOfDate']

    def __init__(self,files=[],cache_date = '', new_cache = True):
        super(MockDataset, self).__init__()
        self.files = []
        self.cache_date = cache_date
        self.new_cache = new_cache

    def cacheOutOfDate(self, maximum_cache_age = None):
        return True

    def isEmpty(self):
        return bool(self.files)
    
    def updateReplicaCache(self, forceUpdate = False):
        pass
