#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import tempfile
import fnmatch
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Lib.Job.Job import Job,JobTemplate

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiDataset(Dataset):
    '''Class for handling Gaudi data sets.
    '''
    schema = {}
    docstr = 'List of PhysicalFile and LogicalFile objects'
    schema['files'] = SimpleItem(defvalue=[],typelist=['str','type(None)'],
                                 sequence=1,doc=docstr)
  
    _schema = Schema(Version(3,0), schema)
    _category = 'datasets'
    _name = "GaudiDataset"
    _exportmethods = ['__len__','__getitem__','__setitem__','__delitem__',
                      '__iter__','__reversed__','__contains__','__getslice__',
                      '__setslice__','__delslice__','__add__','__iadd__','append',
                      'extend','insert','remove','pop','index','count','sort',
                      'reverse']


    def __init__(self, files=[]):
        super(GaudiDataset, self).__init__()
        self.files=files

    def isEmpty(self):
        return len(self.files) == 0


    ## List wrapping methods.
    def __len__(self): return self.files.__len__()
    def __getitem__(self, key): return self.files.__getitem__(key)
    def __setitem__(self, key, value): return self.files.__setitem__(key, value)
    def __delitem__(self, key): return self.files.__delitem__(key)
    def __iter__(self): return self.files.__iter__()
    def __reversed__(self): return self.files.__reversed__()
    def __contains__(self, item): return self.files.__contains__(item)
    def __getslice__(self, i, j): return self.files.__getslice__(i, j)
    def __setslice__(self, i, j, sequence): return self.files.__setslice__(i, j, sequence)
    def __delslice__(self, i, j): return self.files.__delslice__(i, j)
    def __add__(self, other): return self.files.__add__(other)
    def __iadd__(self, other): return self.files.__iadd__(other)
    

    def append(x): return self.files.append(x)
    def extend(L): return self.files.extend(L)
    def insert(i, x): return self.files.insert(i, x)
    def remove(x): return self.files.remove(x)
    def pop(*i): return self.files.pop(*i)
    def index(x): return self.files.index(x)
    def count(x): return self.files.count(x)
    def sort(): return self.files.sort()
    def reverse(): return self.files.reverse()

        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base.Filters import allComponentFilters

def string_dataset_shortcut(files,item):
    filterList  = [Job._schema['inputdata'], Job._schema['outputdata']]
    if type(files) is not type([]): return None
    if item in inputdataList:
        ds = GaudiDataset(files)
        return ds               
    else:
        return None # used to be c'tors, but shouldn't happen now

allComponentFilters['datasets'] = string_dataset_shortcut

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
