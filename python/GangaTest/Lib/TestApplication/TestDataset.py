from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *

# Test Dataset

class TestDataset(Dataset):
    _schema = Schema(Version(1,0), {'files':FileItem(defvalue="",sequence=1)})
    _category = 'datasets'
    _name = "TestDataset"

    def __init__(self):
        super(TestDataset, self).__init__()

    def isEmpty(self):
        return bool(self.files)

# a dataset which looks like a list
class TestListViewDataset(Dataset):
    _schema = Schema(Version(1,0), {'files':FileItem(defvalue="",sequence=1)})
    _category = 'datasets'
    _name = "TestListViewDataset"

    def __init__(self):
        super(TestListViewDataset, self).__init__()

    def isEmpty(self):
        return bool(self.files)

    def _object_filter__get__(self,obj):
        #return self.files
        return obj

from Ganga.GPIDev.Base.Filters import allComponentFilters

def list_assignment_shortcut(v,item):
    if type(v) is type([]):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        d = TestListViewDataset._proxyClass()
        d.files = v
        return d._impl
    else:
        return None
        
allComponentFilters['datasets'] = list_assignment_shortcut
    
