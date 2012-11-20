
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *

class TaskChainInput(Dataset):
    """Dummy dataset to map the output of a transform to the input of another transform"""

    _schema = Schema(Version(1,0), {
        'input_trf_id'          : SimpleItem(defvalue = -1, doc="Input Transform ID" ),
        'single_unit'           : SimpleItem(defvalue = False, doc = 'Create a single unit from all inputs'),
        })
           
    _category = 'datasets'
    _name = 'TaskChainInput'
    _exportmethods = [ ]

    
    def __init__(self):
        super( TaskChainInput, self ).__init__()
