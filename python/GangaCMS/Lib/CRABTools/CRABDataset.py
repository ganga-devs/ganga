#
# CRAB Dataset containing all possible parameters
# grupped into four categories (CMSSW,CRAB,GRID,USER)
#
# 08/06/10 @ ubeda
#

from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *

from GangaCMS.Lib.ConfParams import *

class CRABDataset(Dataset):

    schemadic={}
    schemadic.update(CMSSW().schemadic)
    schemadic.update(CRAB().schemadic)
    schemadic.update(GRID().schemadic)
    schemadic.update(USER().schemadic)
    schemadic['target_site'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='Target site name for the job. Intented to use only on HammerCloud.')

    _schema   = Schema(Version(1,0), schemadic) 
    _category = 'datasets'
    _name     = 'CRABDataset'
                    
    def __init__(self):
        super(CRABDataset,self).__init__()
                
        
