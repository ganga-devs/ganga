from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from GangaCMS.Lib.ConfParams import CMSSW, CRAB, GRID, USER


class CRABDataset(Dataset):
    """Dataset definition for CRAB jobsets."""
    schemadic = {}
    schemadic.update(CMSSW().schemadic)
    schemadic.update(CRAB().schemadic)
    schemadic.update(GRID().schemadic)
    schemadic.update(USER().schemadic)
    schemadic['target_site'] = SimpleItem(defvalue=None,
                                          typelist=['type(None)', 'str'],
                                          doc='Target site name for the job.')
    _schema = Schema(Version(1, 0), schemadic)
    _category = 'datasets'
    _name = 'CRABDataset'

    def __init__(self):
        super(CRABDataset, self).__init__()
