
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem


class TaskChainInput(Dataset):

    """Dummy dataset to map the output of a transform to the input of another transform"""

    _schema = Schema(Version(1, 0), {
        'input_trf_id': SimpleItem(defvalue=-1, doc="Input Transform ID"),
        'single_unit': SimpleItem(defvalue=False, doc='Create a single unit from all inputs in the transform'),
        'use_copy_output': SimpleItem(defvalue=True, doc='Use the copied output instead of default output (e.g. use local copy instead of grid copy)'),
        'include_file_mask': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='List of Regular expressions of which files to include for input'),
        'exclude_file_mask': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='List of Regular expressions of which files to exclude for input'),
    })

    _category = 'datasets'
    _name = 'TaskChainInput'
    _exportmethods = []

    def __init__(self):
        super(TaskChainInput, self).__init__()
