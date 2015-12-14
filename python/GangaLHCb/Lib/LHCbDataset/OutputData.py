#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Base import GangaObject
from LHCbDatasetUtils import strToDataFile

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class OutputData(GangaObject):

    '''Class for handling outputdata for LHCb jobs.

    Example Usage:
    od = OutputData(["file.1","file.2"])
    od[0] # "file.1"
    [...etc...]
    '''
    schema = {}
    schema['files'] = SimpleItem(defvalue=[], typelist=['str'], sequence=1)
    schema['location'] = SimpleItem(defvalue='', typelist=['str'])
    _schema = Schema(Version(1, 1), schema)
    _category = 'datasets'
    _name = "OutputData"
    _exportmethods = ['__len__', '__getitem__']

    def __init__(self, files=None):
        if files is None:
            files = []
        super(OutputData, self).__init__()
        self.files = files

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) not in [list, tuple]):
            super(OutputData, self).__construct__(args)
        else:
            self.files = args[0]

    def __len__(self):
        """The number of files in the dataset."""
        result = 0
        if self.files:
            result = len(self.files)
        return result

    def __nonzero__(self):
        """This is always True, as with an object."""
        return True

    def __getitem__(self, i):
        '''Proivdes scripting (e.g. od[2] returns the 3rd file name) '''
        if type(i) == type(slice(0)):
            return GPIProxyObjectFactory(OutputData(files=self.files[i]))
        else:
            return self.files[i]

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
