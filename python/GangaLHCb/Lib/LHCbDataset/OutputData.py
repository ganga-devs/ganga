#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Base import GangaObject

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class OutputData(GangaObject):

    schema = {}
    schema['files'] = SimpleItem(defvalue=[],typelist=['str'],sequence=1)
    _schema = Schema(Version(1,0), schema)
    _category = 'datasets'
    _name = "OutputData"
    _exportmethods = ['__len__','__getitem__']

    def __init__(self, files=[]):
        super(OutputData, self).__init__()
        self.files = files

    def _auto__init__(self):
        files = []
        for f in self.files:
            if hasattr(f,'name'):
                files.append(f.name.replace('OUTPUTDATA:/',''))
            else: files.append(f)
        self.files = files

    def __len__(self):
        """The number of files in the dataset."""
        result = 0
        if self.files: result = len(self.files)
        return result

    def __nonzero__(self):
        """This is always True, as with an object."""
        return True

    def __getitem__(self,i):
        if type(i) == type(slice(0)):
            return GPIProxyObjectFactory(OutputData(files=self.files[i]))
        else:
            return self.files[i]

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
