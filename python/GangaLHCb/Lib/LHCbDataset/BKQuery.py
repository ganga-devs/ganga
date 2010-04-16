#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Base import GangaObject
from LogicalFile import *
from LHCbDataset import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BKQuery(GangaObject):
    '''Class for handling LHCb bookkeeping queries.

    Example Usage:
    bkq = BKQuery("/some/bk/path/")
    data = bkq.getDataset()
    '''
    schema = {}
    docstr = 'Bookkeeping query path'
    schema['path'] = SimpleItem(defvalue='' ,doc=docstr)
    _schema = Schema(Version(1,0), schema)
    _category = ''
    _name = "BKQuery"
    _exportmethods = ['getDataset']

    def __init__(self, path=''):
        super(BKQuery, self).__init__()
        self.path = path

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(BKQuery,self).__construct__(args)
        else:
            self.path = args[0]

    def getDataset(self):
        '''Gets the dataset from the bookkeeping for current path.'''
        if not self.path: return None
        cmd = 'result = DiracCommands.getDataset("%s")' % self.path
        result = get_result(cmd,'BK query error.','BK query error.')
        files = result['Value']
        ds = LHCbDataset()
        for f in files: ds.files.append(LogicalFile(f))
        return GPIProxyObjectFactory(ds)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
