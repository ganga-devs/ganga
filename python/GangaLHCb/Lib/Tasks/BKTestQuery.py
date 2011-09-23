#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
#from Ganga.GPIDev.Base import GangaObject
#from LogicalFile import *
#from LHCbDataset import *
#from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from GangaLHCb.Lib.LHCbDataset.BKQuery import *

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BKTestQuery(BKQuery):
##     schema = {}
##     docstr = 'Bookkeeping query path (type dependent)'
##     schema['path'] = SimpleItem(defvalue='' ,doc=docstr)
##     docstr = 'Start date string yyyy-mm-dd (only works for type="RunsByDate")'
##     schema['startDate'] = SimpleItem(defvalue='' ,doc=docstr)
##     docstr = 'End date string yyyy-mm-dd (only works for type="RunsByDate")'
##     schema['endDate'] = SimpleItem(defvalue='' ,doc=docstr)
##     docstr = 'Data quality flag (string or list of strings).'
##     schema['dqflag'] = SimpleItem(defvalue='All',typelist=['str','list'],
##                                   doc=docstr)
##     docstr = 'Type of query (Path, RunsByDate, Run, Production)'
##     schema['type'] = SimpleItem(defvalue='Path',doc=docstr)
##     docstr = 'Selection criteria: Runs, ProcessedRuns, NotProcessed (only \
##     works for type="RunsByDate")'
##     schema['selection'] = SimpleItem(defvalue='',doc=docstr)
    _schema = BKQuery._schema.inherit_copy()
    _schema.datadict['dataset']  =  ComponentItem('datasets', defvalue=None, optional=1, load_default=False,doc='dataset',hidden=0)
    _category = 'query'
    _name = "BKTestQuery"
    _exportmethods = BKQuery._exportmethods

##     def __init__(self, path=''):
##         super(BKQuery, self).__init__()
##         self.path = path

##     def __construct__(self, args):
##         if (len(args) != 1) or (type(args[0]) is not type('')):
##             super(BKQuery,self).__construct__(args)
##         else:
##             self.path = args[0]

    def getDataset(self):
        ds = super(BKTestQuery,self).getDataset()
        if self.dataset is None:
            self.dataset = LHCbDataset(ds.files[:3])
        else:
            import sets
            a=set(self.dataset.files)
            b=set(ds.files)
            self.dataset.files += list(b.difference(a))[:3]
        return self.dataset
        
##     def _getDataset(self):
##         '''Gets the dataset from the bookkeeping for current path, etc.'''
##         if not self.path: return None
##         if not self.type in ['Path','RunsByDate','Run','Production']:
##             raise GangaException('Type="%s" is not valid.' % self.type)
##         if not self.type is 'RunsByDate':
##             if self.startDate:
##                 msg = 'startDate not supported for type="%s".' % self.type
##                 raise GangaException(msg)
##             if self.endDate:
##                 msg = 'endDate not supported for type="%s".' % self.type
##                 raise GangaException(msg)
##             if self.selection:
##                 msg = 'selection not supported for type="%s".' % self.type
##                 raise GangaException(msg)            
##         cmd = 'result = DiracCommands.getDataset("%s","%s","%s","%s","%s",\
##         "%s")' % (self.path,self.dqflag,self.type,self.startDate,self.endDate,
##                   self.selection)
##         if type(self.dqflag) == type([]):
##             cmd = 'result = DiracCommands.getDataset("%s",%s,"%s","%s","%s",\
##             "%s")' % (self.path,self.dqflag,self.type,self.startDate,
##                      self.endDate,self.selection)
##         result = get_result(cmd,'BK query error.','BK query error.')
##         files = []
##         value = result['Value']
##         if value.has_key('LFNs'): files = value['LFNs']
##         metadata = {}
##         if not type(files) is list:
##             if files.has_key('LFNs'): # i.e. a dict of LFN:Metadata
##                 metadata = files['LFNs'].copy()
##                 files = files['LFNs'].keys()
        
##         ds = LHCbDataset()
##         for f in files: ds.files.append(LogicalFile(f))
        
##         if metadata:
##             ds.metadata = {'OK':True,'Value':metadata}
        
##         return GPIProxyObjectFactory(ds)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
