#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Base import GangaObject
from LogicalFile import *
from LHCbDataset import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BKQuery(GangaObject):
    '''Class for handling LHCb bookkeeping queries.

    Currently 4 types of queries are supported: Path, RunsByDate, Run and
    Production.  These correspond to the Dirac API methods
    DiracLHCb.bkQuery<type> (see Dirac docs for details).  


    Path formats are as follows:

    type = "Path":
    /<ConfigurationName>/<Configuration Version>/\
<Sim or Data Taking Condition>/<Processing Pass>/<Event Type>/<File Type>

    type = "RunsByDate":
     /<ConfigurationName>/<Configuration Version>/<Processing Pass>/\
<Event Type>/<File Type> 

    type = "Run":
    /<Run Number>/<Processing Pass>/<Event Type>/<File Type>
    - OR -
    /<Run Number 1>-<Run Number 2>/<Processing Pass>/<Event Type>/<File Type>
    
    type = "Production":
    /<ProductionID>/<Processing Pass>/<Event Type>/<File Type>

    Example Usage:

    bkq = BKQuery (
    dqflag = "All" ,
    path = "/LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + \
RecoToDST-07/90000000/DST" ,
    type = "Path" 
    ) 

    bkq = BKQuery (
    startDate = "2010-05-18" ,
    selection = "Runs" ,
    endDate = "2010-05-20" ,
    dqflag = "All" ,
    path = "/LHCb/Collision10/Real Data/90000000/RAW" ,
    type = "RunsByDate" 
    ) 
    
    bkq = BKQuery (
    dqflag = "All" ,
    path = "/63566-63600/Real Data + RecoToDST-07/90000000/DST" ,
    type = "Run" 
    ) 

    bkq = BKQuery (
    dqflag = "All" ,
    path = "/5842/Real Data + RecoToDST-07/90000000/DST" ,
    type = "Production" 
    ) 

    then (for any type) one can get the data set by doing the following:
    data = bkq.getDataset()

    This will query the bookkeeping for the up-to-date version of the data.
    N.B. BKQuery objects can be stored in your Ganga box.

    '''
    schema = {}
    docstr = 'Bookkeeping query path (type dependent)'
    schema['path'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'Start date string yyyy-mm-dd (only works for type="RunsByDate")'
    schema['startDate'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'End date string yyyy-mm-dd (only works for type="RunsByDate")'
    schema['endDate'] = SimpleItem(defvalue='' ,doc=docstr)
    docstr = 'Data quality flag (string or list of strings).'
    schema['dqflag'] = SimpleItem(defvalue='OK',typelist=['str','list'],
                                  doc=docstr)
    docstr = 'Type of query (Path, RunsByDate, Run, Production)'
    schema['type'] = SimpleItem(defvalue='Path',doc=docstr)
    docstr = 'Selection criteria: Runs, ProcessedRuns, NotProcessed (only \
    works for type="RunsByDate")'
    schema['selection'] = SimpleItem(defvalue='',doc=docstr)
    _schema = Schema(Version(1,2), schema)
    _category = 'query'
    _name = "BKQuery"
    _exportmethods = ['getDataset','getDatasetMetadata']

    def __init__(self, path=''):
        super(BKQuery, self).__init__()
        self.path = path

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(BKQuery,self).__construct__(args)
        else:
            self.path = args[0]

    def getDatasetMetadata(self):
        '''Gets the dataset from the bookkeeping for current path, etc.'''
        if not self.path: return None
        if not self.type in ['Path','RunsByDate','Run','Production']:
            raise GangaException('Type="%s" is not valid.' % self.type)
        if not self.type is 'RunsByDate':
            if self.startDate:
                msg = 'startDate not supported for type="%s".' % self.type
                raise GangaException(msg)
            if self.endDate:
                msg = 'endDate not supported for type="%s".' % self.type
                raise GangaException(msg)
            if self.selection:
                msg = 'selection not supported for type="%s".' % self.type
                raise GangaException(msg) 
        cmd = "getDataset('%s','%s','%s','%s','%s',\
        '%s')" % (self.path,self.dqflag,self.type,self.startDate,self.endDate,
                  self.selection)
        if type(self.dqflag) == type([]):
            cmd = "getDataset('%s',%s,'%s','%s','%s',\
            '%s')" % (self.path,self.dqflag,self.type,self.startDate,
                     self.endDate,self.selection)
        result = get_result(cmd,'BK query error.','BK query error.', 'datetime')
        files = []
        metadata = {}
        value = result['Value']
        if value.has_key('LFNs'): files = value['LFNs']
        if not type(files) is list: # i.e. a dict of LFN:Metadata
            #if files.has_key('LFNs'): # i.e. a dict of LFN:Metadata
            metadata = files.copy()
        
        if metadata:
            return {'OK':True,'Value':metadata}
       
        return {'OK':False,'Value':metadata}

    def getDataset(self):
        '''Gets the dataset from the bookkeeping for current path, etc.'''
        if not self.path: return None
        if not self.type in ['Path','RunsByDate','Run','Production']:
            raise GangaException('Type="%s" is not valid.' % self.type)
        if not self.type is 'RunsByDate':
            if self.startDate:
                msg = 'startDate not supported for type="%s".' % self.type
                raise GangaException(msg)
            if self.endDate:
                msg = 'endDate not supported for type="%s".' % self.type
                raise GangaException(msg)
            if self.selection:
                msg = 'selection not supported for type="%s".' % self.type
                raise GangaException(msg)            
        cmd = "getDataset('%s','%s','%s','%s','%s',\
        '%s')" % (self.path,self.dqflag,self.type,self.startDate,self.endDate,
                  self.selection)
        if type(self.dqflag) == type([]):
            cmd = "getDataset('%s',%s,'%s','%s','%s',\
            '%s')" % (self.path,self.dqflag,self.type,self.startDate,
                     self.endDate,self.selection)
        result = get_result(cmd,'BK query error.','BK query error.')
        files = []
        value = result['Value']
        if value.has_key('LFNs'): files = value['LFNs']
        if not type(files) is list: # i.e. a dict of LFN:Metadata
            #if files.has_key('LFNs'): # i.e. a dict of LFN:Metadata
            files = files.keys()
        
        ds = LHCbDataset()
        for f in files: ds.files.append(LogicalFile(f))
        
        return GPIProxyObjectFactory(ds)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class BKQueryDict(GangaObject):
    """Class for handling LHCb bookkeeping queries using dictionaries.
    
    Use BKQuery if you do not know how to use BK dictionaries!
    
    Example Usage:
    
    bkqd = BKQueryDict()
    bkqd.dict['ConfigVersion'] = 'Collision09'
    bkqd.dict['ConfigName'] = 'LHCb'
    bkqd.dict['ProcessingPass'] = 'Real Data + RecoToDST-07'
    bkqd.dict['EventType'] = '90000000'
    bkqd.dict['FileType'] = 'DST'
    bkqd.dict['DataTakingConditions'] = 'Beam450GeV-VeloOpen-MagDown'
    data = bkqd.getDataset()
    """
    
    _bkQueryTemplate = {'SimulationConditions'     : 'All',
                        'DataTakingConditions'     : 'All',
                        'ProcessingPass'           : 'All',
                        'FileType'                 : 'All',
                        'EventType'                : 'All',
                        'ConfigName'               : 'All',
                        'ConfigVersion'            : 'All',
                        'ProductionID'             :     0,
                        'StartRun'                 :     0,
                        'EndRun'                   :     0,
                        'DataQualityFlag'          : 'All'}
    
    schema = {}
    docstr = 'Dirac BK query dictionary.'
    schema['dict'] = SimpleItem(defvalue=_bkQueryTemplate,#typelist=['dict'],
                                doc=docstr)
    _schema = Schema(Version(1,0), schema)
    _category = ''
    _name = "BKQueryDict"
    _exportmethods = ['getDataset','getDatasetMetadata']

    def __init__(self):
        super(BKQueryDict, self).__init__()

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type({})):
            super(BKQueryDict,self).__construct__(args)
        else:
            self.dict = args[0]
            
    def getDatasetMetadata(self):
        '''Gets the dataset from the bookkeeping for current dict.'''
        if not self.dict: return None
        cmd = 'bkQueryDict(%s)' % self.dict
        result = get_result(cmd,'BK query error.','BK query error.', 'datetime')
        files = []
        value = result['Value']
        if value.has_key('LFNs'): files = value['LFNs']
        metadata = {}
        if not type(files) is list:
            if files.has_key('LFNs'): # i.e. a dict of LFN:Metadata
                metadata = files['LFNs'].copy()
        
        if metadata:
            return {'OK':True,'Value':metadata}
        return {'OK':False,'Value':metadata}

    def getDataset(self):
        '''Gets the dataset from the bookkeeping for current dict.'''
        if not self.dict: return None
        cmd = 'bkQueryDict(%s)' % self.dict
        result = get_result(cmd,'BK query error.','BK query error.')
        files = []
        value = result['Value']
        if value.has_key('LFNs'): files = value['LFNs']
        if not type(files) is list:
            if files.has_key('LFNs'): # i.e. a dict of LFN:Metadata
                files = files['LFNs'].keys()
        
        ds = LHCbDataset()
        for f in files: ds.files.append(LogicalFile(f))
         
        return GPIProxyObjectFactory(ds)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
