#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from LHCbDatasetUtils import *
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbDataFile(GangaObject):

    schema = {}
    schema['name'] = SimpleItem(defvalue='',
                                doc='name of the LHCb data file. A string')
    schema['replicas'] = SimpleItem(defvalue=[],sequence=1,
                                    doc='Cached replicas of the datafile',
                                    typelist= ['str'])
    
    _schema = Schema(Version(1,0),schema)

    _category='datafiles'
    _name='LHCbDataFile'
    _exportmethods = ['updateReplicaCache','isLFN']

    def __init__(self,name=''):        
        super(LHCbDataFile,self).__init__()
        self.name = name
        
    def updateReplicaCache(self):
        """Updates the cache of replicas"""

        if self.isLFN():
            result = replicaCache('\'%s\'' % self.name)
            if result is None: return # don't do anything more
            
            replicas = result['Value']['Successful']
            logger.debug('Replica information received is: ' + repr(replicas))
            name = self._stripFileName()
            if replicas.has_key(name):
                self.replicas = replicas[name].keys() 
        else:
            self.replicas = []

    def _stripFileName(self):
        lname = self.name.lower()
        if lname.startswith('lfn:'):
            return self.name[4:]
        return self.name

    def isLFN(self):
        lname = self.name.lower()
        if lname.startswith('lfn:'):
            return True
        else:
            return False

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base.Filters import allComponentFilters

def string_datafile_shortcut(v,item):
    if type(v) is type(''):
        f=LHCbDataFile()
        f.name=v
        return f
    return None

allComponentFilters['datafiles']=string_datafile_shortcut

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
 
