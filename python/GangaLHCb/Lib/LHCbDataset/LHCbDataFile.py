#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from LHCbDatasetUtils import *
import Ganga.Utility.logging
from Ganga.Core import GangaException

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
    _exportmethods = ['updateReplicaCache','isLFN','replicate','removeReplica']

    def __init__(self,name=''):        
        super(LHCbDataFile,self).__init__()
        self.name = name
        
    def updateReplicaCache(self):
        """Updates the cache of replicas"""

        if self.isLFN():
            result = replicaCache('"%s"'%self._stripFileName())
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

    def replicate(self,destSE='',srcSE='',locCache=''):
        '''Replicate this file to destSE.  For a list of valid SE\'s, type
        file.replicate().'''
        tokens = get_dirac_space_tokens()
        if not destSE:
            print "Please choose SE from:",tokens
            return
        if destSE not in tokens:
            msg = '"%s" is not a valid space token. Please choose from: %s' \
                  % (destSE,str(tokens))
            raise GangaException(msg)
        if not self.isLFN():
            raise GangaException('Cannot replicate file (it is not an LFN).')
        replicateFile(self._stripFileName(),destSE,srcSE,locCache)

    def removeReplica(self,sE):
        '''Remove replica of this file from sE.'''
        if not self.isLFN():
            raise GangaException('Cannot rm replica (file is not an LFN).')
        removeReplica(self._stripFileName(),sE)
        
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
 
