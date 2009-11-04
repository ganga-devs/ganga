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
    _exportmethods = ['updateReplicaCache','isLFN','replicate']

    def __init__(self,name=''):        
        super(LHCbDataFile,self).__init__()
        self.name = name
        logger.warning("LHCbDataFile is depricated! It has been replaced by "\
                       "LogicalFile and PhysicalFile (see documentation for "\
                       "help).")
                
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

