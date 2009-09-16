#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.files import expandfilename
from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def get_dirac_space_tokens():
    return ['CERN-USER','CNAF-USER','GRIDKA-USER','IN2P3-USER','NIKHEF-USER',
            'PIC-USER','RAL-USER']

def get_result(cmd,log_msg,except_msg):
    from GangaLHCb.Lib.DIRAC.Dirac import Dirac
    from GangaLHCb.Lib.DIRAC.DiracUtils import result_ok
    result = Dirac.execAPI(cmd)    
    if not result_ok(result):
        logger.warning('%s: %s' % (log_msg,str(result)))
        raise GangaException(except_msg)
    return result

def strip_filename(name):
    if len(name) >= 4 and name[0:4].upper() == 'PFN:':
        msg = 'Can not create LogicalFile from string that begins w/ "PFN:".'\
              ' You probably want to create a PhysicalFile.' 
        raise GangaException(msg)
    if len(name) >= 4 and name[0:4].upper() == 'LFN:': name = name[4:]
    return name

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LogicalFile(GangaObject):

    _schema = Schema(Version(1,0),{'name':SimpleItem(defvalue='',doc='LFN.')})
    _category='datafiles'
    _name='LogicalFile'
    _exportmethods = ['replicate','download','remove','removeReplica',
                      'getMetadata','getReplicas']

    def __init__(self,name=''):        
        super(LogicalFile,self).__init__()
        self.name = strip_filename(name)

    def _auto__init__(self):        
        if self.name: self.name = strip_filename(self.name)

    def _attribute_filter__set__(self,n,v):
        return strip_filename(v)
            
    def getReplicas(self):
        cmd = 'result = DiracCommands.getReplicas("%s")' % self.name
        result = get_result(cmd,'LFC query error','Could not get replicas.')
        replicas = result['Value']['Successful']
        if replicas.has_key(self.name): return replicas[self.name]
        return []
              
    def download(self,dir='.'):
        dir = expandfilename(dir)
        dir = os.path.abspath(dir)
        cmd = 'result = DiracCommands.getFile("%s","%s")' % (self.name,dir)
        result = get_result(cmd,'Problem during download','Download error.')
        from PhysicalFile import PhysicalFile
        return GPIProxyObjectFactory(PhysicalFile(name=result['Value']))

    def remove(self): 
        cmd = 'result = DiracCommands.removeFile("%s")' % self.name
        return get_result(cmd,'Problem during remove','Could not rm file.')

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
        cmd = 'result = DiracCommands.replicateFile("%s","%s","%s","%s")' % \
              (self.name,destSE,srcSE,locCache)
        return get_result(cmd,'Replication error','Error replicating file.')

    def removeReplica(self,diracSE):
        cmd = 'result = DiracCommands.removeReplica("%s","%s")' % \
              (self.name,diracSE)
        return get_result(cmd,'Error removing replica','Replica rm error.')

    def getMetadata(self):
        cmd = 'result = DiracCommands.getMetadata("%s")' % self.name
        result = get_result(cmd,'Error w/ metadata','Could not get metadata.')
        metadata = result['Value']['Successful']
        if metadata.has_key(self.name): return metadata[self.name]
        return {}        

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
