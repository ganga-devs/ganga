#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.files import expandfilename
from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
import Ganga.Utility.logging
from Ganga.Utility.Config import getConfig
from GangaDirac.Lib.Backends.DiracUtils import get_result
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def get_dirac_space_tokens():
    return getConfig('DIRAC')['DiracSpaceTokens']
#    return ['CERN-USER','CNAF-USER','GRIDKA-USER','IN2P3-USER','NIKHEF-USER',
#            'PIC-USER','RAL-USER']

#def get_result(cmd,log_msg,except_msg, eval_includes=''):
#    from GangaDirac.Lib.Backends.DiracUtils import result_ok
#    from GangaDirac.BOOT import dirac_ganga_server
#    result = dirac_ganga_server.execute(cmd,eval_includes=eval_includes)

#    print result
#    if not result_ok(result):#, extra_imports):
#        logger.warning('%s: %s' % (log_msg,str(result)))
#        raise GangaException(except_msg)
#    return result

def strip_filename(name):
    if len(name) >= 4 and name[0:4].upper() == 'PFN:':
        msg = 'Can not create LogicalFile from string that begins w/ "PFN:".'\
              ' You probably want to create a PhysicalFile.' 
        raise GangaException(msg)
    if len(name) >= 4 and name[0:4].upper() == 'LFN:': name = name[4:]
    return name

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LogicalFile(GangaObject):
    '''Class for handling logical files (i.e. LFNs)

    Example Usage:
    lfn = LogicalFile("/some/lfn.file")
    lfn.download("/some/path") # downloads the LFN to dir "/some/path"
    lfn.getReplicas() # returns all replicas of LFN
    lfn.replicate("CERN-USER") # replicate LFN on SE "CERN-USER"
    [...etc...]
    '''
    _schema = Schema(Version(1,0),{'name':SimpleItem(defvalue='',doc='LFN')})
    _category='datafiles'
    _name='LogicalFile'
    _exportmethods = ['replicate','download','remove','removeReplica',
                      'getMetadata','getReplicas','bkMetadata']

    def __init__(self,name=''):
        super(LogicalFile,self).__init__()
        self.name = strip_filename(name)

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(LogicalFile,self).__construct__(args)
        else:    
            self.name = strip_filename(args[0])
            
    def _attribute_filter__set__(self,n,v):
        return strip_filename(v)
            
    def getReplicas(self):
        'Returns replicas for the LFN.'
        cmd = 'getReplicas("%s")' % self.name
        result = get_result(cmd,'LFC query error','Could not get replicas.')
        replicas = result['Value']['Successful']
        if replicas.has_key(self.name): return replicas[self.name]
        return []
              
    def download(self,dir='.'):
        'Downloads the LFN to dir (default is current directory).'
        dir = expandfilename(dir)
        dir = os.path.abspath(dir)
        logger.error("LogicalFile.download method is deprecated as this functionality should be provided by DiracFile")
        logger.error("To get a file based on this LFN please use the following:")
        logger.error("     DiracFile(lfn='%s',localDir='%s').get()"%(self.name, dir))
        return
#        cmd = 'getFile("%s","%s")' % (self.name,dir)
#        result = get_result(cmd,'Problem during download','Download error.')
#        from PhysicalFile import PhysicalFile
#        return GPIProxyObjectFactory(PhysicalFile(name=result['Value']))

    def remove(self):
        'Removes the LFN (and all replicas) from the LFC.'
        cmd = 'removeFile("%s")' % self.name
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
        cmd = 'replicateFile("%s","%s","%s","%s")' % \
              (self.name,destSE,srcSE,locCache)
        return get_result(cmd,'Replication error','Error replicating file.')

    def removeReplica(self,diracSE):
        'Removes replica of LFN from diracSE.'
        cmd = 'removeReplica("%s","%s")' % \
              (self.name,diracSE)
        return get_result(cmd,'Error removing replica','Replica rm error.')

    def getMetadata(self):
        'Returns the metadata for the LFN (e.g. creation time, etc.).'
        cmd = 'getMetadata("%s")' % self.name
        result = get_result(cmd,'Error w/ metadata','Could not get metadata.')
        metadata = result['Value']['Successful']
        if metadata.has_key(self.name): return metadata[self.name]
        return {}        

    def bkMetadata(self):
        'Returns the bookkeeping meta-data for this file.'
        cmd = 'bkMetaData(["%s"])' % self.name
        r = get_result(cmd,'Error removing replica','Replica rm error.')
        return r['Value'].get(self.name,{})

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
