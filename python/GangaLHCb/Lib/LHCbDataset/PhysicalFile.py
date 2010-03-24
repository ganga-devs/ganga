#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import re
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.files import expandfilename
from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def full_expand_filename(name):
    if len(name) >= 4 and name[0:4].upper() == 'LFN:':
        msg = 'Can not create PhysicalFile from string that begins w/ "LFN:".'\
              ' You probably want to create a LogicalFile.' 
        raise GangaException(msg)
    urlprefix=re.compile('^(([a-zA-Z_][\w]*:)+/?)?/')
    if len(name) >= 4 and name[0:4].upper() == 'PFN:': name = name[4:]
    expanded_name = expandfilename(name)
    if urlprefix.match(expanded_name): return expanded_name
    return os.path.abspath(expanded_name)
    
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class PhysicalFile(GangaObject):
    '''Class for handling physical files (i.e. PFNs)

    Example Usage:
    pfn = PhysicalFile("/some/pfn.file")
    pfn.upload("/some/lfn.file","CERN-USER") # upload the PFN to LFC
    [...etc...]
    '''
    _schema = Schema(Version(1,0),{'name':SimpleItem(defvalue='',doc='PFN')})
    _category='datafiles'
    _name='PhysicalFile'
    _exportmethods = ['upload']

    def __init__(self,name=''):        
        super(PhysicalFile,self).__init__()
        self.name = full_expand_filename(name)

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(PhysicalFile,self).__construct__(args)
        else:    
            self.name = full_expand_filename(args[0])
         
    def _attribute_filter__set__(self,n,v):
        return full_expand_filename(v)
        
    def upload(self,lfn,diracSE,guid=None):
        'Upload PFN to LFC on SE "diracSE" w/ LFN "lfn".' 
        from LogicalFile import get_result
        if guid is None:
            cmd = 'result = DiracCommands.addFile("%s","%s","%s",None)' % \
                  (lfn,self.name,diracSE)
        else:
            cmd = 'result = DiracCommands.addFile("%s","%s","%s","%s")' % \
                  (lfn,self.name,diracSE,guid)
        result = get_result(cmd,'Problem w/ upload','Error uploading file.')
        from LogicalFile import LogicalFile
        return GPIProxyObjectFactory(LogicalFile(name=lfn))

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
