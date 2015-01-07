
# PhysicalFile is now a pseudonym for the LocalFile... This is for backwards
# compatability in the simplest of cases, where the user needs to do something more
# complicated they're encouraged to update their code

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import re
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File.IGangaFile         import IGangaFile
from Ganga.Utility.files import expandfilename
from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
#import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def full_expand_filename(name):
    if len(name) >= 4 and name[0:4].upper() == 'LFN:':
        msg = 'Can not create PhysicalFile from string that begins w/ "LFN:".'\
              ' You probably want to create a DiracFile.' 
        raise GangaException(msg)
    urlprefix=re.compile('^(([a-zA-Z_][\w]*:)+/?)?/')
    if len(name) >= 4 and name[0:4].upper() == 'PFN:': name = name[4:]
    expanded_name = expandfilename(name)
    if urlprefix.match(expanded_name): return expanded_name
    return os.path.abspath(expanded_name)
    
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class PhysicalFile(LocalFile):
    '''Class for handling physical files (i.e. PFNs)

    Example Usage:
    pfn = PhysicalFile("/some/pfn.file")
    pfn.upload("/some/lfn.file","CERN-USER") # upload the PFN to LFC
    [...etc...]
    '''
    _schema = Schema(Version(1,1),{'name':SimpleItem(defvalue='',doc='PFN')})
    _category='gangafiles'
    _name='PhysicalFile'
    _exportmethods = ['upload']

    def __init__(self, name=''):
        super(PhysicalFile,self).__init__()
        self.namePattern = full_expand_filename(name)
        self.name = self.namePattern

    def __construct__(self, args):
        if (len(args) != 1) or (type(args[0]) is not type('')):
            super(PhysicalFile,self).__construct__(args)
        else:    
            self.name = full_expand_filename(args[0])
         
    def _attribute_filter__set__(self,n,v):
        return full_expand_filename(v)
        
    def upload(self,lfn,diracSE,guid=None):
        'Upload PFN to LFC on SE "diracSE" w/ LFN "lfn".' 
        from GangaDirac.Lib.Backends.DiracUtils import get_result
        if guid is None:
            cmd = 'addFile("%s","%s","%s",None)' % \
                  (lfn,self.name,diracSE)
        else:
            cmd = 'addFile("%s","%s","%s","%s")' % \
                  (lfn,self.name,diracSE,guid)
        result = get_result(cmd,'Problem w/ upload','Error uploading file.')
        from GangaDirac.Lib.Files.DiracFile import DiracFile
        return GPIProxyObjectFactory(DiracFile(name=lfn))

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
