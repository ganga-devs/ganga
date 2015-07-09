#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.Core import GangaException
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Lib.File import *

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def isLFN(file):
    return isDiracFile(file)

def isDiracFile(file):
    from GangaLHCb.Lib.Files import LogicalFile
    return isinstance(file,DiracFile) or isinstance(file,LogicalFile)

def isPFN(file):
    from GangaLHCb.Lib.Files import PhysicalFile
    return isinstance(file,PhysicalFile) or isinstance(file,LocalFile) or isinstance(file,MassStorageFile)

def strToDataFile(name, allowNone=True):
    if len(name) >= 4 and name[0:4].upper() == 'LFN:':
        return DiracFile(lfn=name[4:])
    elif len(name) >= 4 and name[0:4].upper() == 'PFN:':
        from GangaLHCb.Lib.Files import PhysicalFile
        return PhysicalFile(name)
    else:
        if not allowNone:
            msg = 'Can only convert strings that begin w/ PFN: or '\
                  'LFN: to data files.'\
                  ' Name is: %s' % name
            raise GangaException(msg)
        return None

def getDataFile(file):
    if isinstance(file,DiracFile): return file
    from GangaLHCb.Lib.Files import PhysicalFile
    if isinstance(file,PhysicalFile): return file
    if type(file) == type(''): return strToDataFile(file)
    return None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
