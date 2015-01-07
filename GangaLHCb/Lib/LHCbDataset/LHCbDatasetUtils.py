#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.Core import GangaException
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaLHCb.Lib.Files.LogicalFile import LogicalFile
from GangaLHCb.Lib.Files.PhysicalFile import PhysicalFile

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def isDiracFile(file): return isinstance(file,DiracFile) or isinstance(file,LogicalFile)

def isPFN(file): return isinstance(file,PhysicalFile)

def strToDataFile(name,allowNone=True):
    if len(name) >= 4 and name[0:4].upper() == 'LFN:':
        return DiracFile(lfn = name)
    elif len(name) >= 4 and name[0:4].upper() == 'PFN:':
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
    if isinstance(file,PhysicalFile): return file
    if type(file) == type(''): return strToDataFile(file)
    return None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
