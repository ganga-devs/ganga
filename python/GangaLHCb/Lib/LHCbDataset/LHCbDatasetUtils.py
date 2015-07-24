#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.Core import GangaException
from Ganga.GPIDev.Lib.File import *

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def isLFN(file):
    return isDiracFile(file)

def isDiracFile(file):
    from Ganga.GPI import LogicalFile, DiracFile
    from GangaDirac.Lib.Files.DiracFile import DiracFile as DiracFile_hard
    return isinstance(file, DiracFile) or isinstance(file, LogicalFile) or isinstance(file, DiracFile_hard)

def isPFN(file):
    from GangaLHCb.Lib.Files import PhysicalFile
    return isinstance(file, PhysicalFile) or isinstance(file, LocalFile) or isinstance(file, MassStorageFile)

def strToDataFile(name, allowNone=True):
    if len(name) >= 4 and name[0:4].upper() == 'LFN:':
        from Ganga.GPI import DiracFile
        return DiracFile(lfn=name[4:])
    elif len(name) >= 4 and name[0:4].upper() == 'PFN:':
        logger.warning("PFN is slightly ambiguous, constructing LocalFile")
        from GangaLHCb.Lib.File import LocalFile
        return LocalFile(name)
    else:
        if not allowNone:
            msg = 'Can only convert strings that begin w/ PFN: or '\
                  'LFN: to data files.'\
                  ' Name is: %s' % name
            raise GangaException(msg)
        return None

def getDataFile(file):
    from Ganga.GPI import DiracFile
    if isinstance(file, DiracFile): return file
    from Ganga.GPIDev.Lib.File import LocalFile
    if isinstance(file, LocalFile): return file
    if type(file) == type(''): return strToDataFile(file)
    return None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
