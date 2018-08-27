#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from GangaCore.Utility.Config import getConfig, ConfigError
import GangaCore.Utility.logging
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Lib.File import LocalFile
from GangaCore.GPIDev.Base.Proxy import isType
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaLHCb.Lib.Files import LogicalFile, PhysicalFile

logger = GangaCore.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def isLFN(file):
    return isDiracFile(file)


def isDiracFile(file):
    return isType(file, DiracFile) or isType(file, LogicalFile)


def isPFN(file):
    return isType(file, PhysicalFile) or isType(file, LocalFile)


def strToDataFile(name, allowNone=True):
    if len(name) >= 4 and name[:4].upper() == 'LFN:':
        return DiracFile(lfn=name[4:])
    elif len(name) >= 4 and name[:4].upper() == 'PFN:':
        logger.warning("PFN is slightly ambiguous, constructing LocalFile")
        return LocalFile(name[4:])
    else:
        if allowNone:
            return None
        else:
            raise GangaException( "Cannot construct file object: %s" % str(name) )

def getDataFile(file_):
    if isType(file_, DiracFile):
        return file_
    if isType(file_, LocalFile):
        return file_
    if isinstance(file_, str):
        return strToDataFile(file_)
    return None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
