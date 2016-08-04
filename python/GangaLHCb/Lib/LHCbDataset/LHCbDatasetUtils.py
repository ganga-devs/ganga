#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.Utility.Config import ConfigError
from Ganga.Utility.Config import getConfig
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.Core import GangaException
from Ganga.GPIDev.Lib.File import LocalFile
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaLHCb.Lib.Files import LogicalFile, PhysicalFile

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def isLFN(file):
    return isDiracFile(file)


def isDiracFile(file):
    return isinstance(file, DiracFile) or isinstance(file, LogicalFile)


def isPFN(file):
    return isinstance(file, PhysicalFile) or isinstance(file, LocalFile)


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
    if isinstance(file_, DiracFile):
        return file_
    if isinstance(file_, LocalFile):
        return file_
    if isinstance(file_, str):
        return strToDataFile(file_)
    return None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
