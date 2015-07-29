#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.Core import GangaException
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Base.Proxy import isType
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaLHCb.Lib.Files import LogicalFile, PhysicalFile
from Ganga.GPIDev.Lib.File import LocalFile

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def isLFN(file):
    return isDiracFile(file)

def isDiracFile(file):
    return isType(file, DiracFile) or isType(file, LogicalFile)

def isPFN(file):
    return isType(file, PhysicalFile) or isType(file, LocalFile)

def strToDataFile(name, allowNone=True):
    if len(name) >= 4 and name[0:4].upper() == 'LFN:':
        return DiracFile(lfn=name[4:])
    elif len(name) >= 4 and name[0:4].upper() == 'PFN:':
        logger.warning("PFN is slightly ambiguous, constructing LocalFile")
        return LocalFile(name)
    else:
        if not allowNone:
            msg = 'Can only convert strings that begin w/ PFN: or '\
                  'LFN: to data files.'\
                  ' Name is: %s' % name
            raise GangaException(msg)
        return None

def getDataFile(file):
    if isType(file, DiracFile): return file
    if isType(file, LocalFile): return file
    if type(file) == type(''): return strToDataFile(file)
    return None

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
