from Ganga.GPIDev.Lib.File.Configure import getSharedPath

from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.File import ShareDir
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer

from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from Ganga.GPIDev.Lib.File.MassStorageFile import MassStorageFile
from Ganga.GPIDev.Lib.File.LCGSEFile import LCGSEFile
from Ganga.GPIDev.Lib.File.SandboxFile import SandboxFile
from Ganga.GPIDev.Lib.File.GoogleFile import GoogleFile

import Ganga.Utility.logging

from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.GPIDev.Base.Filters import allComponentFilters
from Ganga.Utility.Config import getConfig, ConfigError


import fnmatch

logger = Ganga.Utility.logging.getLogger()


def getFileConfigKeys():
    keys = getConfig('Output').options.keys()
    keys.remove('PostProcessLocationsFileName')
    keys.remove('ForbidLegacyInput')
    keys.remove('ForbidLegacyOutput')
    keys.remove('AutoRemoveFilesWithJob')
    keys.remove('AutoRemoveFileTypes')
    keys.remove('FailJobIfNoOutputMatched')
    return keys


def decodeExtensionKeys():

    outputfilesConfig = {}

    keys = getFileConfigKeys()

    for key in keys:
        try:
            outputFilePatterns = []

            for configEntry in getConfig('Output')[key]['fileExtensions']:
                outputFilePatterns.append(configEntry)

            outputfilesConfig[key] = outputFilePatterns

        except ConfigError as err:
            logger.debug("ConfigureError: %s" % str(err))
            pass

    return outputfilesConfig


def findOutputFileTypeByFileName(filename):

    matchKeys = []

    outputfilesConfig = decodeExtensionKeys()

    for key in outputfilesConfig.keys():
        for filePattern in outputfilesConfig[key]:
            if fnmatch.fnmatch(filename, filePattern):
                matchKeys.append(key)

    if len(matchKeys) == 1:
        #logger.debug("File name pattern %s matched %s, assigning to %s" % (
        #    filename, str(matchKeys), matchKeys[-1]))
        return matchKeys[-1]
    elif len(matchKeys) > 1:
        #logger.warning("file name pattern %s matched %s, assigning to %s" % (
        #    filename, str(matchKeys), matchKeys[-1]))
        return matchKeys[-1]
    else:
        #logger.debug("File name pattern %s is not matched" % filename)
        return None


def string_file_shortcut(v, item):
    if isinstance(v, str):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        key = findOutputFileTypeByFileName(v)
        if key is not None:
            if key == 'MassStorageFile':
                from .MassStorageFile import MassStorageFile
                return stripProxy(MassStorageFile._proxyClass(v))
            elif key == 'LCGSEFile':
                from .LCGSEFile import LCGSEFile
                return stripProxy(LCGSEFile._proxyClass(v))
            elif key == 'DiracFile':
                try:
                    from GangaDirac.Lib.Files.DiracFile import DiracFile
                    return stripProxy(DiracFile._proxyClass(v))
                except:
                    Ganga.Utility.logging.log_unknown_exception()
                    pass

        return stripProxy(LocalFile._proxyClass(v))

    return None

allComponentFilters['gangafiles'] = string_file_shortcut

