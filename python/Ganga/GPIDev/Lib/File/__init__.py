from __future__ import absolute_import
# Required for ShareDir object
def getSharedPath():
    from Ganga.Utility.files import expandfilename
    from Ganga.Utility.Config import getConfig
    import os.path
    root_default = os.path.join(expandfilename(getConfig(
        'Configuration')['gangadir']), 'shared', getConfig('Configuration')['user'])
    return root_default

from . import Configure

from .File import File
from .File import ShareDir
from .FileBuffer import FileBuffer

from .IGangaFile import IGangaFile
from .LocalFile import LocalFile
from .MassStorageFile import MassStorageFile
from .LCGSEFile import LCGSEFile
from .SandboxFile import SandboxFile

from . import FileUtils

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from .GoogleFile import GoogleFile

from Ganga.GPIDev.Base.Filters import allComponentFilters
from Ganga.Utility.Config import getConfig, ConfigError


import fnmatch


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

        except ConfigError:
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
        logger.debug("File name pattern %s matched %s, assigning to %s" % (
            filename, str(matchKeys), matchKeys[-1]))
        return matchKeys[-1]
    elif len(matchKeys) > 1:
        logger.warning("file name pattern %s matched %s, assigning to %s" % (
            filename, str(matchKeys), matchKeys[-1]))
        return matchKeys[-1]
    else:
        logger.debug("File name pattern %s is not matched" % filename)
        return None


def string_file_shortcut(v, item):
    if isinstance(v, str):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        key = findOutputFileTypeByFileName(v)
        if key is not None:
            if key == 'MassStorageFile':
                from .MassStorageFile import MassStorageFile
                return MassStorageFile._proxyClass(v)._impl
            elif key == 'LCGSEFile':
                from .LCGSEFile import LCGSEFile
                return LCGSEFile._proxyClass(v)._impl
            elif key == 'DiracFile':
                try:
                    from GangaDirac.Lib.Files.DiracFile import DiracFile
                    return DiracFile._proxyClass(v)._impl
                except:
                    Ganga.Utility.logging.log_unknown_exception()
                    pass

        return LocalFile._proxyClass(v)._impl

    return None


allComponentFilters['gangafiles'] = string_file_shortcut
