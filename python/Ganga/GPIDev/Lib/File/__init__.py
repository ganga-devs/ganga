from File import File
from File import ShareDir
from FileBuffer import FileBuffer

from IOutputFile import IOutputFile
from OutputSandboxFile import OutputSandboxFile
from MassStorageFile import MassStorageFile
from LCGStorageElementFile import LCGStorageElementFile

from Ganga.GPIDev.Base.Filters import allComponentFilters
from Ganga.Utility.Config import getConfig, ConfigError

from Ganga.Utility.logging import getLogger
logger = getLogger()

import fnmatch 

outputfilesConfig = {}

keys = getConfig('Output').options.keys()
keys.remove('PostProcessLocationsFileName')

for key in keys:
    try:
        outputFilePatterns = []

        for configEntry in getConfig('Output')[key]['fileExtensions']:
            outputFilePatterns.append(configEntry)
                
        outputfilesConfig[key] = outputFilePatterns

    except ConfigError:
        pass    

def findOutputFileTypeByFileName(filename):      

    matchKeys = []

    for key in outputfilesConfig.keys():
        for filePattern in outputfilesConfig[key]:
            if fnmatch.fnmatch(filename, filePattern):
                matchKeys.append(key)

    if len(matchKeys) == 1:     
        return matchKeys[-1]
    elif len(matchKeys) > 1:
        logger.warning("file name pattern %s matched %s, assigning to %s" % (filename, str(matchKeys), matchKeys[-1]))
        return matchKeys[-1]
    else:
        return None     

def string_file_shortcut(v,item):
    if type(v) is type(''):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        key = findOutputFileTypeByFileName(v)
        if key is not None:
            if key == 'MassStorageFile':
                from MassStorageFile import MassStorageFile
                return MassStorageFile._proxyClass(v)._impl         
            elif key == 'LCGStorageElementFile':
                from LCGStorageElementFile import LCGStorageElementFile
                return LCGStorageElementFile._proxyClass(v)._impl                                
            elif key == 'DiracFile':
                try:
                    from GangaLHCb.Lib.LHCbDataset.DiracFile import DiracFile
                    return  DiracFile._proxyClass(v)._impl                                
                except:
                    pass

        return OutputSandboxFile._proxyClass(v)._impl

    return None 
        
allComponentFilters['outputfiles'] = string_file_shortcut
