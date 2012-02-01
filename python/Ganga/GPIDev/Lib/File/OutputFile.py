################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: OutputFile.py,v 0.1 2011-09-29 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class OutputFile(GangaObject):
    """OutputFile represents base class for output files, such as CompressedFile,
       ScratchFile, LHCbDataFile, CastorFile, etc 
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file')})
    _category = 'outputfiles'
    _name = "OutputFile"

    def __init__(self,name='', **kwds):
        """ name is the name of the output file that is going to be processed
            in some way defined by the derived class
        """
        super(OutputFile, self).__init__()
        self.name = name 
    
    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.name = args[0]
        else:
            super(OutputFile,self).__construct__(args)
        
    def __repr__(self):
        """Get the representation of the file."""

        return "OutputFile(name='%s')"% self.name

    def location(self):
        """
        Return list with the locations of the post processed files (if they was configured to upload the output somewhere)
        """
        raise NotImplementedError

    def get(self, dir):
        """
        Retrieves locally all files matching this OutputFile object pattern
        """
        raise NotImplementedError
        


from Ganga.GPIDev.Base.Filters import allComponentFilters
from CompressedFile import CompressedFile
from ScratchFile import ScratchFile
from LHCbDataFile import LHCbDataFile
from MassStorageFile import MassStorageFile
from LCGStorageElementFile import LCGStorageElementFile

from Ganga.Utility.Config import getConfig, ConfigError

outputfilesConfig = {}
keys = ['CompressedFile', 'ScratchFile', 'LHCbDataFile', 'MassStorageFile', 'LCGStorageElementFile']

for key in keys:
    try:
        outputFileExtensions = []

        for configEntry in getConfig('Output')[key]:
            #get only the extension
            if configEntry.startswith('*.'):
                outputFileExtensions.append(configEntry[2:])
            else:
                outputFileExtensions.append(configEntry)
                
        outputfilesConfig[key] = outputFileExtensions

    except ConfigError:
        #todo:ivan throw some error here
        pass    

def findOutputFileTypeByFileName(filename):

    dotIndex = filename.find('.')       
    #get only the extension     
    if dotIndex > -1 and filename.startswith('*'):
        filename = filename[dotIndex+1:]        

    matchCount = 0

    resultKey = None    

    for key in outputfilesConfig.keys():

        if filename in outputfilesConfig[key]:
            matchCount += 1
            resultKey = key

    if matchCount == 1:
        return resultKey
    elif matchCount > 1:        
        raise ConfigError('filename %s defined more than once in [Output] config section' % filename)
 
    return None

def string_file_shortcut(v,item):
    if type(v) is type(''):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        key = findOutputFileTypeByFileName(v)
        if key is not None:
            if key == 'CompressedFile':
                return CompressedFile._proxyClass(v)._impl
            elif key == 'MassStorageFile':
                return MassStorageFile._proxyClass(v)._impl         
            elif key == 'ScratchFile':
                return ScratchFile._proxyClass(v)._impl         
            elif key == 'LHCbDataFile':
                return LHCbDataFile._proxyClass(v)._impl                                
            elif key == 'LCGStorageElementFile':
                return LCGStorageElementFile._proxyClass(v)._impl                                

        return OutputFile._proxyClass(v)._impl

    return None 
        
allComponentFilters['outputfiles'] = string_file_shortcut
