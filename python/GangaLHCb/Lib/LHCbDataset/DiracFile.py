################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DiracFile.py,v 0.1 2012-16-25 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File.OutputSandboxFile import OutputSandboxFile

import fnmatch 

class DiracFile(OutputSandboxFile):
    """todo DiracFile represents a class marking a file ...todo
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file'),
                                    'joboutputdir': SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
                                    'locations' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')
                                        })

    _category = 'outputfiles'
    _name = "DiracFile"
    _exportmethods = [ "location" , "get", "setLocation" ]
        
    def __init__(self,name='', **kwds):
        """ name is the name of the output file that has to be written ...
        """
        super(DiracFile, self).__init__(name, **kwds)
        self.locations = []

    def __construct__(self,args):
        super(DiracFile,self).__construct__(args)

            
    def __repr__(self):
        """Get the representation of the file."""

        return "DiracFile(name='%s')"% self.name
    

    def setLocation(self, location):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        if location not in self.locations:
            self.locations.append(location)
        
    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        return self.locations

    def get(self, dir):
        """
        Retrieves locally all files matching this DiracFile object pattern
        """
        import os

        if not os.path.isdir(dir):
            print "%s is not a valid directory.... " % dir
            return

        #todo Alex      

    def put(self):
        """
        this method will be called on the client
        """     
        
        #todo Alex

# add DiracFile objects to the configuration scope (i.e. it will be possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile


from Ganga.GPIDev.Base.Filters import allComponentFilters
from Ganga.Utility.Config import getConfig, ConfigError

outputfilesConfig = {}

for key in getConfig('Output').options.keys():
    try:
        outputFilePatterns = []

        for configEntry in getConfig('Output')[key]['fileExtensions']:
            outputFilePatterns.append(configEntry)
                
        outputfilesConfig[key] = outputFilePatterns

    except ConfigError:
        pass    

def findOutputFileTypeByFileName(filename):      

    matchCount = 0

    resultKey = None    

    for key in outputfilesConfig.keys():
        for filePattern in outputfilesConfig[key]:
            if fnmatch.fnmatch(filename, filePattern):
                matchCount += 1
                resultKey = key

    if matchCount == 1:
        return resultKey
    elif matchCount > 1:        
        raise ConfigError('pattern for filename %s defined more than once in [Output] config section' % filename)
 
    return None

def string_file_shortcut(v,item):
    if type(v) is type(''):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        key = findOutputFileTypeByFileName(v)
        if key == 'DiracFile':
            return DiracFile._proxyClass(v)._impl                                

    return None 
        
#allComponentFilters['outputfiles'] = string_file_shortcut

