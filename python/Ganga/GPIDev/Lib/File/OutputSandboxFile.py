################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: OutputSandboxFile.py,v 0.1 2011-09-29 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
import glob 
import fnmatch    

class OutputSandboxFile(GangaObject):
    """OutputSandboxFile represents base class for output files, such as MassStorageFile, LCGStorageElementFile, etc 
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file'),
                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')})
    _category = 'outputfiles'
    _name = "OutputSandboxFile"

    def __init__(self,name='', **kwds):
        """ name is the name of the output file that is going to be processed
            in some way defined by the derived class
        """
        super(OutputSandboxFile, self).__init__()
        self.name = name
    
    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.name = args[0]
        else:
            super(OutputSandboxFile,self).__construct__(args)
        
    def __repr__(self):
        """Get the representation of the file."""

        return "OutputSandboxFile(name='%s')"% self.name

    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        raise NotImplementedError

    def get(self, dir):
        """
        Retrieves locally all files matching this OutputSandboxFile object pattern
        """
        raise NotImplementedError

    def put(self):
        """
        Postprocesses (upload) output file to the desired destination, can be overriden in LCGStorageElementFile, MassStorageFile, etc
        """
        raise NotImplementedError

    def execSyscmdSubprocess(self, cmd):

        import subprocess

        exitcode = -999
        mystdout = ''
        mystderr = ''

        try:
            child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (mystdout, mystderr) = child.communicate()
            exitcode = child.returncode
        finally:
            pass

        return (exitcode, mystdout, mystderr)


from Ganga.GPIDev.Base.Filters import allComponentFilters
from MassStorageFile import MassStorageFile
from LCGStorageElementFile import LCGStorageElementFile

try:
    from GangaLHCb.Lib.LHCbDataset.DiracFile import DiracFile
    lhcbFlavour = True  
except:
    lhcbFlavour = False 
    pass        

from Ganga.Utility.Config import getConfig, ConfigError

outputfilesConfig = {}

for key in getConfig('Output').options.keys():
    try:
        outputFilePatterns = []

        for configEntry in getConfig('Output')[key]['fileExtensions']:
            outputFilePatterns.append(configEntry)
                
        outputfilesConfig[key] = outputFilePatterns

    except ConfigError:
        #todo:ivan throw some error here
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
        if key is not None:
            if key == 'MassStorageFile':
                return MassStorageFile._proxyClass(v)._impl         
            elif key == 'LCGStorageElementFile':
                return LCGStorageElementFile._proxyClass(v)._impl                                
            elif key == 'DiracFile' and lhcbFlavour:
                return  DiracFile._proxyClass(v)._impl                                

        return OutputSandboxFile._proxyClass(v)._impl

    return None 
        
allComponentFilters['outputfiles'] = string_file_shortcut
