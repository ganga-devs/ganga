################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SandboxFile.py,v 0.1 2011-09-29 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from IOutputFile import IOutputFile   


from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

import re
import os

regex = re.compile('[*?\[\]]')

class SandboxFile(IOutputFile):
    """SandboxFile represents base class for output files, such as MassStorageFile, LCGSEFile, etc 
    """
    _schema = Schema(Version(1,1), {'namePattern': SimpleItem(defvalue="",doc='pattern of the file name'),
                                    'localDir': SimpleItem(defvalue="",doc='local dir where the file is stored, used from get and put methods'),
                                    'subfiles'      : ComponentItem(category='gangafiles',defvalue=[], hidden=1, typelist=['Ganga.GPIDev.Lib.File.SandboxFile'], sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),

                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')})
    _category = 'gangafiles'
    _name = "SandboxFile"

    def __init__(self,namePattern='', localDir='', **kwds):
        """ name is the name of the output file that is going to be processed
            in some way defined by the derived class
        """
        super(SandboxFile, self).__init__()
        self.namePattern = namePattern
        self.localDir = localDir
    
    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.namePattern = args[0]
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self.localDir = args[1]     
        
    def __repr__(self):
        """Get the representation of the file."""

        return "SandboxFile(namePattern='%s')"% self.namePattern

    def processWildcardMatches(self):

        import glob

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern   

        sourceDir = self.getJobObject().outputdir       

        if regex.search(fileName) is not None:
            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):

                d=SandboxFile(namePattern=os.path.basename(currentFile))
                d.compressed = self.compressed

                self.subfiles.append(GPIProxyObjectFactory(d))
