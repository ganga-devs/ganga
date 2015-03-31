################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LocalFile.py,v 0.1 2011-09-29 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from IGangaFile import IGangaFile   

from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

import re
import os

regex = re.compile('[*?\[\]]')

class LocalFile(IGangaFile):
    """LocalFile represents base class for output files, such as MassStorageFile, LCGSEFile, etc 
    """
    _schema = Schema(Version(1,1), {'namePattern': SimpleItem(defvalue="",doc='pattern of the file name'),
                                    'localDir': SimpleItem(defvalue="",doc='local dir where the file is stored, used from get and put methods'),
                                    'subfiles'      : ComponentItem(category='gangafiles',defvalue=[], hidden=1, typelist=['Ganga.GPIDev.Lib.File.LocalFile'], sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),

                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')})
    _category = 'gangafiles'
    _name = "LocalFile"

    def __init__(self, namePattern='', localDir='', **kwds):
        """ name is the name of the output file that is going to be processed
            in some way defined by the derived class
        """
        super(LocalFile, self).__init__()
        self.namePattern = namePattern
        self.localDir = localDir
    
    def __construct__(self,args):
        from Ganga.GPIDev.Lib.File.SandboxFile import SandboxFile
        if len(args) == 1 and type(args[0]) == type(''):
            self.namePattern = args[0]
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self.localDir = args[1]     
        elif len(args) == 1 and isinstance( args[0], SandboxFile ):
            super( LocalFile, self ).__construct__( args )

    def __repr__(self):
        """Get the representation of the file."""

        return "LocalFile(namePattern='%s')"% self.namePattern

    def processOutputWildcardMatches(self):
        """This collects the subfiles for wildcarded output LocalFile"""
        import glob
 
        fileName = self.namePattern
 
        if self.compressed:
            fileName = '%s.gz' % self.namePattern  
 
        sourceDir = self.getJobObject().outputdir      
        if regex.search(fileName) is not None:

            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):
 
                d=LocalFile(namePattern=os.path.basename(currentFile))
                d.compressed = self.compressed
 
                self.subfiles.append(GPIProxyObjectFactory(d))

    def processWildcardMatches(self):

        if self.subfiles:
            return self.subfiles

        import glob

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern   

        sourceDir = self.localDir      

        if regex.search(fileName) is not None:
            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):
                d=LocalFile(namePattern=os.path.basename(currentFile), localDir=os.path.dirname(currentFile))
                d.compressed = self.compressed

                self.subfiles.append(GPIProxyObjectFactory(d))

    def getSubFiles(self):
        """Returns the name of a file object throgh a common interface"""
        self.processWildcardMatches()
        if self.subfiles:
            return self.subfiles
        else:
            return [self]
        

    def getFilenameList(self):
        """Return the files referenced by this LocalFile"""
        filelist = []
        self.processWildcardMatches()
        if self.subfiles:
            for f in self.subfiles:
                filelist.append( os.path.join( f.localDir, f.namePattern ) )
        else:
            filelist.append( os.path.join( self.localDir, self.namePattern ) )

        return filelist
