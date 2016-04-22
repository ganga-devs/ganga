from Ganga.GPIDev.Lib.File.LocalFile import LocalFile

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class SandboxFileStub(LocalFile):

    _schema = Schema(Version(1,0), {})
    _name = "SandboxFileStub"
    _hidden = False

    def __init__(self):
        super(SandboxFileStub, self).__init__()

class SandboxFile(SandboxFileStub):
    _schema = Schema(Version(1, 1), {'namePattern': SimpleItem(defvalue="", doc='pattern of the file name'),
                                     'localDir': SimpleItem(defvalue="", doc='local dir where the file is stored, used from get and put methods'),
                                     'subfiles': ComponentItem(category='gangafiles', defvalue=[], hidden=1, typelist=[SandboxFileStub], sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),

                                     'compressed': SimpleItem(defvalue=False, typelist=[bool], protected=0, doc='wheather the output file should be compressed before sending somewhere')})
    _category = 'gangafiles'
    _name = "SandboxFile"

    def __init__(self, namePattern='', localDir='', **kwds):
        """ name is the name of the output file that is going to be processed
           in some way defined by the derived class
        """
        logger.warning(
            "SandboxFile is now deprecated please change your configuration to use LocalFile instead!")
        super(SandboxFile, self).__init__(namePattern, localDir, **kwds)
