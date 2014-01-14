from Ganga.GPIDev.Lib.File.LocalFile import LocalFile

from Ganga.GPIDev.Schema import *

class SandboxFile(LocalFile):
    _schema = Schema(Version(1,1), {'namePattern': SimpleItem(defvalue="",doc='pattern of the file name'),
                                    'localDir': SimpleItem(defvalue="",doc='local dir where the file is stored, used from get and put methods'),
                                    'subfiles'      : ComponentItem(category='gangafiles',defvalue=[], hidden=1, typelist=['Ganga.GPIDev.Lib.File.SandboxFile'], sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),

                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')})
    _category = 'gangafiles'
    _name = "SandboxFile"
    
    #TODO Add deprecation warning
