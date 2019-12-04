##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, GangaFileItem
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Lib.File.File import File
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList

class IVirtualization(GangaObject):

    """
    Contains the interface for all virtualization classes, all virtualization classes should inherit from this object.
    """

    def __init__(self, image):
        super(IVirtualization, self).__init__()
        self.image = image

    _schema = Schema(Version(1, 0), {
        'image': SimpleItem(defvalue="", typelist=[str], doc='Link to the container image'),
        'tokenuser': SimpleItem(defvalue="", typelist=[str], doc='Deploy token username'),
        'tokenpassword': SimpleItem(defvalue="", typelist=[str], doc='Deploy token password'),
        'mounts' : SimpleItem(defvalue={'/cvmfs': '/cvmfs'},
                               doc='Mounts to attempt from the host system. The key is the directory name on the host, and the value inside the container. If the directory is not available on the host, it will just be silently dropped from the list of mount points.'),
        'options': SimpleItem(defvalue=[], typelist=[list, GangaList], sequence=1, doc='A list of options to pass onto the virtualization command.')
    })
    _category = 'virtualization'
    _name = 'IVirtualization'
    _hidden = 1

    def modify_script(self, script):
        """Modify the given script by
        substituting virtualization related placeholders with relevant variables

            Arguments other than self:
               script - Script that need to be modified

            Return value: modified script"""

        return None
