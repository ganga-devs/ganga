##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Base import GangaObject

class IVirtualization(GangaObject):

    """
    Contains the interface for all virtualization classes, all virtualization classes should inherit from this object.
    """

    def __init__(self):
        super(IVirtualization, self).__init__()

    _schema = Schema(Version(1, 0), {
        'imageUrl': SimpleItem(defvalue="", doc='Link to the container image')
    })
    _category = 'virtualization'
    _name = 'IVirtualization'
    _hidden = 1

    def getImageUrl(self):
        return ""

    def getMode(self):
        return ""
