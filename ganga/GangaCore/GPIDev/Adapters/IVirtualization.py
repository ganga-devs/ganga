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
    
    _schema = Schema(Version(0,0), {})
    _category = 'gangavirtualization'
    _name = 'IVirtualization'
    _hidden = 1
