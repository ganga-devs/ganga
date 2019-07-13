##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization
import GangaCore.Utility.logging

logger = GangaCore.Utility.logging.getLogger()

class Docker(IVirtualization):

    """
    Handles all the config related to Docker
    """

    _name = 'Docker'
    _schema = IVirtualization._schema.inherit_copy()
    _schema.datadict['mode'] = SimpleItem(defvalue="P1", doc='Mode of container execution')

    def __init__(self, imageUrl, mode):
        super(Docker, self).__init__()
        self.imageUrl = imageUrl
        self.mode = mode

    def getImageUrl(self):
        return self.imageUrl

    def getMode(self):
        return self.mode
