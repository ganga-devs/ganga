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
    _schema = Schema(Version(1, 0), {
        'imageUrl': SimpleItem(defvalue="", doc='Link to the container image'),
        'mode': SimpleItem(defvalue="P1", doc='Mode of container execution'
        )})

    def __init__(self, imageUrl, mode):
        super(Docker, self).__init__()
        self.imageUrl = imageUrl
        self.mode = mode
