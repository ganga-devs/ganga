##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization
import GangaCore.Utility.logging

logger = GangaCore.Utility.logging.getLogger()

class Singularity(IVirtualization):

    """
    Handles all the config related to Singularity
    """
    _name = 'Singularity'
    _schema = IVirtualization._schema.inherit_copy()

    def __init__(self, imageUrl):
        super(Singularity, self).__init__()
        
        if isinstance(imageUrl, str):
            self.imageUrl = imageUrl
        else:
            logger.error("Unkown type: %s . is not a valid format for image Url" % type(imageUrl))

    def getImageUrl(self):
        return self.imageUrl