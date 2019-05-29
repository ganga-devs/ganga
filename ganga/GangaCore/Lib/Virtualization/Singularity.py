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
    Handles all the config related to Docker
    """
    _name = 'Singularity'
    _schema = Schema(Version(1, 0), {
        'imageUrl': SimpleItem(defvalue="", doc='Link to the container image')
        })

    def __init__(self, imageUrl):
        super(Singularity, self).__init__()
        
        if isinstance(imageUrl, str):
            self.imageUrl = imageUrl
        else:
            logger.error("Unkown type: %s . is not a valid format for image Url" % type(namePattern))
