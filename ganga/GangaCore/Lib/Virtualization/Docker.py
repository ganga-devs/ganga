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
        'mode': SimpleItem(defvalue="", doc='Mode of container execution'
        )})

    def __init__(self, imageUrl, mode=""):
        super(Docker, self).__init__()
        
        if isinstance(imageUrl, str):
            self.imageUrl = imageUrl
        else:
            logger.error("Unkown type: %s . is not a valid format for image Url" % type(namePattern))

        if isinstance(mode, str):
            if mode == "":
                self.mode = "P1"
            else:
                self.mode = mode
        else:
            logger.error("Unkown type: %s . is not a valid format for mode" % type(namePattern))
