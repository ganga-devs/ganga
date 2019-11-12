##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization


class Docker(IVirtualization):

    """
    Handles all the config related to Docker
    """
    _name = 'Docker'
    _schema = IVirtualization._schema.inherit_copy()
    _schema.datadict['mode'] = SimpleItem(defvalue="P1", doc='Mode of container execution')

    def __init__(self, image, mode):
        super().__init__(image)
        self.mode = mode

    def modify_script(self, script):
        """Overides parent's modify_script function
            Arguments other than self:
               script - Script that need to be modified

            Return value: modified script"""

        udockerlocation = getConfig('Configuration')['UDockerlocation']
        script = script.replace('###VIRTUALIZATIONIMAGE###', repr(self.image))
        script = script.replace('###VIRTUALIZATION###', repr("Docker"))
        script = script.replace('###VIRTUALIZATIONMODE###', repr(self.mode))
        script = script.replace('###UDOCKERLOCATION###', repr(udockerlocation))
        return script
