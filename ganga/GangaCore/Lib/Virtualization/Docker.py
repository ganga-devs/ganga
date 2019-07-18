##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization

class Docker(IVirtualization):

    """
    Handles all the config related to Docker
    """
    _name = 'Docker'
    _schema = IVirtualization._schema.inherit_copy()
    _schema.datadict['mode'] = SimpleItem(defvalue="P1", doc='Mode of container execution')

    def __init__(self, imageUrl, mode):
        super(Docker, self).__init__(imageUrl)
        self.imageUrl = imageUrl
        self.mode = mode

    def modify_script(self, script):
        """Overides parent's modify_script function
            Arguments other than self:
               script - Script that need to be modified

            Return value: modified script"""

        script = super(Docker, self).modify_script(script)
        script = script.replace('###VIRTUALIZATION###', repr("Docker"))
        script = script.replace('###VIRTUALIZATIONMODE###', repr(self.mode))
        return script
