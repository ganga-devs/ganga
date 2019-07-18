##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization

class Singularity(IVirtualization):

    """
    Handles all the config related to Singularity
    """
    _name = 'Singularity'
    _schema = IVirtualization._schema.inherit_copy()

    def __init__(self, imageUrl):
        super(Singularity, self).__init__(imageUrl)

    def modify_script(self, script):
        script = super(Singularity, self).modify_script(script)
        script = script.replace('###VIRTUALIZATION###', repr("Singularity"))
        script = script.replace('###VIRTUALIZATION###', repr(None))
        return script
