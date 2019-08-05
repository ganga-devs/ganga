##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Adapters.IVirtualization import IVirtualization
from GangaCore.GPIDev.Lib.File.File import File

class Singularity(IVirtualization):

    """
    Handles all the config related to Singularity
    """
    _name = 'Singularity'
    _schema = IVirtualization._schema.inherit_copy()

    def __init__(self, image):
        super(Singularity, self).__init__(image)

    def modify_script(self, script):
        """Overides parent's modify_script function
                    Arguments other than self:
                       script - Script that need to be modified

                    Return value: modified script"""
        
        if type(self.image) is File:
                script = script.replace('###VIRTUALIZATIONIMAGE###', repr(self.image.name))
        script = script.replace('###VIRTUALIZATIONIMAGE###', repr(self.image))
        script = script.replace('###VIRTUALIZATION###', repr("Singularity"))
        script = script.replace('###VIRTUALIZATIONMODE###', repr(None))
        return script
