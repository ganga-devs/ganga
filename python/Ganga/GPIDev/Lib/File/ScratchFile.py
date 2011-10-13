################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ScratchFile.py,v 0.1 2011-10-03 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *

from OutputFile import OutputFile

class ScratchFile(OutputFile):
    """ScratchFile represents a class marking a file to be written to large scratch disk
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file')})
    _category = 'outputfiles'
    _name = "ScratchFile"

    def __init__(self,name='', **kwds):
        """ name is the name of the output file that has to be written to large scratch disk
        """
        super(ScratchFile, self).__init__(name, **kwds)


    def __construct__(self,args):
            super(ScratchFile,self).__construct__(args)

            
    def __repr__(self):
        """Get the representation of the file."""

        return "ScratchFile(name='%s')"% self.name


# add ScratchFile objects to the configuration scope (i.e. it will be possible to write instatiate ScratchFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['ScratchFile'] = ScratchFile
