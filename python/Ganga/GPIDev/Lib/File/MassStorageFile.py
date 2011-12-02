################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: MassStorageFile.py,v 0.1 2011-11-09 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *

from OutputFile import OutputFile

class MassStorageFile(OutputFile):
    """MassStorageFile represents a class marking a file to be written into mass storage (like Castor at CERN)
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file')})
    _category = 'outputfiles'
    _name = "MassStorageFile"

    def __init__(self,name='', **kwds):
        """ name is the name of the output file that has to be written into mass storage
        """
        super(MassStorageFile, self).__init__(name, **kwds)


    def __construct__(self,args):
            super(MassStorageFile,self).__construct__(args)

            
    def __repr__(self):
        """Get the representation of the file."""

        return "MassStorageFile(name='%s')"% self.name


# add MassStorageFile objects to the configuration scope (i.e. it will be possible to write instatiate MassStorageFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['MassStorageFile'] = MassStorageFile
