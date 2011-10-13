################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: CastorFile.py,v 0.1 2011-10-03 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *

from OutputFile import OutputFile

class CastorFile(OutputFile):
    """CastorFile represents a class marking a file to be written into Castor mass storage
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file')})
    _category = 'outputfiles'
    _name = "CastorFile"

    def __init__(self,name='', **kwds):
        """ name is the name of the output file that has to be written into Castor mass storage
        """
        super(CastorFile, self).__init__(name, **kwds)


    def __construct__(self,args):
            super(CastorFile,self).__construct__(args)

            
    def __repr__(self):
        """Get the representation of the file."""

        return "CastorFile(name='%s')"% self.name


# add CastorFile objects to the configuration scope (i.e. it will be possible to write instatiate CastorFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['CastorFile'] = CastorFile
