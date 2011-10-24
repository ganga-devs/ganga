################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: CompressedFile.py,v 0.1 2011-09-29 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *

from OutputFile import OutputFile

class CompressedFile(OutputFile):
    """CompressedFile represents a class marking a file for compressing
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file')})
    _category = 'outputfiles'
    _name = "CompressedFile"

    def __init__(self,name='', **kwds):
        """ name is the name of the output file that has to be compressed
        """
        super(CompressedFile, self).__init__(name, **kwds)


    def __construct__(self,args):
            super(CompressedFile,self).__construct__(args)

            
    def __repr__(self):
        """Get the representation of the file."""

        return "CompressedFile(name='%s')"% self.name


# add CompressedFile objects to the configuration scope (i.e. it will be possible to write instatiate CompressedFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['CompressedFile'] = CompressedFile
