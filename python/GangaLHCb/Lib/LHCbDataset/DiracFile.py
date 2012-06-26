################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DiracFile.py,v 0.1 2012-16-25 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File.OutputSandboxFile import OutputSandboxFile

import fnmatch 

class DiracFile(OutputSandboxFile):
    """todo DiracFile represents a class marking a file ...todo
    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='name of the file'),
                                    'joboutputdir': SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
                                    'locations' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')
                                        })

    _category = 'outputfiles'
    _name = "DiracFile"
    _exportmethods = [ "location" , "get", "setLocation" ]
        
    def __init__(self,name='', **kwds):
        """ name is the name of the output file that has to be written ...
        """
        super(DiracFile, self).__init__(name, **kwds)
        self.locations = []

    def __construct__(self,args):
        super(DiracFile,self).__construct__(args)

            
    def __repr__(self):
        """Get the representation of the file."""

        return "DiracFile(name='%s')"% self.name
    

    def setLocation(self, location):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        if location not in self.locations:
            self.locations.append(location)
        
    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        return self.locations

    def get(self, dir):
        """
        Retrieves locally all files matching this DiracFile object pattern
        """
        import os

        if not os.path.isdir(dir):
            print "%s is not a valid directory.... " % dir
            return

        #todo Alex      

    def put(self):
        """
        this method will be called on the client
        """     
        
        #todo Alex

# add DiracFile objects to the configuration scope (i.e. it will be possible to write instatiate DiracFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['DiracFile'] = DiracFile


