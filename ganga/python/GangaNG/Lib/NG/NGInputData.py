###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: NGInputData.py,v 1.2 2009-02-17 11:00:23 bsamset Exp $
###############################################################################
# A simple NG input data class
#


import os, re, fnmatch

from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config  import getConfig, makeConfig, ConfigError 
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Shell import Shell
from Ganga.Utility.files import expandfilename
from Ganga.Utility.GridShell import getShell

class NGInputData(Dataset):
    """NGInputData is a list of files on NG storage"""
    
    _schema = Schema(Version(1,0), {
        'names': SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='List of input files with name and grid placement')
    })
    
    _category = 'datasets'
    _name = 'NGInputData'

    _exportmethods = ['get_dataset', 'get_dataset_from_list', 'get_dataset_from_grid_location']

    _GUIPrefs = [ { 'attribute' : 'names',  'widget' : 'String_List' } ]
   
    def __init__(self):
        super(NGInputData, self).__init__()

    def get_dataset_from_grid_location(self,griddirectory):
        """ Fill dataset from a directory on srm or lfc """
        shell = getShell("ARC")

        if not shell:
            logger.warning('ARC-%s UI has not been configured. The plugin has been disabled.' % self.middleware)
            return

        cmd = 'ngls'
        rc, output, m = shell.cmd1('%s%s %s' % ("$ARC_LOCATION/bin/",cmd,griddirectory),allowed_exit=[0,255])
        files = output.split("\n")
        for f in files:
            if f.strip()=='':
                continue
            self.names.append("%s/%s" % (griddirectory,f))
        
    def get_dataset_from_list(self,list_file):
       """Get the dataset files as listed in a text file"""

       logger.info('Reading list file %s ...',list_file)

       if not os.path.exists(list_file):
           logger.error('File %s does not exist',list_file)
           return

       f = open( list_file )
       for ln in f.readlines():

           # split the directory from the file and call get_dataset
           if os.path.isdir(ln.strip()):
               self.get_dataset( ln.strip() )
           else:
               self.get_dataset( os.path.dirname(ln.strip()), os.path.basename( ln.strip() ) )
           
    def get_filenames(self):
        """Get filenames"""
        return self.names

    get_filenames=staticmethod(get_filenames)


