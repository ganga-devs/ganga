################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 17/01/2014
################################################################################
"""@package ND280Dataset
Ganga module to control the input files.
It relies on the basic ND280Dataset class which is used through inheritance in other more specific classes.
"""

from GangaCore.GPIDev.Schema import *
from GangaCore.GPIDev.Lib.Dataset import Dataset
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger

import os, re, fnmatch
import subprocess

logger = getLogger()

class ND280Dataset(Dataset):
    """
    Base class for ND280 Datasets.
        dataset = ND280Dataset()

    You can define the list of files by hand directly if you want:
        dataset.names = ['/path/to/the/files/Input1.root','/path/to/the/files/Input2.root']
    """
    
    _schema = Schema(Version(1,0), {
        'names': SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='List of input files with full path'),
    })
    
    _category = 'datasets'
    _name = 'ND280Dataset'
    _hidden = 1

    def __init__(self):
        super(ND280Dataset, self).__init__()
        
    def get_dataset_from_list(self,list_file):
       """Get the dataset files as listed in a text file."""

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
        
    def set_dataset_into_list(self,list_file):
       """Write the dataset files as a list in a text file."""

       logger.info('Writing list file %s ...',list_file)

       if not len(self.names):
         logger.error('No input files found. The dataset is empty.')
         return 0

       try:
         fileList = open( list_file, 'w')
       except IOError:
         logger.error('File %s cannot be created',list_file)
         return 0

       for inFile in self.names:
         fileList.write(inFile+'\n')

       fileList.close()
       return 1

    def get_dataset(self,directory,filter=None):
        """Get the list of files in the dataset's directory.
        This is not implemented in ND280Dataset but is defined in each class inheriting from ND280Dataset."""
      
        raise NotImplementedError


    def get_dataset_filenames(self):
        """Simply returns a python list containing all the filenames in this ND280Dataset.
        """
        return self.names

    def set_dataset_filenames(self,list_file):
       """Copy the list of files given in input as the list of files in the ND280Dataset.
       NOTE: This will not append the input list to the existing file list but instead replace it."""

       logger.info('Writing list file %s ...',list_file)

       self.names=list_file

    def get_filenames(app):
        """Retrieve the file names starting from an application object"""
      
        job=app._getRoot()
        if not job:
            logger.warning('Application object is not associated to a job.')
            return []
         
        # Jobs without inputdata are allowed
        if not job.inputdata: return []
      
        import re
        classnamematch = re.match(r"ND280.*Dataset", job.inputdata._name)
        if not classnamematch:
            logger.warning('Dataset is not a class inheriting from ND280Dataset.')
            return []

        return job.inputdata.names
         
    get_filenames=staticmethod(get_filenames)


class ND280LocalDataset(ND280Dataset):

    """ND280LocalDataset manages files located in a local directory."""
    
    _schema = Schema(Version(1,1), {
        'names': SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='List of input files with full path'),
    })
    
    _category = 'datasets'
    _name = 'ND280LocalDataset'

    _exportmethods = ['get_dataset', 'get_dataset_filenames', 'get_dataset_from_list', 'get_raw_from_list', 'get_kin_range','set_dataset_into_list', 'set_dataset_filenames']

    _GUIPrefs = [ { 'attribute' : 'names',  'attribute' :  'String_List' } ]
   
    def __init__(self):
        super(ND280LocalDataset, self).__init__()


    def get_dataset(self,directory,filter=None):
       """Get the list of files in the local directory given.
       You can provide a wildcard filter such as 'oa_*.root'"""
      
       logger.info('Reading %s ...',directory)


       if not os.path.isdir(directory):
           logger.error('Path %s is no directory',directory)
           return

       directory = os.path.abspath(directory)
       if filter:
           new_names = [ os.path.join(directory,name) for name in fnmatch.filter(sorted(os.listdir(directory)),filter) ]
       else:
           new_names = [ os.path.join(directory,name) for name in sorted(os.listdir(directory)) ]

       self.names.extend( new_names )

       self._setDirty()

    def get_raw_from_list(self,prfx,list_file):
       """Get the dataset of raw files as listed in a text file as run/subrun combinations."""

       logger.info('Reading list file %s ...',list_file)

       if not os.path.isdir(prfx):
           logger.error('Directory $s does not exist',prfx)
           return

       if not os.path.exists(list_file):
           logger.error('File %s does not exist',list_file)
           return

       f = open( list_file )
       for ln in f.readlines():

           chunks = ln.split()
           run = "%08d" % int(chunks[0])
           sub = "%04d" % int(chunks[1])
           rang = run[:5]+'000_'+run[:5]+'999'
           file = 'nd280_'+run+'_'+sub+'.daq.mid.gz'
           self.get_dataset( os.path.join(prfx,rang), file)

    def get_kin_range(self,fr,to):
        """Get the dataset of kin file numbers"""

        logger.info('Producing a list of kin file numbers in the range from %s to %s.',fr,to)

        self.names.extend([j for j in range(fr,to+1)])


class ND280DCacheDataset(ND280Dataset):

    """ND280 local datasets manages files located in a directory on a DCache server.
    By default, the configured server is TRIUMF.
    And currently the only configured server is TRIUMF but later you will be able to use another server:
      dataset.server = 'TRIUMF'
    """
    
    _schema = Schema(Version(1,0), {
        'names': SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='List of input files with full path to get the file on the server, i.e. "dcap://the/path/thefile'),
        'server': SimpleItem(defvalue = "TRIUMF", doc='Name of the dcache server used'),
    })
    
    _name = 'ND280DCacheDataset'

    _exportmethods = ['get_dataset', 'get_dataset_filenames', 'get_dataset_from_list', 'set_dataset_into_list', 'set_dataset_filenames' ]

    _GUIPrefs = [ { 'attribute' : 'names',  'attribute' :  'server',  'widget' : 'String_List' } ]

    _commandstr = getConfig('ND280')['ND280DCacheDatasetCommandStr']
    _filebasepath = getConfig('ND280')['ND280DCacheDatasetFileBasePath']

    def __init__(self):
        super(ND280DCacheDataset, self).__init__()


    def get_dataset(self,directory,filter=None):
      """Get the list of files in the directory on the dCache server.
      You can provide a wildcard filter such as 'oa_*.root'"""

      if not self.server in list(self._commandstr.keys()):
          logger.error('DCache server %s is unknown.', self.server)
          return

      logger.info('Reading %s ...',directory)

      command = self._commandstr[self.server] % directory
      rawoutput = subprocess.getoutput(command);
      allfiles = rawoutput.split("\n")

      # TODO: return directory error when curl isn't happy
      #if not os.path.isdir(directory):
      #    logger.error('Path %s is no directory',directory)
      #    return

      fullpath = os.path.join(self._filebasepath[self.server], directory )
      if filter:
          new_names = [ os.path.join(fullpath,name) for name in fnmatch.filter(allfiles,filter) ]
      else:
          new_names = [ os.path.join(fullpath,name) for name in allfiles ]

      self.names.extend( new_names )

      self._setDirty()
