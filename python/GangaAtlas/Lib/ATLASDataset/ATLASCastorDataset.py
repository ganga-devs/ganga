##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ATLASCastorDataset.py,v 1.1 2008-07-17 16:41:18 moscicki Exp $
###############################################################################
# A simple ATLAS dataset
#
# ATLAS/ARDA

import os, re

from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *

from Ganga.Utility.logging import getLogger

from Ganga.Utility.Shell import Shell

logger = getLogger()
shell = Shell()
   
class ATLASCastorDataset(Dataset):
    '''ATLAS data as list of files on CASTOR'''
    
    _schema = Schema(Version(1,0), {
        'location' : SimpleItem(defvalue = '',
            doc = 'A directory on castor that contains the datasets in directories'),
        'pattern'  : SimpleItem(defvalue = '',
            doc = 'A regexp filter to select the correct files'),
        'dataset'  : SimpleItem(defvalue = '',
            doc = 'The name of the selected dataset'),
        'names'    : SimpleItem(defvalue = [], typelist=['str'], sequence = 1,
            doc = 'The selected file names')
    })
   
    _category = 'datasets'
    _name = 'ATLASCastorDataset'
   
    _exportmethods = ['set_dataset_type','list_datasets','get_dataset', 'get_dataset_filenames' ]

    _GUIPrefs = [ { 'attribute' : 'location',       'widget' : 'String' },
                  { 'attribute' : 'pattern',        'widget' : 'String' },
                  { 'attribute' : 'dataset',        'widget' : 'String' },
                  { 'attribute' : 'names',          'widget' : 'String_List' } ]
   
    def __init__(self):

        super(ATLASCastorDataset,self).__init__()
        self.set_dataset_type('mc11-aod')

    def set_dataset_type(self,name):
        '''Set the dataset type (rome-aod,rome-aod-merged, mc11-aod,csc11-aod)'''

        if name == 'rome-aod':
            self.location = '/castor/cern.ch/grid/atlas/datafiles/rome/recov10'
            self.pattern = '\S+\.\d+\.reco\S+\.\S+\.AOD\.pool\.root\S*'
        elif name == 'rome-aod-merged':
            self.location = '/castor/cern.ch/grid/atlas/datafiles/rome/recov10/merge'
            self.pattern = 'rome\.\d+\.merge\.\S+\.AOD\.pool\.root'
        elif name == 'csc11-aod':
            self.location = '/castor/cern.ch/atlas/csc/valiprod/sampleA/csc11'
            self.pattern = ''
        elif name == 'mc11-aod':
            self.location = '/castor/cern.ch/atlas/csc/valiprod/sampleA/mc11'
            self.pattern = ''
        else:
            logger.warning('Unknown dataset type %d. It should be rome-aod, rome-aod-merged, mc11-aod or csc11-aod.')
            self.location=''
            self.pattern=''

        self._setDirty()
   
    def list_datasets(self):
        '''List the datasets of a given type'''
     
        if not self.location:
            logger.error('No dataset location has been defined.')
            return

        datasets = []
        if self.location.find('valiprod') > 0:
            rc, output, m = shell.cmd1('nsls %s' % self.location,[0,1])
            for ds1 in re.findall('(\S+)\n',output):
                rc, output, m = shell.cmd1('nsls %s/%s/recon' % (self.location,ds1),[0,1])
                if rc == 1: continue
                datasets += [ '%s.recon.%s' % (ds1,ds2) for ds2 in re.findall('(\S+)\n',output) ]
 
        else:        
            rc, output, m = shell.cmd1('nsls %s' % self.location)
            datasets += [ ds for ds in re.findall('(\S+)\n',output) if ds.count('.') == 3 ]

        return datasets
         
    def get_dataset(self,dataset,start=0,end=-1):
       '''Get the actual files of a dataset.'''
      
       if not self.location:
           logger.error('No dataset location has been defined.')
           return
       
       if self.location.find('valiprod') > 0:
           ds = dataset.split('.')
           if len(ds) == 5:
              path = '%s/%s.%s/recon/%s' % (self.location,ds[1],ds[2],ds[4])
           elif len(ds) == 6:           
              path = '%s/%s.%s/recon/%s' % (self.location,ds[1],ds[2],ds[5])
           else:
              logger.error('Invalid dataset name %s',dataset)
              return
              
       else:
           path = '%s/%s' % (self.location,dataset)

       logger.info('Reading %s ...',path)

       rc, output, m = shell.cmd1('nsls %s' % path,[0,1])
       if rc == 1: 
           logger.error('Dataset %s does not exist at %s',dataset,self.location)
           return

       self.dataset = dataset

       if self.pattern:
           names = re.findall('(%s)\n' % self.pattern,output)
       else:
           names = re.findall('(\S+)\n',output)

       self.names = names[start:end]

       self._setDirty()
           
    def filenames(self):
        '''Get the full filename'''
      
        if self.location.find('valiprod') > 0:

            ds = self.dataset.split('.')
            if len(ds) == 5:
                path = '%s/%s.%s/recon/%s' % (self.location,ds[1],ds[2],ds[4])
            elif len(ds) == 6:
                path = '%s/%s.%s/recon/%s' % (self.location,ds[1],ds[2],ds[5])
            else:
              logger.error('Invalid dataset name %s',dataset)
              return []
                                                                                                                                 
            return [ 'rfio:%s/%s' % (path,name) for name in self.names ]
        else:
            return [ 'rfio:%s/%s/%s' % (self.location,self.dataset,name) for name in self.names ]

    @staticmethod
    def get_filenames(app):
        '''Retrieve the file names starting from an application object'''
      
        job=app._getRoot()
        if not job:
            logger.warning('Application object is not associated to a job.')
	    return []
	 
#       jobs without inputdata are allowed
	 
        if not job.inputdata: return []
      
        if not job.inputdata._name == 'ATLASCastorDataset':
            logger.warning('Dataset is not of type ATLASCastorDataset.')
	    return []

        return job.inputdata.filenames()

    def get_dataset_filenames(self):
        '''Retrieve the file names'''
      
        return self.filenames()
