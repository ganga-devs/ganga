'''This class defines the output of the job and checks if exist a corresponding dataset into DB.'''

import os

from Ganga.Core import ApplicationConfigurationError
from Ganga.Core import GangaException
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Lib.File import File
from Ganga.Utility.Config import *
import Ganga.Utility.logging

import db
import SBDatasetManager
import SBInputDataset
import utils


class SBOutputDataset(Dataset):
    '''Provides methods to set and verify output informations (like output dataset, output files name, output site, etc..).
    The output dataset(s) must exist before submission(it can be created through SBDatasetManager), this is possible because 
    it's known what kind of file the job will be create.'''
    
    _name = "SBOutputDataset"
    _category = "datasets"
    _exportmethods = ["check",
                      "setOutputDataset",]
    _schema = Schema( Version( 1, 0 ), {
        "pairs" : SimpleItem(defvalue={}, hidden=0, protected=0, copyable=1, doc="pattern/dataset_id association"),
    })
    
    def check(self):
        '''This method validates output files informations at submission phase'''
        if len(self.pairs) == 0:
            raise ApplicationConfigurationError(None, 'output dataset pairs cannot be empty')
        
        for key, value in self.pairs.items():
            kwargs = dict()
            kwargs['dataset_id'] = value
            kwargs['owner'] = utils.getOwner()
            
            manager = SBDatasetManager.SBDatasetManager()
            datasets = manager.getDataset(**kwargs)
            
            # only one dataset
            if len(datasets) == 0:
                msg = 'Output dataset %s not found' % value
                raise ApplicationConfigurationError(None, msg)
            assert len(datasets) == 1, 'Dataset consistency error'
            dataset = datasets[0]
            
            # owner
            if dataset['owner'] != utils.getOwner():
                msg = 'You are not the owner of the output dataset %s' % value
                raise ApplicationConfigurationError(None, msg)
            
            # status
            if dataset['status'] not in ['open', 'prepared']:
                msg = 'Output dataset %s status is not open or prepared' % value
                raise ApplicationConfigurationError(None, msg)
            
            # site
            sql = 'SELECT site FROM analysis_dataset_site WHERE dataset_id = %s'
            site = db.read(sql, (r'\x' + value, ))
            if site[0]['site'] != getConfig('SuperB')['submission_site']:
                msg = 'Output site mismatching: the submission site for dataset %s has to be %s' % (value, dataset['site'])
                raise ApplicationConfigurationError(None, msg)
            
            # session
            j = self.getJobObject()
            if isinstance(j.inputdata, SBInputDataset.SBInputPersonalProduction):
                if j.inputdata.session == 'FullSim' and dataset['session'] != 'fullsim':
                    msg = 'Output dataset type should be \'fullsim\''
                    raise ApplicationConfigurationError(None, msg)
                
                if j.inputdata.session == 'FastSim' and dataset['session'] != 'fastsim':
                    msg = 'Output dataset type should be \'fastsim\''
                    raise ApplicationConfigurationError(None, msg)
            else:
                if dataset['session'] != 'analysis':
                    msg = 'Output dataset type should be \'analysis\''
                    raise ApplicationConfigurationError(None, msg)
                
                # parent: exists only for analysis session
                if j.inputdata.dataset_id is None:
                    msg = 'Input dataset is not defined'
                    raise ApplicationConfigurationError(None, msg)
                else:
                    parent_dataset = j.inputdata.dataset_id
                if 'parent' not in dataset['parameters']:
                    sql = 'UPDATE analysis_dataset SET parameters = parameters || %s WHERE dataset_id = %s'
                    db.write(sql, ({'parent': parent_dataset}, r'\x' + value))
                elif dataset['parameters']['parent'] != parent_dataset:
                    msg = 'Input dataset must be %s' % dataset['parameters']['parent']
                    raise ApplicationConfigurationError(None, msg)
    
    def setOutputDataset(self, **kwargs):
        '''Through this method it is possible to define the partern of output files (like *.root) and the corresponding output dataset. 
        To choose the desired dataset, a list of all datasets of the chosen session is printed.'''
        
        key = raw_input('Enter a pattern (eg. *.root): ')
        
        j = self.getJobObject()
        
        if isinstance(j.inputdata, SBInputDataset.SBInputPersonalProduction):
            if j.inputdata.session == 'FastSim':
                kwargs['session'] = 'fastsim'
            elif j.inputdata.session == 'FullSim':
                kwargs['session'] = 'fullsim'
            else:
                raise GangaException('j.inputdata.session is \'%s\'. It must be \'FastSim\' or \'FullSim\'' % j.inputdata.session)
        else:
            kwargs['session'] = 'analysis'
        
        kwargs['owner'] = utils.getOwner()
        kwargs['status'] = ['open', 'prepared']
        
        manager = SBDatasetManager.SBDatasetManager()
        datasets = manager.getDataset(**kwargs)
        dataset = manager.printDatasets(datasets) # print dataset and choose one of them
        
        self.pairs[key] = dataset['dataset_id']

logger = Ganga.Utility.logging.getLogger()
