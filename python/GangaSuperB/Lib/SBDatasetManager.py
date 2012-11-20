'''Unique class to monitor and transfer dataset'''

import copy
import os
import subprocess
import sys
import types

from Ganga.Core import GangaException
from Ganga.GPIDev.Base import GangaObject 
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import *
import Ganga.Utility.logging

import db
import objectid
import utils


class SBDatasetManager(GangaObject):
    '''Methods to manage datasets.'''
    
    _name = "SBDatasetManager"
    _category = "datasetManager"
    _exportmethods = ["badDataset",
                      "closeDataset",
                      "createDataset",
                      "deleteDataset",
                      "downloadDataset",
                      "getFileList",
                      "openDataset",
                      "showDatasets",
                      "whoami"]
    _schema = Schema( Version( 1, 0 ), { })
    
    
    def createDataset(self):
        '''Interactive method to guide the user in dataset creation procedure.
        If the dataset is a 'personal production' type, force user to provide 
        a filter key.'''
        
        def asksParameter(parameter):
            '''Interactive method requesting user the value of each parameter 
            per session (FastSim, FullSim, Analysis)'''
            if parameter['customValue'] and len(parameter['values']) == 0:
                value = raw_input('\nEnter %s: ' % parameter['label'])
            elif not parameter['customValue'] and len(parameter['values']) == 0:
                raise GangaException('Invalid rule (customValue:False and values=0).')
            else:
                table = list()
                
                i = 0
                for value in parameter['values']:
                    table.append({'id': i, 'value': value})
                    i += 1
                
                if parameter['customValue']:
                    table.append({'id': i, 'value': 'Enter a custom value'})
                
                print('\nChoose %s:' % parameter['label'])
                column_names = ('id', 'value')
                print(utils.format_dict_table(table, column_names))
                index = utils.getIndex(maxExclusive=len(table))
                
                if parameter['customValue'] and index == len(table)-1:
                    value = raw_input('Custom value: ')
                else:
                    value = table[index]['value']
            
            # parameter insertion in dictionary. It will be subsequently 
            #inserted into dataset analysis bookkeeping table, hstore field
            new_dataset['parameters'][parameter['name']] = value
            
            return value
        
        
        type = [
            dict(id = 0, dataset_type = 'FastSim Personal Production'),
            dict(id = 1, dataset_type = 'FullSim Personal Production'),
            dict(id = 2, dataset_type = 'Analysis'),
            ]
        
        column_names = ('id', 'dataset_type')
        print(utils.format_dict_table(type, column_names))
        index = utils.getIndex(maxExclusive=len(type))
        
        new_dataset = dict()
        new_dataset['parameters'] = dict()
        
        ####################
        # FAST Simulation session
        ####################
        # parameter check: mandatory, free string param management
        # TODO: parameter type check, evaluate the config file option to store parameters
        
        if index == 0:
            new_dataset['session'] = 'fastsim'
            
            parameters = [
                {"name": "evt_file", "label": "Events per file", "customValue": True, "values": []},
                {"name": "analysis", "label": "Analysis", "customValue": True, "values": ["BtoKNuNu", "BtoKstarNuNu", "DstD0ToXLL", "DstD0ToXLL", "Generics", "HadRecoilCocktail", "KplusNuNu", "SLRecoilCocktail", "tau->3mu"]},
                {"name": "dg", "label": "Geometry", "customValue": True, "values": ["DG_4", "DG_4a", "DG_BaBar"]},
                {"name": "generator", "label": "Generator", "customValue": True, "values": ["B0B0bar_Btag-HD_Cocktail", "B0B0bar_Btag-SL_e_mu_tau_Bsig-HD_SL_Cocktail", "B0B0bar_generic", "B0B0bar_K0nunu", "B0B0bar_K0nunu_SL_e_mu_tau", "B0B0bar_Kstar0nunu_Kpi", "B0B0bar_Kstar0nunu_Kpi_SL_e_mu_tau", "B+B-_Btag-HD_Cocktail", "B+B-_Btag-SL_e_mu_tau_Bsig-HD_SL_Cocktail", "B+B-_generic", "B+B-_K+nunu", "B+B-_K+nunu_SL_e_mu_tau", "B+B-_Kstar+nunu", "B+B-_Kstar+nunu_SL_e_mu_tau", "B+B-_taunu_SL_e_mu_tau", "bhabha_bhwide", "ccbar", "tau+tau-_kk2f", "uds", "udsc", "Upsilon4S_generic"]},
                {"name": "bkg_mixing", "label": "Background Mixing Type", "customValue": True, "values": ["All", "NoPair", "NoMixing"]},
                {"name": "analysis_type", "label": "Analysis Type", "customValue": True, "values": ["BtoKNuNu", "BtoKstarNuNu", "HadRecoil", "SemiLepKplusNuNu"]}
            ]
            
            for parameter in parameters:
                asksParameter(parameter)
        
        ####################
        # FULL Simulation session
        ####################
        elif index == 1:
            new_dataset['session'] = 'fullsim'
            
            parameters = [
                {"name": "evt_file", "label": "Events per file", "customValue": True, "values": []},
                {"name": "sim_type", "label": "Simulation Type", "customValue": False, "values": ["fullsim", "background_frame"]},
                {"name": "generator", "label": "Generator", "customValue": False, "values": ["RadBhaBha", "singleparticle"]},
                {"name": "dg", "label": "Geometry", "customValue": True, "values": ["Geometry_CIPE", "Geometry_CIPE_BGO", "Geometry_CIPE_CSI", "Geometry_CIPE_V00-00-02"]},
                {"name": "pl", "label": "Physics list", "customValue": True, "values": ["QGSP", "QGSP_BERT", "QGSP_BERT_HP"]},
                {"name": "g4ver", "label": "Geant 4 version", "customValue": True, "values": ["9.2", "9.3"]},
                {"name": "opt_photons", "label": "Optical Photons", "customValue": False, "values": ["OFF", "ON"]}
            ]
            radbhabha = [
                {"name": "brunobbbminde", "label": "Min. Delta E", "customValue": True, "values": []}
            ]
            singleParticle = [
                {"name": "brunopdg", "label": "PDG Code", "customValue": True, "values": []},
                {"name": "brunothetamin", "label": "Theta min.", "customValue": True, "values": []},
                {"name": "brunothetamax", "label": "Theta max.", "customValue": True, "values": []},
                {"name": "brunophimin", "label": "Phi min.", "customValue": True, "values": []},
                {"name": "brunophimax", "label": "Phi max.", "customValue": True, "values": []},
                {"name": "brunoemin", "label": "Energy (GeV) min.", "customValue": True, "values": []},
                {"name": "brunoemax", "label": "Energy (GeV) max.", "customValue": True, "values": []}
            ]
            
            for parameter in parameters:
                value = asksParameter(parameter)
                
                # parameter dependencies management
                if parameter['name'] == 'generator':
                    if value == 'singleparticle':
                        parameters.extend(singleParticle)
                    elif value == 'RadBhaBha':
                        parameters.extend(radbhabha)
        
        ####################
        # ANALYSIS session
        ####################
        elif index == 2:
            new_dataset['session'] = 'analysis'
        else:
            raise GangaException('Invalid selection.')
        
        
        while True:
            free_string = raw_input('\nEnter free string: ')
            max_length = 128
            
            if len(free_string) <= max_length:
                new_dataset['parameters']['free_string'] = free_string
                break
            else:
                print('Free string must be <= %d char long.' % max_length)
        
        # dataset-site relation set
        new_dataset['site'] = getConfig('SuperB')['submission_site']
        new_dataset['owner'] = utils.getOwner()
        new_dataset['dataset_id'] = str(objectid.ObjectId())
        
        print('\nNew dataset details:')
        self.printDatasetDetail(new_dataset)
        
        
        value = ''
        while True:
            value = raw_input('Type \'yes\' to confirm the dataset creation or (q)uit: ')
            if value == 'yes':
                break
            elif value == 'q':
                raise utils.QuitException()
        
        sql = '''INSERT INTO analysis_dataset
            (owner, dataset_id, session, parameters, status)
            VALUES (%s, decode(%s, 'hex'), %s, %s, 'prepared');
            
            INSERT INTO analysis_dataset_site
            (dataset_id, site)
            VALUES (decode(%s, 'hex'), %s);'''
        params = (new_dataset['owner'], 
            new_dataset['dataset_id'], 
            new_dataset['session'], 
            new_dataset['parameters'],
            new_dataset['dataset_id'],
            new_dataset['site'])
        db.write(sql, params)
    
    def deleteDataset(self, **kwargs):
        '''to delete empty (prepared status) dataset'''
        kwargs['owner'] = utils.getOwner()
        kwargs['status'] = ['prepared']
        
        datasets = self.getDataset(**kwargs)
        dataset = self.printDatasets(datasets)
        dataset_id = dataset['dataset_id']
        
        sql = 'DELETE FROM analysis_dataset WHERE dataset_id = %s'
        db.write(sql, (r'\x' + dataset_id, ))
    
    def downloadDataset(self, **kwargs):
        '''to retrieve all files belonging to a owned dataset from GRID to 
        submission machine'''
        # TODO: create surl file lists beside the lfn list to permit lcg-cp 
        #fail over chain implamantation and to permit the direct plugin
        # subjob configuration by user given list
        
        kwargs['owner'] = utils.getOwner()
        kwargs['files'] = 0
        
        datasets = self.getDataset(**kwargs)
        dataset = self.printDatasets(datasets)
        
        dataset_id = dataset['dataset_id']
        files = dataset['files']
        occupancy_human = dataset['occupancy_human']
        
        home = os.path.expanduser('~')
        s = os.statvfs(home)
        free_disk = utils.sizeof_fmt_binary(s.f_bsize * s.f_bavail)
        
        #print('\nFree disk space: %s' % free_disk)
        print('\nTotal download size: %s\n' % occupancy_human)
        
        sql = 'SELECT lfn FROM analysis_output WHERE dataset_id = %s'
        lfns = db.read(sql, (r'\x' + dataset_id, ))
        
        localdir = os.path.join(home, dataset_id)
        os.mkdir(localdir)
        
        print('Downloading to %s ...' % localdir)
        i = 1
        
        for lfn in lfns:
            source = lfn['lfn']
            destination = os.path.join(localdir, source.split('/')[-1])
            
            process = subprocess.Popen(['lcg-cp', source, destination], stdout=subprocess.PIPE, close_fds=True)
            outData, errData = process.communicate()
            retCode = process.poll()
            
            if retCode != 0:
                raise Exception('lcg-cp fail with return code %d' % retCode)
            
            sys.stdout.write('\b' * 80 + '%s/%s' % (str(i), str(files)))
            sys.stdout.flush()
            
            i += 1
    
    def getFileList(self, **kwargs):
        '''It creates a <dataset_id>.txt with all LFN (or SRM) belonging to
         the chosen dataset'''
        kwargs['files'] = 0
        
        datasets = self.getDataset(**kwargs)
        dataset = self.printDatasets(datasets)
        dataset_id = dataset['dataset_id']
        
        sql = 'SELECT lfn FROM output_union WHERE dataset_id = %s'
        lfns = db.read(sql, (r'\x' + dataset_id, ))
        
        fileList = os.path.join(os.path.expanduser('~'), dataset_id + '.txt')
        
        # creating $home/<dataset_id>.txt
        try:
            f = open(fileList, 'w')
            for lfn in lfns:
                f.write("%s\n" % lfn['lfn'])
        finally:
            f.close()
        
        print('\nList %s created' % fileList)
    
    def badDataset(self, **kwargs):
        '''to set dataset status to bad'''
        kwargs['status'] = ['open', 'closed']
        self.__changeStatus('bad', **kwargs)
    
    def openDataset(self, **kwargs):
        '''to set dataset status to open'''
        kwargs['status'] = ['closed']
        self.__changeStatus('open', **kwargs)
    
    def closeDataset(self, **kwargs):
        '''to set dataset status to close
        TODO: kill unfinished job.'''
        kwargs['status'] = ['open']
        self.__changeStatus('closed', **kwargs)
    
    def __changeStatus(self, new_status, **kwargs):
        kwargs['owner'] = utils.getOwner()
        
        datasets = self.getDataset(**kwargs)
        dataset = self.printDatasets(datasets)
        dataset_id = dataset['dataset_id']
        
        sql = 'UPDATE analysis_dataset SET status = %s WHERE dataset_id = %s'
        db.write(sql, (new_status, r'\x' + dataset_id))
    
    def showDatasets(self, **kwargs):
        '''Print all metadata of dataset(s).
        Metodo esportato in GPI, usa massicciamente printDataset.'''
        
        def getParent(dataset):
            if 'parent' in dataset['parameters']:
                sql = 'SELECT * FROM dataset_union WHERE dataset_id = %s'
                parent = db.read(sql, (dataset['parameters']['parent'], ))
                
                assert len(parent) == 1, 'Must be only one parent.'
                getParent(parent[0])
                print('')
                self.printDatasetDetail(parent[0])
        
        datasets = self.getDataset(**kwargs)
        dataset = self.printDatasets(datasets)
        
        if dataset['session'] == 'analysis' and dataset['status'] not in ['prepared', 'temp']:
            print('\nParent datasets, older first:')
            getParent(dataset)
            
        print('\nSelected dataset:')
        self.printDatasetDetail(dataset)
    
    def printDatasetDetail(self, dataset):
        '''Print a table containing all the key-value dataset metadata'''
        
        # TODO: show the expiration time for dataset in temp status
        # to solve the nast dictionary problem (see hstore field model) all 
        # the dataset contents have been put to the same level
        
        d = copy.deepcopy(dataset)
        d.update(d['parameters'])
        del d['parameters']
        
        # dataset keys sorting and dictionary list creation
        items = d.items()
        items.sort()
        d = [{'key': key, 'value': value} for key, value in items]
        
        columns = ['key', 'value']
        print(utils.format_dict_table(d, columns))
    
    def printDatasets(self, datasets):
        ''' Given the heterogeneous dataset list, the method splits it in 
        categories and build the table per session. A unique id all over 
        the sessions permit the user to select univocally a dataset. All 
        the metadata will be printed with the parent chain: a parent dataset
        is defined as the one used as input to create the child one. The 
        method is public but not exported in GPI'''
        
        # check the term width
        (width, height) = utils.getTerminalSize()
        if width < 160:
            logger.error("Your terminal's width is %d; must be at least 160\nYou can make your font size smaller" % width)
            raise utils.QuitException()
        
        # better a named tuple but available in Python 2.6 only
        # fullsim: the table elements should be common to all the dataset keys in the list
        # print table rules:
        grouped_datasets = [
            # 0
            {'title': 'Fastsim Official Production',
             'dataset': list(),
             'order_by': ['prod_series', 'analysis', 'generator', 'dg', 'bkg_mixing', 'analysis_type'],
             'columns': ['id', 'prod_series', 'analysis', 'generator', 'dg', 'bkg_mixing', 'analysis_type', 'status']
             },
            # 1
            {'title': 'Fastsim Personal Production',
             'dataset': list(),
             'order_by': ['free_string', 'analysis', 'generator', 'dg', 'bkg_mixing', 'analysis_type'],
             'columns': ['id', 'free_string', 'analysis', 'generator', 'dg', 'bkg_mixing', 'analysis_type', 'status'],
             },
            # 2
            {'title': 'Fullsim Official Production',
             'dataset': list(),
             'order_by': ['prod_series', 'simtype', 'generator', 'dg', 'pl', 'g4ver', 'opt_photons'],
             'columns': ['id', 'prod_series', 'simtype', 'generator', 'dg', 'pl', 'g4ver', 'opt_photons', 'status']
             },
            # 3
            {'title': 'Fullsim Personal Production',
             'dataset': list(),
             'order_by': ['free_string', 'generator', 'dg', 'pl', 'g4ver', 'opt_photons'],
             'columns': ['id', 'free_string', 'generator', 'dg', 'pl', 'g4ver', 'opt_photons', 'status']
             },
            # 4
            {'title': 'Analysis',
             'dataset': list(),
             'order_by': ['free_string', 'creation_date'],
             'columns': ['id', 'free_string', 'creation_date', 'status']
             }
        ]
        
        for dataset in datasets:
            # put sub dictionary elements to level zero dictionary 
            for key, value in dataset.items():
                if type(dataset[key]) is types.DictType:
                    for key1, value1 in dataset[key].iteritems():
                        dataset[key1] = value1
                    #del dataset[key]
            
            # dataset selection
            if dataset['session'] == 'fastsim':
                if dataset['owner'] == 'Official':
                    grouped_datasets[0]['dataset'].append(dataset)
                else:
                    grouped_datasets[1]['dataset'].append(dataset)
            elif dataset['session'] == 'fullsim':
                if dataset['owner'] == 'Official':
                    grouped_datasets[2]['dataset'].append(dataset)
                else:
                    grouped_datasets[3]['dataset'].append(dataset)
            elif dataset['session'] == 'analysis':
                grouped_datasets[4]['dataset'].append(dataset)
            else:
                raise GangaException('session not recognized: %s' % dataset['session'])
        
        i = 0
        
        # field sort, adding id and print
        for group in grouped_datasets:
            if len(group['dataset']) > 0:
                print('\n%s' % group['title'])
                
                # dictionary sorting
                group['dataset'] = sorted(group['dataset'], key=lambda elem: ('%s ' * len(group['order_by'])) % tuple([elem[d] for d in group['order_by']]))
                
                # id adding
                for dataset in group['dataset']:
                    dataset['id'] = i
                    i += 1
                
                print(utils.format_dict_table(group['dataset'], group['columns']))
        
        
        # ask for input and print dataset details 
        if i == 1:
            index = 0
            print('\nAutomatically selected the only entry')
        else:
            print('\nChoose the dataset:')
            index = utils.getIndex(maxExclusive=i)
        
        
        # Object oriented solution to investigate and/or binary search
        # datasets have been grouped per print rules
        for group in grouped_datasets:
            for d in group['dataset']:
                if d['id'] == index:
                     for dataset in datasets:
                         if dataset['dataset_id'] == d['dataset_id']:
                            return dataset
    
    def getDataset(self, **kwargs):
        '''Get all metadata of all datasets.
        Public method, not exported to GPI.'''
        
        db_view_column = ['dataset_id', 'creation_date', 'occupancy']
        sql = 'SELECT * FROM dataset_union WHERE true'
        kwargs['owner'] = kwargs.get('owner', ['official', utils.getOwner()])
        
        # add filter to query
        if len(kwargs) > 0:
            for key, value in kwargs.iteritems():
                if key in db_view_column:
                    sql += " AND %s ILIKE '%s%%'" % (key, value)
                elif key == 'files':
                    sql += " AND files > %s" % value
                elif key in ['status', 'session', 'owner']:
                    if not isinstance(value, list):
                        value = [value]
                    
                    sql += " AND (false"
                    for s in value:
                        sql += " OR %s ILIKE '%s%%'" % (key, s)
                    sql += ")"
                    
                else:
                    sql += " AND parameters->'%s' ILIKE '%s%%'" % (key, value)
        
        # clean up the query
        sql = sql.replace('false OR ', '')
        sql = sql.replace('true AND ', '')
        
        # TODO: add control to prevent sql injection
        datasets = db.read(sql)
        
        if len(datasets) == 0:
            raise GangaException('No dataset found')
        
        i = 0
        for dataset in datasets:
            dataset['id'] = i
            i += 1
            dataset['occupancy_human'] = utils.sizeof_fmt_binary(dataset['occupancy'])
            if 'evt_file' in dataset['parameters'] and not 'evt_tot' in dataset['parameters']:
                evt_file = int(dataset['parameters']['evt_file'])
                if dataset['files'] is None:
                    dataset['files'] = 0
                files = int(dataset['files'])
                dataset['parameters']['evt_tot'] = evt_file * files
            if 'evt_tot' in dataset['parameters']:
                dataset['parameters']['evt_tot_human'] = utils.sizeof_fmt_decimal(int(dataset['parameters']['evt_tot']))
        
        return datasets
    
    def whoami(self):
        '''Print the User id string'''
        print(utils.getOwner())
    
logger = Ganga.Utility.logging.getLogger()
