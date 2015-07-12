'''This module contains a class for each use case'''

import math
import os
from subprocess import *

from Ganga.Core import ApplicationConfigurationError
from Ganga.Core import GangaException
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Lib.File import File
import Ganga.Utility.logging

import db
import SBDatasetManager
import utils


logger = Ganga.Utility.logging.getLogger()


class SBInputProductionAnalysis(Dataset):
    '''Input dataset class for (official or personal) production data analysis'''
    
    _name = "SBInputProductionAnalysis"
    _category = "datasets"
    _exportmethods = ["check",
                      "getDataset"]
    _schema = Schema( Version( 1, 0 ), {
        # not protected
        "dataset_id" : SimpleItem(defvalue='', hidden=0, protected=0, copyable=1, doc="Input dataset_id"),
        "events_total" : SimpleItem(defvalue=0, hidden=0, protected=0, copyable=1, doc="minumun number of events to analyze, zero for all"),
        "events_per_subjobs" : SimpleItem(defvalue=0, hidden=0, protected=0, copyable=1, doc="maximum number of events for each subjob"),
        # protected
        "input_path" : SimpleItem(defvalue=[], hidden=0, protected=1, copyable=1, doc="List of input files"),
        "input_mode" : SimpleItem(defvalue='list', hidden=0, protected=1, copyable=1, doc="Input mode"),
        "number_of_subjobs" : SimpleItem(defvalue=0, protected=1, copyable=1, doc="Number of Subjobs"),
        "run_site" : SimpleItem(defvalue=[], protected=1, copyable=1, doc="Site where job will be running"),
    })
    
    def check(self):
        '''Checks done during submit phase. Protected elements are written.'''
        
        if self.dataset_id == '':
            raise ApplicationConfigurationError(None, 'You must define an input dataset')
        
        if self.events_per_subjobs == 0:
            raise ApplicationConfigurationError(None, 'You must define events_per_subjobs')
        
        kwargs = dict()
        kwargs['dataset_id'] = self.dataset_id
        
        manager = SBDatasetManager.SBDatasetManager()
        datasets = manager.getDataset(**kwargs)
        
        # only one dataset
        if len(datasets) == 0:
            msg = 'Input dataset %s not found' % self.dataset_id
            raise ApplicationConfigurationError(None, msg)
        assert len(datasets) == 1, 'Dataset consistency error'
        dataset = datasets[0]
        
        # status
        if dataset['status'] not in ['open', 'closed']:
            msg = 'Input dataset %s status is not open or closed' % self.dataset_id
            raise ApplicationConfigurationError(None, msg)
        
        # session
        if dataset['session'] not in ['fastsim', 'fullsim']:
            msg = 'Input dataset %s session is not fastsim or fullsim' % self.dataset_id
            raise ApplicationConfigurationError(None, msg)
        
        if self.events_total == 0:
            self.events_total = int(dataset['parameters']['evt_tot'])
        
        if self.events_total != 0 and self.events_total > int(dataset['parameters']['evt_tot']):
            msg = 'Input dataset %s total events is %d' % (self.dataset_id, dataset['parameters']['evt_tot'])
            raise ApplicationConfigurationError(None, msg)
        
        if self.events_per_subjobs < int(dataset['parameters']['evt_file']):
            msg = 'events_per_subjobs must be >= %s' % dataset['parameters']['evt_file']
            raise ApplicationConfigurationError(None, msg)
        
        if self.events_per_subjobs >= self.events_total:
            msg = 'events_per_subjobs cannot be >= events_total'
            raise ApplicationConfigurationError(None, msg)
        
        lfns = self.__getLFNs(dataset['parameters']['evt_file'])
        self.__createInputPath(lfns, dataset['parameters']['evt_file'])
        self.input_mode = 'list'
    
    def __getLFNs(self, evt_file):
        '''This method returns a list of LFNs from sbk database for the 
        selected dataset_id. The number of LFNs is calculated from the 
        number of selected events (events_total).'''
        
        if self.events_total == 0:
            num_files = 0
        else:
            num_files = int(math.ceil(float(self.events_total)/int(evt_file)))
        
        sql = 'SELECT lfn, size FROM output_union WHERE dataset_id = %s'
        param = [r'\x' + self.dataset_id]
        
        if num_files > 0:
            sql += ' ORDER BY RANDOM() LIMIT %s'
            param.append(num_files)
        
        return db.read(sql, param)
    
    def __createInputPath(self, lfns, evt_file):
        '''This method splits the list of LFNs between subjobs and writes a 
        text file for each one.'''
        
        evt_file = int(evt_file)
        
        # split all lfns between subjobs
        job = list()
        job.append(list())
        size = 0
        events = 0
        maxInput = 10 * (2**30) # 10GiB
        minInput = 2 * (2**30) # 2GiB
        
        # fill the subjobs al long as there are LFNs,
        # to determine the number of subjobs required 
        for lfn in lfns:
            if (size + int(lfn['size'])) < maxInput and (events + evt_file) <= self.events_per_subjobs:
                size += int(lfn['size'])
                events += evt_file
            else:
                job.append(list())
                size = int(lfn['size'])
                events = evt_file
            
            job[-1].append(lfn)
        
        self.number_of_subjobs = len(job)
        
        # level the number of LFNs between the subjob.
        tot_files = len(lfns)
        balanced_number_lfn_per_subjob = int(math.ceil(float(tot_files)/self.number_of_subjobs))
        job = list()
        self.input_path = list()
        max_size = 0
        jobInputDir = self.getJobObject().inputdir
        lfns_index = 0
        
        for subjob_id in xrange(self.number_of_subjobs):
            subjob = dict()
            size = 0
            events = 0
            number_lfns = 0
            subjob['id'] = str(subjob_id)
            subjob['list_path'] = os.path.join(jobInputDir, "list_%d.txt" % subjob_id)
            
            f = open(subjob['list_path'], 'w')
            try:
                for lfn in lfns[lfns_index:lfns_index + balanced_number_lfn_per_subjob]:
                    f.write(lfn['lfn'] + '\n')
                    size += int(lfn['size'])
                    events += evt_file
                    number_lfns += 1
            finally:
                f.close()
            
            lfns_index += balanced_number_lfn_per_subjob
            self.input_path.append(File(f.name))
            subjob['size'] = utils.sizeof_fmt_binary(size)
            subjob['events'] = utils.sizeof_fmt_decimal(int(events))
            subjob['lfns'] = number_lfns
            job.append(subjob)
            
            if size > max_size:
                max_size = size
        
        if max_size < minInput:
            logger.warning('These subjobs input is very small, to improve the \
            efficiency you could increase the numbers of events per subjob.')
        
        return job
    
    def getDefaultRunSite(self):
        '''This method is called only if backend is LCG.
        It retrieves the sites associated to dataset_id.'''
        
        sql = 'SELECT site FROM dataset_site_union WHERE dataset_id = %s'
        sites = db.read(sql, (r'\x' + self.dataset_id, ))
        
        for site in sites:
            self.run_site.append(site['site'])
    
    def getDataset(self, **kwargs):
        '''Interactive mathod. It prints the datasets (the user can apply filters),
        the user chooses one of them and inserts the number of events he wants.'''
        
        manager = SBDatasetManager.SBDatasetManager()
        
        def validateFilter(filter, allowed):
            kwargs[filter] = kwargs.get(filter, allowed)
            if not isinstance(kwargs[filter], list):
                kwargs[filter] = [kwargs[filter]]
            if not set(kwargs[filter]).issubset(set(allowed)):
                raise GangaException('%s must be %s' % (filter, allowed))
        
        validateFilter('status', ['open', 'closed'])
        validateFilter('session', ['fastsim', 'fullsim'])
        
        datasets = manager.getDataset(**kwargs)
        dataset = manager.printDatasets(datasets)
        
        self.dataset_id = dataset['dataset_id']
        
        print('\nChosen dataset details:')
        manager.printDatasetDetail(dataset)
        
        print('\nInsert the minimum number of events that you need for your analysis (zero for all):')
        self.events_total = utils.getIndex(maxInclusive=int(dataset['parameters']['evt_tot']))
        
        lfns = self.__getLFNs(dataset['parameters']['evt_file'])
        
        tot_size = 0
        tot_files = len(lfns)
        tot_events = int(dataset['parameters']['evt_file']) * tot_files
        
        for lfn in lfns:
            tot_size += int(lfn['size'])
        
        print('\nTotal job input size: ' + str(utils.sizeof_fmt_binary(tot_size)))
        print('Total selected number of events: ' + str(utils.sizeof_fmt_decimal(tot_events)))
        print('Total number of involved lfns: ' + str(tot_files))
        
        print('\nInsert the maximum number of events for each subjob. Remember:')
        print('- maximum output size is 2GiB.')
        print('- suggested maximum job duration 18h.')
        print('- maximum input size job is 10GiB.')
        print('- at least %s (that is the number of events of one file).' % dataset['parameters']['evt_file'])
        
        self.events_per_subjobs = utils.getIndex(minInclusive=int(dataset['parameters']['evt_file']), maxInclusive=tot_events)
        job = self.__createInputPath(lfns, dataset['parameters']['evt_file'])
        
        print('\nSubjobs details:')
        column_names = ['id', 'list_path', 'size', 'events', 'lfns']
        print(utils.format_dict_table(job, column_names))




class SBInputPureAnalysis(Dataset):
    '''Input dataset class that manage input dataset coming from use case analysis'''
    
    _name = "SBInputPureAnalysis"
    _category = "datasets"
    _exportmethods = ["check",
                      "getDataset"]
    _schema = Schema( Version( 1, 0 ), {
        # not protected
        "dataset_id" : SimpleItem(defvalue='', hidden=0, protected=0, copyable=1, doc="Input dataset_id"),
        "files_total" : SimpleItem(defvalue=0, hidden=0, protected=0, copyable=1, doc="minumun number of file to analyze, zero for all"),
        "files_per_subjobs" : SimpleItem(defvalue=0, hidden=0, protected=0, copyable=1, doc="maximum number of file for each subjob"),
        # protected
        "input_path" : SimpleItem(defvalue=[], hidden=0, protected=1, copyable=1, doc="List of input files"),
        "input_mode" : SimpleItem(defvalue='list', hidden=0, protected=1, copyable=1, doc="Input mode"),
        "number_of_subjobs" : SimpleItem(defvalue=0, protected=1, copyable=1, doc="Number of Subjobs"),
        "run_site" : SimpleItem(defvalue=[], protected=1, copyable=1, doc="Site where job will be running"),
    })
    
    def check(self):
        '''Checks done during submit phase. Protected elements are written.'''
        
        if self.dataset_id == '':
            raise ApplicationConfigurationError(None, 'You must define an input dataset')
        
        if self.files_per_subjobs == 0:
            raise ApplicationConfigurationError(None, 'You must define events_per_subjobs')
        
        kwargs = dict()
        kwargs['dataset_id'] = self.dataset_id
        
        manager = SBDatasetManager.SBDatasetManager()
        datasets = manager.getDataset(**kwargs)
        
        # only one dataset
        if len(datasets) == 0:
            msg = 'Input dataset %s not found' % self.dataset_id
            raise ApplicationConfigurationError(None, msg)
        assert len(datasets) == 1, 'Dataset consistency error'
        dataset = datasets[0]
        
        # status
        if dataset['status'] not in ['open', 'closed']:
            msg = 'Input dataset %s status is not open or closed' % self.dataset_id
            raise ApplicationConfigurationError(None, msg)
        
        # session
        if dataset['session'] not in ['analysis']:
            msg = 'Input dataset %s is not analysis' % self.dataset_id
            raise ApplicationConfigurationError(None, msg)
        
        if self.files_total == 0:
            self.files_total = int(dataset['files'])
        
        if self.files_total != 0 and self.files_total > int(dataset['files']):
            msg = 'Input dataset %s total files is %d' % (self.dataset_id, dataset['files'])
            raise ApplicationConfigurationError(None, msg)
        
        if self.files_per_subjobs >= self.files_total:
            msg = 'files_per_subjobs cannot be >= files_total'
            raise ApplicationConfigurationError(None, msg)
        
        lfns = self.__getLFNs()
        self.__createInputPath(lfns)
        self.input_mode = 'list'
    
    def __getLFNs(self):
        '''This method returns a list of LFNs from sbk database for the 
        selected dataset_id. The number of LFNs is provided by user.'''
        
        sql = 'SELECT lfn, size FROM output_union WHERE dataset_id = %s'
        param = [r'\x' + self.dataset_id]
        
        if self.files_total > 0:
            sql += ' ORDER BY RANDOM() LIMIT %s'
            param.append(self.files_total)
        
        return db.read(sql, param)
    
    def __createInputPath(self, lfns):
        '''This method splits the list of LFNs between subjobs and writes a 
        text file for each one.'''
        
        # split all lfns between subjobs
        job = list()
        job.append(list())
        size = 0
        files = 0
        maxInput = 10 * (2**30) # 10GiB
        minInput = 2 * (2**30) # 2GiB
        
        # fill the subjobs al long as there are LFNs,
        # to determine the number of subjobs required 
        for lfn in lfns:
            if (size + int(lfn['size'])) < maxInput and (files + 1) <= self.files_per_subjobs:
                size += int(lfn['size'])
                files += 1
            else:
                job.append(list())
                size = int(lfn['size'])
                files = 1
            
            job[-1].append(lfn)
        
        self.number_of_subjobs = len(job)
        
        # level the number of LFNs between the subjob.
        tot_files = len(lfns)
        balanced_number_lfn_per_subjob = int(math.ceil(float(tot_files)/self.number_of_subjobs))
        job = list()
        self.input_path = list()
        max_size = 0
        jobInputDir = self.getJobObject().inputdir
        lfns_index = 0
        
        for subjob_id in xrange(self.number_of_subjobs):
            subjob = dict()
            size = 0
            events = 0
            number_lfns = 0
            subjob['id'] = str(subjob_id)
            subjob['list_path'] = os.path.join(jobInputDir, "list_%d.txt" % subjob_id)
            
            f = open(subjob['list_path'], 'w')
            try:
                for lfn in lfns[lfns_index:lfns_index + balanced_number_lfn_per_subjob]:
                    f.write(lfn['lfn'] + '\n')
                    size += int(lfn['size'])
                    number_lfns += 1
            finally:
                f.close()
            
            lfns_index += balanced_number_lfn_per_subjob
            self.input_path.append(File(f.name))
            subjob['size'] = utils.sizeof_fmt_binary(size)
            subjob['lfns'] = number_lfns
            job.append(subjob)
            
            if size > max_size:
                max_size = size
        
        if max_size < minInput:
            logger.warning('These subjobs input is very small, to improve the efficiency you could increase the numbers of events per subjob.')
        
        return job
    
    def getDefaultRunSite(self):
        '''This method is called only if backend is LCG.
        It retrieves the sites associated to dataset_id.'''
        
        sql = 'SELECT site FROM dataset_site_union WHERE dataset_id = %s'
        sites = db.read(sql, (r'\x' + self.dataset_id, ))
        
        for site in sites:
            self.run_site.append(site['site'])
    
    def getDataset(self, **kwargs):
        '''Interactive method. It prints the datasets (the user can apply filters),
        the user chooses one of them and inserts the number of LFNs he wants.'''
        
        manager = SBDatasetManager.SBDatasetManager()
        
        def validateFilter(filter, allowed):
            kwargs[filter] = kwargs.get(filter, allowed)
            if not isinstance(kwargs[filter], list):
                kwargs[filter] = [kwargs[filter]]
            if not set(kwargs[filter]).issubset(set(allowed)):
                raise GangaException('%s must be %s' % (filter, allowed))
        
        validateFilter('status', ['open', 'closed'])
        validateFilter('session', ['analysis'])
        kwargs['files'] = 0
        
        datasets = manager.getDataset(**kwargs)
        dataset = manager.printDatasets(datasets)
        
        self.dataset_id = dataset['dataset_id']
        
        print('\nChosen dataset details:')
        manager.printDatasetDetail(dataset)
        
        print('\nInsert the minimum number of files that you need for your analysis (zero for all):')
        self.files_total = utils.getIndex(maxInclusive=int(dataset['files']))
        
        lfns = self.__getLFNs()
        
        tot_size = 0
        tot_files = len(lfns)
        
        for lfn in lfns:
            tot_size += int(lfn['size'])
        
        print('\nTotal job input size: ' + str(utils.sizeof_fmt_binary(tot_size)))
        print('Total number of involved lfns: ' + str(tot_files))
        
        print('\nInsert the maximum number of files for each subjob. Remember:')
        print('- maximum output size is 2GiB.')
        print('- suggested maximum job duration 18h.')
        print('- maximum input size job is 10GiB.')
        
        self.files_per_subjobs = utils.getIndex(minInclusive=1, maxInclusive=tot_files)
        job = self.__createInputPath(lfns)
        
        print('\nSubjobs details:')
        column_names = ['id', 'list_path', 'size', 'lfns']
        print(utils.format_dict_table(job, column_names))




class SBInputPersonalProduction(Dataset):
    '''Input dataset class for personal production use case. If session is:
    - fullsim: no input is requiered.
    - fastsim: user can enable last approved background frame as job input.'''
    
    _name = "SBInputPersonalProduction"
    _category = "datasets"
    _exportmethods = ["check",
                      "interactive",
                      "setSoftwareVersion"
                      ]
    _schema = Schema( Version( 1, 0 ), {
        "input_path" : SimpleItem(defvalue=[], hidden=0, protected=1, copyable=1, doc="List of input files"),
        "input_mode" : SimpleItem(defvalue='none', hidden=0, protected=1, copyable=1, doc="Input mode"),
        "run_site" : SimpleItem(defvalue=[], protected=1, copyable=1, doc="Site where job will be running"),
        
        "background_frame" : SimpleItem(defvalue=False, protected=0, copyable=1, doc="Enable input background frame if session is FastSim"),
        "number_of_subjobs" : SimpleItem(defvalue=0, doc="Number of Subjobs (1-250"),
        "soft_version" : SimpleItem(defvalue="", protected=0, copyable=1, doc="Simulation software version. Can be empty."),
        "session" : SimpleItem(defvalue="", protected=0, copyable=1, doc="Simulation type name. Can be empty."),
    })
    
    def __sbcurrent(self):
        '''Reads file .sbcurrent in user software directory to find session
        name and software version. If it can not find them, throws an exception.'''
        
        j = self.getJobObject()
        
        line = None
        
        try:
            sbcurrent = os.path.join(j.application.software_dir, '.sbcurrent')
            f = open(sbcurrent)
            try:
                line = f.read().splitlines()[0]
            finally:
                f.close()
        except:
            pass
        
        if line is None or len(line) == 0:
            raise GangaException('Unable to find software and the version in .sbcurrent')
        
        (self.session, self.soft_version) = line.split('/')
        logger.info('Found in .sbcurrent: %s %s' % (self.session, self.soft_version))
    
    def check(self):
        '''Checks done during submit phase. Protected elements are written.'''
        
        # controllare che session sia FastSim o FullSim
        if self.soft_version == '' and self.session == '':
            self.__sbcurrent()
        
        allowed_session = ['FastSim', 'FullSim']
        if self.session not in allowed_session:
            raise ApplicationConfigurationError(None, 'session must be %s' % allowed_session)
        
        if self.number_of_subjobs < 1 or self.number_of_subjobs > 250:
            raise ApplicationConfigurationError(None, 'number_of_subjobs must be between 1 and 250')
        
        sql = '''SELECT DISTINCT soft_version
            FROM session_site_soft
            WHERE session_name = %s
            ORDER BY soft_version'''
        
        supported_soft_version = db.read(sql, (self.session, ))
        # convert from list of dict to list of strings
        supported_soft_version = [s['soft_version'] for s in supported_soft_version]
        
        if self.soft_version not in supported_soft_version:
            raise ApplicationConfigurationError(None, 'supported soft_version are: %s' % supported_soft_version)
        
        if self.session == 'FastSim' and self.background_frame == True:
            results = db.read('''SELECT prod_series, lfn_dir
                FROM background_frame
                WHERE valid = true
                ORDER BY validation_timestamp DESC
                LIMIT 1''')
            
            self.input_path = list()
            self.input_path.append(results[0]['lfn_dir'])
            self.input_mode = 'dir'
            self.background_frame_prod_series = results[0]['prod_series']
            logger.info('Last approved (%s) background frame has been setup \
            for job input' % self.background_frame_prod_series)
    
    def interactive(self):
        '''Interactive find software version inside user software directory'''
        
        try:
            self.__sbcurrent()
        except GangaException as e:
            logger.debug(e)
            self.setSoftwareVersion()
    
    def setSoftwareVersion(self):
        '''Set software version. This function is called when interactive() 
        cannot find it automatically or is called by user if prefer set it 
        manually'''
        
        results = db.read('''SELECT session_name, soft_version
            FROM session_site_soft
            GROUP BY session_name, soft_version
            ORDER BY session_name, soft_version''')
        
        i = 0
        for result in results:
            result['id'] = i
            i += 1
        
        print('Choose simulation type:')
        column_names = ('id', 'session_name', 'soft_version')
        print(utils.format_dict_table(results, column_names))
        
        index = utils.getIndex(maxExclusive=len(results))
        
        self.session = results[index]['session_name']
        self.soft_version = results[index]['soft_version']
    
    def getDefaultRunSite(self):
        '''This method is called only if backend is LCG.
        It populates the run_site list with all compatible sites.'''
        
        sql = '''SELECT site
            FROM session_site_soft
            WHERE session_name = %s AND soft_version = %s'''
        param = [self.session, self.soft_version]
        
        if self.background_frame:
            sql += ''' AND site IN (
                SELECT site
                FROM background_frame_site
                WHERE prod_series = %s)'''
            param.append(self.background_frame_prod_series)
        
        results = db.read(sql, param)
        
        if len(results) == 0:
            raise ApplicationConfigurationError(None, 'No site found with the specified requirements')
        
        for result in results:
            self.run_site.append(result['site'])

