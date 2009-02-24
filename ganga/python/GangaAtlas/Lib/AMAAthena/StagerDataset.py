
##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: StagerDataset.py,v 1.4 2008-11-27 15:42:38 hclee Exp $
###############################################################################
# A DQ2 dataset

import sys, os, os.path, re, urllib, commands, imp, threading, time
from tempfile import mkstemp

from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import *
from Ganga.Lib.LCG import ElapsedTimeProfiler

from dq2.info import TiersOfATLAS
from dq2.info.TiersOfATLAS import ToACache
from dq2.clientapi.DQ2 import DQ2
from dq2.common.DQException import * 

from threading import Thread, Lock
from Queue import Queue, Empty

def find(dirs, pattern):
    """
    finds files within the given directories. Only the filename matching the 
    given pattern will be returned.
    """

    fpaths = []
    re_pat = re.compile(pattern)

    def __file_picker__(re_pat, dirname, names):
        for n in names:
            fpath = os.path.join(dirname,n)
            if re_pat.match(n):
                fpaths.append(fpath)
            else:
                logger.debug('ignore file: %s' % fpath)

    for dir in dirs:
        absdir = os.path.abspath( os.path.expandvars( os.path.expanduser(dir) ) )
        if not os.path.isdir(absdir):
            logger.warning('dir. not exist: %s' % absdir)
        else:
            os.path.walk(absdir, __file_picker__, arg=re_pat)

    return fpaths

def urisplit(uri):
   """
   Basic URI Parser according to STD66 aka RFC3986

   >>> urisplit("scheme://authority/path?query#fragment")
   ('scheme', 'authority', 'path', 'query', 'fragment') 

   """
   # regex straight from STD 66 section B
   regex = '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?'
   p = re.match(regex, uri).groups()
   scheme, authority, path, query, fragment = p[1], p[3], p[4], p[6], p[8]
   #if not path: path = None
   return (scheme, authority, path, query, fragment)

def get_srm_endpoint(site):
    '''
    Gets the SRM endpoint of a site registered in TiersOfATLAS. 
    '''

    srm_endpoint_info = {'token':None, 'endpt':None}
    re_srm2 = re.compile('^token:(.*):(srm:\/\/.*)\s*$')

    tmp = TiersOfATLAS.getSiteProperty(site,'srm')
    if tmp:
        srm_endpoint_info['endpt'] = tmp

    mat = re_srm2.match(tmp)
    if mat:
        srm_endpoint_info['token'] = mat.group(1)
        srm_endpoint_info['endpt'] = mat.group(2)

    return srm_endpoint_info

def get_srm_host(site):
    '''
    Gets the SRM hostname of the given site. 
    '''
    srm_endpoint_info = get_srm_endpoint(site)
    
    authority = urisplit(srm_endpoint_info['endpt'])[1]
    
    return authority.split(':')[0]

def get_lfc_host(site):
    '''
    Gets the LFC host of a site registered in TiersOfATLAS.
    '''

    lfc_url = TiersOfATLAS.getLocalCatalog(site)
    if lfc_url:
        return lfc_url.split('/')[2][:-1]
    else:
        return None

def get_pfns(lfc_host, guids, nthread=10, dummyOnly=False, debug=False):
    '''getting pfns corresponding to the given list of files represented
       by guids). If dummyOnly, then only the pfns doublely copied on the 
       the same SE are presented (determinated by SE hostname parsed from
       the PFNs).'''

    logger.info('resolving physical locations of replicas')

    try:
        import lfcthr
    except ImportError:
        logger.error('unable to load LFC python module. Please check LCG UI environment.')
        return {}

    pfns = {}

    # divide guids into chunks if the list is too large
    chunk_size = 1000
    num_chunks = len(guids) / chunk_size
    if len(guids) % chunk_size > 0:
        num_chunks += 1

    chunk_offsets = []
    for i in range(num_chunks):
        chunk_offsets.append(i*chunk_size)

    # backup the current LFC_HOST
    lfc_backup = None
    try:
        lfc_backup = os.environ['LFC_HOST'] 
    except:
        pass

    # set to use a proper LFC_HOST 
    os.putenv('LFC_HOST', lfc_host)

    def _resolveDummy(_pfns):
        '''resolving the dummy PFNs based on SE hostname'''
        _pfns_dummy = {}
        for _guid in _pfns.keys():
            _replicas = _pfns[_guid]
            _replicas.sort()
            seCache  = None
            pfnCache = None
            id = -1
            for _pfn in _replicas:
                id += 1 
                _se = urisplit(_pfn)[1]
                if _se != seCache:
                    seCache  = _se
                    pfnCache = _pfn
                else:
                    # keep the dummy PFN
                    if not _pfns_dummy.has_key(_guid):
                        _pfns_dummy[_guid] = [pfnCache]
                    _pfns_dummy[_guid].append(_pfn)
        return _pfns_dummy

    ## setup worker queue for LFC queries
    wq = Queue(len(chunk_offsets))
    for offset in chunk_offsets:
        wq.put(offset)

    mylock = Lock()
    def worker(id):
        # try to connect to LFC
        if lfcthr.lfc_startsess('', '') == 0:
            while not wq.empty():
                try:
                    idx_beg = wq.get(block=True, timeout=1)
                    idx_end = idx_beg + chunk_size
                    if idx_end > len(guids):
                        idx_end = len(guids)

                    logger.debug('worker id: %d on GUID chunk: %d-%d' % (id, idx_beg, idx_end))

                    result, list1 = lfcthr.lfc_getreplicas(guids[idx_beg:idx_end],"")
                 
                    if len(list1) > 0:
                        ## fill up global pfns dictionary
                        mylock.acquire()
                        for s in list1:
                            if s != None:
                                if s.sfn:
                                    if not pfns.has_key(s.guid):
                                        pfns[s.guid] = []
                                    pfns[s.guid].append(s.sfn)
                        mylock.release()
                except Empty:
                    pass
            # close the LFC session
            lfcthr.lfc_endsess()
        else:
            logger.error('cannot connect to LFC')

    # initialize lfcthr
    lfcthr.init()

    # prepare and run the query threads
    profiler = ElapsedTimeProfiler(logger=logger)
    profiler.start()
    threads = []
    for i in range(nthread):
        t = Thread(target=worker, kwargs={'id': i})
        t.setDaemon(False)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    if dummyOnly:
        pfns = _resolveDummy(pfns) 

    # roll back to the original LFC_HOST setup in the environment
    if lfc_backup:
        os.putenv('LFC_HOST', lfc_host)

    profiler.check( 'resolving %d files' % (len(guids)) )

    return pfns

def get_srmv2_sites(cloud=None, token=None, debug=False):
    '''
    Gets a list of SRMV2 enabled DDM sites in a given cloud.

    if token is given, only the site with the specific token type
    will be selected.
    '''

    srmv2_sites = []

    ## a better way of getting all sites within a cloud
    ## however, it seems there is a bug in DQ2 API so it  
    ## always returns an empty site list. 
    # all_sites   = TiersOfATLAS.getSitesInCloud(cloud)

    ## a bit of hack with non-public DQ2 API interface
    cache = TiersOfATLAS.ToACache
    
    print 'cache'

    all_sites = []
    if not cloud:
        all_sites = TiersOfATLAS.getAllDestinationSites()
    else:
        if cloud == 'T0':
            return ['CERNPROD']
        if cloud not in cache.dbcloud:
            return []
        all_sites = TiersOfATLAS.getSites(cache.dbcloud[cloud])

    for site in all_sites:
        srm = TiersOfATLAS.getSiteProperty(site,'srm')

        # presuming the srm endpoint looks like:
        #   token:ATLASDATADISK:srm://grid-cert-03.roma1.infn.it ...
        if srm is not None and srm.find('token') != -1:
            if token:
                if srm.split(':')[1] == token:
                    srmv2_sites.append(site)
            else: 
                srmv2_sites.append(site)
    
    return srmv2_sites

def resolve_file_locations(dataset, sites=None, cloud=None, token='ATLASDATADISK', debug=False):
    '''
    Summarize the locations of files (in terms of sitename) of a dataset.
    If the sites argument is given, ignoring cloud and token arguments;
    otherwise using cloud and toke to retrieve sites from TiersOfATLAS.
    '''

    if not sites:
        logger.debug('resolving sites with token: %s' % token)
        sites = get_srmv2_sites(cloud, token=token, debug=debug)

    logger.debug('checking replicas at sites: %s' % str(sites))

    replicas = {}
    # preparing the queue for querying lfn 
    wq = Queue(len(sites))
    for site in sites:
        wq.put(site)

    mylock = Lock()

    def worker(id):
        dq2 = DQ2()
        while not wq.empty():
            try:
                site = wq.get(block=True, timeout=1)
                replicaInfo = dq2.listFileReplicas(site, dataset)
                logger.debug('resolving dataset files at %s, no files: %d' % (site,len(replicaInfo[0]['content'])) )
                if replicaInfo:
                    mylock.acquire()
                    for guid in replicaInfo[0]['content']:
                        if not replicas.has_key(guid):
                            replicas[guid] = []
                        replicas[guid].append(site)
                    mylock.release()
            except Empty:
                pass
            except DQException, err:
                logger.warning(str(err))
                logger.warning('site %s excluded' % site)
                pass

    threads = []
    nthread = len(sites)
    if nthread > 10: nthread = 10

    for i in range(nthread):
        t = Thread(target=worker, kwargs={'id': i})
        t.setDaemon(False)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    return replicas    

class StagerDataset(DQ2Dataset):
    '''A customized DQ2 Dataset for AMA Stager'''

    _schema = Schema(Version(1,1), {
        'dataset': SimpleItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, doc='Dataset Name(s) or a root path in which the dataset files are located'),
        'guids'      : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc='GUID of Logical File Names'),

        'tagdataset' : SimpleItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, hidden=1, doc = 'Tag Dataset Name'),
        'use_aodesd_backnav' : SimpleItem(defvalue = False, doc = 'Use AOD to ESD Backnavigation',hidden=1),
        'names'      : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical File Names to use for processing', hidden=1),
        'exclude_names'      : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical File Names to exclude from processing', hidden=1),
        'number_of_files' : SimpleItem(defvalue = 0, doc = 'Number of files. ', hidden=1),
        'type'       : SimpleItem(defvalue = 'DQ2',  doc = 'Dataset type, DQ2 or LOCAL (refer to a local directory)', hidden=0),
        'datatype'   : SimpleItem(defvalue = '', doc = 'Data type: DATA, MC or MuonCalibStream', hidden=1),
        'accessprotocol'       : SimpleItem(defvalue = '', doc = 'Accessprotocol to use on worker node, e.g. Xrootd', hidden=1),
        'match_ce_all' : SimpleItem(defvalue = False, doc = 'Match complete and incomplete sources of dataset to CE during job submission', hidden=1),
        'min_num_files' : SimpleItem(defvalue = 0, doc = 'Number of minimum files at incomplete dataset location', hidden=1),
        'complete_files_replicas' : SimpleItem(defvalue={},protected=1,copyable=0,transient=1,hidden=1,doc='The hidden cache of the info of the file replicas of the complete datasets'),
        'check_md5sum' : SimpleItem(defvalue = False, doc = 'Check md5sum of input files on storage elemenet - very time consuming !', hidden=1)
    })

    _category = 'datasets'
    _name = 'StagerDataset'
    _exportmethods = [ 'list_datasets', 'list_contents', 'get_surls', 'get_locations', 'get_file_locations', 'make_input_option_file', 'get_complete_files_replicas', 'make_sample_file' ]

    _GUIPrefs = [ { 'attribute' : 'dataset',     'widget' : 'String_List' }]

    def __init__(self):
        super( StagerDataset, self ).__init__()

    def __setattr__(self, attr, value):

        DQ2Dataset.__setattr__(self, attr, value)

        if attr == 'dataset':
            self.complete_files_replicas = {} 

    def __resolve_containers(self, containers, nthreads=10):
        '''resolving dataset containers'''

        datasets = {} 
        
        wq = Queue(len(containers))
        for ds in containers:
            wq.put(ds)

        mylock = Lock()
        def worker(id):
            dq2 = DQ2()
            while not wq.empty():
                try:
                    ds = wq.get(block=True, timeout=1)
                    logger.debug('worker id: %d on dataset container: %s' % (id, ds))
       
                    datasets[ds] = []
 
                    ds_tmp = dq2.listDatasetsInContainer(ds)

                    mylock.acquire()
                    datasets[ds] = ds_tmp
                    mylock.release()
                except DQException, err:
                    logger.warning(str(err))
                except Empty:
                    pass

        profiler = ElapsedTimeProfiler(logger=logger)
        profiler.start()
        threads = []
        for i in range(nthreads):
            t = Thread(target=worker, kwargs={'id': i})
            t.setDaemon(False)
            threads.append(t)
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        profiler.check('resolving %d containers' % len(containers))

        return datasets

    def __expand_datasets(self):

        logger.debug('resolving wildcard dataset patterns')

        # try to expand the dataset if a wildcard pattern is given
        expanded_datasets = []
        for dsn in self.dataset:
            if dsn.find('*') > -1:
                expanded_datasets += [ds[0] for ds in self.get_datasets(dsn)]
            else:
                expanded_datasets.append(dsn)

        # resolving container datasets
        containers = []
        datasets   = []
        for dsn in expanded_datasets:
            if dsn[-1] == '/':
                containers += [ dsn ]
            else:
                datasets += [ dsn ]

        if len(containers) > 0:
            logger.debug('resolving dataset containers')
            container_ds = self.__resolve_containers(containers)
            for c in container_ds.keys():
                datasets += container_ds[c]

        self.dataset = datasets

    def dataset_exists(self):
        return DQ2Dataset.dataset_exists(self) 

    def tagdataset_exists(self):
        logger.error('method not supported')
        return None

    def get_datasets(self, name, filter=True):
        '''Get datasets names, especially when the name is given as a wildcard pattern'''
        profiler = ElapsedTimeProfiler(logger=logger)
        profiler.start()
        datasets = listDatasets(name, filter)
        profiler.check('listing %d datasets/containers' % len(datasets))

        return datasets
    
    def get_contents(self,backnav=False, overlap=True):
        '''Helper function to access dataset content'''

        # expand datasets in case there is a wildcard 
        self.__expand_datasets()

        # always get all contents
        self.number_of_files = 0

        return DQ2Dataset.get_contents(self, backnav=backnav, overlap=overlap)

    def get_tag_contents(self):
        '''Helper function to access tag datset content'''

        logger.error('method not supported')
        return None 

    def get_locations(self, complete=0, backnav=False, overlap=True):
        '''helper function to access the dataset location'''

        # expand datasets in case there is a wildcard 
        self.__expand_datasets()

        logger.debug('getting dataset locations')

        return DQ2Dataset.get_locations(self,complete=complete, backnav=backnav, overlap=overlap) 

    def get_file_locations(self, complete=1, sites=None, cloud=None):
        '''Helper function to resolve replica locations of files of the given datasets
           
           NB: by default, it checks only sites with complete datasets   
        '''

        profiler = ElapsedTimeProfiler(logger=logger)

        # get the dataset locations 
        ds_sites = self.get_locations(complete=complete, overlap=False)

        logger.info('resolving dataset file locations')
        profiler.start()
        replicas = {}
         
        #if complete:
        #    pass
        #else:
        for ds in ds_sites.keys():
            logger.debug('dataset: %s' % ds)
            logger.debug('sites: %s' % repr(ds_sites[ds]))
            replicas.update(resolve_file_locations(ds, sites=ds_sites[ds]))
         
        profiler.check('%d datasets %d files' % ( len(ds_sites.keys()), len(replicas.keys()) ))

        return replicas

    def get_complete_files_replicas(self, nthread=10, diskOnly=True):
        '''Gets a comprehensive dataset information about the contents and the
           location of COMPLETE replicas'''

        if not self.complete_files_replicas:

            re_tapeSite = re.compile('.*TAPE$') 


            ds_info = {}
            self.__expand_datasets()
         
            wq = Queue(len(self.dataset))
            for ds in self.dataset:
                wq.put(ds)
         
            mylock = Lock()
            def worker(id):
         
                dq2 = DQ2()
                while not wq.empty():
                    try:
         
                        ds = wq.get(block=True, timeout=1)
                        logger.debug('worker id: %d on dataset: %s' % (id, ds))
         
                        # get contents (guids) of the complete dataset
                        contents = dq2.listFilesInDataset(ds)
         
                        # get locations of the complete dataset replicas
                        locations = dq2.listDatasetReplicas(ds,complete=1)
         
                        vuid = None
                        try:
                            vuid = locations.keys()[0]
                        except IndexError, err:
                            pass
         
                        mylock.acquire()
         
                        # updating ds_info hastable
                        if vuid:
                            ds_info[ds] = []
                            ds_sites = []
 
                            if diskOnly:
                                for site in locations[vuid][1]:
                                    if not re_tapeSite.match(site):
                                        ds_sites.append(site)
                            else:
                                ds_sites = locations[vuid][1]
 
                            ds_info[ds] += [ contents[0], ds_sites ]
                        else:
                            logger.warning('dataset not available: %s' % ds)
         
                        mylock.release()
         
                    except DQException, err:
                        logger.warning(str(err))
         
                    except Empty:
                        pass
         
            # prepare and run the query threads
            profiler = ElapsedTimeProfiler(logger=logger)
            profiler.start()
            threads = []
            for i in range(nthread):
                t = Thread(target=worker, kwargs={'id': i})
                t.setDaemon(False)
                threads.append(t)
         
            for t in threads:
                t.start()
         
            for t in threads:
                t.join()

            self.complete_files_replicas = ds_info

            profiler.check( 'information collected: %d datasets' % ( len(self.complete_files_replicas.keys()) ) )
        else:
            logger.debug('using cached complete_files_replicas')
            pass
 
        return self.complete_files_replicas 

    def list_datasets(self,name,filter=True):
        '''List datasets names'''

        return DQ2Dataset.list_datasets(self, name=name, filter=filter)

    def list_contents(self,dataset=None):
        '''List dataset content'''

        self.__expand_datasets()

        return DQ2Dataset.list_contents(self, dataset=dataset)

    def list_locations(self,dataset=None,complete=0):
        '''List dataset locations'''

        # expand datasets in case there is a wildcard 
        self.__expand_datasets()

        return DQ2Dataset.list_locations(self, dataset=dataset, complete=complete)

    def list_locations_ce(self,dataset=None,complete=0):
        '''List the CE associated to the dataset location'''

        return DQ2Dataset.list_locations_ce(self, dataset=dataset, complete=complete)

    def list_locations_num_files(self,dataset=None,complete=-1,backnav=False):
        '''List the number of files replicated to the dataset locations'''

        return DQ2Dataset.list_locations_num_files(self, dataset=dataset, complete=complete, backnav=backnav) 

    def get_replica_listing(self,dataset=None,SURL=True,complete=0,backnav=False):
        '''Return list of guids/surl replicated dependent on dataset locations'''

        return DQ2Dataset.get_replica_listing(self, dataset=dataset, SURL=SURL, complete=complete, backnav=backnav)

    def list_locations_siteindex(self,dataset=None, timeout=15, days=2):
        return DQ2Dataset.list_locations_siteindex(self, dataset=dataset, timeout=timeout, days=days)

    def fill_guids(self):
        '''fill up the guids attribute of this StagerDataset object'''

        ds_info = self.get_complete_files_replicas()

        self.guids = []

        if ds_info:
            for ds in ds_info.keys():
                self.guids += ds_info[ds][0].keys()

    def get_surls(self, ddmSiteName=None, guidRefill=True):
        '''Gets a list of physical files located at certain site.
           Using DQ2_LOCAL_SITE_ID if site name is not specified explicitely.'''

        if ddmSiteName is None:
            ddmSiteName = config['DQ2_LOCAL_SITE_ID']

        srm_host = get_srm_host(ddmSiteName)
        lfc_host = get_lfc_host(ddmSiteName)

        if not srm_host or (not lfc_host):
            logger.error('site information not found in ToA: %s' % ddmSiteName)

        logger.debug('SRM Host: %s' % srm_host) 
        logger.debug('LFC Host: %s' % lfc_host)

        if guidRefill:
            self.fill_guids()

        # looks like self.guids is not a type of list
        # therfore a conversion is needed
        my_guids = []
        for guid in self.guids:
            my_guids.append(guid)

        pfns_all = get_pfns(lfc_host, my_guids)

        pfns = {}
        for guid in self.guids:
            try:
                for pfn in pfns_all[guid]:
                    if pfn.find(srm_host) > -1:
                        pfns[guid] = pfn
            except KeyError:
                logger.warning('replica of guid:%s not found at %s' % (guid, ddmSiteName))

        return pfns

    def make_sample_file(self, sampleName='MySample', ddmSiteName=None, filepath=None):
        '''Generates a grid sample file containing a list of SURLs of the dataset contents'''   
  
        # prepare for the  
        if not filepath:
            sfx      = '.sample'
            pfx      = '%s_' % sampleName 
            tmpf     = mkstemp(suffix=sfx, prefix=pfx)
            filepath = tmpf[1]

        # write out the sample file
        f = open(filepath,'w')
        f.write('TITLE: %s\n' % sampleName)

        if self.type in ['', 'DQ2']: 
            f.write('FLAGS: GridCopy=1\n')
            pfns = self.get_surls(guidRefill=False, ddmSiteName=ddmSiteName)
            for guid in pfns.keys():
                f.write('gridcopy://%s\n' % pfns[guid])
        elif self.type in ['LOCAL']:
            ## work through underlying directories to get '*.root*' files
            f.write('FLAGS: GridCopy=0\n')
            fpaths = find(dirs=self.dataset, pattern='.*\.root.*')
            for fpath in fpaths:
                f.write('%s\n' % fpath)

        # NOTE: test if empty line cause AMAAthena to crash 
        #f.write('\n')
        f.close()

        logger.debug('set grid_sample_file:%s' % filepath)
        self.grid_sample_file = File(filepath)

        return filepath

    def make_input_option_file(self, job=None, filepath=None):

        # determing the filepath of the input option file 
        if job:
            filepath   = os.path.join(job.inputdir,'input.py')
        elif not filepath:
            sfx      = '.py'
            pfx      = 'input_'
            tmpf     = mkstemp(suffix=sfx, prefix=pfx)
            filepath = tmpf[1]

        input_option = '''
if not SampleFile:
    SampleFile = 'grid_sample.list'

# input with FileStager
from FileStager.FileStagerTool import FileStagerTool
stagetool = FileStagerTool(sampleFile=SampleFile)

## get Reference to existing Athena job
from FileStager.FileStagerConf import FileStagerAlg
from AthenaCommon.AlgSequence import AlgSequence

thejob = AlgSequence()

if stagetool.DoStaging():
    thejob += FileStagerAlg('FileStager')
    thejob.FileStager.InputCollections = stagetool.GetSampleList()
    #thejob.FileStager.PipeLength = 2
    #thejob.FileStager.VerboseStager = True
    thejob.FileStager.BaseTmpdir    = stagetool.GetTmpdir()
    thejob.FileStager.InfilePrefix  = stagetool.InfilePrefix
    thejob.FileStager.OutfilePrefix = stagetool.OutfilePrefix
    thejob.FileStager.CpCommand     = stagetool.CpCommand
    thejob.FileStager.CpArguments   = stagetool.CpArguments
    thejob.FileStager.FirstFileAlreadyStaged = stagetool.StageFirstFile

## set input collections
if stagetool.DoStaging():
    ic = stagetool.GetStageCollections()
else:
    ic = stagetool.GetSampleList()

## get a handle on the ServiceManager
svcMgr = theApp.serviceMgr()
svcMgr.EventSelector.InputCollections = ic
'''
        f = open(filepath, 'w')
        f.write(input_option)
        f.close()

        if job:
            job.inputsandbox += [ File(filepath) ]

        logger.debug('input option file generated:%s' % filepath)

        return filepath

logger = getLogger()
config = getConfig('DQ2')
