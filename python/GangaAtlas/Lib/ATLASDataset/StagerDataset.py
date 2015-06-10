
##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: StagerDataset.py,v 1.6 2009-03-26 20:33:11 hclee Exp $
###############################################################################
# ATLAS input dataset plugin for using Athena/FileStager with local/batch jobs

import os
import os.path
import re
import socket
from tempfile import mkstemp

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.Athena import dm_util
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import *
from Ganga.Lib.LCG import ElapsedTimeProfiler

from dq2.info import TiersOfATLAS
from dq2.clientapi.DQ2 import DQ2
from dq2.common.DQException import * 

from Ganga.Core.GangaThread import GangaThread
from Queue import Queue, Empty

#subprocess.py crashes if python 2.5 is used
#try to import subprocess from local python installation before an
#import from PYTHON_DIR is attempted some time later
try:
    import subprocess
except ImportError:
    pass

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

def list_castor_files(dirs, pattern):
    '''Lists castor files in the given castor directory''' 
    fpaths = []

    re_pat = re.compile(pattern)

    re_dir = re.compile('^d[r|w|x|-]*$')

    def __is_dir__(fpath, fpath_mode):
        logger.debug('%s is a dir' % fpath)
        return re_dir.match(fpath_mode)

    def __file_picker__(re_pat, dirname):
        'looking for files iteratively into subdirectories' 

        my_fpaths = []

        fpath_modes = {} 

        cmd = 'rfdir %s | awk \'{print $1,$NF}\'' % dirname
        (rc, out, err) = execSyscmdSubprocess(cmd)

        if rc != 0:
            logger.error('error listing files: %s' % err)
        else:

            for l in map(lambda x:x.strip(), out.strip().split('\n')):
                (fpath_mode, name) = l.split()
                fpath_modes[name.strip()] = fpath_mode.strip()

            for n in fpath_modes.keys():
                fpath = os.path.join(dirname,n)
                if __is_dir__(fpath, fpath_modes[n]):
                    my_fpaths += __file_picker__(re_pat, fpath)
                else: 
                    if re_pat.match(n):
                        my_fpaths.append(fpath)
                    else:
                        logger.debug('ignore file: %s' % fpath)
        return my_fpaths

    ## top-level directories given by user
    for dir in dirs:
        fpaths += __file_picker__(re_pat, dir)

    return fpaths

def resolve_file_locations(dataset, sites=None, cloud=None, token='ATLASDATADISK', debug=False):
    '''
    Summarize the locations of files (in terms of sitename) of a dataset.
    If the sites argument is given, ignoring cloud and token arguments;
    otherwise using cloud and toke to retrieve sites from TiersOfATLAS.
    '''

    if not sites:
        logger.debug('resolving sites with token: %s' % token)
        sites = dm_util.get_srmv2_sites(cloud, token=token, debug=debug)

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
                        if guid not in replicas:
                            replicas[guid] = []
                        replicas[guid].append(site)
                    mylock.release()
            except Empty:
                pass
            except DQException as err:
                logger.warning(str(err))
                logger.warning('site %s excluded' % site)
                pass

    threads = []
    nthread = len(sites)
    if nthread > 10: nthread = 10

    for i in range(nthread):
        t = GangaThread(name='stager_ds_w_%d' % i, target=worker, kwargs={'id': i})
#        t.setDaemon(False)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    return replicas

## system command executor with subprocess
def execSyscmdSubprocess(cmd, wdir=os.getcwd()):
    '''executes system command vir subprocess module'''

    import subprocess

    exitcode = -999

    mystdout = ''
    mystderr = ''

    try:

        ## resetting essential env. variables
        my_env = os.environ

        if 'LD_LIBRARY_PATH_ORIG' in my_env:
            my_env['LD_LIBRARY_PATH'] = my_env['LD_LIBRARY_PATH_ORIG']

        if 'PATH_ORIG' in my_env:
            my_env['PATH'] = my_env['PATH_ORIG']

        if 'PYTHONPATH_ORIG' in my_env:
            my_env['PYTHONPATH'] = my_env['PYTHONPATH_ORIG']

        child = subprocess.Popen(cmd, cwd=wdir, env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (mystdout, mystderr) = child.communicate()

        exitcode = child.returncode

    finally:
        pass

    return (exitcode, mystdout, mystderr)

class StagerDatasetConfigError(ConfigError):
    '''An exception object for stager dataset configuration'''
    def __init__(self,what):
        ConfigError.__init__(self, what)
        
    def __str__(self):
        return "StagerDatasetConfigError: %s "%(self.what)

class StagerDataset(DQ2Dataset):
    '''Input dataset specification for using Athena/FileStager module to get input files for analysis.

    Usage:

      - work with DQ2 dataset given a DQ2 dataset 'user.RichardHawkings.0108175.topmix_Muon.AOD.v4':

        In [n]: j.inputdata = StagerDataset()
        In [n]: j.inputdata.type = 'DQ2'
        In [n]: j.inputdata.dataset += [ 'user.RichardHawkings.0108175.topmix_Muon.AOD.v4' ]

        Please note that 'DQ2' type works only when the current Ganga client is in the same domain as the SE
        associated with the DDM site defined by 'config.DQ2.DQ2_LOCAL_SITE_ID'.

      - work with CASTOR/DPM given a directory '/castor/cern.ch/grid/atlas/atlasgroupdisk/phys-top/dq2/user/RichardHawkings/user.RichardHawkings.0108175.topmix_Muon.AOD.v4' in CASTOR/DPM name space:

        In [n]: j.inputdata = StagerDataset()
        In [n]: j.inputdata.type = 'CASTOR'
        In [n]: j.inputdata.dataset += ['/castor/cern.ch/grid/atlas/atlasgroupdisk/phys-top/dq2/user/RichardHawkings/user.RichardHawkings.0108175.topmix_Muon.AOD.v4']

      - work with files in a given directory '/home/data/user.RichardHawkings.0108175.topmix_Muon.AOD.v4' on local disk:

        In [n]: j.inputdata = StagerDataset()
        In [n]: j.inputdata.type = 'LOCAL'
        In [n]: j.inputdata.dataset += ['/home/data/user.RichardHawkings.0108175.topmix_Muon.AOD.v4']

      For reading files from CASTOR/DPM, one can also ask FileStager to use Xrootd protocol to copy the file (thus then "xrdcp" is used instead of "rfcp"):

        In [n]: j.inputdata.xrootd_access = True
    '''

    _schema = Schema(Version(1,1), {
        'dataset': SimpleItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, doc='Dataset Name(s) or a root path in which the dataset files are located'),
        'guids'      : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc='GUID of Logical File Names'),
        'tagdataset' : SimpleItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, hidden=1, doc = 'Tag Dataset Name'),
        'use_aodesd_backnav' : SimpleItem(defvalue = False, doc = 'Use AOD to ESD Backnavigation',hidden=1),
        'names'      : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical File Names to use for processing', hidden=0),
        'exclude_names'      : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, copyable=0, doc = 'Logical File Names to exclude from processing', hidden=1),
        'number_of_files' : SimpleItem(defvalue = 0, doc = 'Number of files. ', copyable=0, hidden=1),
        'type'       : SimpleItem(defvalue = 'DQ2',  doc = 'Dataset type: DQ2 (refer to a DQ2 dataset), CASTOR/DPM (refer to a castor/dpm path) or LOCAL (refer to a local directory)', hidden=0),
        'xrootd_access': SimpleItem(defvalue = False,  doc = 'Sets to True for staging the dataset files via xrootd protocol.', hidden=0),
        'datatype'   : SimpleItem(defvalue = '', doc = 'Data type: DATA, MC or MuonCalibStream', hidden=0),
        'accessprotocol'       : SimpleItem(defvalue = '', doc = 'Accessprotocol to use on worker node, e.g. Xrootd', hidden=1),
        'match_ce_all' : SimpleItem(defvalue = False, doc = 'Match complete and incomplete sources of dataset to CE during job submission', hidden=1),
        'min_num_files' : SimpleItem(defvalue = 0, doc = 'Number of minimum files at incomplete dataset location', hidden=1),
        'complete_files_replicas' : SimpleItem(defvalue={},protected=1,copyable=0,transient=1,hidden=1,doc='The hidden cache of the info of the file replicas of the complete datasets'),
        'check_md5sum' : SimpleItem(defvalue = False, doc = 'Check md5sum of input files on storage elemenet - very time consuming !', hidden=1)
    })

    _category = 'datasets'
    _name = 'StagerDataset'
    _exportmethods = [ 'list_datasets', 'list_contents', 'get_surls', 'get_locations', 'get_file_locations', 'make_input_option_file', 'get_complete_files_replicas', 'make_FileStager_jobOptions' ]

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
                except DQException as err:
                    logger.warning(str(err))
                except Empty:
                    pass

        profiler = ElapsedTimeProfiler(logger=logger)
        profiler.start()
        threads = []
        for i in range(nthreads):
            t = GangaThread(name='stager_ds_w_%d' % i, target=worker, kwargs={'id': i})
#            t.setDaemon(False)
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
                        except IndexError as err:
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
         
                    except DQException as err:
                        logger.warning(str(err))
         
                    except Empty:
                        pass
         
            # prepare and run the query threads
            profiler = ElapsedTimeProfiler(logger=logger)
            profiler.start()
            threads = []
            for i in range(nthread):
                t = GangaThread(name='stager_ds_w_%d' % i, target=worker, kwargs={'id': i})
#                t.setDaemon(False)
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
           Using DQ2_LOCAL_SITE_ID if site name is not specified explicitely.

           If the domain of the SE associated with the "ddmSiteName" is not matching the
           domain of the current Ganga client, an StagerDatasetConfigError exception is thrown.
           This is a protection to avoid local jobs to do lcg-cp across domains.'''

        pfns = {}
        if self.type in ['DQ2']:
            if ddmSiteName is None:
                ddmSiteName = config['DQ2_LOCAL_SITE_ID']
         
            srm_host = dm_util.get_srm_host(ddmSiteName)
            lfc_host = dm_util.get_lfc_host(ddmSiteName)
            cli_host = socket.getfqdn()
         
            if not srm_host or (not lfc_host):
                raise StagerDatasetConfigError('site information not found in ToA: %s' % ddmSiteName)

            # try to make a protection if srm_host and cli_host are not in the same domain
            # 1. determin the domainname patter by pasing "cli_host": the hostname of the Ganga client
            # 2. do a pattern matching on srm_host.
            # 3. if the matching on step 2 failed, srm_host and cli_host are not
            #    in the same domain (by protection) this should be failed
            cli_host_info = cli_host.split('.', 1)
            re_cli_domain = re.compile( '.*%s.*' % cli_host_info[-1] )

            if not re_cli_domain.match( srm_host ):
                raise StagerDatasetConfigError('SE (%s) not in the same domain as this client (%s)'% (srm_host, cli_host) )

            logger.debug('CLI Host: %s' % cli_host)
            logger.debug('SRM Host: %s' % srm_host)
            logger.debug('LFC Host: %s' % lfc_host)
         
            if guidRefill:
                self.fill_guids()
         
            # looks like self.guids is not a type of list
            # therfore a conversion is needed
            my_guids = []
            for guid in self.guids:
                my_guids.append(guid)
         
            pfns_all, cksum_all = dm_util.get_pfns(lfc_host, my_guids)
         
            for guid in self.guids:
                try:
                    for pfn in pfns_all[guid]:
                        if pfn.find(srm_host) > -1:
                            pfns[guid] = pfn
                except KeyError:
                    logger.warning('replica of guid:%s not found at %s' % (guid, ddmSiteName))

        elif self.type in ['DPM','CASTOR']:
            #fpaths = list_castor_files(dirs=self.dataset, pattern='.*\.root.*')
            fpaths = list_castor_files(dirs=self.dataset, pattern='.*')
            fpaths.sort()
            id = 0
            for fpath in fpaths:
                pfns[id] = fpath
                id += 1
            
        elif self.type in ['LOCAL']:
            #fpaths = find(dirs=self.dataset, pattern='.*\.root.*')
            fpaths = find(dirs=self.dataset, pattern='.*')
            fpaths.sort()
            id = 0
            for fpath in fpaths:
                pfns[id] = fpath
                id += 1

        return pfns

    def resolve_FileStager_Configurations(self, ddmSiteName=None):
        '''Resolves the PFNs into FileStager TURLs, e.g. srm://... -> gridcopy://srm://...'''

        turls        = []
        gridcopy     = False
        fs_cp_cmd    = ''
        fs_cp_args   = []
        fs_of_prefix = 'file:'

        if self.type in ['', 'DQ2']:

            if not self.names:

                guidRefill = False

                if not self.guids:
                    guidRefill = True
                
                pfns = self.get_surls(guidRefill=guidRefill, ddmSiteName=ddmSiteName)

                self.guids = pfns.keys()
                self.guids.sort()
                for guid in self.guids:
                    self.names.append( pfns[guid] )

            gridcopy  = True
            fs_cp_cmd = './fs-copy.py'
            for fpath in self.names:
                turls.append( '%s' % fpath )

        elif self.type in ['CASTOR','DPM']:
            
            if not self.names:
                self.names = self.get_surls().values()

            if self.xrootd_access:

                def __get_stage_host__(setype):
                    stage_host = ''
                    env_key = ''
                    try:
                        if setype in ['CASTOR']:
                            env_key = 'STAGE_HOST'
                        elif setype in ['DPM']:
                            env_key = 'DPM_HOST'
                        stage_host = os.environ[env_key]
                    except KeyError:
                        logger.warning('$%s not defined. xrootd_access disabled.' % env_key)
                    return stage_host

                ## try to resolve the xrootd host
                ##  - for CASTOR, it's the env. variable $STAGE_HOST
                ##  - for DPM, it's the env. variable $DPM_HOST (to be confirmed)
                stage_host = __get_stage_host__(self.type)

                if not stage_host:
                    self.xrootd_access = False

            if self.xrootd_access and stage_host:

                gridcopy     = True
                fs_cp_cmd    = 'xrdcp'
                fs_of_prefix = ''
                for fpath in self.names:
                    turls.append( 'root://%s/%s' % (stage_host, fpath) )
            else:

                gridcopy     = True
                fs_cp_cmd    = 'rfcp'
                fs_of_prefix = ''
                for fpath in self.names:
                    turls.append( '%s' % fpath )

        elif self.type in ['LOCAL']:

            gridcopy     = False
            fs_cp_cmd    = 'cp'
            fs_of_prefix = ''

            ## work through underlying directories to get '*.root*' files
            if not self.names:
                self.names = self.get_surls().values()
                
            for fpath in self.names:
                turls.append( '%s' % fpath )

        return (turls, gridcopy, fs_cp_cmd, fs_cp_args, fs_of_prefix)


    def make_FileStager_jobOptions(self, job=None, wdir=None, max_events=-1):
        '''makes FileStager job options and input collection job options'''

        (turls, gridcopy, cp_cmd, cp_args, of_prefix) = self.resolve_FileStager_Configurations()

        jo_name = 'FileStager_jobOption.py'
        ic_name = 'input.py'

        # determing the filepath of the input option file
        #  - if job object is given, store the job options in job's inputdir
        #  - else takes the wdir argument
        #  - else creates a temporary directory
        if job:
            wdir = job.inputdir
        else:
            if not wdir:
                wdir = tempfile.mkdtemp()

        jo_path  = os.path.join( wdir, jo_name )
        ic_path  = os.path.join( wdir, ic_name )

        if ( dm_util.make_FileStager_jobOption(turls, gridcopy=gridcopy, fs_cp_cmd=cp_cmd, fs_cp_args=cp_args, fs_of_prefix=of_prefix, maxEvent=max_events, ic_jo_path=ic_path, fs_jo_path=jo_path) ):

            if not os.path.exists( jo_path ):
                raise ApplicatonConfigurationError(None, 'job option files for FileStager not found: %s' % jo_path)

            if not os.path.exists( ic_path ):
                raise ApplicatonConfigurationError(None, 'job option files for FileStager not found: %s' % ic_path)

        else:
            raise ApplicatonConfigurationError(None, 'Unable to find/generate job option files for FileStager.')

        return (jo_path, ic_path)


logger = getLogger()
config = getConfig('DQ2')
