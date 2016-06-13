
##############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DQ2Dataset.py,v 1.38 2009-07-24 14:53:46 elmsheus Exp $
###############################################################################
# A DQ2 dataset

import sys, os, re, urllib, commands, imp, threading, time, fnmatch

from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.Utility.files import expandfilename
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig, ConfigError

from dq2.common.DQException import *
from dq2.info.TiersOfATLAS import _refreshToACache, ToACache, getSites
from dq2.repository.DQRepositoryException import DQUnknownDatasetException
from dq2.location.DQLocationException import DQLocationExistsException
from dq2.common.DQException import DQInvalidRequestException
from dq2.content.DQContentException import DQInvalidFileMetadataException
from dq2.common.client.DQClientException import DQInternalServerException
from dq2.common.dao.DQDaoException import DQDaoException
from dq2.info.TiersOfATLASValidator import is_site
from dq2.repository.DQRepositoryException import DQFrozenDatasetException

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname 
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm

_refreshToACache()

def cmpfun(a,b):
    """helper function for sorting tuples"""
    return cmp(a[1],b[1])

def convertDQ2ToClient(dataset):

    try:
        #dq2_lock.acquire()
        tmpListdq2 = dq2.listFilesInDataset(dataset, long=False)[0]
    except:
        tmpListdq2 = {}
    finally:
        #dq2_lock.release()
        pass

    tmpListPanda = {}

    for key, value in tmpListdq2.iteritems():
        tmpvalue = {}
        tmpvalue['scope'] = value['scope']
        tmpvalue['md5sum'] = value['checksum']
        lfn = value['lfn']
        tmpvalue['fsize'] = value['filesize']
        tmpvalue['guid'] = str(key)
        tmpListPanda[lfn] = tmpvalue

    return tmpListPanda


def getLocations(dataset):
    
    try:
        #dq2_lock.acquire()
        try:
            locations = dq2.listDatasetReplicas(dataset)
        except:
            logger.error('Dataset %s not found !', dataset)
            return []
    finally:
        #dq2_lock.release()
        pass

    return locations


def getDatasets(name):
    '''helper function for data dict'''

    try:
        #dq2_lock.acquire()
        datasets = dq2.listDatasets(name)
    finally:
        #dq2_lock.release()
        pass

    return datasets

def getElementsFromContainer(name):
    '''helper function for container content'''

    try:
        #dq2_lock.acquire()
        datasets = dq2.listDatasetsInContainer(name)
    finally:
        #dq2_lock.release()
        pass

    return datasets

def listDatasets(name,filter=True):
    '''helper function to filter out temporary datasets'''

    try:
        #dq2_lock.acquire()
        datasets = [ (lfn,ids['vuids'][0]) for lfn, ids in dq2.listDatasets(name).iteritems() ]
    finally:
        #dq2_lock.release()
        pass

    if filter:
        re_tmp = re.compile('bnl$|bnlcoll$|sub\d+$|dis\d+$')
        datasets = [ (dsn, vuid) for dsn, vuid in datasets if not re_tmp.search(dsn) ]

    return datasets

def getLocationsCE(locations):
    '''helper function to access the CE associated to a list of locations'''

    ces = []
    for location in locations:
        try:
            temp_ces = ToACache.sites[location]['ce']
            if temp_ces !=[""]:
                ces += temp_ces
        except KeyError:
            pass

    return ces
  
def getIncompleteLocationsCE(locations, minnum = 0):
    '''helper function to access the CE associated to a list of locations from incomplete list '''

    ces = []
    for dataset, info in locations.iteritems():
        for location, num in info.iteritems():
            if num >= minnum:
                try:
                    temp_ces = ToACache.sites[location]['ce']
                    if temp_ces !=[""]:
                        ces += temp_ces
                except KeyError:
                    pass

    return ces

def getIncompleteLocations(locations, minnum = 0):
    '''helper function to access a list of locations from incomplete list '''

    ces = []
    for dataset, info in locations.iteritems():
        for location, num in info.iteritems():
            if num >= minnum:
                ces.append(location)

    return ces


def isDQ2SRMSite(location):
    '''helper function to verify a location'''
    
    try:
        return 'srm' in ToACache.sites[location]
    except KeyError:
        return False


def dq2_list_locations_siteindex(datasets=[], timeout=15, days=2, replicaList=False, allowed_sites = [], fax_sites = [], skipReplicaLookup=False ):

    if datasets.__class__.__name__=='str':
        datasets = [ datasets ]

    dataset_locations_list = { }
    dataset_guid_location_list = {}
    guidLocation = {}
        
    for dataset in datasets:
        try:
            #dq2_lock.acquire()
            try:
                locations = dq2.listDatasetReplicas(dataset)
            except:
                logger.error('Dataset %s not found !', dataset)
                return {}
        finally:
            #dq2_lock.release()
            pass

        try:
            #dq2_lock.acquire()
            try:
                datasetinfo = dq2.listDatasets(dataset)
            except:
                datasetinfo = {}
        finally:
            #dq2_lock.release()
            pass

        # Rucio patch
        #if dataset.find(":")>0:
        #    try:
        #        datasettemp = dataset.split(":",1)[1]
        #    except:
        #        pass
        #    newdatasetinfo = {}
        #    newdatasetinfo[dataset] = datasetinfo[datasettemp]
        #    datasetinfo = newdatasetinfo

        try:
            datasetvuid = datasetinfo[dataset]['vuids'][0]
        except KeyError:
            try:
                datasetvuid = datasetinfo.values()[0]['vuids'][0]
            except:
                try:
                    datasetvuid = dq2.getMetaDataAttribute(dataset,['latestvuid'])['latestvuid']
                    import uuid
                    datasetvuid = str(uuid.UUID(datasetvuid))
                except:
                    logger.warning('Dataset %s not found',dataset)
                    return {}

        if datasetvuid not in locations:
            logger.warning('Dataset %s not found',dataset)
            return {}

        alllocations = locations[datasetvuid][0] + locations[datasetvuid][1]
        logger.info('Dataset %s has %s locations', dataset, len(alllocations))

        try:
            #dq2_lock.acquire()
            contents = dq2.listFilesInDataset(dataset, long=False)
        except:
            contents = {}
        finally:
            #dq2_lock.release()
            pass

        if not contents:
            logger.error('Dataset %s is empty.', dataset)
            return {}

        contents = contents[0]
        guidsDataset = []

        completeLocations = [ str(i) for i in locations[datasetvuid][1]]

        for guid, keys in contents.iteritems():
            guidsDataset.append(str(guid))
            if skipReplicaLookup:
                guidLocation[guid] = completeLocations
            else:
                guidLocation[guid] = []
            
        if replicaList and skipReplicaLookup:
            return guidLocation

        locations_checktime = {}
        locations_num = {}
        retry = 0
        allchecked = False

        if fax_sites:
            allowed_sites = allowed_sites+fax_sites
        
        if allowed_sites:
            alllocations = [ site for site in alllocations if site in allowed_sites ]

        while not allchecked and retry<4: 
            for location in alllocations:
                try:
                    #dq2_lock.acquire()
                    try:
                        datasetinfo = dq2.listMetaDataReplica(location, dataset)
                        logger.debug(datasetinfo)
                    except:
                        continue
                finally:
                    #dq2_lock.release()
                    pass

                if 'checkdate' in datasetinfo:
                    checkdate = datasetinfo['checkdate']
                    try:
                        checktime = time.mktime(time.strptime(checkdate.split(".")[0],'%Y-%m-%d %H:%M:%S'))
                    except ValueError:    
                        checktime = -time.time()
                    except TypeError:
                        # RUCIO fix
                        import datetime
                        checktime = time.mktime(checkdate.timetuple()) + checkdate.microsecond / 1E6
                else:
                    checktime = -time.time()
                    continue

                if (time.time()-checktime > days*86000): 
                    try:
                        #dq2_lock.acquire()
                        try:
                            dq2.checkDatasetConsistency(location, dataset)
                        except:
                            logger.warning("Dataset consistency check failed - continuing but may encounter other problems.")                        
                    finally:
                        #dq2_lock.release()
                        pass

                    logger.warning('Please be patient - waiting for site-index update at site %s ...', location)
                    locations_checktime[location] = False
                else:
                    locations_checktime[location] = True                    

            for location, value in locations_checktime.iteritems():
                if not value:
                    allchecked = False
                    break
                else:
                    allchecked = True

            if allchecked or len(locations_checktime) == 0:
                break

            retry = retry + 1        
            time.sleep(timeout)

        for location in alllocations:
            try:
                #dq2_lock.acquire()
                datasetsiteinfo = dq2.listFileReplicas(location, dataset)                
            except:
                datasetsiteinfo = {}
                return {}
            finally:
                #dq2_lock.release()
                pass

            # Rucio patch
            try:
                isfound = datasetsiteinfo[0]['found'] 
            except:
                isfound = datasetsiteinfo['found'] 

            if isfound != None: 
                try:
                    numberoffiles = datasetsiteinfo[0]['found']
                except:
                    numberoffiles = datasetsiteinfo['found']
                locations_num[str(location)]=int(numberoffiles)
             
                try:
                    guidsSite = datasetsiteinfo[0]['content']
                except:
                    guidsSite = datasetsiteinfo['content']

                for guid in guidsDataset:
                    if guid in guidsSite:
                        temp = guidLocation[guid]
                        temp.append(str(location))
                        if fax_sites:
                            for faxsite in fax_sites:
                                if not faxsite in temp:
                                    temp.append(faxsite)
                        guidLocation[guid] = temp
            else:
                logger.warning('cannot get file replica info - ignore site %s' % location)

        dataset_locations_list[dataset] = locations_num

    if replicaList:
        return guidLocation
    else:
        return dataset_locations_list


def resolve_container(datasets):
    """Helper function to resolver dataset containers"""
    container_datasets = []
    for dataset in datasets:
        if dataset.endswith("/"):
            try:
                #dq2_lock.acquire() 
                try:
                    contents = dq2.listDatasetsInContainer(dataset)
                except:
                    contents = []
            finally:
                #dq2_lock.release()
                pass
   
            if not contents:
                contents = []
            container_datasets = container_datasets + contents
    if container_datasets:
        return container_datasets
    else:
        return datasets

def _resolveSites(sites):

    new_sites = []
    for site in sites:
        if site in ToACache.topology:
            new_sites += _resolveSites(ToACache.topology[site])
        else:
            new_sites.append(site)

    return new_sites

def whichCloud (site):
    is_site(site)

    for cloudID, eachCloud in ToACache.dbcloud.iteritems():
        sites = getSites(eachCloud)
        if site in sites:
            return cloudID

    info = { 'CERN' : 'T0', 'CNAF' : 'IT', 'PIC': 'ES', 'LYON': 'FR',
             'RAL' : 'UK', 'FZK': 'DE', 'SARA' : 'NL', 'ASGC' : 'TW',
             'TRIUMF' : 'CA', 'BNL' : 'US', 'NDGF' : 'NG' }
    for sitename, cloud in info.iteritems():
        if site == sitename:
            return cloud
        
    return None

def dq2datasetstate(dataset):
    """Helper function to determine dataset status """
    if not dataset: return -1

    try:
        #dq2_lock.acquire()
        try:
            state = dq2.getState(dataset)
        except:
            state = -1
    finally:
        #dq2_lock.release()
        pass

    return state 

def dq2outputdatasetname(datasetname, jobid, isGroupDS, groupname):

    jobdate = time.strftime('%Y%m%d%H%M%S')
    usertag = config['usertag']

    # Get DN or nickname
    from Ganga.GPIDev.Credentials import GridProxy
    gridProxy = GridProxy()
    username = gridProxy.identity(safe=True)
    if config['ALLOW_MISSING_NICKNAME_DQ2OUTPUTDATASET']:
        nickname = getNickname(allowMissingNickname=True) 
    else:
        nickname = getNickname(allowMissingNickname=False) 
    
    if nickname and config['USE_NICKNAME_DQ2OUTPUTDATASET']:
        username = nickname
        if usertag.endswith('10'):
            usertag = 'user'

    # Remove apostrophe
    username = re.sub("'","",username)

    # prepare Group Dataset names
    if isGroupDS==True:
        usertag = re.sub("user", "group", usertag)
        if not usertag.startswith('group'):
            usertag = 'group' + time.strftime('%Y')[2:]
        if groupname:
            username = groupname
    
    # Automatic dataset name pattern
    if config['USE_NICKNAME_DQ2OUTPUTDATASET']:
        patName = '%s.%s.%s.%s' % (usertag, username, jobdate, jobid)
        patLfn = '%s/%s/%s/%s/' % (usertag, username, jobdate, patName)
    else:
        patName = '%s.%s.ganga.%s.%s' % (usertag, username, jobid, jobdate)
        patLfn = '%s/%s/ganga/%s/' % (usertag,username, patName)

    # Datasetname exists (configured or resubmission)
    if datasetname:
        # new datasetname during job resubmission
        pat = re.compile(r'^%s\.%s\.' % (usertag,username))
        if re.findall(pat,datasetname):
            datasetState = dq2datasetstate(datasetname)
            # dataset exists and is open or dataset does not exist
            if datasetState==0 or datasetState==-1:
                output_datasetname = datasetname
                if config['USE_NICKNAME_DQ2OUTPUTDATASET']:
                    output_lfn = '%s/%s/%s/%s/' % (usertag, username, jobdate, datasetname)
                else:
                    output_lfn = '%s/%s/ganga/%s/' % (usertag, username, datasetname)
            else:
                output_datasetname = patName                        
                output_lfn = patLfn
                logger.warning('Output dataset already exists and is closed/frozen. Overriding to %s', patName)
        else:
            output_datasetname = '%s.%s.%s' % (usertag, username, datasetname)
            if config['USE_NICKNAME_DQ2OUTPUTDATASET']:
                output_lfn = '%s/%s/%s/%s/' % (usertag, username, jobdate, output_datasetname)
            else:
                output_lfn = '%s/%s/ganga/%s/' % (usertag, username, output_datasetname)
    else:
        # No datasetname is given
        output_datasetname = patName
        output_lfn = patLfn

    # container limit: 131, Dataset limit: 200
    if output_datasetname[-1:] == '/' and len(output_datasetname)>config['OUTPUTDATASET_NAMELENGTH']:
        raise ApplicationConfigurationError(None,'DQ2OutputDataset.datasetname = %s is longer than limit of %s characters ! ' %(output_datasetname,config['OUTPUTDATASET_NAMELENGTH']))


    if output_datasetname[-1:] != '/' and len(output_datasetname)>200:
        raise ApplicationConfigurationError(None,'DQ2OutputDataset.datasetname = %s is longer than limit of 200 characters ! ' %(output_datasetname))

    return output_datasetname, output_lfn 

def dq2_set_dataset_lifetime(datasetname, location):

    rc = 1
    if config['OUTPUTDATASET_LIFETIME']:
        mylifetime = config['OUTPUTDATASET_LIFETIME']
        mylifetime = mylifetime.replace('_',' ')
        try:
            #dq2_lock.acquire() 
            try:
                rc = dq2.setReplicaMetaDataAttribute(datasetname, location, 'lifetime', mylifetime)
            except:
                rc = 0
        finally:
            #dq2_lock.release()
            pass
    else:
        pass

    return rc

class DQ2Dataset(Dataset):
    '''ATLAS DDM Dataset'''

    _schema = Schema(Version(1,0), {
        'dataset'            : SimpleItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, doc="Dataset Name(s)" ),
        'tag_info'          : SimpleItem(defvalue = {}, doc = 'TAG information used to split the job'),
        'tag_files'          : SimpleItem(defvalue = [], doc = 'Input TAG/ELSSI files to run over. tag_info structure will get filled on submission'),
        'tag_coll_ref'       : SimpleItem(defvalue = '', doc = 'Provide the collection ref if not in primary JOs (useful for TRF usage): AOD, ESD, RAW'),
        'tagdataset'         : SimpleItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, doc = 'Tag Dataset Name'),
        'use_cvmfs_tag'      : SimpleItem(defvalue = False, doc = 'Use CVMFS to access TAG files'),
        'use_aodesd_backnav' : SimpleItem(defvalue = False, doc = 'Use AOD to ESD Backnavigation'),
        'names'              : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical File Names to use for processing'),
        'names_pattern'      : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical file name pattern to use for processing'),
        'exclude_names'      : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical File Names to exclude from processing'),
        'exclude_pattern'    : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical file name pattern to exclude from processing'),
        'number_of_files'    : SimpleItem(defvalue = 0, doc = 'Number of files. '),
        'guids'              : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'GUID of Logical File Names'),
        'sizes'              : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Sizes of input files'),
        'checksums'          : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'md5sum or adler checksums of input files'),
        'scopes'             : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'scopes of the input files for RUCIO testing'),
        'type'               : SimpleItem(defvalue = '', doc = 'Dataset access on worker node: DQ2_LOCAL (default), DQ2_COPY, LFC'),
        'failover'           : SimpleItem(defvalue = False, doc = 'Use DQ2_COPY automatically if DQ2_LOCAL fails'),
        'datatype'           : SimpleItem(defvalue = '', doc = 'Data type: DATA, MC or MuonCalibStream'),
        'accessprotocol'     : SimpleItem(defvalue = '', doc = 'Accessprotocol to use on worker node, e.g. Xrootd'),
        'match_ce_all'       : SimpleItem(defvalue = False, doc = 'Match complete and incomplete sources of dataset to CE during job submission'),
        'min_num_files'      : SimpleItem(defvalue = 0, doc = 'Number of minimum files at incomplete dataset location'),
        'check_md5sum'       : SimpleItem(defvalue = False, doc = 'Check md5sum of input files on storage elemenet - very time consuming !')
    })

    _category = 'datasets'
    _name = 'DQ2Dataset'
    _exportmethods = [ 'list_datasets', 'list_contents', 'list_locations',
                       'list_locations_ce', 'list_locations_num_files',
                       'get_contents', 'get_locations', 'list_locations_siteindex' ]

    _GUIPrefs = [ { 'attribute' : 'dataset',         'widget' : 'String_List' },
                  { 'attribute' : 'tagdataset',      'widget' : 'String_List' },
                  { 'attribute' : 'names',           'widget' : 'String_List' },
                  { 'attribute' : 'names_pattern',   'widget' : 'String_List' },
                  { 'attribute' : 'exclude_names',   'widget' : 'String_List' },
                  { 'attribute' : 'exclude_pattern', 'widget' : 'String_List' },
                  { 'attribute' : 'number_of_files', 'widget' : 'String' },
                  { 'attribute' : 'guids',           'widget' : 'String_List' },
                  { 'attribute' : 'type',            'widget' : 'String_Choice', 'choices':['DQ2_LOCAL', 'DQ2_COPY', 'LFC']},
                  { 'attribute' : 'failover',        'widget' : 'Bool' },
                  { 'attribute' : 'datatype',        'widget' : 'String_Choice', 'choices':['DATA', 'MC', 'MuonCalibStream' ]},
                  { 'attribute' : 'accessprotocol',  'widget' : 'String' },
                  { 'attribute' : 'match_ce_all',    'widget' : 'Bool' },
                  { 'attribute' : 'min_num_files',   'widget' : 'Int' },
                  { 'attribute' : 'check_md5sum',    'widget' : 'Bool' } ]

    def __init__(self):
        super( DQ2Dataset, self ).__init__()

    def dataset_exists(self):

        if not self.dataset: return False

        for dataset in self.dataset:
            try:
                #dq2_lock.acquire()
                try:
                    state = dq2.getState(dataset)
                except:
                    state = None
            finally:
                #dq2_lock.release()
                pass
            if not state:
                break

        return not state is None

    def tagdataset_exists(self):

        if not self.tagdataset: return False

        for tagdataset in self.tagdataset:
            try:
                #dq2_lock.acquire()
                try:
                    state = dq2.getState(tagdataset)
                except DQUnknownDatasetException:
                    state = None
            finally:
                #dq2_lock.release()
                pass
            if not state:
                break

        return not state is None
    
    def get_contents(self,backnav=False, overlap=True, filesize=False, size=False, event=False):
        '''Helper function to access dataset content'''

        allcontents = []
        diffcontents = {}
        contents_size = {}
        contents_checksum = {}
        contents_scope = {}
        contents = []
        contents_new = []

        datasets = resolve_container(self.dataset)

        evtsperfile = 0
        # Get info from AMI for AMIDataset
        if (self._name == 'AMIDataset'):
                metadata = self.get_files_metadata()
                if (self._getParent()) and (self._getParent()).splitter and (self._getParent()).splitter.numevtsperfile:
                    evtsperfile = (self._getParent()).splitter.numevtsperfile

        for dataset in datasets:
            if backnav:
                dataset = re.sub('AOD','ESD',dataset)

            try:
                #dq2_lock.acquire()
                try:
                    contents = dq2.listFilesInDataset(dataset, long=False)
                except:
        
                    contents = []
                    raise ApplicationConfigurationError(None,'DQ2Dataset.get_contents(): problem in call dq2.listFilesInDataset(%s, long=False)' %dataset )
                    
            finally:
                #dq2_lock.release()
                pass

            if not contents:
                contents = []
                pass

            if not len(contents):
                continue

            # Convert 0.3 output to 0.2 style
            contents = contents[0]
            contents_new = []
            for guid, info in contents.iteritems():
                # Rucio patch
                contents_new.append( (str(guid), info['lfn']) )
                contents_size[guid] = info['filesize']
                contents_checksum[guid] = info['checksum']
                contents_scope[guid] = info['scope'] 
            contents = contents_new
            # Sort contents
            try:
                contents.sort(cmp=cmpfun)
            except:
                pass

            if backnav:
                return contents

            # Process only certain filenames ?
            if self.names:
                #job = self.getJobObject()
                contents = [ (guid,lfn) for guid, lfn in contents if lfn in self.names ]

            # Process only certain file name pattern ?
            if self.names_pattern:
                old_contents = contents
                contents = []
                for expattern in self.names_pattern:
                    regex = fnmatch.translate(expattern)
                    pat = re.compile(regex, re.IGNORECASE)
                    contents += [ (guid,lfn) for guid, lfn in old_contents if pat.match(lfn) and not (guid,lfn) in contents ]
                    
                    
            # Exclude certain filenames ?
            if self.exclude_names:
                #job = self.getJobObject()
                contents = [ (guid,lfn) for guid, lfn in contents if not lfn in self.exclude_names ]

            # Exclude certain file pattern ?
            if self.exclude_pattern:
                for expattern in self.exclude_pattern:
                    regex = fnmatch.translate(expattern)
                    pat = re.compile(regex, re.IGNORECASE)
                    contents = [ (guid,lfn) for guid, lfn in contents if not pat.match(lfn) ]


            # Exclude log files
            contents = [ (guid,lfn) for guid, lfn in contents if not lfn.endswith('log.tgz') ]
            pat = re.compile(r'.*log.tgz.[\w+]+$')
            contents = [ (guid,lfn) for guid, lfn in contents if not pat.match(lfn) ]

            # Process only certain number of files ?
            if self.number_of_files:
                numfiles = self.number_of_files
                if numfiles.__class__.__name__ == 'str':
                     numfiles = int(numfiles)

                if numfiles>0 and numfiles<len(contents):
                    contents_new = []
                    for i in xrange(0,numfiles):
                        contents_new.append(contents[i])

                    contents = contents_new

            allcontents = allcontents + contents
            diffcontents[dataset] = contents
            self.number_of_files = len(allcontents)

        diffcontentsNew = {}
        allcontentsSize = []
        diffcontentsSize = {}
        amicontent = {}
        if filesize or size or event:
            # Sum up all dataset filesizes:
            sumfilesize = 0 
            for guid, lfn in allcontents:
                if guid in contents_size:
                    try:
                        sumfilesize += contents_size[guid]
                        allcontentsSize.append((guid, (lfn, contents_size[guid],contents_checksum[guid],contents_scope[guid])))
                    except:
                        pass
            # Sum up dataset filesize per dataset:
            sumfilesizeDatasets = {}
            for dataset, contents in diffcontents.iteritems():
                contentsSize = []
                tmpInfo = []
                sumfilesizeDataset = 0
                for guid, lfn in contents:
                    if guid in contents_size:
                        try:
                            sumfilesizeDataset += contents_size[guid]
                            contentsSize.append((guid, (lfn, contents_size[guid], contents_checksum[guid],contents_scope[guid])))
                            if self._name == 'AMIDataset':
                                nevents = metadata.setdefault(guid,{'events':0, 'lfn':lfn, 'filesize': contents_size[guid]})['events']
                                if event and (nevents <=  0):
                                    if evtsperfile > 0:
                                        metadata[guid]['events'] = evtsperfile
                                    else:
                                        logger.warning("Couldn't get number of events  from AMI for dataset %s" %dataset)
                                        user_input = raw_input("Enter the number of events per file : ")
                                        evtsperfile = int(user_input)
                                        metadata[guid]['events'] = evtsperfile
                                tmpInfo.append((guid, (lfn, contents_size[guid], metadata[guid]['events'])))

                        except:
                            pass
                diffcontentsNew[dataset] = (contents, sumfilesizeDataset)
                diffcontentsSize[dataset] = contentsSize
                amicontent[dataset] = tmpInfo
        
        if overlap:
            if filesize:
                return allcontents, sumfilesize
            elif size:
                return allcontentsSize
            else:
                return allcontents
        else:
            if filesize:
                return diffcontentsNew
            elif size:
                return diffcontentsSize
            elif event:
                return amicontent 
            else:
                return diffcontents

    def get_tag_contents(self, size=False, spec_dataset = None):
        '''Helper function to access tag datset content'''

        allcontents = []

        datasets = []

        for d in self.tagdataset:
            datasets += resolve_container([d])
            
        allcontentsSize = []
            
        for tagdataset in datasets:

            if spec_dataset and tagdataset != spec_dataset:
                continue
            
            try:
                #dq2_lock.acquire()
                contents=dq2.listFilesInDataset(tagdataset, long=False)
            except:
                contents = {}
            finally:
                #dq2_lock.release()
                pass

            if not contents:
                return [] # protects against crash in next line if contents is empty
            if not len(contents):
                continue

            # Convert 0.3 output to 0.2 style
            contents = contents[0]
            contents_new = []
            contents_size = {}
            for guid, info in contents.iteritems():
                contents_new.append( (guid, info['lfn'], tagdataset, info['scope']) )
                allcontentsSize.append((guid, (info['lfn'],info['filesize'], info['scope'] ), tagdataset))
                
            allcontents = allcontents + contents_new

        if size:
            return allcontentsSize
        else:
            return allcontents
        
    def get_locations(self, complete=0, backnav=False, overlap=True):
        '''helper function to access the dataset location'''

        alllocations = {}
        overlaplocations = []

        datasets = resolve_container(self.dataset)
        
        for dataset in datasets:
            if backnav:
                dataset = re.sub('AOD','ESD',dataset)

            try:
                #dq2_lock.acquire()
                try:
                    locations = dq2.listDatasetReplicas(dataset)
                except:
                    logger.error('Dataset %s not found !', dataset)
                    return []
            finally:
                #dq2_lock.release()
                pass
            try:
                #dq2_lock.acquire()
                try:
                    datasetinfo = dq2.listDatasets(dataset)
                except:
                    datasetinfo = {}
            finally:
                #dq2_lock.release()
                pass

            # Rucio patch
            #if dataset.find(":")>0:
            #    try:
            #        datasettemp = dataset.split(":",1)[1]
            #    except:
            #        pass
            #    newdatasetinfo = {}
            #    newdatasetinfo[dataset] = datasetinfo[datasettemp]
            #    datasetinfo = newdatasetinfo

            try:
                datasetvuid = datasetinfo[dataset]['vuids'][0]
            except KeyError:
                try:
                    datasetvuid = datasetinfo.values()[0]['vuids'][0]
                except:
                    try:
                        datasetvuid = dq2.getMetaDataAttribute(dataset,['latestvuid'])['latestvuid']
                        import uuid
                        datasetvuid = str(uuid.UUID(datasetvuid))
                    except:
                        logger.warning('Dataset %s not found',dataset)
                        continue
                #return []

            if datasetvuid not in locations:
                logger.warning('Dataset %s not found',dataset)
                continue
                #return []
            if complete==0:
                templocations = locations[datasetvuid][0] + locations[datasetvuid][1]
            else:
                templocations = locations[datasetvuid][1]

            alllocations[dataset] = templocations

            if overlaplocations == []:
                overlaplocations = templocations

            overlaplocations_temp = []    
            for location in templocations:
                if location in overlaplocations:
                    overlaplocations_temp.append(location)
            overlaplocations = overlaplocations_temp

        if overlap:
            return overlaplocations
        else:
            return alllocations

    def list_datasets(self,name,filter=True):
        '''List datasets names'''

        datasets = listDatasets(name,filter)
        if not datasets:
            logger.error('No datasets found.')
            return

        for dsn, vuid in datasets:
            print dsn

    def list_contents(self,dataset=None):
        '''List dataset content'''

        if not dataset:
            datasets = self.dataset
        else:
            datasets = [ dataset ]

        for dataset in datasets:
            try:
                #dq2_lock.acquire()
                contents = dq2.listFilesInDataset(dataset, long=False)
            except:
                contents = {}
            finally:
                #dq2_lock.release()
                pass

            if not contents:
                print 'Dataset %s is empty.' % dataset
                return

            print 'Dataset %s' % dataset
            contents = contents[0]
            for guid, info in contents.iteritems():
                print '    %s' % info['lfn']
            print 'In total %d files' % len(contents)

    def list_locations(self,dataset=None,complete=0):
        '''List dataset locations'''

        if not dataset:
            datasets = self.dataset
        else:
            datasets = [ dataset ]

        datasets = resolve_container(datasets)

        for dataset in datasets:
            try:
                #dq2_lock.acquire()
                try:
                    locations = dq2.listDatasetReplicas(dataset,complete)
                except DQUnknownDatasetException:
                    logger.error('Dataset %s not found !', dataset)
                    return
                except DQDaoException:
                    completestr = 'complete'
                    if not complete: completestr = 'incomplete'

                    logger.error('Dataset %s has no %s location', dataset, completestr)
                    return

            finally:
                #dq2_lock.release()
                pass

            try:
                #dq2_lock.acquire()
                datasetinfo = dq2.listDatasets(dataset)
            finally:
                #dq2_lock.release()
                pass

            # RUCIO fix
            #try:
            #    dataset = dataset.split(":",1)[1]
            #except:
            #    pass

            try:
                datasetvuid = datasetinfo[dataset]['vuids'][0]
            except:
                try:
                    datasetvuid = datasetinfo.values()[0]['vuids'][0]
                except:
                    try:
                        datasetvuid = dq2.getMetaDataAttribute(dataset,['latestvuid'])['latestvuid']
                        import uuid
                        datasetvuid = str(uuid.UUID(datasetvuid))
                    except:
                        datasetvuid = ''

            if datasetvuid not in locations:
                print 'Dataset %s not found' % dataset
                return

            locations = locations[datasetvuid]

            print 'Dataset %s' % dataset
            if len(locations[1]): print 'Complete:', ' '.join(locations[1])
            if len(locations[0]): print 'Incomplete:', ' '.join(locations[0])

    def list_locations_ce(self,dataset=None,complete=0):
        '''List the CE associated to the dataset location'''

        if not dataset:
            datasets = self.dataset
        else:
            datasets = dataset

        datasets = resolve_container(datasets)

        for dataset in datasets:
            try:
                #dq2_lock.acquire()
                try:
                    locations = dq2.listDatasetReplicas(dataset,complete)
                except DQUnknownDatasetException:
                    logger.error('Dataset %s not found !', dataset)
                    return
            finally:
                #dq2_lock.release()
                pass

            try:
                #dq2_lock.acquire()
                datasetinfo = dq2.listDatasets(dataset)
            finally:
                #dq2_lock.release()
                pass

            try:
                datasetvuid = datasetinfo[dataset]['vuids'][0]
            except:
                try:
                    datasetvuid = datasetinfo.values()[0]['vuids'][0]
                except:
                    try:
                        datasetvuid = dq2.getMetaDataAttribute(dataset,['latestvuid'])['latestvuid']
                        import uuid
                        datasetvuid = str(uuid.UUID(datasetvuid))
                    except:
                        logger.warning('Dataset %s not found',dataset)
                        return

            if datasetvuid not in locations:
                print 'Dataset %s not found' % dataset
                return
            locations = locations[datasetvuid]

            print 'Dataset %s' % dataset
            if len(locations[1]): print 'Complete:', ' '.join(getLocationsCE(locations[1]))
            if len(locations[0]): print 'Incomplete:', ' '.join(getLocationsCE(locations[0]))

    def list_locations_num_files(self,dataset=None,complete=-1,backnav=False):
        '''List the number of files replicated to the dataset locations'''

        if not dataset:
            datasets = self.dataset
        else:
            datasets = [ dataset ]
            
        datasets = resolve_container(datasets)

        dataset_locations_num = {}

        from Ganga.Utility.GridShell import getShell
        gridshell = getShell()
        gridshell.env['LFC_CONNTIMEOUT'] = '45'

        gridshell.env['DQ2_URL_SERVER']=config['DQ2_URL_SERVER']
        gridshell.env['DQ2_URL_SERVER_SSL']=config['DQ2_URL_SERVER_SSL']
        gridshell.env['DQ2_LOCAL_ID']=''
        import GangaAtlas.PACKAGE
        try:
            pythonpath=GangaAtlas.PACKAGE.setup.getPackagePath2('DQ2Clients','PYTHONPATH',force=False)
        except:
            pythonpath = ''
        gridshell.env['PYTHONPATH'] = gridshell.env['PYTHONPATH']+':'+pythonpath
        ## exclude the Ganga-owned external package for LFC python binding
        pythonpaths = []
        for path in gridshell.env['PYTHONPATH'].split(':'):
            if not re.match('.*\/external\/lfc\/.*', path):
                pythonpaths.append(path)
        gridshell.env['PYTHONPATH'] = ':'.join(pythonpaths)

        ## exclude any rubbish from Athena
        ld_lib_paths = []
        for path in gridshell.env['LD_LIBRARY_PATH'].split(':'):
            if not re.match('.*\/external\/lfc\/.*', path) and not re.match('.*\/sw\/lcg\/external\/.*', path):
                ld_lib_paths.append(path)
        gridshell.env['LD_LIBRARY_PATH'] = ':'.join(ld_lib_paths)

        paths = []
        for path in gridshell.env['PATH'].split(':'):
            if not re.match('.*\/external\/lfc\/.*', path) and not re.match('.*\/sw\/lcg\/external\/.*', path):
                paths.append(path)
        gridshell.env['PATH'] = ':'.join(paths)

        for dataset in datasets:
            if backnav:
                dataset = re.sub('AOD','ESD',dataset)

            locations_num = {}
            exe = os.path.join(os.path.dirname(__file__)+'/ganga-readlfc.py')        
            cmd= exe + " %s %s " % (dataset, complete) 
            rc, out, m = gridshell.cmd1(cmd,allowed_exit=[0,142])

            if rc == 0 and not out.startswith('ERROR'):
                for line in out.split():
                    if line.startswith('#'):
                        info = line[1:].split(':')
                        if len(info)==2:
                            locations_num[info[0]]=int(info[1])
            elif rc==142:
                logger.error("LFC file catalog query time out - Retrying...")
                removelfclist = ""
                while rc!=0:
                    output = out.split()
                    try:
                        removelfc = output.pop()
                        if removelfclist == "":
                            removelfclist=removelfc
                        else:
                            removelfclist= removelfclist+","+removelfc
                    except IndexError:
                        logger.error("Empty LFC string of broken catalogs")
                        return {}
                    cmd = exe + " -r " + removelfclist + " %s %s" % (dataset, complete)
                    rc, out, m = gridshell.cmd1(cmd,allowed_exit=[0,142])

                if rc == 0 and not out.startswith('ERROR'):
                    for line in out.split():
                        if line.startswith('#'):
                            info = line[1:].split(':')
                            if len(info)==2:
                                locations_num[info[0]]=int(info[1])

            dataset_locations_num[dataset] = locations_num
        return dataset_locations_num

    def get_replica_listing(self,dataset=None,SURL=True,complete=0,backnav=False):
        '''Return list of guids/surl replicated dependent on dataset locations'''
        if not dataset:
            datasets = self.dataset
        else:
            datasets = [ dataset ]

        datasets = resolve_container(datasets)

        dataset_locations_list = {}
        for dataset in datasets:
            if backnav:
                dataset = re.sub('AOD','ESD',dataset)

            locations_list = {}
            from Ganga.Utility.GridShell import getShell
            gridshell = getShell()
            gridshell.env['LFC_CONNTIMEOUT'] = '45'
            exe = os.path.join(os.path.dirname(__file__)+'/ganga-readlfc.py')

            if SURL:
                cmd= exe + " -l %s %s " % (dataset, complete)
            else:
                cmd= exe + " -g %s %s " % (dataset, complete) 
            rc, out, m = gridshell.cmd1(cmd,allowed_exit=[0,142])

            if rc == 0 and not out.startswith('ERROR'):
                for line in out.split():
                    if line.startswith('#'):
                        info = line[1:].split(',')
                        if len(info)>1:
                            locations_list[info[0]]=info[1:]
            elif rc==142:
                logger.error("LFC file catalog query time out - Retrying...")
                removelfclist = ""
                while rc!=0:
                    output = out.split()
                    try:
                        removelfc = output.pop()
                        if removelfclist == "":
                            removelfclist=removelfc
                        else:
                            removelfclist= removelfclist+","+removelfc
                    except IndexError:
                        logger.error("Empty LFC string of broken catalogs")
                        return {}
                    cmd = exe + " -l -r " + removelfclist + " %s %s" % (dataset, complete)
                    rc, out, m = gridshell.cmd1(cmd,allowed_exit=[0,142])

                if rc == 0 and not out.startswith('ERROR'):
                    for line in out.split():
                        if line.startswith('#'):
                            info = line[1:].split(',')
                            if len(info)>1:
                                locations_list[info[0]]=info[1:]

            dataset_locations_list[dataset] = locations_list

        if dataset:
            return dataset_locations_list[dataset]
        else:
            return dataset_locations_list

    def list_locations_siteindex(self,dataset=None, timeout=15, days=2, replicaList=False, faxSites=[], skipReplicaLookup=False):

        if not dataset:
            datasets = self.dataset
        else:
            datasets = [ dataset ]

        datasets = resolve_container(datasets)

        return dq2_list_locations_siteindex(datasets, timeout, days, replicaList, fax_sites=faxSites, skipReplicaLookup=skipReplicaLookup)

class DQ2OutputDataset(Dataset):
    """DQ2 Dataset class for a dataset of output files"""
    
    _schema = Schema(Version(1,1), {
        'outputdata'     : SimpleItem(defvalue = [], typelist=['str'], sequence=1, doc='Output files to be returned via SE'), 
        'output'         : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, protected=1, doc = 'Output information automatically filled by the job'),
        'isGroupDS'      : SimpleItem(defvalue = False, doc = 'Use group datasetname prefix'),
        'groupname'      : SimpleItem(defvalue='', doc='Name of the group to be used if isGroupDS=True'),
        #'datasetname'    : SimpleItem(defvalue='', copyable=0, doc='Name of the DQ2 output dataset automatically filled by the job'),
        'datasetname'    : SimpleItem(defvalue='', filter="checkNameConsistency", doc='Name of the DQ2 output dataset automatically filled by the job'),
        'datasetList'    : SimpleItem(defvalue = [], typelist=['str'],  sequence = 1,protected=1, doc='List of DQ2 output datasets automatically filled by the AthenaMC job'),
        'location'       : SimpleItem(defvalue='',doc='SE output path location'),
        'spacetoken'     : SimpleItem(defvalue='',doc='SE output spacetoken'),
        'local_location' : SimpleItem(defvalue='',doc='Local output path location'),
#        'use_datasetname' : SimpleItem(defvalue = False, doc = 'Use datasetname as it is and do not prepend users.myname.ganga'),
        'use_shortfilename' : SimpleItem(defvalue = False, doc = 'Use shorter version of filenames and do not prepend users.myname.ganga'),
        'transferredDS' : SimpleItem(defvalue='',doc='Panda only: Specify a comma-separated list of patterns so that only datasets which match the given patterns are transferred when outputdata.location is set. Either \ or "" is required when a wildcard is used. If omitted, all datasets are transferred')
        })
    
    _category = 'datasets'
    _name = 'DQ2OutputDataset'

    _exportmethods = [ 'retrieve', 'fill', 'create_dataset','create_datasets', 'dataset_exists', 'get_locations', 'create_subscription', 'clean_duplicates_in_dataset', 'clean_duplicates_in_container', 'check_content_consistency' ]

    _GUIPrefs = [ { 'attribute' : 'outputdata',     'widget' : 'String_List' },
                  { 'attribute' : 'output',         'widget' : 'String_List' },
                  { 'attribute' : 'datasetname',    'widget' : 'String' },
                  { 'attribute' : 'datasetList',    'widget' : 'String_List' },
                  { 'attribute' : 'location',       'widget' : 'String_List' },
                  { 'attribute' : 'local_location', 'widget' : 'File' },
#                  { 'attribute' : 'use_datasetname',    'widget' : 'Bool' },
                  { 'attribute' : 'isGroupDS',      'widget' : 'Bool' },
                  { 'attribute' : 'groupname',      'widget' : 'String' },
                  { 'attribute' : 'use_shortfilename',    'widget' : 'Bool' }
                  ]
    
    nameChecked = False
    dq2datasetname = ''

    def __init__(self):
        super(DQ2OutputDataset, self).__init__()

    def checkNameConsistency(self, datasetname):
        if datasetname == '':
            return ''
        elif datasetname != '':
            dq2datasetname, output_lfn = dq2outputdatasetname(datasetname, -999 , self.isGroupDS, self.groupname)
            return dq2datasetname

    def clean_duplicates_in_dataset(self, datasetname = None, outputInfo = None):
        """Clean duplicate files from dataset if e.g. shallow retry count occured"""

        trashFiles = []
        if not datasetname:
            datasetname = self.datasetname

        logger.warning('Checking for file dulipates in %s...' %datasetname)
        
        # Get filenames from repository (actually finished jobs) 
        filenames = []
        # Use subjob outputInfo if not provided as parameter
        if not outputInfo:
            outputInfo = self.output
        for file in outputInfo:
            filenames.append(file.split(',')[1])

        # Get filenames from dataset
        contents = []
        try:
            #dq2_lock.acquire()
            try:
                contents = dq2.listFilesInDataset(datasetname, long=False)
            except:
                contents = ({},'')
                pass
        finally:
            #dq2_lock.release()
            pass

        if contents:
            contents = contents[0]

        # Convert 0.3 output to 0.2 style
        contents_new = {}
        for guid, info in contents.iteritems():
            contents_new[ info['lfn'] ] = guid 

        # Loop over all files in dataset
        for filename in contents_new.keys():
            if not filename in filenames:
                trashFiles.append(filename)

        # Determine dataset location
        try:
            #dq2_lock.acquire()
            try:
                location = dq2.listDatasetReplicas(datasetname).values()[0][1][0]
            except:
                location = datasetname.split('.')[-1]
                pass
        finally:
            #dq2_lock.release()
            pass

        if not isDQ2SRMSite(location):
            logger.error('clean_duplicates_in_dataset failed since %s in no proper DQ2 location', location) 
            return

        # Create trash dataset
        trashFilesInfo = []
        trashFilesGuids = []
        trashDatasetname = datasetname + '.trash'
        for trashFile in trashFiles:
            guid = contents_new[trashFile]
            trashFilesGuids.append(guid)
            infoLine = trashDatasetname + ',' + trashFile + ',' + guid + ',' + '%s' %contents[guid]['filesize']  +  ',' + contents[guid]['checksum'].replace('ad:','') + ',' +  location
            trashFilesInfo.append(infoLine)
            
        if trashFiles:
            logger.warning('Removing file duplicates from %s outputdataset: %s', datasetname, trashFiles )
            try:
                self.create_dataset(trashDatasetname)
            except:
                logger.warning('Trash dataset %s already exists !', trashDatasetname )
            
            self.register_datasets_details( trashDatasetname, trashFilesInfo)
            logger.warning('Duplicate files are now in dataset: %s', trashDatasetname)
            # Delete duplicate files from original dataset
            try:
                #dq2_lock.acquire()
                try:
                    dq2.deleteFilesFromDataset(datasetname, trashFilesGuids)
                except:
                    logger.error('Failure during removal of duplicates from dataset %s', datasetname)
                    pass
            finally:
                #dq2_lock.release()
                pass

            # Delete trash dataset
            if config['DELETE_DUPLICATES_DATASET']:
                try:
                    #dq2_lock.acquire()
                    try:
                        dq2.deleteDatasetReplicas(trashDatasetname, location)                    
                    except:
                        logger.error('Failure during removal of duplicates dataset %s', trashDatasetname)
                        pass
                finally:                                                        
                    #dq2_lock.release()
                    pass

        return

    def clean_duplicates_in_container(self, containername = None):
        """Clean duplicate files from container and its datasets if e.g. shallow retry count occured"""

        if not containername:
            containername = self.datasetname

        logger.warning('Checking for file dulipates in %s...' %containername)

        if not containername.endswith('/'):
            logger.warning('%s is not a dataset container - doing nothing!' %containername)
            return
       
        # Resolved container into datasets
        datasets = resolve_container([containername])
        # Use master job info
        outputInfo = self.output
        # Clean all dataset individually
        for dataset in datasets:
            self.clean_duplicates_in_dataset(dataset, outputInfo )

        return

    def dataset_exists(self, datasetname = None):
        """Check if dataset already exists"""
        exist = False
        if not datasetname: datasetname=self.datasetname
        try:
            #dq2_lock.acquire()
            try:
                content = dq2.listDatasets(datasetname)
            except:
                content = []
        finally:
            #dq2_lock.release()
            pass
        if len(content)>0:
            exist = True
            
        return exist

    def get_locations(self, datasetname = None, complete=0, quiet = False):
        '''helper function to access the dataset location'''

        if not datasetname: datasetname=self.datasetname

        try:
            #dq2_lock.acquire()
            try:
                locations = dq2.listDatasetReplicas(datasetname)
            except:
                logger.error('Dataset %s not found !', datasetname)
                return
        finally:
            #dq2_lock.release()
            pass
        try:
            #dq2_lock.acquire()
            datasetinfo = dq2.listDatasets(datasetname)
        finally:
            #dq2_lock.release()
            pass

        try:
            datasetvuid = datasetinfo[datasetname]['vuids'][0]
        except:
            try:
                datasetvuid = datasetinfo.values()[0]['vuids'][0]
            except:
                try:
                    datasetvuid = dq2.getMetaDataAttribute(datasetname,['latestvuid'])['latestvuid']
                    import uuid
                    datasetvuid = str(uuid.UUID(datasetvuid))
                except:
                    logger.warning('Dataset %s not found',datasetname)

        if datasetvuid not in locations:
            logger.warning('Dataset %s not found',datasetname)
            return []
        if complete==0:
            return locations[datasetvuid][0] + locations[datasetvuid][1]
        else:
            return locations[datasetvuid][1]

    def create_dataset(self, datasetname = None):
        """Create dataset in central DQ2 database"""

        if datasetname:
            try:
                #dq2_lock.acquire()
                dq2.registerNewDataset(datasetname)
            finally:
                #dq2_lock.release()
                pass

            self.datasetname = datasetname

    def create_datasets(self, datasets):
        # first, ensure uniqueness of name
        for dataset in datasets:
            if dataset not in self.datasetList:
                self.datasetList.append(dataset)
        for dataset in self.datasetList:
            try:
                #dq2_lock.acquire()
                content = dq2.listDatasets(dataset)
            finally:
                #dq2_lock.release()
                pass

            if len(content)>0:
                logger.warning("dataset %s already exists: skipping", dataset)
                continue
            logger.debug("creating dataset: %s", dataset)
            self.create_dataset(dataset)
        
        self.datasetname="" # mandatory to avoid confusing the fill method
        return

    def create_subscription(self, datasetname = None, location = None):
        """Create a subscription for a dataset"""
        if datasetname and location:
            if isDQ2SRMSite(location) and \
                   (location.find('LOCALGROUPDISK')>0 or location.find('SCRATCHDISK')>0):
                try:
                    #dq2_lock.acquire()
                    dq2.registerDatasetSubscription(datasetname, location)
                    logger.warning('Dataset %s has been subscribed to %s.', datasetname, location)
                finally:
                    #dq2_lock.release()
                    pass
                    
        return

    def register_dataset_location(self, datasetname, siteID):
        """Register location of dataset into DQ2 database"""
        alllocations = []

        try:
            #dq2_lock.acquire()
            try:
                datasetinfo = dq2.listDatasets(datasetname)
            except:
                datasetinfo = {}
        finally:
            #dq2_lock.release()
            pass

        if datasetinfo=={}:
            logger.error('Dataset %s is not defined in DQ2 database !' , datasetname )
            return -1
        
        try:
            #dq2_lock.acquire()
            try:
                locations = dq2.listDatasetReplicas(datasetname)
            except:
                locations = {}
        finally:
            #dq2_lock.release()
            pass

        if locations != {}: 
            try:
                datasetvuid = datasetinfo[datasetname]['vuids'][0]
            except KeyError:
                logger.error('Dataset %s not found', datasetname )
                return -1
            if datasetvuid not in locations:
                logger.error( 'Dataset %s not found', datasetname )
                return -1
            alllocations = locations[datasetvuid][0] + locations[datasetvuid][1]

        try:
            #dq2_lock.acquire()
            if not siteID in alllocations:
                try:
                    dq2.registerDatasetLocation(datasetname, siteID)
                except DQInvalidRequestException as Value:
                    logger.error('Error registering location %s of dataset %s: %s', datasetname, siteID, Value) 
        finally:
            #dq2_lock.release()
            pass

        # Verify registration
        try:
            #dq2_lock.acquire()
            try:
                locations = dq2.listDatasetReplicas(datasetname)
            except:
                locations = {}
        finally:
            #dq2_lock.release()
            pass

        if locations != {}: 
            datasetvuid = datasetinfo[datasetname]['vuids'][0]
            alllocations = locations[datasetvuid][0] + locations[datasetvuid][1]
        else:
            alllocations = []

        return alllocations


    def register_file_in_dataset(self,datasetname,lfn,guid, size, checksum):
        """Add file to dataset into DQ2 database"""
        # Check if dataset really exists

        try:
            #dq2_lock.acquire()
            content = dq2.listDatasets(datasetname)
        finally:
            #dq2_lock.release()
            pass

        if content=={}:
            logger.error('Dataset %s is not defined in DQ2 database !',datasetname)
            return
        # Add file to DQ2 dataset
        ret = []
        #sizes = []
        #checksums = []
        #for i in xrange(len(lfn)):
        #    sizes.append(None)
        #    checksums.append(None)
        
        try:
            #dq2_lock.acquire()
            try:
                ret = dq2.registerFilesInDataset(datasetname, lfn, guid, size, checksum) 
            except (DQInvalidFileMetadataException, DQInvalidRequestException, DQFrozenDatasetException) as Value:
                logger.warning('Warning, some files already in dataset or dataset is frozen: %s', Value)
                pass
        finally:
            #dq2_lock.release()
            pass

        return 

    def register_datasets_details(self,datasets,outdata):

        reglines=[]
        for line in outdata:
            try:
                #[dataset,lfn,guid,siteID]=line.split(",")
                [dataset,lfn,guid,size,md5sum,siteID]=line.split(",")
            except ValueError:
                continue
            size = long(size)
            adler32='ad:'+md5sum
            if len(md5sum)==32:
                adler32='md5:'+md5sum
            
            siteID=siteID.strip() # remove \n from last component
            regline=dataset+","+siteID
            if regline in reglines:
                logger.debug("Registration of %s in %s already done, skipping" % (dataset,siteID))
                #continue
            else:
                reglines.append(regline)
                logger.info("Registering dataset %s in %s" % (dataset,siteID))
                # use another version of register_dataset_location, as the "secure" one does not allow to keep track of datafiles saved in the fall-back site (CERNCAF)
                try:
                    #dq2_lock.acquire()
                    content = dq2.listDatasets(dataset)
                finally:
                    #dq2_lock.release()
                    pass

                if content=={}:
                    logger.error('Dataset %s is not defined in DQ2 database !',dataset)
                else: 
                    try:
                        #dq2_lock.acquire()
                        try:
                            dq2.registerDatasetLocation(dataset, siteID)
                        except (DQLocationExistsException, DQInternalServerException):
                            logger.debug("Dataset %s is already registered at location %s", dataset, siteID )
                        
                    finally:
                        #dq2_lock.release()
                        pass

            self.register_file_in_dataset(dataset,[lfn],[guid],[size],[adler32])

    def check_content_consistency(self, numsubjobs, **options ):
        """Check outputdataset consistency"""
        
        # Resolve container into datasets
        datasets = resolve_container([self.datasetname])

        for dataset in datasets:
            try:
                #dq2_lock.acquire()
                try:
                    contents = dq2.listFilesInDataset(dataset, long=False)
                except:
                    contents = []
                    pass
            finally:
                #dq2_lock.release()
                pass

            if not contents:
                contents = []
                pass

            if not len(contents):
                continue
       
            contents = contents[0]
            contents_new = []
            contents_files = []
            for guid, info in contents.iteritems():
                contents_new.append( (guid, info['lfn']) )
                contents_files.append( info['lfn'] ) 
            contents = contents_new

            numoutputfiles = len(contents)
            
            # Master job
            if numsubjobs:
                numrequestedoutputfiles = len(self.outputdata)*numsubjobs
                if not numrequestedoutputfiles == numoutputfiles:
                    logger.warning('%s output files in outputdataset %s is not consistent with %s subjobs and %s requested outputfiles - Please carefully check output !', numoutputfiles, dataset,numsubjobs, numrequestedoutputfiles)
            # Subjob
            else:
                for outputinfo in self.output:
                    filename = outputinfo.split(',')[1]
                    if not filename in contents_files:
                        logger.warning('output file %s is not in outputdataset %s - Please carefully check output !', filename, dataset)

        return

    def fill(self, type=None, name=None, **options ):
        """Determine outputdata and outputsandbox locations of finished jobs
        and fill output variable"""

        from Ganga.GPIDev.Lib.Job import Job
        from GangaAtlas.Lib.ATLASDataset import filecheck

        job = self._getParent()

        # Determine local output path to store files
        if job.outputdata.local_location:
            outputlocation = expandfilename(job.outputdata.local_location)
        elif job.outputdata.location and (job.backend._name in [ 'Local', 'LSF', 'PBS', 'SGE']):
            outputlocation = expandfilename(job.outputdata.location)
        else:
            try:
                tmpdir = os.environ['TMPDIR']
            except:
                tmpdir = '/tmp/'
            outputlocation = tmpdir

        # Output files on SE
        outputfiles = job.outputdata.outputdata
        
        # Search output_guid files from LCG jobs in outputsandbox
        jobguids = []

        if job.backend._name in [ 'LCG', 'CREAM', 'Local', 'LSF', 'PBS', 'SGE']:
            pfn = job.outputdir + "output_guids"
            fsize = filecheck(pfn)
            if (fsize>0):
                jobguids.append(pfn)
                logger.debug('jobguids: %s', jobguids)
                
            
            # Get guids from output_guid files
            for ijobguids in jobguids: 
                f = open(ijobguids)
                templines =  [ line.strip() for line in f ]
                if not self.output:
                    for templine in templines:
                        tempguid = templine.split(',')
                        #self.output = self.output + tempguid

                f.close()
                
            # Get output_location
            pfn = job.outputdir + "output_location"
            fsize = filecheck(pfn)
            if (fsize>0):
                f = open(pfn)
                line = f.readline()
                self.location = line.strip()
                f.close()
                
                #  Register DQ2 location
                # FMB: protection against empty strings
                if self.datasetname and not (job.application._name in ['Athena', 'AthenaTask'] and job.backend._name in [ 'LCG', 'CREAM', 'Local', 'LSF', 'PBS', 'SGE']):
                    self.register_dataset_location(self.datasetname, self.location)
                    
            pfn = job.outputdir + "output_data"
            fsize = filecheck(pfn)
            if fsize>0:
                f=open(pfn)
                for line in f.readlines():
                    self.output.append( line.strip() )
                f.close()

            # Extract new dataset name and fill it into repository
            for outputInfo in self.output:
                datasetnameTemp = outputInfo.split(',')[0]
                try:
                    if not datasetnameTemp.endswith('.' + self.location):
                        datasetnameComp = self.datasetname + '.' + self.location
                    else:
                        datasetnameComp = self.datasetname
                except:
                    datasetnameComp = self.datasetname + '.' + outputInfo.split(',')[5]
                match = re.search(datasetnameComp, datasetnameTemp)
                if match:
                    logger.debug('Changed outputdata.dataset from %s to %s', self.datasetname, datasetnameTemp)
                    self.datasetname = datasetnameTemp
                    # Work around for failing location registration on worker node
                    out = self.register_dataset_location(self.datasetname, self.location)
                    if not self.location in out:
                        logger.error('Error during dataset location registration of %s at %s', self.datasetname, self.location)
                
        # Local host execution
        if (job.backend._name in [ 'Local', 'LSF', 'PBS', 'SGE']): 
            for file in outputfiles:
                pfn = outputlocation+"/"+file
                fsize = filecheck(pfn)
                if (fsize>0):
                    self.output.append(pfn)

        # Output files in the sandbox 
        outputsandboxfiles = job.outputsandbox
        for file in outputsandboxfiles:
            pfn = job.outputdir+"/"+file
            fsize = filecheck(pfn)
            if (fsize>0):
                self.output.append(pfn)

        # Set Replica lifteime
        dq2_set_dataset_lifetime(self.datasetname, self.location)

        # Master job finish
        if not job.master and job.subjobs:
            self.location = []
            self.output = []
            self.allDatasets = []
            for subjob in job.subjobs:
                self.output+=subjob.outputdata.output
                self.datasetname=subjob.outputdata.datasetname
                self.location.append(subjob.outputdata.location)
                if not subjob.outputdata.datasetname in self.allDatasets:
                    for outputInfo in subjob.outputdata.output:
                        if len(outputInfo.split(','))>1:
                            datasetnameTemp = outputInfo.split(',')[0]
                            if not datasetnameTemp in self.allDatasets:
                                self.allDatasets.append(datasetnameTemp)
                                
            if (job.application._name in ['Athena','AthenaTask'] and job.backend._name in [ 'LCG', 'CREAM', 'Local', 'LSF', 'PBS', 'SGE']):
                newDatasetname = job.outputdata.datasetname
                for dataset in self.allDatasets:
                    # Clean dataset from duplicates on LCG backend
                    if config['CHECK_OUTPUT_DUPLICATES'] and job.backend._name in [ 'LCG' ]:
                        self.clean_duplicates_in_dataset(dataset)
                    # output container name
                    for location in self.location:
                        if location:
                            match = re.search('^(\S*).%s.*'%location, dataset)
                        else:
                            match = re.search('^(\S*)\..*', dataset)
                        if match:
                            newDatasetname = match.group(1)
                            break
                        
                # Create output container
                containerName = newDatasetname+'/'
                #try:
                #    dq2_lock.acquire()
                #    try:
                #        dq2.registerContainer(containerName)
                #    except:
                #        logger.warning('Problem registering container %s', containerName)
                #        pass
                #finally:
                #    dq2_lock.release()
                try:
                    #dq2_lock.acquire()
                    for dataset in self.allDatasets:
                        try:
                            dq2.freezeDataset(dataset)
                        except DQFrozenDatasetException:
                            pass
                finally:
                    #dq2_lock.release()
                    pass

                #try:
                #    dq2_lock.acquire()
                #    try:
                #        dq2.registerDatasetsInContainer(containerName, self.allDatasets)
                #    except:
                #        logger.warning('Problem registering datasets %s in container %s',  self.allDatasets, containerName)
                #        pass
                #finally:
                #    dq2_lock.release()

                self.datasetname = containerName
        else:
            # AthenaMC: register dataset location and insert file in dataset only within subjobs (so that if one subjob fails, the master job fails, but the dataset is saved...). Master job completion does not do anything...
            if not (job.application._name in ['Athena', 'AthenaTask'] and job.backend._name in [ 'LCG', 'CREAM', 'Local', 'LSF', 'PBS', 'SGE']):
                self.register_datasets_details(self.datasetname,self.output)
            elif not job.master and not job.subjobs:
                self.allDatasets = [ ]
                for outputInfo in self.output:
                    datasetnameTemp = outputInfo.split(',')[0]
                    if not datasetnameTemp in self.allDatasets:
                        self.allDatasets.append(datasetnameTemp)
                for datasetnameFreeze in self.allDatasets:
                    try:
                        #dq2_lock.acquire()
                        try:
                            dq2.freezeDataset(datasetnameFreeze)
                        except DQFrozenDatasetException:
                            pass
                    finally:
                        #dq2_lock.release()
                        pass
                

    def retrieve(self, type=None, name=None, **options ):
        """Retrieve files listed in outputdata and registered in output from
        remote SE to local filesystem in background thread"""
        from Ganga.GPIDev.Lib.Job import Job
        from GangaAtlas.Lib.ATLASDataset import Download
        import os, threading

        subjobDownload = options.get('subjobDownload')
        blocking = options.get('blocking')
        use_dsname = options.get('useDSNameForDir')
        output_names_re = options.get('outputNamesRE')
        thread_pool = options.get('threadPool')

        job = self._getParent()

        # Master job finish
        if not job.master and job.subjobs:
            masterJob = True
        else:
            masterJob = False

        # call the subjob retrieve method if available
        if len(job.subjobs) > 0 and subjobDownload:
            
            thread_pool = DQ2OutputDownloader(numThread = config['NumberOfDQ2DownloadThreads'])
            for sj in job.subjobs:
                sj.outputdata.retrieve(blocking=False, useDSNameForDir=use_dsname, outputNamesRE=output_names_re, threadPool=thread_pool)

            thread_pool.start()

            if blocking:
                thread_pool.join()
            return

        os.environ['DQ2_URL_SERVER'] = config['DQ2_URL_SERVER']
        os.environ['DQ2_URL_SERVER_SSL'] = config['DQ2_URL_SERVER_SSL']
        
        if 'DQ2_LOCAL_ID' not in os.environ:
            os.environ['DQ2_LOCAL_ID'] = "DUMMY"
        if 'DQ2_COPY_COMMAND' not in os.environ:
            os.environ['DQ2_COPY_COMMAND']="lcg-cp --vo atlas"

        if (job.outputdata.outputdata and job.backend._name in [ 'LCG', 'CREAM'] and job.outputdata.output) or (job.backend._name == 'Panda'):
            # Determine local output path to store files
            local_location = options.get('local_location')

            if job._getRoot().subjobs:
                id = "%d" % (job._getRoot().id)
            else:
                id = "%d" % job.id

            if local_location:
                outputlocation = expandfilename(local_location)
                if not use_dsname:
                    try:
                        outputlocation = os.path.join( outputlocation, id )
                        os.makedirs(outputlocation)
                    except OSError:
                        pass
            elif job.outputdata.local_location:
                outputlocation = expandfilename(job.outputdata.local_location)
                if not use_dsname:
                    try:
                        outputlocation = os.path.join( outputlocation, id )
                        os.makedirs(outputlocation)
                    except OSError:
                        pass
            else:
                # User job repository location
                outputlocation = job.outputdir

            # Use single download if called from master job
            if masterJob and not subjobDownload:
                if not use_dsname:
                    exe = 'dq2-get --client-id=ganga -L ROAMING -a -d -D '
                    temp_location = outputlocation
                else:
                    exe = 'dq2-get --client-id=ganga -L ROAMING -a -d '
                    temp_location = os.path.join(outputlocation, job.outputdata.datasetname)
                    

                if job.backend._name == 'Panda':
                    cmd = '%s -H %s %s' %(exe, temp_location, job.outputdata.datasetname)
                else:
                    cmd = '%s -H %s %s' %(exe, temp_location, job.outputdata.datasetname)
                
                logger.warning("Please be patient - background execution of dq2-get of %s to %s", job.outputdata.datasetname, temp_location )

                threads=[]
                thread = Download.download_dq2(cmd)
                thread.setDaemon(True)
                thread.start()
                threads.append(thread)
            
            else: # User download per subjob 
                # loop over all filenames
                if job.outputdata.output:
                    filenames = job.outputdata.output
                    for fileinfo in filenames:
                        filename = fileinfo.split(',')[1]

                        # re if required
                        if output_names_re and not re.search(output_names_re, filename):
                            continue
                        
                        if not use_dsname:
                            exe = 'dq2-get --client-id=ganga -L ROAMING -a -d -D '
                            temp_location = outputlocation
                        else:
                            exe = 'dq2-get --client-id=ganga -L ROAMING -a -d '
                            temp_location = os.path.join(outputlocation, job.outputdata.datasetname)

                        if job.backend._name == 'Panda':
                            cmd = '%s -H %s -f %s %s' %(exe, temp_location, filename, job.outputdata.datasetname)
                        else:
                            cmd = '%s -s %s -H %s -f %s %s' %(exe, job.outputdata.location, temp_location, filename, job.outputdata.datasetname)

                        logger.warning("Please be patient - background execution of dq2-get of %s to %s", job.outputdata.datasetname, temp_location )
                        if thread_pool:
                            thread_pool.addTask(cmd)
                        else:
                            threads=[]
                            thread = Download.download_dq2(cmd)
                            thread.setDaemon(True)
                            thread.start()
                            threads.append(thread)
                else:
                    logger.warning('job.outputdata.output emtpy - nothing to download')

            if blocking:
                for thread in threads:
                    thread.join()
                                        
            #for thread in threads:
            #    thread.join()

        else:
            logger.error("Nothing to download")


class DQ2OutputDownloadTask:
    """
    Class for defining a data object for each output downloading task.
    """

    _attributes = ('cmd')

    def __init__(self, cmd):
        self.cmd = cmd


class DQ2OutputDownloadAlgorithm(Algorithm):
    """
    Class for implementing the logic of each downloading task.
    """

    def process(self, item):
        """
        downloads output of one DQ2 job
        """
        from GangaAtlas.Lib.ATLASDataset import Download

        thread = Download.download_dq2(item.cmd)
        thread.setDaemon(True)
        thread.start()
        thread.join()

        return True

class DQ2OutputDownloader(MTRunner):

    """
    Class for managing multi-threaded downloading of DQ2 Output
    """

    def __init__(self, numThread=5):

        MTRunner.__init__(self, name='dq2_output_downloader', data=Data(collection=[]), algorithm=DQ2OutputDownloadAlgorithm())

        #self.keepAlive = True
        self.numThread = numThread

    def countAliveAgent(self):

        return self.__cnt_alive_threads__()

    def addTask(self, cmd):
        task = DQ2OutputDownloadTask(cmd)
        self.addDataItem(task)

        return True


logger = getLogger()

# New for DQ2 client 2.3.0
from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()
username = gridProxy.identity(safe=True)
nickname = getNickname(allowMissingNickname=False)
if nickname:
    username = nickname
os.environ['RUCIO_ACCOUNT'] = username
logger.debug("Using RUCIO_ACCOUNT = %s " %(os.environ['RUCIO_ACCOUNT'])) 

from dq2.clientapi.DQ2 import DQ2
dq2=DQ2(force_backend='rucio')

from threading import Lock
dq2_lock = Lock()

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

config = getConfig('DQ2')

baseURLDQ2 = config['DQ2_URL_SERVER']
baseURLDQ2SSL = config['DQ2_URL_SERVER_SSL']
   
verbose = False

#$Log: not supported by cvs2svn $
#Revision 1.37  2009/07/20 14:07:03  mslater
#Fix for bug in retry of dataset consistency check (52066)
#
#Revision 1.36  2009/07/15 13:07:29  elmsheus
#Enable group dataset names in LCG/Athena, #53155
#
#Revision 1.35  2009/06/29 13:47:15  elmsheus
#Fix IFAE
#
#Revision 1.34  2009/06/04 10:19:46  elmsheus
#Add exception for listMetaReplica
#
#Revision 1.33  2009/06/04 10:04:55  elmsheus
#Aquire locks for dq2 calls in dq2_list_locations_siteindex
#
#Revision 1.32  2009/05/31 16:59:54  elmsheus
#Protection for listMetaDataReplica
#
#Revision 1.31  2009/05/19 09:33:48  elmsheus
#Add feature #50558 and remove getJobObject() dependency for self.names, self.exclude_names
#
#Revision 1.30  2009/05/12 13:06:36  elmsheus
#Correct Panda retrieve
#
#Revision 1.29  2009/05/12 12:47:25  elmsheus
#Change Panda retrieve logic
#
#Revision 1.28  2009/04/30 12:21:17  ebke
#Added log file downloading and outputdata filling (DQ2Dataset) to Panda backend
#Not tested AthenaMCDataset yet!
#
#Revision 1.27  2009/04/28 12:58:34  mslater
#Removed trailing newline from DQ2OutputDataset.output
#
#Revision 1.26  2009/03/30 14:48:54  elmsheus
#Replace USERDISK with SCRATCHDISK token
#
#Revision 1.25  2009/03/05 12:33:32  elmsheus
#Add size parameter to get_contents
#
#Revision 1.24  2009/03/02 08:17:33  elmsheus
#Fix #47520: TypeError when no file size information available in DDM
#
#Revision 1.23  2009/02/26 14:26:05  elmsheus
#Sort DQ2Dataset by lfn/names
#
#Revision 1.22  2009/02/22 15:31:27  elmsheus
#Correct md5sum length
#
#Revision 1.21  2009/02/20 09:28:58  elmsheus
#Fix for no replica datasets
#
#Revision 1.20  2009/02/18 15:54:25  elmsheus
#Add --client-id=ganga tp dq2-get calls
#
#Revision 1.19  2009/02/05 10:00:36  elmsheus
#Change to adler32
#
#Revision 1.18  2009/02/05 09:50:53  dvanders
#Remove DQ2OutputDataset.use_datasetname
#
#Revision 1.17  2009/02/02 13:16:54  mslater
#Small fix for odd dq2-get issue
#
#Revision 1.16  2009/01/29 14:34:32  mslater
#Fix to DQ2OutputDataset.retrieve() method
#
#Revision 1.15  2009/01/14 09:47:56  elmsheus
#Change algorithm for filesize retrieval and return
#
#Revision 1.14  2009/01/13 11:08:06  elmsheus
#Introduce get_contents(filesize=True) - returns the dataset filesize
#
#Revision 1.13  2008/11/18 09:40:44  elmsheus
#Change log level of dataset registration
#
#Revision 1.12  2008/10/28 10:59:38  elmsheus
#Fix bug #42944
#
#Revision 1.11  2008/09/30 09:53:57  elmsheus
#Small fix
#
#Revision 1.10  2008/09/30 09:38:49  elmsheus
#Add DQ2Dataset.failover
#
#Revision 1.9  2008/09/28 15:20:34  elmsheus
#Remove obsolte DQ2Output class
#
#Revision 1.8  2008/09/12 07:16:11  elmsheus
#Add usertag config option and change DQ2OutputDataset name
#
#Revision 1.7  2008/09/02 16:06:27  elmsheus
#Athena:
#* Fix SE type detection problems for space tokens at DPM sites
#* Change default DQ2 stage-out to use _USERDISK token
#* Change default DQ2Dataset default values of
#  DQ2_BACKUP_OUTPUT_LOCATIONS
#* Disable functionality of DQ2Dataset.match_ce_all and
#  DQ2Dataset.min_num_files and print out warning.
#  These are obsolete and DQ2JobSplitter should be used instead.
#* Add option config['DQ2']['USE_STAGEOUT_SUBSCRIPTION'] to allow DQ2
#  subscription to final output SE destinations instead of "remote lcg-cr"
#  Will be enabled in future version if DQ2 site services are ready for this
#=============================================
#A few changes to enforce the computing model:
#=============================================
#* DQ2OutputDataset: j.backend.location is verified to be in the same cloud
#  as j.backend.requirements.cloud during job submission to LCG
#* Add AtlasLCGRequirements.cloud option.
#  Use: T0, IT, ES, FR, UK, DE, NL, TW, CA, (US, NG) or
#       CERN, ITALYSITES, SPAINSITES, FRANCESITES, UKSITES, FZKSITES,
#       NLSITES, TAIWANSITES, CANADASITES, (USASITES, NDGF)
#
#  *********************************************************
#  Job submission to LCG requires now one of the following options:
#  - j.backend.requirements.cloud='ID'
#  - j.backend.requirements.sites=['SITENAME']
#  ********************************************************
#* Sites specified with j.backend.requirements.sites need to be in the same
#  cloud for the LCG backend
#* Restrict the number of subjobs of AthenaSplitterJob to value of
#  config['Athena']['MaxJobsAthenaSplitterJobLCG'] (100) if glite WMS is
#  used.
#
#scripts:
#* athena: Add --cloud option
#
#Revision 1.6  2008/08/01 07:18:39  elmsheus
#Remove enforcing of DQ2OutputDatatset loction
#
#Revision 1.5  2008/07/31 14:14:19  elmsheus
#Fix but #39568: Panda/DQ2Dataset: Need better error message if DS doesn't exist
#
#Revision 1.4  2008/07/29 10:08:32  elmsheus
#Remove DQ2_OUTPUT_LOCATIONS again
#
#Revision 1.3  2008/07/28 16:56:30  elmsheus
#* ganga-stage-in-out-dq2.py:
#  - Add fix for DPM setup for NIKHEF and SARA
#  - remove special SE setup for RAL
#  - add MPPMU defaultSE=lcg-lrz-se.lrz-muenchen.de
#  - add DQ2_BACKUP_OUTPUT_LOCATIONS reading
#  - add DQ2_OUTPUT_SPACE_TOKENS reading
#  - change stage-out order for DQ2OutputDataset:
#  (1) j.outputdata.location, (2) DQ2_OUTUT_LOCATIONS,
#  (3) DQ2_BACKUP_OUTPUT_LOCATIONS (4) [ siteID ]
#
#* Add config options config['DQ2']:
#  DQ2_OUTPUT_SPACE_TOKENS: Allowed space tokens names of
#                           DQ2OutputDataset output
#  DQ2_OUTPUT_LOCATIONS: Default locations of
#                        DQ2OutputDataset output
#  DQ2_BACKUP_OUTPUT_LOCATIONS: Default backup locations of
#                               DQ2OutputDataset output
#
#* AthenaLCGRTHandler/AthenaLocalRTHandler
#  Enforce setting of j.outputdata.location for DQ2OutputDataset
#
#Revision 1.2  2008/07/28 14:27:34  elmsheus
#* Upgrade to DQ2Clients 0.1.17 and DQ2 API
#* Add full support for DQ2 container datasets in DQ2Dataset
#* Change in DQ2OutputDataset.retrieve(): use dq2-get
#* Fix bug #39286: Athena().atlas_environment omits type_list
#
#Revision 1.1  2008/07/17 16:41:18  moscicki
#migration of 5.0.2 to HEAD
#
#the doc and release/tools have been taken from HEAD
#
#Revision 1.72.2.15  2008/07/12 08:58:12  elmsheus
#* DQ2JobSplitter.py: Add numsubjobs option - now jobs can also be
#  splitted by number of subjobs
#* Athena.py: Introduce Athena.atlas_exetype, choices: ATHENA, PYARA, ROOT
#  Execute the following executable on worker node:
#  ATHENA: athena.py jobsOptions input.py
#  PYARA: python jobOptions
#  ROOT: root -q -b jobOptions
#* ganga-stage-in-out-dq2.py: produce now in parallel to input.py also a
#  flat file input.txt containing the inputfiles list. This files can be
#  read in but PYARA or ROOT application flow
#* Change --split and --splitfiles to use DQ2JobSplitter if LCG backend is used
#* Add --athena_exe ATHENA or PYARA or ROOT (see above)
#
#Revision 1.72.2.14  2008/07/10 16:36:58  elmsheus
#Add allowed_sites to dq2_list_locations_siteindex
#
#Revision 1.72.2.13  2008/07/10 06:26:12  elmsheus
#* athena-lch.sh: Fix problems with some DPM sites in athena v14
#Hurng-Chuns updates:
#* BOOT.py/Athena.py: improvements of cmtconfig magic function
#* DQ2Dataset.py: Fix wrong return value of get_contents
#
#Revision 1.72.2.12  2008/06/27 14:24:22  elmsheus
#* DQ2JobSplitter: Change from AMGA siteindex to location file catalog
#* Expand and fix DQ2Dataset.list_location_siteindex()
#* Correct Local() backend dataset list problem, bug #38202
#* Change pybin behaviour in athena-local.sh and athena-lcg.sh
#
#Revision 1.72.2.11  2008/05/12 15:55:38  elmsheus
#Fix small typo
#
#Revision 1.72.2.10  2008/05/12 09:36:35  elmsheus
#Change sh to python for dq2_get
#
#Revision 1.72.2.9  2008/05/12 09:07:31  elmsheus
#Add SGE output support
#
#Revision 1.72.2.8  2008/05/01 16:36:06  elmsheus
#Migrate GangaAtlas-4-4-12 changes
#
#Revision 1.72.2.7  2008/04/01 13:33:32  elmsheus
#* New feature: DQ2Dataset and all other routine support multiple
#  datasets
#* Update athena 14 support
#* Change from ccdcapatlas to ccdcache for LYON
#* Add addition SE for BEIJING
#* Fix AtlasPoint1 setup problem in athena-lcg.sh and athena-local.sh
#
#Revision 1.72.2.6  2008/03/28 15:31:19  elmsheus
#* Add DQ2Dataset.list_locations_siteindex
#* Add extra BEIJING SE
#
#Revision 1.72.2.5  2008/03/27 13:53:30  elmsheus
#* Updates for DQ2_COPY
#* Updates for v13 TAGs
#* Remove setting of X509_CERT_DIR during start-up
#* Add additional exception in DQ2Dataset
#* New version of dq2_get
#
#Revision 1.72.2.4  2008/03/20 12:53:42  elmsheus
#* Apply GangaAtlas-4-16 update
#* New option DQ2Dataset.type='DQ2_COPY'
#  copies input file from SE to worker node instead of Posix I/O
#* Fix configuration option problems
#
#Revision 1.75  2008/03/19 13:15:53  elmsheus
#Fix AOD/ESD backnavigation problem
#
#Revision 1.72.2.3  2008/03/07 20:26:22  elmsheus
#* Apply Ganga-5-0-restructure-config-branch patch
#* Move GangaAtlas-4-15 tag to GangaAtlas-5-0-branch
#
#Revision 1.72.2.2  2008/02/18 11:03:22  elmsheus
#Copy GangaAtlas-4-13 to GangaAtlas-5-0-branch and config updates
#
#Revision 1.74  2008/01/22 08:29:34  elmsheus
#Export get_contents
#
#Revision 1.73  2008/01/07 13:44:40  elmsheus
#* Add option 'check_md5sum' to DQ2Dataset to enable md5sum checking
#  of inputfiles on worker node
#* Add option 'datatype' = 'MC' or 'DATA' to enable reading of cosmics
#  data (like M5 data)
#* Fix typo in ganga-stage-in-out-dq2.py affecting DQ2_LOCAL mode
#
#Revision 1.72  2007/11/12 14:34:28  elmsheus
#Add list guid/location option
#
#Revision 1.71  2007/11/06 07:43:32  elmsheus
#Fix type in tagdataset_exists
#
#Revision 1.70  2007/09/28 12:31:35  elmsheus
#Add local_location option to retrieve method
#
#Revision 1.69  2007/09/26 09:14:47  elmsheus
#Introduce DQ2OutputDataset.use_shortfilename
#
#Revision 1.68  2007/09/25 21:40:27  liko
#Improve error messages
#
#Revision 1.67  2007/09/24 08:42:10  elmsheus
#Apply patches to migrate to Ganga.Utility.GridShell
#
#Revision 1.66  2007/09/11 12:31:49  elmsheus
#Introduce DQ2OutputDataset.use_datasetname=True to allow fully user defined
#output dataset names
#
#Revision 1.65  2007/09/03 00:31:42  elmsheus
#Fix Athena submission error for min_num_files
#
#Revision 1.64  2007/08/02 09:11:30  elmsheus
#* Add ignorefailed variable to AthenaOutputMerger
#* Add 'numfiles_subjob' variable to AthenaSplitterJob
#  Enables job splitting based on the number of files per job
#* Fix logic of j.inputdata.match_ce_all and j.inputdata.min_num_files>X
#  Now min_num_files is chosen over match_ce_all
#* Display complete+incomplete locations as default of
#  DQ2Dataset.list_locations_num_files()
#* Change TAG usage:
#  - j.inputdata.type='TAG' is used now for TAG/AOD reading and ntuple output
#  - j.inputdata.type='TAG_REC' is now used for TAG/AOD reading and
#    AOD production via RecExCommon_topOptions.py
#
#Revision 1.63  2007/07/30 08:41:27  elmsheus
#Move new Merging to main branch
#
#Revision 1.62  2007/07/18 14:23:35  elmsheus
#merge from brach
#
#Revision 1.61  2007/07/13 13:31:39  fbrochu
#get_contents and get_tag_contents methods: protected against crash following failed call to dq2.listFilesInDatasets
#
#Revision 1.60.6.1  2007/07/17 15:24:36  elmsheus
#* Migrate to new RootMerger
#* Fix DQ2OutputLocation path problem
#
#Revision 1.60.6.1  2007/07/17 15:24:36  elmsheus
#* Migrate to new RootMerger
#* Fix DQ2OutputLocation path problem
#
#Revision 1.60  2007/07/03 07:21:35  elmsheus
#Increase LFC_CONNTIMEOUT
#
#Revision 1.59  2007/07/02 12:49:01  elmsheus
#Fix DQ2Dataset.list_locations() exception output
#
#Revision 1.58  2007/06/28 15:58:58  elmsheus
#Fix get_tag_contents
#
#Revision 1.57  2007/06/21 09:32:27  elmsheus
#Change to 0.3 catalogs
#
#Revision 1.56  2007/06/19 16:06:28  elmsheus
#Fix DQ2OutputDataset.get_locations
#
#Revision 1.55  2007/06/13 16:30:35  elmsheus
#Migrate to DQClient 0.3
#
#Revision 1.54  2007/06/07 14:32:01  elmsheus
#Improve LFC reading if timeout occurs
#
#Revision 1.53  2007/05/31 10:06:45  elmsheus
#Add get_replica_listing method for site replica listing
#
#Revision 1.52  2007/05/30 19:50:06  elmsheus
#Introduce AOD->ESD back navigation
#
#Revision 1.51  2007/05/23 08:03:19  elmsheus
#Small fix
#
#Revision 1.50  2007/05/22 19:10:06  elmsheus
#Small fixes for LFC bulk reading
#
#Revision 1.49  2007/05/22 13:50:19  elmsheus
#Add timeout and work-around for broken LFCs
#
#Revision 1.48  2007/05/10 09:07:38  elmsheus
#Add -a option for dq2_get
#
#Revision 1.47  2007/05/08 20:42:22  elmsheus
#Add xrootd accessprotocol
#
#Revision 1.46  2007/05/02 11:34:59  fbrochu
#Removed double registration of dataset in fill() method for AthenaMC (leading to DQ2 warnings when the master job completes). Also extended register_dataset_details (pure AthenaMC method) to allow registration of alternative locations for datasets
#
#Revision 1.45  2007/04/30 18:37:06  elmsheus
#Add get_loations to DQ2OutputDataset
#
#Revision 1.44  2007/04/30 17:47:35  elmsheus
#Add dataset_exists to DQ2Outputdatasets
#
#Revision 1.43  2007/04/26 13:55:38  elmsheus
#Add min_num_files
#
#Revision 1.42  2007/04/26 13:14:55  elmsheus
#Add getIncompleteLocationCE
#
#Revision 1.41  2007/04/26 12:44:19  elmsheus
#Add list_locations_num_files, LFC bulk reading dataset content
#
#Revision 1.40  2007/04/19 13:06:41  elmsheus
#Add threading fix2
#
#Revision 1.39  2007/04/19 13:05:06  elmsheus
#Add threading fix
#
#Revision 1.38  2007/04/19 12:48:49  fbrochu
#removed orphan call to getDatasets()
#
#Revision 1.37  2007/04/03 07:40:07  elmsheus
#Add correct number_of_files for jobs and subjobs
#
#Revision 1.36  2007/04/02 15:51:55  elmsheus
#Fix DQ2OutputDataset.retrieve problems
#
#Revision 1.35  2007/04/02 09:55:44  elmsheus
#* Add number_of_files option in DQ2Dataset
#* Update splitting etc to new get_contents method
#
#Revision 1.34  2007/04/02 08:07:25  elmsheus
#* Fix directory scanning procedure in Athena.prepare()
#* Fix GUIPrefs problems
#
#Revision 1.33  2007/03/22 11:29:02  elmsheus
#Fix retrieve environment
#
#Revision 1.32  2007/03/21 15:11:29  elmsheus
#Add GUIPrefs
#
#Revision 1.31  2007/03/20 16:46:40  elmsheus
#Small fixes
#
#Revision 1.30  2007/03/13 13:42:41  elmsheus
#Remove match_ce and introduce match_ce_all
#
#Revision 1.29  2007/03/13 13:11:04  liko
#Add a thread lock to protect DQ2
#
#Revision 1.28  2007/03/05 09:55:00  liko
#DQ2Dataset leanup
#
#Revision 1.27  2007/02/22 12:55:41  elmsheus
#Fix output path and use gridShell
#
#Revision 1.26  2007/02/19 08:24:45  elmsheus
#Change TiersOfAtlasCache loading
#
#Revision 1.25  2007/02/12 18:14:20  elmsheus
#Register Dataset registration
#
#Revision 1.24  2007/02/12 15:31:42  elmsheus
#Port 4.2.8 changes to head
#Fix job.splitter in Athena*RTHandler
#
#Revision 1.23  2007/01/22 09:51:01  elmsheus
#* Port changes from Ganga 4.2.7 to head:
#  - Athena.py: fix bug #22129 local athena jobs on lxplus - cmt interference
#               Fix athena_compile problem
#  - ganga-stage-in-out-dq2.py, athena-lcg:
#    Revise error exit codes correpsonding to ProdSys WRAPLCG schema
#  - DQ2Dataset.py: fix logger hick-ups
#  - Add patch to access DPM SE
#
#Revision 1.22  2006/12/21 17:24:17  elmsheus
#* Remove DQ2 curl functionality
#* Introduce dq2_client library and port all calls
#* Remove curl calls and use urllib instead
#* Remove ganga-stagein-dq2.py and ganga-stageout-dq2.py and merge into
#  new ganga-stage-in-out-dq2.py
#* Move DQ2 splitting from Athena*RTHandler.py into AthenaSplitterJob
#  therefore introduce new field DQ2Dataset.guids
#* Use AthenaMC mechanism to register files in DQ2 also for Athena plugin
#  ie. all DQ2 communication is done in the Ganga UI
#
#Revision 1.21  2006/11/27 12:18:02  elmsheus
#Fix CVS merging errors
#
#Revision 1.20  2006/11/24 15:39:13  elmsheus
#Small fixes
#
#Revision 1.19  2006/11/24 13:32:37  elmsheus
#Merge changes from Ganga-4-2-2-bugfix-branch to the trunk
#Add Frederics changes and improvement for AthenaMC
#
#Revision 1.18.2.3  2006/11/23 14:50:18  elmsheus
#Fix ce bug
#
#Revision 1.18.2.2  2006/11/22 14:20:52  elmsheus
#* introduce prefix_hack to lcg-cp/lr calls in
#  ATLASOutputDataset.retrieve()
#* fixed double downloading feature in
#  ATLASOutputDataset.retrieve()
#* move download location for ATLASOutputDataset.retrieve()
#  to job.outputdir from temp directory if local_location is not given
#* Print out clear error message if cmt parsing fails in Athena.py
#* Migrate to GridProxy library in Athena*RTHandler.py
#* Changes in output renaming schema for DQ2OutputDataset files
#
#* Fix proxy name bug in AthenaMCLCGRTHandler.py
#* Fix path problem in wrapper.sh
#
#Revision 1.18.2.1  2006/11/01 07:08:49  elmsheus
#Fix bug #21234
#
#Revision 1.18  2006/10/12 15:17:32  elmsheus
#Fix for actually one DQ2 call
#
#Revision 1.17  2006/10/12 09:04:51  elmsheus
#DQ2 code clean-up
#
#Revision 1.16  2006/10/03 12:09:48  elmsheus
#Minor fix
#
#Revision 1.15  2006/10/03 11:59:54  elmsheus
#Fix bugs #20262, 20288
#
#Revision 1.14  2006/09/29 12:23:02  elmsheus
#Small fixes
#
#Revision 1.13  2006/09/26 11:19:19  elmsheus
#Frederic updates for AthenaMC
#
#Revision 1.12  2006/09/09 09:36:23  elmsheus
#Fix DQ2OutputDataset for local backend
#
#Revision 1.11  2006/09/08 16:11:44  elmsheus
#Expand SimpleItem directory variables with expandfilenames
#
#Revision 1.10  2006/08/25 18:27:24  elmsheus
#Add master job info output
#
#Revision 1.9  2006/08/14 12:40:15  elmsheus
#Fix dataset handling during job submission, add match_ce flag for DQ2Dataset, enable ATLASDataset also for Local backend
#
#Revision 1.8  2006/08/10 15:57:00  elmsheus
#Introduction of TAG analysis
#
#Revision 1.7  2006/08/09 16:47:12  elmsheus
#Introduction of DQ2OutputDataset, fix minor bugs
#
#Revision 1.6  2006/07/31 13:42:03  elmsheus
#Apdapt to framework changes
#
#Revision 1.5  2006/07/09 08:41:02  elmsheus
#ATLASOutputDataset introduction, DQ2 updates, Splitter and Merger code clean-up, and more
#
#Revision 1.3  2006/06/19 13:32:15  elmsheus
#Update to DQ2 0.2.10+
#
#Revision 1.2  2006/05/15 20:30:49  elmsheus
#* DQ2Dataset.py:
#  return contents correctly if inputdata.names are given
#  introduce: type variable, choose LFC or DQ2
#* AthenaLCGRTHandler.py, AthenaLocalRTHandler.py:
#  remove code for trailing number removal in pool.root files and insert
#  code in ganga-stagein-dq2.py and input.py (choose LFC or DQ2)
#  save filenames in inputdata.names for subjobs
#  typo ATLASDataset
#* ganga-stagein-dq2.py:
#  code for trailing number removal in pool.root files
#  choose LFC or DQ2 dataset type
#* Athena.py:
#  code for trailing number removal in pool.root files in generated
#  input.py
#
#Revision 1.1  2006/05/09 13:45:30  elmsheus
#Introduction of
# Athena job splitting based on number of subjobs
# DQ2Dataset and DQ2 file download
# AthenaLocalDataset
#
