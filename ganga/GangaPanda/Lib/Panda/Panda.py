################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Panda.py,v 1.47 2009-07-23 23:46:50 dvanders Exp $
################################################################################
                                                                                                              

import os, sys, time, commands, re, tempfile, exceptions, urllib, fnmatch
import cPickle as pickle
import json

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Adapters.IBackend import IBackend
from GangaCore.GPIDev.Credentials import require_credential
from GangaCore.GPIDev.Credentials.VomsProxy import VomsProxy
from GangaCore.GPIDev.Schema import *
from GangaCore.GPIDev.Lib.File import *
from GangaCore.GPIDev.Lib.Job import JobStatusError
from GangaCore.Core.exceptions import BackendError
from GangaCore.Core import Sandbox
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GangaCore.Core import FileWorkspace
from GangaCore.Utility.Shell import Shell
from GangaCore.Utility.Config import makeConfig, ConfigError, getConfig, setConfigOption
from GangaCore.Utility.logging import getLogger

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import ToACache
from GangaAtlas.Lib.ATLASDataset.ATLASDataset import Download
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import convertDQ2ToClient, getLocations, getDatasets, getElementsFromContainer

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname, inject_proxy

logger = getLogger()
config = getConfig('Panda')

try:
    from pandatools import Client
    if config['baseURL']:
        Client.baseURL = config['baseURL']
    if config['baseURLSSL']:
        Client.baseURLSSL = config['baseURLSSL']
except:
    logger.error("Failed to correctly configure Client.baseURL or Client.baseURLSSL ")

import urllib2
try:
    import simplejson
except:
    import json as simplejson

try:
    agisinfos = simplejson.load(urllib2.urlopen("http://atlas-agis-api.cern.ch/request/site/query/list/?json&state=ACTIVE&rc_site_state=ACTIVE"))
except:
    agisinfos = []
    logger.error("Failed to load AGIS info from http://atlas-agis-api.cern.ch/request/site/query/list/?json&state=ACTI\
VE&rc_site_state=ACTIVE")

def setChirpVariables():
    """Helper function to fill chirp config variables"""
    configPanda = getConfig('Panda')

    nickname = getNickname(allowMissingNickname=False) 
    if configPanda['chirpserver'] and not configPanda['chirpconfig']:
        tempchirpconfig = 'chirp^%s^/%s^-d chirp' %(configPanda['chirpserver'],nickname)
        GangaCore.Utility.Config.setConfigOption('Panda','chirpconfig', tempchirpconfig)
    return 

# AGISSpecTS = time.time()
# def refreshAGISSpecs():
#     global AGISSpecsTS
#     global agisinfos
#     try:
#         if time.time() - AGISSpecsTS > 3600:
#             agisinfosLocal = simplejson.load(urllib2.urlopen("http://atlas-agis-api.cern.ch/request/site/query/list/?json&state=ACTIVE&rc_site_state=ACTIVE"))
#             if agisinfosLocal:
#                 agisinfos = agisinfosLocal
#             AGISSpecsTS = time.time()
#     except:
#         AGISSpecsTS = time.time()
#     return

pandaSpecTS = time.time()
def refreshPandaSpecs():
    #from pandatools import Client

    global pandaSpecsTS
    try:
        if time.time() - pandaSpecsTS > 600:
            logger.debug('Calling Client.refreshSpecs')
#            Client.refreshSpecs()
            Client.PandaSites = Client.getSiteSpecs(config['siteType'])[1]
            pandaSpecsTS = time.time()
    except NameError:
#        Client.refreshSpecs()
        Client.PandaSites = Client.getSiteSpecs(config['siteType'])[1]
        pandaSpecsTS = time.time()

def convertDQ2NamesToQueueName(locations):
    info = {}

    # refreshAGISSpecs()
    # 
    # for entry in agisinfos:
    #     try:
    #         temp_queuename = [i.keys() for i in entry['presources'].values() ]
    #         queuename = [item for sublist in temp_queuename for item in sublist]
    #     except:
    #         queuename = []
    #         pass
    #     try:
    #         tokens = entry['ddmendpoints'].keys()
    #     except:
    #         tokens = []
    #         pass

    #     for location in locations:
    #         if location in tokens:
    #             #queuename = [ i for i in queuename if i.startswith("ANALY") ]
    #             if location not in info:
    #                 info[location] = queuename
    #             else:
    #                 info[location] = append(queuename)

    # if info:
    #     return info
    
    # fall back to old code
    for location in locations:
        sites = []
        for queue, queueinfo in Client.PandaSites.iteritems():
            queuelocations = queueinfo['setokens'].values()
            for queuelocation in queuelocations:
                if Client.convSrmV2ID(location) == Client.convSrmV2ID(queuelocation) and not queue in sites:
                    sites.append(queue)
        info[location] = sites
    return info


def convertQueueNameToDQ2Names(queue):

    # check if we can load info from an AGIS JSON
    # Code provided by jschovan (PR #470)
    if not convertQueueNameToDQ2Names.pandaresources:
        if not os.path.exists(config['AGISJSONFile']):
            logger.warning("Cannot find AGIS file '%s' - falling back to using Panda Client" % config['AGISJSONFile'])
        else:
            with open(config['AGISJSONFile'], 'r') as agis_file:
                convertQueueNameToDQ2Names.pandaresources = dict([(q['name'], q) for q in json.loads(agis_file.read())])

    # if we have the AGIS info, use it. Otherwise fall back
    if convertQueueNameToDQ2Names.pandaresources:
        ddm_names = ''
        for q in convertQueueNameToDQ2Names.pandaresources:
            if convertQueueNameToDQ2Names.pandaresources[q]['name'] == queue:
                ddm_names = convertQueueNameToDQ2Names.pandaresources[q]['ddm']

        if not ddm_names:
            logger.error("Could not find queue name '%s' in AGIS file or ddm info not present. "
                         "Returning no sites" % queue)
            return []

        return ddm_names.split(',')

    # fallback to old code
    logger.debug("convertQueueNameToDQ2Names fall back")
    refreshPandaSpecs()
    sites = []
    for site in Client.PandaSites[queue]['setokens'].values():
        sites.append(Client.convSrmV2ID(site))

    logger.debug(sites)
    allowed_sites = []
    for site in ToACache.sites:
        if site not in allowed_sites and Client.convSrmV2ID(site) in sites:
            allowed_sites.append(site)

    return allowed_sites

convertQueueNameToDQ2Names.pandaresources = {}

def queueToAllowedSites(queue):
    #from pandatools import Client
    refreshPandaSpecs()

    try:
        ddm = Client.PandaSites[queue]['ddm']
    except KeyError:
        raise BackendError('Panda','Queue %s has no ddm field in SchedConfig'%queue)
    allowed_sites = []
    alternate_names = []
    for site in ToACache.sites:
        if site not in allowed_sites:
            try:
                if ddm == site:
                    alternate_names = ToACache.sites[site]['alternateName']
                    allowed_sites.append(site)
                    [allowed_sites.append(x) for x in alternate_names]
                elif ddm in ToACache.sites[site]['alternateName']:
                    allowed_sites.append(site)
                else:
                    for alternate_name in alternate_names:
                        if (alternate_name in ToACache.sites[site]['alternateName']):
                            allowed_sites.append(site)
            except (TypeError,KeyError):
                continue
    for site in ToACache.sites:
        if site not in allowed_sites:
            try:
                if ddm == site:
                    alternate_names = ToACache.sites[site]['alternateName']
                    allowed_sites.append(site)
                    [allowed_sites.append(x) for x in alternate_names]
                elif ddm in ToACache.sites[site]['alternateName']:
                    allowed_sites.append(site)
                else:
                    for alternate_name in alternate_names:
                        if (alternate_name in ToACache.sites[site]['alternateName']):
                            allowed_sites.append(site)
            except (TypeError,KeyError):
                continue

    # add special extra tokens
    for setoken in Client.PandaSites[queue]['setokens'].values():
        if setoken not in allowed_sites:
            allowed_sites.append(setoken)

    disallowed_sites = ['CERN-PROD_TZERO']
    allowed_allowed_sites = []
    for site in allowed_sites:
        if site not in disallowed_sites:
            allowed_allowed_sites.append(site)

    return allowed_allowed_sites

def runPandaBrokerage(job):
    #from pandatools import Client
    refreshPandaSpecs()

    tmpSites = []
    # get locations when site==AUTO
    if job.backend.site == "AUTO":
        libdslocation = []
        if job.backend.libds:
            try:
                # RUCIO patch
                #libdslocation = Client.getLocations(job.backend.libds,[],job.backend.requirements.cloud,False,False)
                info = getLocations(job.backend.libds)
                location = info.values()[0][1]
                libdslocation = convertDQ2NamesToQueueName(location)
            except exceptions.SystemExit:
                raise BackendError('Panda','Error in Client.getLocations for libDS')
            if not libdslocation:
                raise ApplicationConfigurationError('Could not locate libDS %s'%job.backend.libds)
            else:
                libdslocation = libdslocation.values()[0]
                try:
                    job.backend.requirements.cloud = Client.PandaSites[libdslocation[0]]['cloud']
                except:
                    raise BackendError('Panda','Could not map libds site %s to a cloud'%libdslocation)

        dataset = ''
        if job.inputdata:
            try:
                dataset = job.inputdata.dataset[0]
            except:
                try:
                    dataset = job.inputdata.DQ2dataset
                except:
                    raise ApplicationConfigurationError('Could not determine input datasetname for Panda brokerage')
            if not dataset:
                raise ApplicationConfigurationError('Could not determine input datasetname for Panda brokerage')

            fileList = []
            try:
                # RUCIO patch
                #fileList  = Client.queryFilesInDataset(dataset,False)
                fileList = convertDQ2ToClient(dataset)
            except exceptions.SystemExit:
                raise BackendError('Panda','Error in Client.queryFilesInDataset')
            try:

                # RUCIO patch
                # FIXME: no cloud selection
                # dsLocationMap = Client.getLocations(dataset,fileList,job.backend.requirements.cloud,False,False,expCloud=True)
                info = getLocations(dataset)
                location = info.values()[0][1]
                dsLocationMap = convertDQ2NamesToQueueName(location)
                if not dsLocationMap:
                    logger.info('Dataset not found in cloud %s, searching all clouds...'%job.backend.requirements.cloud)
                    # RUCIO patch
                    #dsLocationMap = Client.getLocations(dataset,fileList,job.backend.requirements.cloud,False,False,expCloud=False)
                    info = getLocations(dataset)
                    location = info.values()[0][1]
                    dsLocationMap = convertDQ2NamesToQueueName(location)

            except exceptions.SystemExit:
                raise BackendError('Panda','Error in Client.getLocations')
            # no location
            if dsLocationMap == {}:
                raise BackendError('Panda',"ERROR : could not find supported locations in the %s cloud for %s" % (job.backend.requirements.cloud,dataset))
            # run brokerage
            for tmpItem in dsLocationMap.values():
                if not libdslocation or tmpItem == libdslocation:
                    tmpSites.append(tmpItem[0])
        else:
            for site,spec in Client.PandaSites.iteritems():
                if spec['cloud']==job.backend.requirements.cloud and spec['status']=='online' and not Client.isExcudedSite(site):
                    if not libdslocation or site == libdslocation:
                        tmpSites.append(site)
    
        newTmpSites = []
        for site in tmpSites:
            if site not in job.backend.requirements.excluded_sites:
                newTmpSites.append(site)
        tmpSites=newTmpSites
    else:
        tmpSites = [job.backend.site]
 
    if not tmpSites: 
        raise BackendError('Panda',"ERROR : could not find supported locations in the %s cloud for %s, %s" % (job.backend.requirements.cloud,dataset,job.backend.libds))
    
    tag = ''
    try:
        if job.application.atlas_production=='':
            tag = 'Atlas-%s' % job.application.atlas_release
        else:
            tag = '%s-%s' % (job.application.atlas_project,job.application.atlas_production)
    except:
        # application is probably AthenaMC
        try:
            if len(job.application.atlas_release.split('.')) == 3:
                tag = 'Atlas-%s' % job.application.atlas_release
            else:
                tag = 'AtlasProduction-%s' % job.application.atlas_release
        except:
            logger.debug("Could not determine athena tag for Panda brokering")
    try:
        status,out = Client.runBrokerage(tmpSites,tag,verbose=False,trustIS=config['trustIS'],processingType=config['processingType'])
    except exceptions.SystemExit:
        job.backend.reason = 'Exception in Client.runBrokerage: %s %s'%(sys.exc_info()[0],sys.exc_info()[1])
        raise BackendError('Panda','Exception in Client.runBrokerage: %s %s'%(sys.exc_info()[0],sys.exc_info()[1]))
    if status != 0:
        job.backend.reason = 'Non-zero to run brokerage for automatic assignment: %s' % out
        raise BackendError('Panda','Non-zero to run brokerage for automatic assignment: %s' % out)
    if out not in Client.PandaSites:
        job.backend.reason = 'brokerage gave wrong PandaSiteID:%s' % out
        raise BackendError('Panda','brokerage gave wrong PandaSiteID:%s' % out)
    # set site
    job.backend.site = out

    # patch for BNL
    if job.backend.site == "ANALY_BNL":
        job.backend.site = "ANALY_BNL_ATLAS_1"

    # long queue
    if job.backend.requirements.long:
        job.backend.site = re.sub('ANALY_','ANALY_LONG_',job.backend.site)
    job.backend.actualCE = job.backend.site
    # correct the cloud in case site was not AUTO
    job.backend.requirements.cloud = Client.PandaSites[job.backend.site]['cloud']
    logger.info('Panda brokerage results: cloud %s, site %s'%(job.backend.requirements.cloud,job.backend.site))


def selectPandaSite(job,sites):
    #from pandatools import Client
    refreshPandaSpecs()

    pandaSites = []
    if job.backend.site == 'AUTO':
        
        # exclude any DDM sites
        ddmSites =  [ x for x in sites.split(':') ]
        for s in job.backend.requirements.excluded_sites:
            if s in ddmSites:
                ddmSites.remove(s)

            if s.find("*") != -1:
                rem_sites = []
                for s2 in ddmSites:
                    if fnmatch.fnmatch(s2, s):
                        rem_sites.append(s2)

                for s2 in rem_sites:
                    ddmSites.remove(s2)
        pandaSites = [Client.convertDQ2toPandaID(x) for x in ddmSites]

        # exclude any Panda sites
        for s in job.backend.requirements.excluded_sites:
            if s in pandaSites:
                pandaSites.remove(s)

            if s.find("*") != -1:
                rem_sites = []
                for s2 in pandaSites:
                    if fnmatch.fnmatch(s2, s):
                        rem_sites.append(s2)

                for s2 in rem_sites:
                    pandaSites.remove(s2)
    else:
        return job.backend.site
    tag = ''
    try:
        if job.application.atlas_production=='':
            tag = 'Atlas-%s' % job.application.atlas_release
        else:
            tag = '%s-%s' % (job.application.atlas_project,job.application.atlas_production)
    except:
        # application is probably AthenaMC
        try:
            if len(job.application.atlas_release.split('.')) == 3:
                tag = 'Atlas-%s' % job.application.atlas_release
            else:
                tag = 'AtlasProduction-%s' % job.application.atlas_release
        except:
            logger.debug("Could not determine athena tag for Panda brokering")
    try:
        status,out = Client.runBrokerage(pandaSites,tag,verbose=False,trustIS=config['trustIS'],processingType=config['processingType'])
    except exceptions.SystemExit:
        job.backend.reason = 'Exception in Client.runBrokerage: %s %s'%(sys.exc_info()[0],sys.exc_info()[1])
        raise BackendError('Panda','Exception in Client.runBrokerage: %s %s'%(sys.exc_info()[0],sys.exc_info()[1]))
    if status != 0:
        job.backend.reason = 'Non-zero to run brokerage for automatic assignment: %s' % out
        raise BackendError('Panda','Non-zero to run brokerage for automatic assignment: %s' % out)
    if out not in Client.PandaSites:
        job.backend.reason = 'brokerage gave wrong PandaSiteID:%s' % out
        raise BackendError('Panda','brokerage gave wrong PandaSiteID:%s' % out)
    # set site
    return out

def uploadSources(path,sources):
    #from pandatools import Client

    logger.info('Uploading source tarball %s in %s to Panda...'%(sources,path))
    try:
        cwd = os.getcwd()
        os.chdir(path)
        #rc, output = Client.putFile(sources)
        rc, output = Client.putFile(sources, useCacheSrv=True)
        os.chdir(cwd)
        if output != 'True':
            if 'Cannot overwrite file' not in output:
                logger.error('Uploading sources %s/%s failed. Status = %d', path, sources, rc)
                logger.error(output)
                raise BackendError('Panda','Uploading sources to Panda failed')
            else:
                logger.warning('Uploading sources %s/%s failed. Status = %d', path, sources, rc)
                logger.warning(output)
                logger.warning('Reusing existing input sandbox on panda')
    except:
        raise BackendError('Panda','Exception while uploading archive: %s %s'%(sys.exc_info()[0],sys.exc_info()[1]))


def getLibFileSpecFromLibDS(libDS):
    #from pandatools import Client
    from taskbuffer.FileSpec import FileSpec

    # query files in lib dataset to reuse libraries
    logger.info("query files in %s" % libDS)
    # RUCIO patch
    #tmpList = Client.queryFilesInDataset(libDS,False)
    tmpList = convertDQ2ToClient(libDS)

    tmpFileList = []
    tmpGUIDmap = {}
    tmpMD5Sum  = None
    tmpFSize   = None
    tmpScope   = None
    for fileName in tmpList.keys():
        # ignore log file
        if len(re.findall('.log.tgz.\d+$',fileName)) or len(re.findall('.log.tgz$',fileName)):
            continue
        tmpFileList.append(fileName)
        tmpGUIDmap[fileName] = tmpList[fileName]['guid'] 
        tmpMD5Sum  = tmpList[fileName]['md5sum']
        tmpFSize   = tmpList[fileName]['fsize']
        tmpScope   = tmpList[fileName]['scope']
    # incomplete libDS
    if tmpFileList == []:
        # query files in dataset from Panda
        status,tmpMap = Client.queryLastFilesInDataset([libDS],False)
        # look for lib.tgz
        for fileName in tmpMap[libDS]:
            # ignore log file
            if len(re.findall('.log.tgz.\d+$',fileName)) or len(re.findall('.log.tgz$',fileName)):
                continue
            tmpFileList.append(fileName)
            tmpGUIDmap[fileName] = None
    # incomplete libDS
    if tmpFileList == []:
        raise BackendError('Panda',"lib dataset %s is empty" % libDS)
    # check file list                
    if len(tmpFileList) != 1:
        raise BackendError('Panda',"dataset %s contains multiple lib.tgz files : %s" % (libDS,tmpFileList))
    # instantiate FileSpec
    fileBO = FileSpec()
    fileBO.lfn = tmpFileList[0]
    fileBO.GUID = tmpGUIDmap[fileBO.lfn]
    fileBO.dataset = libDS
    fileBO.destinationDBlock = libDS
    if fileBO.GUID != 'NULL':
        fileBO.status = 'ready'
    if tmpMD5Sum is not None:
        fileBO.md5sum = tmpMD5Sum
    if tmpFSize is not None:
        fileBO.fsize = tmpFSize
    if tmpScope is not None:
        fileBO.scope = tmpScope
    return fileBO

def checkMergeJobStatus(job, pandaJobDefId):
    """check what state the merging jobs are in"""
    ick       = False
    status    = ''
    num_mjobs = 0

    (ec, info) = Client.checkMergeGenerationStatus(pandaJobDefId)

    if ec == 0:

        try:
            status         = info['status']
            mergeJobDefIds = info['mergeIDs']

            ick = True

        except Exception as e:
            logger.error('general merge job information retrieval error')
            raise e

    else:
        logger.error('checkMergeGenerationStatus returns non-zero exit code: %d' % ec)

    return (ick, status)

def retrieveMergeJobs(job, pandaJobDefId):
    '''
    methods for retrieving panda job ids of merging jobs given a jobDefId
    '''
    #from pandatools import Client

    ick       = False
    status    = ''
    num_mjobs = 0

    (ec, info) = Client.checkMergeGenerationStatus(pandaJobDefId)

    if ec == 0:

        try:
            status         = info['status']
            mergeJobDefIds = info['mergeIDs']

            if status == 'NA':
                logger.warning('No merging jobs expected')

            elif status == 'generating':
                logger.debug('merging jobs are generating')

            elif status == 'standby':
                logger.debug('merging jobs to be created')

            elif status == 'generated':
                logger.debug('merging jobs are generated')

                for id in mergeJobDefIds:
                    logger.debug("merging jobDefId: %d" % id)

                    ## retrieve merging job id,status given the jobDefId
                    (ec2, mjs) = Client.getPandIDsWithJobID(id)

                    if ec2 == 0:
                        
                        for jid,jinfo in mjs.items():
                            mjobj = PandaMergeJob()
                            mjobj.id     = jid
                            #mjobj.status = jinfo[0]
                            mjobj.url    = 'http://panda.cern.ch/?job=%d' % jid

                            if mjobj.id not in [mj2.id for mj2 in job.backend.mergejobs]:
                                job.backend.mergejobs.append(mjobj)
                                num_mjobs += 1
                            else:
                                logger.debug("merging job %s already exists locally" % mjobj.id)
                    else:
                        logger.warning("getPandIDsWithJobID returns non-zero exit code: %d" % ec2)

            ick = True

        except KeyError:
            logger.error('unexpected job information: %s' % repr(info))

        except Exception as e:
            logger.error('general merge job information retrieval error')
            raise e

    else:
        logger.error('checkMergeGenerationStatus returns non-zero exit code: %d' % ec)

    return (ick, status, num_mjobs)

def checkForRebrokerage(string):
    import re
    matchObj = re.match('reassigned to another site by rebrokerage. new PandaID=(\d+) JobsetID=(\d+) JobID=(\d+)', string)
    if matchObj:
        newPandaID = long(matchObj.group(1))
        newJobsetID = long(matchObj.group(2))
        newJobID = long(matchObj.group(3))
        return newPandaID
    raise BackendError('Panda','Error getting new PandaID for rebrokered job. Report to DA Help')





class PandaBuildJob(GangaObject):
    _schema = Schema(Version(2,1), {
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=0,copyable=0,doc='Panda Job id'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=0,doc='Panda Job status'),
        'jobSpec'       : SimpleItem(defvalue={},optional=1,protected=1,copyable=0,doc='Panda JobSpec'),
        'url'           : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Web URL for monitoring the job.')
    })

    _category = 'PandaBuildJob'
    _name = 'PandaBuildJob'

    def __init__(self):
        super(PandaBuildJob,self).__init__()

class PandaMergeJob(GangaObject):
    _schema = Schema(Version(2,1), {
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=0,copyable=0,doc='Panda Job id'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=0,doc='Panda Job status'),
        'jobSpec'       : SimpleItem(defvalue={},optional=1,protected=1,copyable=0,doc='Panda JobSpec'),
        'url'           : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Web URL for monitoring the job.')
    })

    _category = 'PandaMergeJob'
    _name = 'PandaMergeJob'

    def __init__(self):
        super(PandaMergeJob,self).__init__()

    def __eq__(self, other):
        return other.id == self.id

class Panda(IBackend):
    '''Panda backend: submission to the PanDA workload management system
    '''

    _schema = Schema(Version(2,6), {
        'site'          : SimpleItem(defvalue='AUTO',protected=0,copyable=1,doc='Require the job to run at a specific site'),
        'requirements'  : ComponentItem('PandaRequirements',doc='Requirements for the resource selection'),
        'extOutFile'    : SimpleItem(defvalue=[],typelist=['str'],sequence=1,protected=0,copyable=1,doc='define extra output files, e.g. [\'output1.txt\',\'output2.dat\']'),        
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=1,copyable=0,doc='PandaID of the job'),
        'url'           : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Web URL for monitoring the job.'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Panda job status'),
        'actualCE'      : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Actual CE where the job is run'),
        'libds'         : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=1,doc='Existing Library dataset to use (disables buildjob)'),
        'buildjob'      : ComponentItem('PandaBuildJob',load_default=0,optional=1,protected=1,copyable=0,doc='Panda Build Job'),
        'buildjobs'     : ComponentItem('PandaBuildJob',sequence=1,defvalue=[],optional=1,protected=1,copyable=0,doc='Panda Build Job'),
        'mergejobs'     : ComponentItem('PandaMergeJob',sequence=1,defvalue=[],optional=1,protected=1,copyable=0,doc='Panda Output Merging Jobs'),
        'jobSpec'       : SimpleItem(defvalue={},optional=1,protected=1,copyable=0,doc='Panda JobSpec'),
        'exitcode'      : SimpleItem(defvalue='',protected=1,copyable=0,doc='Application exit code (transExitCode)'),
        'piloterrorcode': SimpleItem(defvalue='',protected=1,copyable=0,doc='Pilot Error Code'),
        'reason'        : SimpleItem(defvalue='',protected=1,copyable=0,doc='Error Code Diagnostics'),
        'accessmode'    : SimpleItem(defvalue='',protected=0,copyable=1,doc='EXPERT ONLY'),
        'forcestaged'   : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Force staging of input DS'),
        'individualOutDS': SimpleItem(defvalue=False,protected=0,copyable=1,doc='Create individual output dataset for each data-type. By default, all output files are added to one output dataset'),
        'bexec'         : SimpleItem(defvalue='',protected=0,copyable=1,doc='String for Executable make command - if filled triggers a build job for the Execuatble'),
        'nobuild'       : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Boolean if no build job should be sent - use it together with Athena.athena_compile variable'),
        'domergeretrieve' : SimpleItem(defvalue=True,protected=1,hidden=1,copyable=1,doc='Should merge jobs be retrieved'),
        'credential_requirements': ComponentItem('CredentialRequirement', defvalue=VomsProxy()),
    })

    _category = 'backends'
    _name = 'Panda'
    _exportmethods = ['list_sites','get_stats','list_ddm_sites', 'resplit', 'rebroker']
  
    def __init__(self):
        super(Panda,self).__init__()

    @require_credential
    def master_submit(self,rjobs,subjobspecs,buildjobspec):
        '''Submit jobs'''
       
        logger.debug("Using Panda server baseURL=%s" %Client.baseURL)
        logger.debug("Using Panda server baseURLSSL=%s" %Client.baseURLSSL)

        #from pandatools import Client
        from GangaCore.Core.exceptions import IncompleteJobSubmissionError
        from GangaCore.Utility.logging import log_user_exception

        assert(implies(rjobs,len(subjobspecs)==len(rjobs))) 
       
        if self.libds:
            buildjobspec = None

        job = self.getJobObject()

        multiSiteJob=False

        if job.backend.requirements.express:
            logger.info("Enabling express mode for this job")
            for js in subjobspecs:
                js.specialHandling = 'express'
            if type(buildjobspec)==type([]):
                for js in buildjobspec:
                    js.specialHandling = 'express'
            else:
                buildjobspec.specialHandling = 'express'

        # JEM
        if job.backend.requirements.enableJEM:
            logger.info("Enabling Job Execution Monitor for this job")
            for js in subjobspecs:
                js.jobParameters += ' --enable-jem'
                if job.backend.requirements.configJEM != '':
                    js.jobParameters += " --jem-config %s" % job.backend.requirements.configJEM
        # Overwrite Panda queue data
        if job.backend.requirements.overwriteQueuedata:
            logger.info("Overwriting Panda queuedata for this job")
            for js in subjobspecs:
                if job.backend.requirements.overwriteQueuedataConfig != '':
                    js.jobParameters += " --overwriteQueuedata={%s} " % job.backend.requirements.overwriteQueuedataConfig

        # Merging jobs
        if job.backend.requirements.enableMerge:
            logger.info("Enabling output merging for this job")
            for js in subjobspecs:
                js.jobParameters += ' --mergeOutput'

                ## set merging type and user executable if the configurations are given
                merge_type = ''
                merge_exec = ''
                try:
                    merge_type = job.backend.requirements.configMerge['type']
                    merge_exec = job.backend.requirements.configMerge['exec']
                except KeyError:
                    pass

                if not merge_type:
                    pass

                elif merge_type.lower() in ['hist','pool','ntuple','text','log']:

                    js.jobParameters += ' --mergeType "%s"' % merge_type.lower()

                elif merge_type.lower() in ['user']:

                    if not merge_exec:
                        logger.error("user merging executable not set for merging type 'user': set PandaRequirements.configMerge['user_exec']")
                        return False

                    else:
                        js.jobParameters += ' --mergeType "%s"'   % merge_type.lower()
                        js.jobParameters += ' --mergeScript "%s"' % merge_exec
                else:
                    logger.error("merging type '%s' not supported" % merge_type)
                    return False

        jobspecs = []
        if buildjobspec:
            if type(buildjobspec)==type([]):
                multiSiteJob = True
            else:
                jobspecs = [buildjobspec] + subjobspecs
        else:
            jobspecs = subjobspecs

        if multiSiteJob:
            jobsetID = -1
            for bjspec in buildjobspec:
                bjspec.jobsetID = jobsetID
                logger.info('Submitting to %s'%bjspec.computingSite)
                jobspecs = [bjspec]
                thisbulk = []
                for subjob, sjspec in zip(rjobs,subjobspecs):
                    if sjspec.computingSite == bjspec.computingSite:
                        sjspec.jobsetID = jobsetID
                        jobspecs.append(sjspec)
                        thisbulk.append(subjob)
                        subjob.updateStatus('submitting')
                        
                if len(jobspecs) > config['serverMaxJobs']:
                    raise BackendError('Panda','Cannot submit %d subjobs. Server limits to %d.' % (len(jobspecs),config['serverMaxJobs']))

                configSys = getConfig('System')
                for js in jobspecs:
                    js.lockedby = configSys['GANGA_VERSION']

                verbose = logger.isEnabledFor(10)
                with inject_proxy(self.credential_requirements):
                    status, jobids = Client.submitJobs(jobspecs,verbose)
                if status:
                    logger.error('Status %d from Panda submit',status)
                    return False
                if "NULL" in [jobid[0] for jobid in jobids]:
                    logger.error('Panda could not assign job id to some jobs. Dataset name too long?')
                    return False

                njobs = len(jobids)

                jobdefID = jobids[0][1]
                try:
                    jobsetID = jobids[0][2]['jobsetID']
                except:
                    jobsetID = -1

                if buildjobspec:
                    pbj = PandaBuildJob()
                    pbj.id = jobids[0][0]
                    pbj.url = 'http://panda.cern.ch/?job=%d'%jobids[0][0]
                    job.backend.buildjobs.append(pbj)
                    del jobids[0]

                assert(len(thisbulk)==len(jobids))

                for subjob, jobid in zip(thisbulk,jobids):
                    subjob.backend.id = jobid[0]
                    subjob.backend.url = 'http://panda.cern.ch/?job=%d'%jobid[0]
                    subjob.updateStatus('submitted')

                logger.info("Added jobdefinitionID %d (%d subjobs at %s) to jobsetID %d"%(jobdefID,njobs,bjspec.computingSite,jobsetID))

                if njobs < len(jobspecs):
                    logger.error('Panda server accepted only %d of your %d jobs. Confirm serverMaxJobs=%d is correct.'%(njobs,len(jobspecs),config['serverMaxJobs']))

        else:
            for subjob in rjobs:
                subjob.updateStatus('submitting')

            if len(jobspecs) > config['serverMaxJobs']:
                raise BackendError('Panda','Cannot submit %d subjobs. Server limits to %d.' % (len(jobspecs),config['serverMaxJobs']))

            configSys = getConfig('System')
            for js in jobspecs:
                js.lockedby = configSys['GANGA_VERSION']
                js.jobsetID = -1

            verbose = logger.isEnabledFor(10)
            with inject_proxy(self.credential_requirements):
                status, jobids = Client.submitJobs(jobspecs,verbose)
            if status:
                logger.error('Status %d from Panda submit',status)
                return False
            if "NULL" in [jobid[0] for jobid in jobids]:
                logger.error('Panda could not assign job id to some jobs. Dataset name too long?')
                return False

            njobs = len(jobids)

            if buildjobspec:
                job.backend.buildjob = PandaBuildJob() 
                job.backend.buildjob.id = jobids[0][0]
                job.backend.buildjob.url = 'http://panda.cern.ch/?job=%d'%jobids[0][0]
                del jobids[0]

            for subjob, jobid in zip(rjobs,jobids):
                subjob.backend.id = jobid[0]
                subjob.backend.url = 'http://panda.cern.ch/?job=%d'%jobid[0]
                subjob.updateStatus('submitted')

            if njobs < len(jobspecs):
                logger.error('Panda server accepted only %d of your %d jobs. Confirm serverMaxJobs=%d is correct.'%(njobs,len(jobspecs),config['serverMaxJobs']))

        return True

    @require_credential
    def master_kill(self):
        '''Kill jobs'''  

        #from pandatools import Client

        job = self.getJobObject()
        logger.debug('Killing job %s' % job.getFQID('.'))

        active_status = [ None, 'defined', 'unknown', 'assigned', 'waiting', 'activated', 'sent', 'starting', 'running', 'holding', 'transferring' ]

        jobids = []
        if self.buildjob and self.buildjob.id and self.buildjob.status in active_status: 
            jobids.append(self.buildjob.id)
        for bj in self.buildjobs:
            if bj.id and bj.status in active_status:
                jobids.append(bj.id)
        if self.id and self.status in active_status: 
            jobids.append(self.id)

#       subjobs cannot have buildjobs
                
        jobids += [subjob.backend.id for subjob in job.subjobs if subjob.backend.id and subjob.backend.status in active_status]

        with inject_proxy(self.credential_requirements):
            status, output = Client.killJobs(jobids)
        if status:
             logger.error('Failed killing job (status = %d)',status)
             return False
        return True

    def resplit(self, newDS = False, sj_status = ['killed', 'failed'], splitter = None, auto_exclude = True, newDSName = ""):
        """ Rerun the splitting for subjobs. Basically a helper function that creates a new master job from
        this parent and submits it"""
        
        from GangaCore.GPIDev.Lib.Job import Job
        from GangaCore.Core.GangaRepository import getRegistry
        import uuid
        from GangaAtlas.Lib.Athena.DQ2JobSplitter import DQ2JobSplitter

        if self._getParent()._getParent(): # if has a parent then this is a subjob
            raise BackendError('Panda','Resplit on subjobs is not supported for Panda backend. \nUse j.backend.resplit() (i.e. rebroker the master job) and your failed (by default) subjobs \nwill be automatically selected and split again.')

        if self._getParent().splitter and self._getParent().splitter.numevtsperjob > 0:
            raise BackendError('Panda','Resplit while using numevtsperjob currently not supported. Please supply a new splitter.')

        if not newDS and (('running' in sj_status) or ('submitted' in sj_status)):
            raise BackendError('Panda','Cannot resplit running jobs without specifying a new DS. Either specify a new DS or kill the active subjobs (j.kill())')
            
        # create a new job and copy the main parts
        job = self._getParent()
        mj = Job()
        mj.info.uuid = uuid.uuid4()()
        mj.name = job.name
        mj.application = job.application
        mj.application.run_event   = []
        mj.outputdata = job.outputdata
        if newDS:
            mj.outputdata.datasetname = newDSName
            
        mj.inputdata = job.inputdata
        mj.backend = job.backend
        mj.inputsandbox  = job.inputsandbox
        mj.outputsandbox = job.outputsandbox
        mj.backend.site = 'AUTO'
        
        # libDS set?
        if mj.backend.libds:
            logger.warning("No libDS allowed when resplitting.")
            mj.backend.libds = None

        # run prepare if necessary        
        if not os.path.exists( mj.application.user_area.name ):
            logger.warning("Previous user area not available. Re-running prepare()...(note this will pick up the current install area, etc.)")
            mj.application.prepare()
        else:
            logger.warning("Re-using previous user area")
        

        # sort out the input data and excluded sites from the failed subjobs
        inDS = []
        inDSNames = []
        exc_sites = []
        num_sj = 0
        
        if self._getParent().subjobs:

            for sj in self._getParent().subjobs:

                if not sj.status in sj_status:
                    continue

                if 'provenanceID' not in mj.backend.jobSpec:
                    mj.backend.jobSpec['provenanceID'] = sj.backend.jobSpec['jobExecutionID']

                num_sj += 1
                
                # indata
                if not sj.inputdata.dataset[0] in inDS:
                    inDS.append(sj.inputdata.dataset[0])
                inDSNames += sj.inputdata.names

                # sites
                if not sj.backend.site in exc_sites:
                    exc_sites.append(sj.backend.site)
                
            if len(inDS) == 0:
                raise BackendError('Panda','No subjobs in state %s to resplit!' % sj_status)
            
            mj.inputdata.dataset = inDS
            mj.inputdata.names = inDSNames
            
        else:            
            mj.inputdata = job.inputdata
            exc_sites.append(job.backend.site)
            num_sj = 1

        # excluded sites
        if auto_exclude:
            mj.backend.requirements.excluded_sites.extend(exc_sites)
        
        # splitter
        if splitter:            
            mj.splitter = splitter._impl
        else:
            mj.splitter = DQ2JobSplitter()
            mj.splitter.numsubjobs = num_sj
        
        # Add into repository
        registry = getRegistry("jobs")
        registry._add(mj)

        # submit the job
        mj.submit()

    @require_credential
    def rebroker(self, cloud=''):
        '''Rebroker failed subjobs'''
        #from pandatools import Client
        jobs = self._getParent()

        if self._getParent()._getParent(): # if has a parent then this is a subjob
            raise BackendError('Panda','Rebroker on subjobs is not supported. \nUse j.backend.rebroker() (i.e. rebroker the master job) and your activated subjobs \nwill be automatically selected and rebrokered.')

        jobIDs = {}
        for job in jobs.subjobs: 
            jobIDs[job.backend.id] = job

        with inject_proxy(self.credential_requirements):
                rc,jspecs = Client.getFullJobStatus(jobIDs.keys(),False)
        if rc:
            logger.error('Return code %d retrieving job status information.',rc)
            raise BackendError('Panda','Return code %d retrieving job status information.' % rc)

        brokeredjobs = []
        for job in jspecs:
            if not job.jobDefinitionID in brokeredjobs:
                if job.jobStatus in ['frozen']:
                    logger.info('All subJobs in Job already finished/failed' )
                    continue

                if not jobs.backend.libds:
                    libds = ''
                else:
                    libds = jobs.backend.libds
                logger.info('Sending rebrokerage request ...')

                with inject_proxy(self.credential_requirements):
                    status,output = Client.runReBrokerage(job.jobDefinitionID, libds, cloud, False)
                brokeredjobs.append(job.jobDefinitionID)
                if status != 0:
                    logger.error(output)
                    logger.error("Failed to rebroker job with Panda jobDefinitionID=%s" % job.jobDefinitionID)
                    continue
                # done

                logger.info('Rebrokerage done for job with Panda jobDefinitionID=%s' % job.jobDefinitionID)
        return

    def check_auto_resubmit(self):
        """ Only auto resubmit if the master job has failed """
        j = self._getParent()
        if j.status == 'failed':
            return True

        return False

    @require_credential
    def master_resubmit(self,jobs):
        '''Resubmit failed subjobs'''
        #from pandatools import Client

        if self._getParent()._getParent(): # if has a parent then this is a subjob
            raise BackendError('Panda','Resubmit on subjobs is not supported for Panda backend. \nUse j.resubmit() (i.e. resubmit the master job) and your failed subjobs \nwill be automatically selected and retried.')

        jobIDs = {}
        for job in jobs: 
            jobIDs[job.backend.id] = job

        with inject_proxy(self.credential_requirements):
            rc,jspecs = Client.getFullJobStatus(jobIDs.keys(),False)
        if rc:
            logger.error('Return code %d retrieving job status information.',rc)
            raise BackendError('Panda','Return code %d retrieving job status information.' % rc)
       
        newJobsetID = -1 # get jobset
        retryJobs = [] # jspecs
        retrySite    = None
        retryElement = None
        retryDestSE  = None
        resubmittedJobs = [] # ganga jobs
        for job in jspecs:
            if job.jobStatus in ['failed', 'killed', 'cancelled']:
                retrySite    = None
                retryElement = None
                retryDestSE  = None
                oldID = job.PandaID
                # unify sitename
                if retrySite is None:
                    retrySite = job.computingSite
                    retryElement = job.computingElement
                    retryDestSE = job.destinationSE
                # reset
                job.jobStatus = None
                job.commandToPilot = None
                job.startTime = None
                job.endTime = None
                job.attemptNr = 1+job.attemptNr
                for attr in job._attributes:
                    if attr.endswith('ErrorCode') or attr.endswith('ErrorDiag'):
                        setattr(job,attr,None)
                job.transExitCode = None
                job.computingSite = retrySite
                job.computingElement = retryElement
                job.destinationSE = retryDestSE
                job.dispatchDBlock = None
                job.jobExecutionID = job.jobDefinitionID
                job.parentID = oldID
                if job.jobsetID != ['NULL',None,-1]:
                    job.sourceSite          = job.jobsetID
                    job.jobsetID            = newJobsetID

                for file in job.Files:
                    file.rowID = None
                    if file.type == 'input':
                        if file.lfn.endswith('.lib.tgz') and file.GUID == 'NULL':
                            raise BackendError('Panda','GUID for %s is unknown. Cannot retry when corresponding buildJob failed' % file.lfn)
                        file.status = 'ready'
                    elif file.type in ('output','log'):
                        file.destinationSE = retryDestSE
                        file.destinationDBlock = re.sub('_sub\d+$','',file.destinationDBlock)
                        # add retry num
                        if file.dataset.endswith('/'):
                            retryMatch = re.search('_r(\d+)$',file.destinationDBlock)
                            if retryMatch is None:
                                file.destinationDBlock += '_r1'
                            else:
                                tmpDestinationDBlock = re.sub('_r(\d+)$','',file.destinationDBlock)
                                file.destinationDBlock = tmpDestinationDBlock + '_r%d' % (1+int(retryMatch.group(1)))                                
                            jobIDs[oldID].outputdata.datasetname = file.destinationDBlock
                                
                        # add attempt nr
                        oldName  = file.lfn
                        file.lfn = re.sub("\.\d+$","",file.lfn)
                        file.lfn = "%s.%d" % (file.lfn,job.attemptNr)
                        newName  = file.lfn
                        # modify jobParameters
                        job.jobParameters = re.sub("'%s'" % oldName ,"'%s'" % newName, job.jobParameters)
                        # The previous line does not work on AthenaMC jobs, so the next lines are for them
                        if jobIDs[oldID].application._name == 'AthenaMC':
                            job.jobParameters = re.sub("%s" % oldName,"%s" % newName, job.jobParameters)
                retryJobs.append(job)
                resubmittedJobs.append(jobIDs[oldID])
            elif job.jobStatus == 'finished':
                pass
            else:
                logger.warning("Cannot resubmit. Some jobs are still running.")
                return False

        # register datasets
        addedDataset = []
        for rj in retryJobs:
            for tmpFile in rj.Files:
                if tmpFile.type in ['output','log'] and tmpFile.dataset.endswith('/'):
                    # add datasets
                    if not tmpFile.destinationDBlock in addedDataset:
                        with inject_proxy(self.credential_requirements):
                            tmpOutDsLocation = Client.PandaSites[rj.computingSite]['ddm']
                        # check this dataset doesn't already exist (in case of previous screw ups in resubmit)
                        # RUCIO patch
                        #res = Client.getDatasets(tmpFile.destinationDBlock)
                        res = getDatasets(tmpFile.destinationDBlock)
                        if not tmpFile.destinationDBlock in res.keys():
                            # DS doesn't exist - create it
                            try:
                                with inject_proxy(self.credential_requirements):
                                    Client.addDataset(tmpFile.destinationDBlock,False,location=tmpOutDsLocation)
                            except exceptions.SystemExit:
                                raise BackendError('Panda','Exception in Client.addDataset %s: %s %s'%(tmpFile.destinationDBlock,sys.exc_info()[0],sys.exc_info()[1]))


                        # check if this DS is in the container
                        # RUCIO patch
                        #res = Client.getElementsFromContainer(tmpFile.dataset)
                        res = getElementsFromContainer(tmpFile.dataset)
                        if not tmpFile.destinationDBlock in res:
                            try:
                                # add to container
                                with inject_proxy(self.credential_requirements):
                                    Client.addDatasetsToContainer(tmpFile.dataset,[tmpFile.destinationDBlock],False)
                                logger.warning('Created dataset %s and added to container %s.'%(tmpFile.destinationDBlock,tmpFile.dataset))
                            except exceptions.SystemExit:
                                raise BackendError('Panda','Exception in Client.addDatasetsToContainer %s, %s: %s %s'%(tmpFile.destinationDBlock,tmpFile.dataset,sys.exc_info()[0],sys.exc_info()[1]))
                        # append
                        addedDataset.append(tmpFile.destinationDBlock)

        # submit
        if len(retryJobs)==0:
            logger.warning("No failed jobs to resubmit")
            return False

        with inject_proxy(self.credential_requirements):
            status,newJobIDs = Client.submitJobs(retryJobs)
        if status:
            logger.error('Error: Status %d from Panda submit',status)
            return False
       
        for job, newJobID in zip(resubmittedJobs,newJobIDs):
            job.backend.id = newJobID[0]
            job.backend.url = 'http://panda.cern.ch?job=%d'%newJobID[0]
            job.backend.status = None
            job.backend.jobSpec = {}
            job.updateStatus('submitted')

            

        # mark merge job retrieval to be done for any new merges created
        self.domergeretrieve = True
        self.mergejobs = []
        
        logger.info('Resubmission successful')
        return True

    @staticmethod
    def master_updateMonitoringInformation(jobs):
        '''Monitor jobs'''       
        #from pandatools import Client

        active_status = [ None, 'defined', 'unknown', 'assigned', 'waiting', 'activated', 'sent', 'starting', 'running', 'holding', 'transferring' ]

        jobdict = {}
        monitoredjobids = []

        for job in jobs:

            monitoredjobids.append(job.id)
            buildjob = job.backend.buildjob
            if buildjob and buildjob.id and buildjob.status in active_status:
                jobdict[buildjob.id] = job

            ## adding merging jobs for status checking
            for mj in job.backend.mergejobs:
                logger.debug('adding merging job %s for status checking' % mj.id)
                jobdict[mj.id] = job

            for bj in job.backend.buildjobs:
                if bj.id and bj.status in active_status:
                    jobdict[bj.id] = job

            if job.backend.id and job.backend.status in active_status:
                jobdict[job.backend.id] = job 

            for subjob in job.subjobs:
                if subjob.backend.status in active_status or subjob.status in ['running', 'submitted', 'submitting']:
                    jobdict[subjob.backend.id] = subjob
                    if not subjob.backend.id:
                        logger.debug("WARNING!! Job: %d - Subjob: %d - Status: %s - Backend ID: %d " % (job.id, subjob.id, subjob.status, subjob.backend.id))
                else:
                    logger.debug('Skipping monitoring of Jobid: %d - Subjob id: %d - Status: %s - Backend ID: %d' % (job.id, subjob.id, subjob.status, subjob.backend.id))

            if job.status in ['running', 'submitted'] and job.backend.domergeretrieve:
                job.backend.mergejobs = []
                
        # split into 2000-job pieces
        allJobIDs = jobdict.keys()
        logger.debug("Monitored JobIDs: %s" % monitoredjobids)
        logger.debug("Monitored Panda JobIDs: %s" % allJobIDs)
        jIDPieces = [allJobIDs[i:i+2000] for i in range(0,len(allJobIDs),2000)]

        jlist_merge_finished = []
        clear_merge_jobs = []
        update_merge_master_status = []
        
        for jIDs in jIDPieces:
            rc, jobsStatus = Client.getFullJobStatus(jIDs,False)
            if rc:
                logger.error('Return code %d retrieving job status information.',rc)
                raise BackendError('Panda','Return code %d retrieving job status information.' % rc)
         
            for status in jobsStatus:

                if not status: continue

                try:
                    import taskbuffer
                    if not isinstance(status, taskbuffer.JobSpec.JobSpec):
                        continue
                except:
                    continue

                job = jobdict[status.PandaID]
                if job.backend.id == status.PandaID:

                    if job.backend.status != status.jobStatus:
                        job.backend.jobSpec = dict(zip(status._attributes,status.values()))

                        for k in job.backend.jobSpec.keys():
                            if type(job.backend.jobSpec[k]) not in [type(''),type(1)]:
                                job.backend.jobSpec[k]=str(job.backend.jobSpec[k])

                        logger.debug('Job %s has changed status from %s to %s',job.getFQID('.'),job.backend.status,status.jobStatus)
                        job.backend.actualCE = status.computingSite
                        job.backend.status = status.jobStatus
                        job.backend.exitcode = str(status.transExitCode)
                        job.backend.piloterrorcode = str(status.pilotErrorCode)
                        job.backend.reason = ''
                        for k in job.backend.jobSpec.keys():
                            if k.endswith('ErrorDiag') and job.backend.jobSpec[k]!='NULL':
                                job.backend.reason += '%s: %s, '%(k,str(job.backend.jobSpec[k]))
                        #if job.backend.jobSpec['transExitCode'] != 'NULL':
                        job.backend.reason += 'transExitCode: %s'%job.backend.jobSpec['transExitCode']

                    # can have odd race condition that updates panda info but NOT job status - force check every time
                    if status.jobStatus in ['defined','unknown','assigned','waiting','activated','sent']:
                        if job.status != 'submitted':
                            job.updateStatus('submitted')
                    elif status.jobStatus in ['starting','running','holding','transferring']:
                        if job.status == 'submitting': # Fix for HammerCloud in case jobs are left in submitting state
                            job.updateStatus('submitted')
                        elif job.status != 'running':
                            job.updateStatus('running')
                    elif status.jobStatus == 'finished':
                        if job.status == 'submitting': # Fix for HammerCloud in case jobs are left in submitting state
                            job.updateStatus('submitted')
                        elif job.status != 'completed':
                            if not job.backend._name=='PandaBuildJob' and job.status != "completed":
                                job.backend.fillOutputData(job, status)
                                if config['enableDownloadLogs']:
                                    job.backend.getLogFiles(job.getOutputWorkspace().getPath(), status)

                            if job.master and job.master.backend.requirements.enableMerge:
                                job.updateStatus('completed', update_master = False)
                            else:
                                job.updateStatus('completed')
                    elif status.jobStatus == 'failed':
                        if job.status == 'submitting': # Fix for HammerCloud in case jobs are left in submitting state
                            job.updateStatus('submitted')
                        elif job.status != 'failed':
                            # check for server side retry
                            if 'taskBufferErrorDiag' in job.backend.jobSpec and job.backend.jobSpec['taskBufferErrorDiag'].find("PandaID=") != -1:
                                # grab the new panda ID
                                newPandaID = long(job.backend.jobSpec['taskBufferErrorDiag'].split("=")[1])
                                job.backend.id = newPandaID
                                job.backend.status = None
                                job.backend.url = 'http://panda.cern.ch/?job=%d'%newPandaID
                            else:
                                if job.master and job.master.backend.requirements.enableMerge:
                                    job.updateStatus('failed', update_master = False)
                                else:
                                    job.updateStatus('failed')
                                    
                    elif status.jobStatus == 'cancelled' and job.status not in ['completed','failed']: # bug 67716
                        if job.status == 'submitting': # Fix for HammerCloud in case jobs are left in submitting state
                            job.updateStatus('submitted')
                        elif job.status != 'killed':
                            if 'taskBufferErrorDiag' in job.backend.jobSpec and "rebrokerage" in job.backend.jobSpec['taskBufferErrorDiag']:
                                newPandaID = checkForRebrokerage(job.backend.jobSpec['taskBufferErrorDiag'])
                                logger.warning("Subjob rebrokered by Panda server. Job %d moved to %d."%(job.backend.id, newPandaID))
                                job.backend.id = newPandaID
                                job.backend.status = None
                            else:
                                job.updateStatus('failed')
                    else:
                        logger.warning('Unexpected job status %s',status.jobStatus)

                    # Fix for HammerCloud in case jobs are left in submitting state
                    if job.status == 'submitting' and status.jobStatus in [ 'registered', 'defined', 'assigning', 'ready', 'pending', 'scouting',
                                                                            'scouted', 'running', 'prepared', 'done', 'failed', 'finished',
                                                                            'aborting', 'aborted', 'finishing', 'topreprocess', 'preprocessing',
                                                                            'tobroken', 'broken', 'toretry', 'toincexec', 'rerefine', 'paused',
                                                                            'throttled', 'exhausted', 'passed',
                                                                            'unknown', 'assigned', 'waiting', 'activated', 'sent', 
                                                                            'starting', 'holding', 'transferring' ]:
                        job.updateStatus('submitted')

                elif job.backend.buildjob and job.backend.buildjob.id == status.PandaID:
                    if job.backend.buildjob.status != status.jobStatus:
                        job.backend.buildjob.jobSpec = dict(zip(status._attributes,status.values()))
                        for k in job.backend.buildjob.jobSpec.keys():
                            if type(job.backend.buildjob.jobSpec[k]) not in [type(''),type(1)]:
                                job.backend.buildjob.jobSpec[k]=str(job.backend.buildjob.jobSpec[k])

                        logger.debug('Buildjob %s has changed status from %s to %s',job.getFQID('.'),job.backend.buildjob.status,status.jobStatus)
                        if config['enableDownloadLogs'] and not job.backend._name=='PandaBuildJob' and status.jobStatus == "finished" and job.backend.buildjob.status != "finished":
                            job.backend.getLogFiles(job.getOutputWorkspace().getPath("buildJob"), status)

                        job.backend.buildjob.status = status.jobStatus
       
                        try: 
                            if status.jobStatus == 'finished':
                                job.backend.libds = job.backend.buildjob.jobSpec['destinationDBlock']
                        except KeyError:
                            pass
                        
                        if status.jobStatus in ['defined','unknown','assigned','waiting','activated','sent','finished']:
                            job.updateStatus('submitted')
                        elif status.jobStatus in ['starting','running','holding','transferring']:
                            job.updateStatus('running')
                        elif status.jobStatus == 'failed':
                            if 'taskBufferErrorDiag' in job.backend.buildjob.jobSpec and job.backend.buildjob.jobSpec['taskBufferErrorDiag'].find("PandaID=") != -1:
                                # grab the new panda ID
                                newPandaID = long(job.backend.buildjob.jobSpec['taskBufferErrorDiag'].split("=")[1])
                                job.backend.buildjob.id = newPandaID
                                job.backend.buildjob.status = None
                                job.backend.buildjob.url = 'http://panda.cern.ch/?job=%d'%newPandaID
                            else:
                                job.updateStatus('failed')
                        elif status.jobStatus == 'cancelled':
                            if 'taskBufferErrorDiag' in job.backend.jobSpec and "rebrokerage" in job.backend.jobSpec['taskBufferErrorDiag']:
                                newPandaID = checkForRebrokerage(job.backend.jobSpec['taskBufferErrorDiag'])
                                logger.warning("Subjob rebrokered by Panda server. Job %d moved to %d."%(job.backend.id, newPandaID))
                                job.backend.id = newPandaID
                                job.backend.status = None
                            else:
                                job.updateStatus('failed')
                        else:
                            logger.warning('Unexpected job status %s',status.jobStatus)

                            
                        #un = job.backend.buildjob.jobSpec['prodUserID'].split('/CN=')[-2]
                        #jdid = job.backend.buildjob.jobSpec['jobDefinitionID']
                        #job.backend.url = 'http://panda.cern.ch/?job=*&jobDefinitionID=%s&user=%s'%(jdid,un)
                elif job.backend.buildjobs and status.PandaID in [bj.id for bj in job.backend.buildjobs]:
                    for bj in job.backend.buildjobs:
                        if bj.id == status.PandaID and bj.status != status.jobStatus:
                            bj.jobSpec = dict(zip(status._attributes,status.values()))
                            for k in bj.jobSpec.keys():
                                if type(bj.jobSpec[k]) not in [type(''),type(1)]:
                                    bj.jobSpec[k]=str(bj.jobSpec[k])

                            logger.debug('Buildjob %s has changed status from %s to %s',job.getFQID('.'),bj.status,status.jobStatus)
#                            if config['enableDownloadLogs'] and not job.backend._name=='PandaBuildJob' and status.jobStatus == "finished" and job.backend.buildjob.status != "finished":
#                                job.backend.getLogFiles(job.getOutputWorkspace().getPath("buildJob"), status)

                            bj.status = status.jobStatus
          
                            try: 
                                if len(job.backend.buildjobs) == 1 and status.jobStatus == 'finished':
                                    job.backend.libds = job.backend.buildjobs[0].jobSpec['destinationDBlock']
                            except KeyError:
                                pass

                            # update the status of the master job based on what all build jobs are doing
                            bjstats = [bj2.status for bj2 in job.backend.buildjobs]
                            new_stat = None
                            
                            for s in ['defined','unknown','assigned','waiting','activated','sent','finished', 'starting','running','holding','transferring', 'failed', 'cancelled']:
                                if s in bjstats:
                                    new_stat = s
                                    break
                            if job.status == 'submitting': # Fix for HammerCloud in case jobs are left in submitting state
                                job.updateStatus('submitted')
    
                            if new_stat in ['defined','unknown','assigned','waiting','activated','sent','finished']:
                                job.updateStatus('submitted')
                            elif new_stat in ['starting','running','holding','transferring']:
                                job.updateStatus('running')
                            elif new_stat == 'failed':
                                if 'taskBufferErrorDiag' in bj.jobSpec and bj.jobSpec['taskBufferErrorDiag'].find("PandaID=") != -1:
                                    # grab the new panda ID
                                    newPandaID = long(bj.jobSpec['taskBufferErrorDiag'].split("=")[1])
                                    bj.id = newPandaID
                                    bj.status = None
                                    bj.url = 'http://panda.cern.ch/?job=%d'%newPandaID
                                else:                                    
                                    job.updateStatus('failed')
                            elif new_stat == 'cancelled':
                                if 'taskBufferErrorDiag' in job.backend.jobSpec and "rebrokerage" in job.backend.jobSpec['taskBufferErrorDiag']:
                                    newPandaID = checkForRebrokerage(job.backend.jobSpec['taskBufferErrorDiag'])
                                    logger.warning("Subjob rebrokered by Panda server. Job %d moved to %d."%(job.backend.id, newPandaID))
                                    job.backend.id = newPandaID
                                    job.backend.status = None
                                else:
                                    job.updateStatus('failed')
                            else:
                                logger.warning('Unexpected job status %s',status.jobStatus)


                            #un = job.backend.buildjob.jobSpec['prodUserID'].split('/CN=')[-2]
                            #jdid = job.backend.buildjob.jobSpec['jobDefinitionID']
                            #job.backend.url = 'http://panda.cern.ch/?job=*&jobDefinitionID=%s&user=%s'%(jdid,un)
                elif job.backend.mergejobs and status.PandaID in [mj.id for mj in job.backend.mergejobs]:
                    tmp_mj    = PandaMergeJob()
                    tmp_mj.id = status.PandaID

                    mj = job.backend.mergejobs[ job.backend.mergejobs.index( tmp_mj ) ]

                    # update job spec
                    mj.jobSpec = dict(zip(status._attributes,status.values()))
                    for k in mj.jobSpec.keys():
                        if type(mj.jobSpec[k]) not in [type(''),type(1)]:
                            mj.jobSpec[k]=str(mj.jobSpec[k])

                    if mj.status != status.jobStatus:
                        logger.debug('Mergejob %s has changed status from %s to %s',job.getFQID('.'), mj.status, status.jobStatus)

                        mj.status = status.jobStatus

                    # check for Panda resubmission
                    if mj.status == 'failed':
                        if 'taskBufferErrorDiag' in mj.jobSpec and mj.jobSpec['taskBufferErrorDiag'].find("PandaID=") != -1:
                            logger.info("Merge job failed for job %s. Panda has resubmitted" % job.fqid)

                            # clear the merge jobs and force it to grab them again
                            clear_merge_jobs.append(job)
                        
                    update_merge_master_status.append(job)

                else:
                    logger.warning('Unexpected Panda ID %s',status.PandaID)

        ## clear merge jobs as required
        for j in clear_merge_jobs:
            j.backend.mergejobs = []
            j.backend.domergeretrieve = True

        # update merge status's
        for job in update_merge_master_status:

            if job.backend.domergeretrieve:
                continue
            
            mjstats = [mj2.status for mj2 in job.backend.mergejobs]

            merge_finished = True

            for s in mjstats:
                if s not in ['failed','finished','cancelled']:
                    merge_finished = False

            if merge_finished:
                ## merge jobs finished, update the master job status respecting the status of subjobs
                job.updateMasterJobStatus()
                jlist_merge_finished.append(job)

            elif job.status != 'running':
                ## merge jobs are still running, master job status kept running
                job.updateStatus('running')
                        
        ## going through all jobs to find those with merging jobs to be retrieved
        jlist_for_masterjob_update = []
        for job in jobs:

            if job.backend.requirements.enableMerge:

                ## check if there is a necessary to retrieve merging jobs
                if job.backend.domergeretrieve:

                    # check subjobs
                    do_merge_retrieve = True

                    for sj in job.subjobs:
                        ## skip merging job retrieval if there are still subjobs not in the final state
                        if sj.backend.status not in ['failed','finished','cancelled']:
                            do_merge_retrieve = False
                            break

                    if do_merge_retrieve:

                        tot_num_mjobs = 0

                        jdefids = list( set( [bj.backend.jobSpec['jobDefinitionID'] for bj in job.subjobs] ))

                        # first check all merge jobs are generated
                        retrieve_merge_jobs = True
                        do_master_update = False
                        for jdefid in jdefids:
                            ick,status = checkMergeJobStatus(job, jdefid)
                            if not ick:
                                logger.warning('merging job check failure')

                            if status not in ['standby','generating','generated']:
                                ## no merging jobs are expected in this case
                                ## skip merging jobs checking by updating the master job status
                                do_master_update = True

                            if status != "generated":
                                retrieve_merge_jobs = False

                        if retrieve_merge_jobs:

                            for jdefid in jdefids:

                                ick,status,num_mjobs = retrieveMergeJobs(job, jdefid)

                                logger.debug('retrieveMergeJobs returns: %s %s' % (repr(ick),status))

                                if not ick:
                                    logger.warning('merging job retrival failure')

                                tot_num_mjobs += num_mjobs
                                                
                        # set the flag to prevent further retrievals unless a resub is called
                        if tot_num_mjobs > 0:
                            job.backend.domergeretrieve = False
                            logger.info('job %s retrieved %d merging jobs' % (job.getFQID('.'),tot_num_mjobs) )

                        
                        if do_master_update:
                            jlist_for_masterjob_update.append(job)

                    else:
                        ## need to update the master job status when there are still subjobs not in final state
                        jlist_for_masterjob_update.append(job)
            
            else:
                jlist_for_masterjob_update.append(job)

        ## doing master job status update only on those without merging jobs
        for job in jlist_for_masterjob_update:
            if job.subjobs and job.status != 'failed':
                job.updateMasterJobStatus()

        ## ensure the master job status to be "running" if merging jobs are running or about to be generated
        ## (i.e. those jobs not included for master job status update
        for job in list( set(jobs) - set(jlist_for_masterjob_update) - set(jlist_merge_finished)):
            if job.status != 'running':
                job.updateStatus('running')
#                try:
#                    job.updateStatus('running')
#                except JobStatusError:
#                    pass

    def list_sites(self):
        #from pandatools import Client
        refreshPandaSpecs()

        sites=Client.PandaSites.keys()
        spacetokens = []
        for s in sites:
            spacetokens.append(convertQueueNameToDQ2Names(s))
        return spacetokens

    def list_ddm_sites(self,allowTape=False):
        #from pandatools import Client
        refreshPandaSpecs()

        spacetokens = []
        if self.site == 'AUTO':
            if self.libds:
                logger.info('Locating libds %s'%self.libds)
                try:
                    # RUCIO patch
                    #libdslocation = Client.getLocations(self.libds,[],self.requirements.cloud,False,False)
                    info = getLocations(self.libds)
                    location = info.values()[0][1]
                    libdslocation = convertDQ2NamesToQueueName(location)
                except exceptions.SystemExit:
                    raise BackendError('Panda','Error in Client.getLocations for libDS')
                if not libdslocation:
                    raise ApplicationConfigurationError('Could not locate libDS %s'%self.libds)
                else:
                    libdslocation = libdslocation.values()[0]
                    try:
                        self.requirements.cloud = Client.PandaSites[libdslocation[0]]['cloud']
                    except:
                        raise BackendError('Panda','Could not map libds site %s to a cloud'%libdslocation)
                sites = libdslocation
                logger.info('LibDS is at %s. Run jobs will execute there.'%sites)
            else: 
                sites=Client.PandaSites.keys()

            for s in sites:
                if Client.PandaSites[s]['status'] not in ['online'] or s in self.requirements.excluded_sites or Client.PandaSites[s]['cloud'] in self.requirements.excluded_clouds or (not self.requirements.anyCloud and Client.PandaSites[s]['cloud'] != self.requirements.cloud):
                    continue
                tokens = convertQueueNameToDQ2Names(s)
                for t in tokens:
                    if (allowTape or not Client.isTapeSite(t)) and t.find("TZERO") == -1:
                        spacetokens.append(t)
        else: # direct site submission
            try:
                s = Client.PandaSites[self.site]
            except KeyError:
                raise BackendError('Panda','Site %s not in a known Panda Sites'%self.site)
            if s['status'] not in ['online','brokeroff'] and not (s['status'] == 'test' and config['processingType'].startswith('gangarobot')):
                raise BackendError('Panda','Cannot submit to %s in status %s'%(self.site,s['status']))
            if self.site in self.requirements.excluded_sites:
                raise BackendError('Panda','Cannot submit to %s because it is in your requirements.excluded_sites list'%self.site)
            tokens = convertQueueNameToDQ2Names(self.site)
            for t in tokens:
                if (allowTape or not Client.isTapeSite(t)) and t.find("TZERO") == -1:
                    spacetokens.append(t)

        # Remove any empty strings
        spacetokens = [t for t in spacetokens if t]
        logger.debug('site %s spacetokens %s' % (self.site, spacetokens))

        return spacetokens

    def get_stats(self):
        fields = {
            'site':"self.jobSpec['computingSite']",
            'exitstatus':"self.jobSpec['transExitCode']",
            'outse':"self.jobSpec['destinationSE']",
            'jdltime':"''",
            'submittime':"int(time.mktime(time.strptime(self.jobSpec['creationTime'],'%Y-%m-%d %H:%M:%S')))",
            'starttime':"int(time.mktime(time.strptime(self.jobSpec['startTime'],'%Y-%m-%d %H:%M:%S')))",
            'stoptime':"int(time.mktime(time.strptime(self.jobSpec['endTime'],'%Y-%m-%d %H:%M:%S')))",
            'totalevents':"int(self.jobSpec['nEvents'])", 
            'nInputDataFiles':"int(self.jobSpec['nInputDataFiles'])", 
            'inputFileType':"int(self.jobSpec['inputFileType'])", 
            'inputFileProject':"self.jobSpec['inputFileProject']", 
            'inputFileBytes':"int(self.jobSpec['inputFileBytes'])", 
            'nOutputDataFiles':"int(self.jobSpec['nOutputDataFiles'])", 
            'outputFileBytes':"int(self.jobSpec['outputFileBytes'])", 
            'jobMetrics':"self.jobSpec['jobMetrics']", 
            'wallclock':"(int(time.mktime(time.strptime(self.jobSpec['endTime'],'%Y-%m-%d %H:%M:%S')))-int(time.mktime(time.strptime(self.jobSpec['startTime'],'%Y-%m-%d %H:%M:%S'))))",
            'percentcpu':"int(100*self.jobSpec['cpuConsumptionTime']/float(self.jobSpec['cpuConversion'])/(int(time.mktime(time.strptime(self.jobSpec['endTime'],'%Y-%m-%d %H:%M:%S')))-int(time.mktime(time.strptime(self.jobSpec['startTime'],'%Y-%m-%d %H:%M:%S')))))",
            'numfiles':'""',
            'gangatime1':'""',
            'gangatime2':'""',
            'gangatime3':'""',
            'gangatime4':'""',
            'gangatime5':'""',
            'pandatime1':"int(self.jobSpec['pilotTiming'].split('|')[0])",
            'pandatime2':"int(self.jobSpec['pilotTiming'].split('|')[1])",
            'pandatime3':"int(self.jobSpec['pilotTiming'].split('|')[2])",
            'pandatime4':"int(self.jobSpec['pilotTiming'].split('|')[3])",
            'pandatime5':"int(self.jobSpec['pilotTiming'].split('|')[4])",
            'NET_ETH_RX_PREATHENA':'""',
            'NET_ETH_RX_AFTERATHENA':'""'
            }
        stats = {}
        for k in fields.keys():
            try:
                stats[k] = eval(fields[k])
            except:
                pass
        return stats


    def getLogFiles(self, workspace, status):
        for lf in [f for f in status.Files if f.type == "log"]:
            untar = ""
            if "tgz" in lf.lfn:
                untar = "tar xzf %s; mv tarball_PandaJob*/* .; rm tarball_PandaJob* -rf; rm %s;" % (lf.lfn, lf.lfn)
            cmd = "pushd .; mkdir -p %s; cd %s; dq2-get -D -f %s %s; %s popd;" % (workspace, workspace, lf.lfn, lf.dataset, untar)
            Download.download_dq2(cmd).run()
            
        
    def fillOutputData(self, job, status):
        # format for outputdata is: dataset,lfn,guid,size,md5sum,siteID\ndataset...
        outputdata = []
        locations = {}
        for of in [f for f in status.Files if f.type in ["output","log"]]:
            outputdata.append("%s,%s,%s,%s,%s,%s" % (of.dataset,of.lfn,of.GUID,of.fsize,of.checksum,of.destinationSE))
            locations[of.destinationSE] = 1
        if len(locations.keys()) > 1:
            logger.warning("Outputfiles of job %s saved at different locations! (%s)" % (job.fqid, locations.keys()))
        if len(locations.keys()) > 0:
            job.outputdata.location = locations.keys()[0]
        job.outputdata.output = outputdata

# jobSpec = {'startTime': '2011-11-01 09:37:51', 'modificationTime': '2011-11-01 10:03:22', 'creationTime': '2011-11-01 09:37:40', 'endTime': '2011-11-01 09:47:16', } ,



    def getStateTime(self, status):
        """Obtains the timestamps for the 'running', 'completed', and 'failed' states.

           The __jobstatus__ file in the job's output directory is read to obtain the start and stop times of the job.
           These are converted into datetime objects and returned to the user.
        """

        import datetime
        checkstr=''
        if status == 'running':
            checkstr='startTime'
        elif status == 'completed':
            checkstr='endTime'
        elif status == 'failed':
            checkstr='endTime'

        if not checkstr:
            return None

        try:
            t = datetime.datetime(*(time.strptime(self.jobSpec[checkstr], "%Y-%m-%d %H:%M:%S")[0:6]))
        except:
            return None
        return t


    def timedetails(self):
        """Return all available timestamps from this backend.
        """

        import datetime
        r = self.getStateTime('running')
        c = self.getStateTime('completed')
        d = {'START' : r, 'STOP' : c}

        return d



#
#
# $Log: not supported by cvs2svn $
# Revision 1.46  2009/07/21 11:15:30  dvanders
# fix for https://savannah.cern.ch/bugs/?53470
#
# Revision 1.45  2009/07/14 08:29:23  dvanders
# change pandamon url
#
# Revision 1.44  2009/06/18 08:35:46  dvanders
# panda-client 0.1.71
# trust the information system (jobs won't submit if athena release not installed).
#
# Revision 1.43  2009/06/10 13:47:13  ebke
# Check for NULL return string of Panda job Id and suggest to shorten dataset name
#
# Revision 1.42  2009/06/08 13:02:10  dvanders
# force to submitted (because jobs can go from running to activated)
#
# Revision 1.41  2009/06/08 08:25:24  dvanders
# backend.CE doesn't exist
#
# Revision 1.40  2009/05/30 08:31:59  dvanders
# limit submit to 2000 subjobs
#
# Revision 1.39  2009/05/30 07:22:09  dvanders
# Panda server has a per call limit of 2500 subjobs per getFullJobStatus.
#
# Revision 1.38  2009/05/30 05:48:38  dvanders
# getFullJobStatus
#
# Revision 1.37  2009/05/29 18:10:22  dvanders
# fill libds if buildjob succeeds.
#
# Revision 1.36  2009/05/28 21:46:21  dvanders
# processingType=ganga
#
# Revision 1.35  2009/05/28 11:11:20  ebke
# Outputdata now filled even if enableDownloadLogs disabled
#
# Revision 1.34  2009/05/20 12:41:22  dvanders
# DQ2Dataset->DQ2dataset
#
# Revision 1.33  2009/05/18 12:28:41  dvanders
# startime -> starttime
#
# Revision 1.32  2009/05/12 13:52:37  dvanders
# concat all ErrorDiags for backend reason
#
# Revision 1.31  2009/05/12 13:06:09  elmsheus
# Correct logic for enableDownloadLogs
#
# Revision 1.30  2009/05/12 12:49:33  elmsheus
# Add config[enableDownloadLogs] and remove build job output download
#
# Revision 1.29  2009/04/30 12:21:17  ebke
# Added log file downloading and outputdata filling (DQ2Dataset) to Panda backend
# Not tested AthenaMCDataset yet!
#
# Revision 1.28  2009/04/27 15:14:50  dvanders
# Fixed ARA again
# Fixed libds support
# changes to ARA test case
# new libds testcase
#
# Revision 1.27  2009/04/27 11:13:08  ebke
# Added user-settable libds support, and fixed submission without local athena setup
#
# Revision 1.26  2009/04/22 11:42:52  dvanders
# change stats. schema 2.0
#
# Revision 1.25  2009/04/22 08:35:13  dvanders
# Error codes in the Panda object
#
# Revision 1.24  2009/04/22 07:59:50  dvanders
# percentcpu is an int
#
# Revision 1.23  2009/04/22 07:43:44  dvanders
# - Move requirements to PandaRequirements
# - Store the Panda JobSpec in a backend.jobSpec dictionary
# - Added backend.get_stats()
#
# Revision 1.22  2009/04/17 07:24:17  dvanders
# add processingType
#
# Revision 1.21  2009/04/07 15:20:35  dvanders
# runPandaBrokerage works for no inputdata
#
# Revision 1.20  2009/04/07 08:18:45  dvanders
# massive Panda changes:
#   Many Panda options moved to Athena.py.
#   Athena RT handler now uses prepare from Athena.py
#   Added Executable RT handler. Not working for me yet.
#   Added test cases for Athena and Executable
#
# Revision 1.19  2009/03/24 10:50:22  dvanders
# small fix
#
# Revision 1.18  2009/03/05 15:58:15  dvanders
# https://savannah.cern.ch/bugs/index.php?47473
# dbRelease option is now deprecated in Panda backend.
#
# Revision 1.17  2009/03/05 15:03:28  dvanders
# https://savannah.cern.ch/bugs/?46836
#
# Revision 1.16  2009/01/29 17:22:27  dvanders
# extFile option for additional files to ship to worker node
#
# Revision 1.15  2009/01/29 14:14:05  dvanders
# use panda-client 0.1.6
# use AthenaUtils to extract run config and detect athena env
#
# Revision 1.14  2008/12/12 15:04:34  dvanders
# dbRelease option
#
# Revision 1.13  2008/11/13 16:28:09  dvanders
# supStream support: suppress some output streams. e.g., ['ESD','TAG']
# improved logging messages
#
# Revision 1.12  2008/10/21 14:30:34  dvanders
# comment out prints
#
# Revision 1.11  2008/10/16 21:56:52  dvanders
# add runPandaBrokerage and queueToAllowedSites functions
#
# Revision 1.10  2008/10/06 15:27:48  dvanders
# add extOutFile
#
# Revision 1.9  2008/09/29 08:14:53  dvanders
# fix for type checking
#
# Revision 1.8  2008/09/06 17:53:02  dvanders
# less spammy status changes. (Only updateStatus when panda status has changed).
#
# Revision 1.7  2008/09/06 09:18:30  dvanders
# don't marked completed when build job finishes!
#
# Revision 1.6  2008/09/05 12:06:54  dvanders
# fix bug in update
#
# Revision 1.5  2008/09/05 09:07:00  dvanders
# removed 'completing' state
#
# Revision 1.4  2008/09/04 15:33:10  dvanders
# added unknown, starting panda statuses
#
# Revision 1.3  2008/09/03 17:04:56  dvanders
# Use external PandaTools
# Added cloud
# Removed useless dq2_get and getQueue
# EXPERIMENTAL: Added resubmission:
#     job(x).resubmit() will resubmit the _failed_ subjobs to Panda.
# Removed useless gridshell
# Cleaned up status update function
#
# Revision 1.2  2008/07/28 15:45:44  dvanders
# list_sites now gets from Panda server
#
# Revision 1.1  2008/07/17 16:41:31  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.11.2.3  2008/07/08 00:42:14  dvanders
# add ara option
#
# Revision 1.11.2.2  2008/07/01 09:20:23  dvanders
# fixed warning when setting site
# added corCheck and notSkipMissing options
#
# Revision 1.11.2.1  2008/04/04 08:00:31  elmsheus
# Change to new configuation schema
#
# Revision 1.11  2008/02/23 14:07:33  liko
# Fix stupid bug in returning the sites
#
# Revision 1.10  2007/10/15 14:24:50  liko
# *** empty log message ***
#
# Revision 1.9  2007/10/15 11:46:15  liko
# *** empty log message ***
#
# Revision 1.8  2007/10/08 15:15:05  liko
# *** empty log message ***
#
# Revision 1.7  2007/10/03 15:55:09  liko
# *** empty log message ***
#
# Revision 1.6  2007/07/18 13:00:46  liko
# *** empty log message ***
#
# Revision 1.5  2007/07/03 09:39:53  liko
# *** empty log message ***
#
# Revision 1.4  2007/06/27 12:44:57  liko
# Works more or less
#
# Revision 1.3  2007/04/07 21:43:24  liko
# *** empty log message ***
#
# Revision 1.2  2007/04/07 19:52:26  liko
# *** empty log message ***
#
# Revision 1.1  2007/03/21 10:18:16  liko
# Next try
#
# Revision 1.3  2007/01/15 11:21:47  liko
# Updates
#
# Revision 1.2  2006/11/14 15:46:53  liko
# Initial version
#
# Revision 1.1  2006/11/13 14:45:25  liko
# Some bug fixes, but some open points remain...
# 
