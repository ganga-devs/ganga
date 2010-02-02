###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DQ2JobSplitter.py,v 1.41 2009-07-27 13:03:24 mslater Exp $
###############################################################################
# Athena DQ2JobSplitter

import math, socket, operator

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Schema import *

from Ganga.Utility.logging import getLogger

import Ganga.Utility.external.ARDAMDClient.mdclient as mdclient
import Ganga.Utility.external.ARDAMDClient.mdinterface as mdinterface

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import *
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError

from Ganga.Utility import Caching

from Ganga.GPIDev.Credentials import GridProxy
gridProxy = GridProxy()

logger = getLogger()

def dq2_siteinfo(dataset, allowed_sites, locations, udays):

    result = dq2_list_locations_siteindex(datasets=dataset, days=udays, replicaList=True, allowed_sites= allowed_sites) 

    siteinfo = {}
    for guid, sites in result.iteritems():
        newsites = [ site for site in sites if site in allowed_sites ]
        # Remove inconsistencies 
        newsites = [ site for site in newsites if site in locations ]

        for site in newsites:
            if site.find('TAPE')>=0:
                newsites.remove(site)
    
        if not newsites: continue
        newsites.sort()
        sitekey = ':'.join(newsites)
        if sitekey in siteinfo:
            siteinfo[sitekey].append(guid)
        else:
            siteinfo[sitekey] = [ guid ]

    return siteinfo



def lfc_siteinfo(result,allowed_sites):
   
    siteinfo = {}
    for guid, sites in result.iteritems():
        newsites = [ site for site in sites if site in allowed_sites ]
        if not newsites: continue
        newsites.sort()
        sitekey = ':'.join(newsites)
        if sitekey in siteinfo:
            siteinfo[sitekey].append(guid)
        else:
            siteinfo[sitekey] = [ guid ]

    return siteinfo
 
class DQ2JobSplitter(ISplitter):
    '''Dataset driven job splitting'''

    _name = 'DQ2JobSplitter'
    _schema = Schema(Version(1,0), {
        'numfiles'          : SimpleItem(defvalue=0,doc='Number of files per subjob'),
        'numsubjobs'        : SimpleItem(defvalue=0,sequence=0, doc="Number of subjobs"),
        'use_lfc'           : SimpleItem(defvalue = False, doc = 'Use LFC catalog instead of default site catalog/tracker service'),
        'update_siteindex'  : SimpleItem(defvalue = True, doc = 'Update siteindex during job submission to get the latest file location distribution.'),
        'use_blacklist'     : SimpleItem(defvalue = True, doc = 'Use black list of sites create by GangaRobot functional tests.'),
        'filesize'          : SimpleItem(defvalue=0, doc = 'Maximum filesize sum per subjob im MB.'),
    })

    _GUIPrefs = [ { 'attribute' : 'numfiles',         'widget' : 'Int' },
                  { 'attribute' : 'numsubjobs',       'widget' : 'Int' },
                  { 'attribute' : 'use_lfc',          'widget' : 'Bool' },
                  { 'attribute' : 'update_siteindex', 'widget' : 'Bool' },
                  { 'attribute' : 'use_blacklist',    'widget' : 'Bool' },
                  { 'attribute' : 'filesize',         'widget' : 'Int' }
                  ]


    def split(self,job):

        logger.debug('DQ2JobSplitter called')

        if job.inputdata._name <> 'DQ2Dataset'  and job.inputdata._name <> 'AMIDataset':
            raise ApplicationConfigurationError(None,'DQ2 Job Splitter requires a DQ2Dataset or AMIDataset as input')

        if job.backend._name <> 'LCG' and job.backend._name <> 'Panda' and job.backend._name <> 'NG':
            raise ApplicationConfigurationError(None,'DQ2JobSplitter requires an LCG, Panda or NG backend')

        #AMIDataset
        if job.inputdata._name == 'AMIDataset':
            job.inputdata.dataset = job.inputdata.search(pattern = job.inputdata.logicalDatasetName)

        # before we do anything, check for tag info for this dataset
        additional_datasets = {}
        local_tag = False
        grid_tag = False
        if job.inputdata.tag_info:

            logger.warning('TAG information present - overwriting previous DQ2Dataset definitions')

            job.inputdata.names = []
            job.inputdata.dataset = []
            
            # assemble the tag datasets to split over
            for tag_file in job.inputdata.tag_info:

                if job.inputdata.tag_info[tag_file]['dataset'] != '' and job.inputdata.tag_info[tag_file]['path'] == '':
                    grid_tag = True
                    job.inputdata.names.append( tag_file )

                    if not job.inputdata.tag_info[tag_file]['dataset'] in job.inputdata.dataset:
                        job.inputdata.dataset.append(job.inputdata.tag_info[tag_file]['dataset'])

                    # add to additional datasets list
                    for tag_ref in job.inputdata.tag_info[tag_file]['refs']:
                        if not additional_datasets.has_key(job.inputdata.tag_info[tag_file]['dataset']):
                            additional_datasets[job.inputdata.tag_info[tag_file]['dataset']] = []

                        if not tag_ref[1] in additional_datasets[job.inputdata.tag_info[tag_file]['dataset']]:
                            additional_datasets[job.inputdata.tag_info[tag_file]['dataset']].append(tag_ref[1])
                elif job.inputdata.tag_info[tag_file]['path'] != '' and job.inputdata.tag_info[tag_file]['dataset'] == '':
                    local_tag = True
                    if len(job.inputdata.tag_info[tag_file]['refs']) > 1:
                        raise ApplicationConfigurationError(None,'Problems with TAG entry for %s. Mulitple references for local TAG file.' % tag_file)

                    job.inputdata.names.append( job.inputdata.tag_info[tag_file]['refs'][0][0] )
                    
                    if not job.inputdata.tag_info[tag_file]['refs'][0][1] in job.inputdata.dataset:
                        job.inputdata.dataset.append(job.inputdata.tag_info[tag_file]['refs'][0][1])
                    
                else:
                    raise ApplicationConfigurationError(None,'Problems with TAG entry for %s' % tag_file)
                
            if grid_tag and local_tag:
                raise ApplicationConfigurationError(None,'Problems with TAG info - both grid and local TAG files selected.')
                    
                    
        # now carry on as before
        orig_numfiles = self.numfiles
        orig_numsubjobs = self.numsubjobs

        if self.numfiles <= 0: 
            self.numfiles = 1

        cache_get_locations = Caching.FunctionCache(job.inputdata.get_locations, job.inputdata.dataset)
        locations = cache_get_locations(overlap=False)
#       locations = job.inputdata.get_locations(overlap=False)
        
        allowed_sites = []
        if job.backend._name == 'LCG':
            if job.backend.requirements._name == 'AtlasLCGRequirements':
                if job.backend.requirements.sites:
                    allowed_sites = job.backend.requirements.sites
                elif job.backend.requirements.cloud:

                    # to a check for the 'ALL' cloud option and if given, reduce the selection
                    if job.backend.requirements.cloud == 'ALL' and not job.backend.requirements.sites and job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
                        logger.warning('DQ2OutputDataset being used with \'ALL\' cloud option. Restricting to a single cloud. Note this may not allow all data to be analysed.')

                        avail_clouds = {}
                        for key in locations:
                            avail_clouds[key] = []

                            info = job.backend.requirements.cloud_from_sites(locations[key])

                            for all_site in info:
                                if not info[all_site] in avail_clouds[key] and not info[all_site] in ['US', 'NG']:
                                    if info[all_site] == 'T0':
                                        info[all_site] = 'CERN'                     
                                    avail_clouds[key].append(info[all_site])

                        # perform logical AND to find a cloud that has all data
                        from sets import Set

                        cloud_set = Set(job.backend.requirements.list_clouds())

                        for key in avail_clouds:
                            cloud_set = cloud_set & Set(avail_clouds[key])

                        # find users cloud and submit there by preference
                        fav_cloud = ''
                        for ln in gridProxy.info('--all').split('\n'):
                            if ln.find('attribute') == -1 or ln.find('atlas') == -1:
                                continue

                            toks = ln.split('/')
                            
                            if len(toks) < 3:
                                continue
                            
                            if toks[2].upper() in job.backend.requirements.list_clouds():
                                fav_cloud = toks[2].upper()
                                break

                        if len(cloud_set) == 0:
                            logger.error('Cloud option \'ALL\' could not find a complete replica of the dataset in any cloud. Please try a specific site or cloud.')
                            allowed_sites = []
                        else:
                            cloud_list = list(cloud_set)
                            if not fav_cloud in cloud_list:
                                fav_cloud = cloud_list[0]
                        
                            logger.warning('\'%s\' cloud selected. Continuing job submission...' % fav_cloud)
                            allowed_sites = job.backend.requirements.list_sites_cloud( fav_cloud )
                    else:
                        allowed_sites = job.backend.requirements.list_sites_cloud()
                else: 
                    raise ApplicationConfigurationError(None,'DQ2JobSplitter requires a cloud or a site to be set - please use the --cloud option, j.backend.requirements.cloud=CLOUDNAME (T0, IT, ES, FR, UK, DE, NL, TW, CA, US, NG) or j.backend.requirements.sites=SITENAME')
                allowed_sites_all = job.backend.requirements.list_sites(True,True)
                # Apply GangaRobot blacklist
                if self.use_blacklist:
                    newsites = []
                    for site in allowed_sites:
                        if site in allowed_sites_all:
                            newsites.append(site)
                    allowed_sites = newsites
                # Check atlas_dbrelease
                if job.application._name != 'TagPrepare' and job.application.atlas_dbrelease:
                    try:
                        db_dataset = job.application.atlas_dbrelease.split(':')[0]
                    except:
                        raise ApplicationConfigurationError(None,'Problem in DQ2JobSplitter - j.application.atlas_dbrelease is wrongly configured ! ')
                    from dq2.clientapi.DQ2 import DQ2
                    from dq2.info import TiersOfATLAS
                    dq2=DQ2()
                    try:
                        db_locations = dq2.listDatasetReplicas(db_dataset).values()[0][1]
                    except:
                        raise ApplicationConfigurationError(None,'Problem in DQ2JobSplitter - j.application.atlas_dbrelease is wrongly configured ! ')
                    db_allowed_sites=[]
                    dq2alternatenames=[]
                    for site in allowed_sites:
                        if site in db_locations:
                            db_allowed_sites.append(site)
                            dq2alternatenames.append(TiersOfATLAS.getSiteProperty(site,'alternateName'))
                    for sitename in TiersOfATLAS.getAllSources():
                        if TiersOfATLAS.getSiteProperty(sitename,'alternateName'):
                            if TiersOfATLAS.getSiteProperty(sitename,'alternateName') in dq2alternatenames and not sitename in db_allowed_sites:
                                db_allowed_sites.append(sitename)
                    allowed_sites = db_allowed_sites
                # Check for additional datasets
                if len(additional_datasets) > 0:
                    from dq2.clientapi.DQ2 import DQ2
                    from dq2.info import TiersOfATLAS
                    dq2=DQ2()
                    add_locations_all = []

                    additional_datasets_all = []
                    
                    for tag_dataset in additional_datasets:
                        for add_dataset in additional_datasets[tag_dataset]:
                            if not add_dataset in additional_datasets_all:
                                additional_datasets_all.append(add_dataset)
                            
                    for add_dataset in additional_datasets_all:
                        if len(add_locations_all) == 0:
                            add_locations_all = dq2.listDatasetReplicas(add_dataset).values()[0][1]
                        else:
                            add_locations = dq2.listDatasetReplicas(add_dataset).values()[0][1]
                            
                            for add_location in add_locations_all:
                                if not add_location in add_locations:
                                    add_locations_all.remove(add_location)
                                    
                    add_allowed_sites=[]
                    dq2alternatenames=[]
                    for site in allowed_sites:
                        if site in add_locations_all:
                            add_allowed_sites.append(site)
                            dq2alternatenames.append(TiersOfATLAS.getSiteProperty(site,'alternateName'))
                    for sitename in TiersOfATLAS.getAllSources():
                        if TiersOfATLAS.getSiteProperty(sitename,'alternateName'):
                            if TiersOfATLAS.getSiteProperty(sitename,'alternateName') in dq2alternatenames and not sitename in add_allowed_sites:
                                add_allowed_sites.append(sitename)
                    allowed_sites = add_allowed_sites

                # Check if site is online:
                newsites = []
                for asite in allowed_sites:
                    if not job.backend.requirements.list_ce(asite):
                        logger.warning('Site %s is currently down or does not allow user analysis - please check carefully if all inputfiles are available for your jobs. Maybe switch to a different cloud.',asite)
                    else:
                        newsites.append(asite)
                allowed_sites = newsites
                    
        elif job.backend._name == 'Panda':
            from GangaPanda.Lib.Panda.Panda import runPandaBrokerage,queueToAllowedSites
            runPandaBrokerage(job)
            allowed_sites = queueToAllowedSites(job.backend.site)
        elif job.backend._name == 'NG':
            allowed_sites = config['AllowedSitesNGDQ2JobSplitter']

        if not allowed_sites:
            raise ApplicationConfigurationError(None,'DQ2JobSplitter found no allowed_sites for dataset')
        
        logger.debug('allowed_sites = %s ', allowed_sites)

        cache_get_contents = Caching.FunctionCache(job.inputdata.get_contents, job.inputdata.dataset)
        contents_temp = cache_get_contents(overlap=False, size=True)

        contents = {}
        datasetLength = {}
        datasetFilesize = {}
        allfiles = 0
        allsizes = 0
        for dataset, content in contents_temp.iteritems():
            contents[dataset] = content
            datasetLength[dataset] = len(contents[dataset])
            allfiles += datasetLength[dataset]
            datasetFilesize[dataset] = reduce(operator.add, map(lambda x: x[1][1],content))
            allsizes += datasetFilesize[dataset]
            logger.info('Dataset %s contains %d files in %d bytes'%(dataset,datasetLength[dataset],datasetFilesize[dataset]))
        logger.info('Total num files=%d, total file size=%d bytes'%(allfiles,allsizes))

        siteinfos = {}
        allcontents = {}
        for dataset, content in contents.iteritems():
            content = dict(content)
            if self.use_lfc:
                logger.warning('Please be patient - scanning LFC catalogs ...')
                result = job.inputdata.get_replica_listing(dataset,SURL=False,complete=-1)
                siteinfo = lfc_siteinfo(result, allowed_sites)
            else:
                if self.update_siteindex:
                    udays = 2
                else:
                    udays = 10000
                if locations and locations.has_key(dataset):
                    cache_dq2_siteinfo = Caching.FunctionCache(dq2_siteinfo, dataset)
                    siteinfo = cache_dq2_siteinfo(dataset, allowed_sites, locations[dataset], udays)
                    #siteinfo = dq2_siteinfo( dataset, allowed_sites, locations[dataset], udays )
                else:
                    siteinfo = {}
            siteinfos[dataset]=siteinfo
            allcontents[dataset]=content

        logger.debug('siteinfos = %s', siteinfos)

        subjobs = []
        totalfiles = 0
        
        for dataset, siteinfo in siteinfos.iteritems():
            
            self.numfiles = orig_numfiles
            self.numsubjobs = orig_numsubjobs
            if self.numfiles <= 0: 
                self.numfiles = 1

            for sites, guids in siteinfo.iteritems():
                # at these sites process these guids belonging to dataset

                if self.numfiles <= 0: 
                    self.numfiles = 1

                allcontent = allcontents[dataset]

                # Fix bug 42044
                # drop unused guids
                removal = []
                for g in guids:
                    if not g in allcontent.keys():
                        logger.debug("Removing guid %s" % g)
                        removal += [g]

                for g in removal:
                    guids.remove(g)
                    
                nrfiles = self.numfiles
                nrjob = int(math.ceil(len(guids)/float(nrfiles)))
                if nrjob > self.numsubjobs and self.numsubjobs!=0:
                    nrfiles = int(math.ceil(len(guids)/float(self.numsubjobs)))
                    nrjob = int(math.ceil(len(guids)/float(nrfiles)))

                if nrjob > config['MaxJobsDQ2JobSplitter']:
                    nrfiles = int(math.ceil(len(guids)/float(config['MaxJobsDQ2JobSplitter'])))
                    nrjob = int(math.ceil(len(guids)/float(nrfiles)))

                if nrfiles > len(guids):
                    nrfiles = len(guids)

                max_subjob_filesize = 0
                # Restriction based on the maximum dataset filesize
                if self.filesize > 0:
                    max_subjob_filesize = self.filesize*1024*1024
                elif job.backend._name == 'NG' and (self.filesize < 1 or config['MaxFileSizeNGDQ2JobSplitter'] < self.filesize):
                    max_subjob_filesize = config['MaxFileSizeNGDQ2JobSplitter']*1024*1024
                elif job.backend._name == 'Panda' and (self.filesize < 1 or config['MaxFileSizePandaDQ2JobSplitter'] < self.filesize):
                    max_subjob_filesize = config['MaxFileSizePandaDQ2JobSplitter']*1024*1024

                # sort the guids by name order
                names = [allcontent[g][0] for g in guids]
                namesAndGuids = zip(names,guids)
                namesAndGuids.sort()
                names,guids = zip(*namesAndGuids)

                # now assign the files to subjobs
                max_subjob_numfiles = nrfiles
                if max_subjob_filesize:
                    logger.info('DQ2JobSplitter will attempt to create %d subjobs using %d files per subjob subject to a limit of %d Bytes per subjob.'%(nrjob,max_subjob_numfiles,max_subjob_filesize))
                else:
                    logger.info('DQ2JobSplitter will attempt to create %d subjobs using %d files per subjob.'%(nrjob,max_subjob_numfiles))
                    max_subjob_filesize = 1180591620717411303424 # 1 zettabyte
                remaining_guids = list(guids)
                while remaining_guids and len(subjobs)<config['MaxJobsDQ2JobSplitter']:
                    num_remaining_guids = len(remaining_guids)
                    j = Job()
                    j.name = job.name
                    j.inputdata = DQ2Dataset()
                    j.inputdata.dataset = dataset
                    j.inputdata.sizes = []
                    while remaining_guids and len(j.inputdata.guids)<max_subjob_numfiles and sum(j.inputdata.sizes)<max_subjob_filesize:
                        for next_guid in remaining_guids:
                            if sum(j.inputdata.sizes)+allcontent[next_guid][1] < max_subjob_filesize:
                                remaining_guids.remove(next_guid)
                                j.inputdata.guids.append(next_guid)
                                j.inputdata.names.append(allcontent[next_guid][0])
                                j.inputdata.sizes.append(allcontent[next_guid][1])
                                break
                        else:
                            break
                        
                    j.inputdata.number_of_files = len(j.inputdata.guids)
                    if num_remaining_guids == len(remaining_guids):
                        logger.warning('Filesize constraint blocked the assignment of %d files having guids: %s'%(len(remaining_guids),remaining_guids))
                        break
                    #print j.inputdata.names
                    #print j.inputdata.sizes
                    #print sum(j.inputdata.sizes)
                    j.outputdata    = job.outputdata
                    j.application   = job.application
                    j.backend       = job.backend
                    if j.backend._name == 'LCG':
                        j.backend.requirements.sites = sites.split(':')
                    j.inputsandbox  = job.inputsandbox
                    j.outputsandbox = job.outputsandbox 

                    j.inputdata.tag_info = {}
                    if job.inputdata.tag_info:
                        if grid_tag:
                            for tag_file in j.inputdata.names:
                                j.inputdata.tag_info[tag_file] = job.inputdata.tag_info[tag_file]

                        if local_tag:
                            for tag_file in job.inputdata.tag_info:
                                if job.inputdata.tag_info[tag_file]['refs'][0][0] in j.inputdata.names:
                                    j.inputdata.tag_info[tag_file]  = job.inputdata.tag_info[tag_file]
                    
                    subjobs.append(j)

                    totalfiles = totalfiles + len(j.inputdata.guids) 


        if not subjobs:
            logger.error('DQ2JobSplitter did not produce any subjobs! Either the dataset is not present in the cloud or at the site or all chosen sites are black-listed for the moment.')

        logger.info('Total files assigned to subjobs is %d'%totalfiles)
        if not totalfiles == allfiles:
            logger.error('DQ2JobSplitter was only able to assign %s out of %s files to the subjobs ! Please check your job configuration if this is intended and possibly change to a different cloud or choose different sites!', totalfiles, allfiles)

        return subjobs
    
config = getConfig('Athena')
config.addOption('MaxJobsDQ2JobSplitter', 1000, 'Maximum number of allowed subjobs of DQ2JobSplitter')
config.addOption('MaxFileSizeNGDQ2JobSplitter', 14336, 'Maximum total sum of filesizes per subjob of DQ2JobSplitter at the NG backend (in MB)')
config.addOption('MaxFileSizePandaDQ2JobSplitter', 14336, 'Maximum total sum of filesizes per subjob of DQ2JobSplitter at the Panda backend (in MB)')
config.addOption('AllowedSitesNGDQ2JobSplitter', [ 'NDGF-T1_DATADISK', 'NDGF-T1_MCDISK', 'NDGF-T1_PRODDISK', 'NDGF-T1_SCRATCHDISK' ], 'Allowed space tokens/sites for DQ2JobSplitter on NG backend' )
