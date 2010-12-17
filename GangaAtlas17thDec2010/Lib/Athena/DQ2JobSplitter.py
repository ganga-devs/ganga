###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DQ2JobSplitter.py,v 1.41 2009-07-27 13:03:24 mslater Exp $
###############################################################################
# Athena DQ2JobSplitter

import math, socket, operator, copy, os

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

def appendFile(file_path, archive_path):
    file_name = os.path.split(file_path)[1]
    test_area = os.environ['TestArea']
    
    #uncompress
    cmd = "gzip -d %s" % archive_path
    out = commands.getoutput(cmd)
    archive = archive_path.replace(".tar.gz", ".tar")
    
    #create tar ball from file list
    cmd = "cd %s ; cat %s|xargs tar cf event_pick.tar" % (test_area, file_name) 
    #print " Tar cmd %s " %cmd
    out = commands.getoutput(cmd)

    #append
    cmd = "cd %s ; tar Af %s event_pick.tar" % (test_area, archive) 
    #print "Append cmd %s " %cmd
    out = commands.getoutput(cmd)

    #compress
    cmd = "gzip %s " %(archive)
    out = commands.getoutput(cmd)
    
    #remove file
    files = ''
    for line in open(file_path, 'r').readlines():
        files += "%s " % line.strip('\n')

    cmd = " cd %s ; rm event_pick.tar %s; rm %s " %(test_area, file_name, files)
    #print "Delete cmd %s " %cmd
    out = commands.getoutput(cmd)

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
    _schema = Schema(Version(1,1), {
        'numfiles'          : SimpleItem(defvalue=0,doc='Number of files per subjob'),
        'numsubjobs'        : SimpleItem(defvalue=0,sequence=0, doc="Number of subjobs"),
        'use_lfc'           : SimpleItem(defvalue = False, doc = 'Use LFC catalog instead of default site catalog/tracker service'),
        'update_siteindex'  : SimpleItem(defvalue = True, doc = 'Update siteindex during job submission to get the latest file location distribution.'),
        'use_blacklist'     : SimpleItem(defvalue = True, doc = 'Use black list of sites create by GangaRobot functional tests.'),
        'filesize'          : SimpleItem(defvalue=0, doc = 'Maximum filesize sum per subjob im MB.'),
        'numevtsperjob'     : SimpleItem(defvalue=0, doc='Number of events per subjob'),
        'numevtsperfile'    : SimpleItem(defvalue=0,doc='Maximum number of events in a file of input dataset'),
        'missing_files'     : SimpleItem(defvalue=[],typelist=['str'],sequence=1,protected=1,doc='List of names that could not be assigned to a subjob')
    })

    _GUIPrefs = [ { 'attribute' : 'numfiles',         'widget' : 'Int' },
                  { 'attribute' : 'numsubjobs',       'widget' : 'Int' },
                  { 'attribute' : 'use_lfc',          'widget' : 'Bool' },
                  { 'attribute' : 'update_siteindex', 'widget' : 'Bool' },
                  { 'attribute' : 'use_blacklist',    'widget' : 'Bool' },
                  { 'attribute' : 'filesize',         'widget' : 'Int' },
                  { 'attribute' : 'numevtsperjob',       'widget' : 'Int' },
                  { 'attribute' : 'numevtsperfile',       'widget' : 'Int' }
                  ]


    def split(self,job):

        logger.debug('DQ2JobSplitter called')

        if job.inputdata._name <> 'DQ2Dataset'  and job.inputdata._name <> 'AMIDataset' and job.inputdata._name <> 'EventPicking':
            raise ApplicationConfigurationError(None,'DQ2 Job Splitter requires a DQ2Dataset or AMIDataset or EventPicking as input')

        if not job.backend._name in [ 'LCG', 'CREAM', 'Panda', 'NG' ] and not ( job.backend._name in ['SGE'] and config['ENABLE_SGE_DQ2JOBSPLITTER'] ):
            raise ApplicationConfigurationError(None,'DQ2JobSplitter requires an LCG, CREAM, Panda or NG backend')
        
        if (self.numevtsperjob <= 0 and self.numfiles <=0 and self.numsubjobs <=0):
            raise ApplicationConfigurationError(None,"Specify one of the parameters of DQ2JobSplitter for job splitting: numsubjobs, numfiles, numevtsperjob")
 
        if (self.numevtsperjob > 0 and job.inputdata._name <> 'AMIDataset'):
            raise ApplicationConfigurationError(None,"Event based splitting is supported only for AMIDataset as input dataset type")
        # split options are mutually exclusive
        if ( (self.numfiles > 0 or self.numsubjobs > 0) and self.numevtsperjob > 0):
            raise ApplicationConfigurationError(None,"Split by files (or subjobs) and events can not be defined simultaneously")

        if (job.application.max_events > 0 and self.numevtsperjob > 0):
            raise ApplicationConfigurationError(None,"Split by maximum events and split by events can not be defined simultaneously")
        

        #AMIDataset
        if job.inputdata._name == 'AMIDataset':
            job.inputdata.dataset = job.inputdata.search()

        #EventPicking
        if job.inputdata._name == 'EventPicking':
            guid_run_evt_map = job.inputdata.get_pick_dataset()

        # before we do anything, check for tag info for this dataset
        additional_datasets = {}
        local_tag = False
        grid_tag = False
        if job.inputdata.tag_info:

            # check for conflicts with TAG_LOCAL or TAG_COPY
            if job.inputdata.type in ['TAG_LOCAL', 'TAG_COPY']:
                raise ApplicationConfigurationError(None, "Cannot provide both tag_info and run as '%s'. Please use one or the other!" % job.inputdata.type)
            
            logger.warning('TAG information present - overwriting previous DQ2Dataset definitions')

            job.inputdata.names = []
            job.inputdata.dataset = []

            # check if FILE_STAGER is used
            if job.inputdata.type == 'FILE_STAGER':
                logger.warning("TAG jobs currently can't use the FILE_STAGER. Switching to DQ2_COPY instead.")
                job.inputdata.type = 'DQ2_COPY'
                
            
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
                    if not job.inputdata.tag_info[tag_file]['refs'][0][1] in job.inputdata.dataset:
                        job.inputdata.dataset.append(job.inputdata.tag_info[tag_file]['refs'][0][1])      

                    ## add the referenced files and guids and check for multiple datasets per file
                    ref_dataset = job.inputdata.tag_info[tag_file]['refs'][0][1]
                    for ref in job.inputdata.tag_info[tag_file]['refs']:
                        if ref[1] != ref_dataset:
                            raise ApplicationConfigurationError(None,'Problems with TAG entry for %s. Multiple datasets referenced for local TAG file.' % tag_file)

                        job.inputdata.names.append( ref[0] )
                        job.inputdata.guids.append( ref[2] )

                else:
                    raise ApplicationConfigurationError(None,'Problems with TAG entry for %s' % tag_file)
                
            if grid_tag and local_tag:
                raise ApplicationConfigurationError(None,'Problems with TAG info - both grid and local TAG files selected.')
                    
                    
        # now carry on as before
        orig_numfiles = self.numfiles
        orig_numsubjobs = self.numsubjobs

        if self.numfiles <= 0: 
            self.numfiles = 1
        
        dset_names = str(job.inputdata.dataset)
        cache_get_locations = Caching.FunctionCache(job.inputdata.get_locations, dset_names)
        locations = cache_get_locations(overlap=False)
        
        allowed_sites = []
        if job.backend._name in [ 'LCG', 'CREAM' ]:
            if job.backend.requirements._name in [ 'AtlasLCGRequirements', 'AtlasCREAMRequirements']:
                if job.backend.requirements.sites:
                    allowed_sites = job.backend.requirements.sites
                elif job.backend.requirements.cloud:

                    if job.backend.requirements.cloud == 'ALL' and not job.backend.requirements.sites and job.outputdata and job.outputdata._name == 'DQ2OutputDataset':
                        logger.warning('DQ2OutputDataset being used with \'ALL\' cloud option. Attempting to find suitable sites - this may take some time...')
                        allowed_sites = job.backend.requirements.list_sites()
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
                if hasattr(job.application,"atlas_dbrelease") and job.application.atlas_dbrelease:
                    if job.application.atlas_dbrelease == 'LATEST':       
                        from pandatools import Client
                        my_atlas_dbrelease = Client.getLatestDBRelease(False)
                        job.application.atlas_dbrelease = my_atlas_dbrelease
                        
                        match = re.search('DBRelease-(.*)\.tar\.gz', my_atlas_dbrelease )
                        if match:
                            dbvers = match.group(1)
                            job.application.atlas_environment+=['DBRELEASE_OVERRIDE=%s'%dbvers] 

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

                    # Update allowed_sites to contain all possible spacetokens of a site
                    dq2alternatenames = []
                    allowed_sites_all = []
                    for site in allowed_sites:
                        alternatename = TiersOfATLAS.getSiteProperty(site,'alternateName')
                        if not alternatename in dq2alternatenames:
                            dq2alternatenames.append(alternatename)
                        else:
                            continue
                        for sitename in TiersOfATLAS.getAllSources():
                            if TiersOfATLAS.getSiteProperty(sitename,'alternateName') == alternatename and not sitename in allowed_sites_all: 
                                allowed_sites_all.append(sitename)

                    allowed_sites = allowed_sites_all

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
            allowed_sites = job.backend.list_ddm_sites()
        elif job.backend._name == 'NG':
            allowed_sites = config['AllowedSitesNGDQ2JobSplitter']
        elif job.backend._name == 'SGE':
            from dq2.clientapi.DQ2 import DQ2
            from dq2.info import TiersOfATLAS
            allowed_sites = TiersOfATLAS.getAllSources()
            
        if not allowed_sites:
            raise ApplicationConfigurationError(None,'DQ2JobSplitter found no allowed_sites for dataset')
        
        logger.debug('allowed_sites = %s ', allowed_sites)

        cache_get_contents = Caching.FunctionCache(job.inputdata.get_contents, dset_names)
        contents_temp = cache_get_contents(overlap=False, size=True)

        if self.numevtsperjob > 0:
            contents_temp = cache_get_contents(overlap=False, event=True)
        else:
            contents_temp = cache_get_contents(overlap=False, size=True)

        contents = {}
        datasetLength = {}
        datasetFilesize = {}
        nevents = {}
        allfiles = 0
        allsizes = 0
        allevents = 0
        allnames = []
        for dataset, content in contents_temp.iteritems():
            contents[dataset] = content
            datasetLength[dataset] = len(contents[dataset])
            allfiles += datasetLength[dataset]
            datasetFilesize[dataset] = reduce(operator.add, map(lambda x: x[1][1],content))
            allsizes += datasetFilesize[dataset]
            allnames += map(lambda x: x[1][0],content)
            if self.numevtsperjob > 0:
                nevents[dataset] = reduce(operator.add, map(lambda x: x[1][2],content))
                allevents += nevents[dataset]
                logger.info('Dataset %s contains %d events in %d files '%(dataset, nevents[dataset], datasetLength[dataset]))
            else:
                logger.info('Dataset %s contains %d files in %d bytes'%(dataset,datasetLength[dataset],datasetFilesize[dataset]))
        logger.info('Total num files=%d, total file size=%d bytes'%(allfiles,allsizes))

        # to a check for the 'ALL' cloud option and if given, reduce the selection
        if hasattr(job.backend, 'requirements') and hasattr(job.backend.requirements, 'cloud') and job.backend.requirements.cloud == 'ALL' and not job.backend.requirements.sites and job.outputdata and job.outputdata._name == 'DQ2OutputDataset':

            if not job.backend.requirements.anyCloud:
            
                logger.warning('DQ2OutputDataset being used with \'ALL\' cloud option. Restricting to a single cloud. Set the \'requirements.anyCloud\' to True to prevent this restriction.')

                avail_clouds = {}
                for key in locations:
                    avail_clouds[key] = []

                    new_locations = []
                    for loc in locations[key]:
                        if loc in allowed_sites:
                            new_locations.append(loc)
                        
                    info = job.backend.requirements.cloud_from_sites(new_locations)
                
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
                    raise ApplicationConfigurationError(None, 'Cloud option \'ALL\' could not find a complete replica of the dataset in any cloud. Please try a specific site or cloud.')
                else:
                    cloud_list = list(cloud_set)
                    if not fav_cloud in cloud_list:
                        fav_cloud = cloud_list[0]

                    logger.warning('\'%s\' cloud selected. Continuing job submission...' % fav_cloud)

            new_allowed_sites = []
            for site in allowed_sites:
                if not job.backend.requirements.anyCloud:
                    if job.backend.requirements.cloud_from_sites( site )[site] == fav_cloud:
                        new_allowed_sites.append(site)
                elif not job.backend.requirements.cloud_from_sites( site )[site] in ['US', 'NG']:
                    new_allowed_sites.append(site)

            allowed_sites = new_allowed_sites
                

        siteinfos = {}
        allcontents = {}
        if job.inputdata._name == 'EventPicking' and job.backend._name == 'Panda':
            if (job.inputdata.pick_filter_policy == 'reject'):
                raise ApplicationConfigurationError(None,"Pick event filter policy 'reject' not supported on Panda backend.")
            # create a file containing list of files
            test_area = os.environ['TestArea']
            eventPickFileList = '%s/epFileList_%s.dat' % (test_area, commands.getoutput('uuidgen'))
            evFileList = open(eventPickFileList,'w') 

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
                else:
                    siteinfo = {}
            siteinfos[dataset]=siteinfo
            allcontents[dataset]=content

        logger.debug('siteinfos = %s', siteinfos)
        logger.debug('allcontents = %s', allcontents)

        subjobs = []
        totalfiles = 0
        events_processed = 0
        
        #print "%10s %20s %10s %10s %10s %10s %10s %10s %10s "  %("nrjob", "guid", "nevents", "skip_events", "max_events", "unused_evts", "id_lower", "id_upper", "counter")

        for dataset, siteinfo in siteinfos.iteritems():

            self.numfiles = orig_numfiles
            self.numsubjobs = orig_numsubjobs
            if self.numfiles <= 0: 
                self.numfiles = 1

            for sites, guids in siteinfo.iteritems():
                # at these sites process these guids belonging to dataset
                
                counter = 0 ; id_lower = 0;  id_upper = 0; tmpid = 0
                unused_events = 0; nskip =0; second_loop =0; nevtstoskip = 0;  
                totalevent = 0; used_events = 0; first_loop = 0
                left_events = 0; first_iteration = False; last_loop = False; num_of_events = 0;
                dset_size = 0; nsubjob = 0

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

                if len(guids) == 0:
                    continue

                max_subjob_filesize = 0
                # do a Panda brokerage for these sites
                pandaSite = ''
                if job.backend._name == 'Panda':
                    from GangaPanda.Lib.Panda.Panda import selectPandaSite
                    pandaSite = selectPandaSite(job,sites)
                    # set maximum input size per subjob 
                    from pandatools import Client
                    max_subjob_filesize = (Client.PandaSites[pandaSite]['maxinputsize']-1000)*1024*1024
                    if not max_subjob_filesize:
                        max_subjob_filesize = config['MaxFileSizePandaDQ2JobSplitter']*1024*1024

                    # direct access site ?
                    from pandatools import PsubUtils
                    # FIXME - set correct parameter values
                    inBS = False
                    inTRF = False
                    inARA = False
                    isDirectAccess = PsubUtils.isDirectAccess(pandaSite, inBS, inTRF, inARA)
                    if isDirectAccess: 
                       max_subjob_filesize = 0
                       # use numfiles if user has set it, else use MaxSubjobFilesPandaDQ2JobSplitter
                       if not orig_numsubjobs>0 and not orig_numfiles>0:
                           self.numfiles = config['DefaultNumFilesPandaDirectDQ2JobSplitter']
                           self.numsubjobs = 0
                           logger.info('DQ2JobSplitter has restricted the maximum of files per subjob to %s files.', self.numfiles)
                       else:
                           if orig_numsubjobs>0:
                               nrfiles = int(math.ceil(len(guids)/float(orig_numsubjobs)))
                               if nrfiles > config['DefaultNumFilesPandaDirectDQ2JobSplitter']:
                                   self.numfiles = config['DefaultNumFilesPandaDirectDQ2JobSplitter']
                                   self.numsubjobs = 0
                           if orig_numfiles>config['DefaultNumFilesPandaDirectDQ2JobSplitter']:
                               self.numfiles = config['DefaultNumFilesPandaDirectDQ2JobSplitter']
                               self.numsubjobs = 0
                           logger.info('DQ2JobSplitter has restricted the maximum of files per subjob to %s files.', self.numfiles)

                # Restriction based on the maximum dataset filesize
                if self.filesize > 0:
                    max_subjob_filesize = self.filesize*1024*1024
                elif job.backend._name == 'NG' and (self.filesize < 1 or config['MaxFileSizeNGDQ2JobSplitter'] < self.filesize):
                    max_subjob_filesize = config['MaxFileSizeNGDQ2JobSplitter']*1024*1024
                elif job.backend._name == 'Panda':
                    if max_subjob_filesize > 0:
                    #max_subjob_filesize = config['MaxFileSizePandaDQ2JobSplitter']*1024*1024
                        logger.info('DQ2JobSplitter has restricted the maximum inputsize per subjob to %s Bytes.', max_subjob_filesize)
                    else:
                        max_subjob_filesize = 1180591620717411303424 # 1 zettabyte
                else:
                    max_subjob_filesize = 1180591620717411303424 # 1 zettabyte

                nrfiles = self.numfiles
                nrjob = int(math.ceil(len(guids)/float(nrfiles)))
                if nrjob > self.numsubjobs and self.numsubjobs!=0:
                    nrfiles = int(math.ceil(len(guids)/float(self.numsubjobs)))
                    nrjob = int(math.ceil(len(guids)/float(nrfiles)))
                elif self.numevtsperjob > 0:
                    for g in guids:
                        totalevent += allcontent[g][2]
                        dset_size += allcontent[g][1]
                    nrjob = int(math.ceil(totalevent/float(self.numevtsperjob)))
                   
                    # Restriction based on the maximum dataset filesize
                    filesize_per_event = dset_size/totalevent 
                    filesize_per_subjob = filesize_per_event * self.numevtsperjob
                    if filesize_per_subjob > max_subjob_filesize :
                        events_per_subjob = max_subjob_filesize/filesize_per_event 
                        self.numevtsperjob = events_per_subjob
                        nrjob = int(math.ceil(totalevent/float(events_per_subjob)))

                if job.inputdata.tag_info and local_tag and nrjob > len(job.inputdata.tag_info):
                    nrfiles = int(math.ceil(len(guids)/float(len(job.inputdata.tag_info))))
                    nrjob = len(job.inputdata.tag_info)

                if nrjob > config['MaxJobsDQ2JobSplitter']: 
                    if self.numevtsperjob > 0:
                        self.numevtsperjob = int(math.ceil(totalevent/float((config['MaxJobsDQ2JobSplitter'] -1))))
                        nrjob = int(math.ceil(totalevent/float(self.numevtsperjob)))
                    else:
                        logger.warning('!!! Number of subjobs %s is larger than maximum allowed of %s - reducing to %s !!!', nrjob, config['MaxJobsDQ2JobSplitter'], config['MaxJobsDQ2JobSplitter'] )
                        nrfiles = int(math.ceil(len(guids)/float(config['MaxJobsDQ2JobSplitter'])))
                        nrjob = int(math.ceil(len(guids)/float(nrfiles)))
                        
                # Restrict number of subjobs is compilation is used
                if job.backend._name in ['LCG', 'CREAM' ] and job.application.athena_compile==True and  nrjob > config['MaxJobsDQ2JobSplitterLCGCompile']: 
                    logger.error('!!! The number of allowed subjobs on the %s backend and with athena_compile=True is %s, but DQ2JobSplitter is trying to split into %s subjobs !!!', job.backend._name, config['MaxJobsDQ2JobSplitterLCGCompile'], nrjob )
                    logger.error('!!! Please pre-compile the code locally and use athena_compile=False in a new job submission if more subjobs are required !!!' )
                    raise ApplicationConfigurationError(None,'!!! Submission stopped !!!')

                if nrfiles > len(guids):
                    nrfiles = len(guids)

                # sort the guids by name order
                names = [allcontent[g][0] for g in guids]
                namesAndGuids = zip(names,guids)
                namesAndGuids.sort()
                names,guids = zip(*namesAndGuids)

                # now assign the files to subjobs
                max_subjob_numfiles = nrfiles
                if self.numevtsperjob > 0:
                    logger.info('DQ2JobSplitter will attempt to create %d subjobs using  %d events per subjob subject to a limit of %d Bytes per subjob.' %(nrjob,self.numevtsperjob, max_subjob_filesize))
                elif max_subjob_filesize and  self.filesize > 0:
                    logger.info('DQ2JobSplitter will attempt to create %d subjobs using %d files per subjob subject to a limit of %d Bytes per subjob.'%(nrjob,max_subjob_numfiles,max_subjob_filesize))
                remaining_guids = list(guids)

                # sort out the tag files that reference this if required
                if job.inputdata.tag_info and local_tag:
                    
                    remaining_tags = []   # c.f. remaining guids: list of tag files that ref. this dataset

                    for tag_file in job.inputdata.tag_info:

                        # check all guids are referenced
                        num = 0                            
                        for ref in job.inputdata.tag_info[tag_file]['refs']:                
                            if ref[2] in remaining_guids:
                                num += 1

                        if num > 0 and num != len(job.inputdata.tag_info[tag_file]['refs']):
                            for ref in job.inputdata.tag_info[tag_file]['refs']:
                                if ref[2] in remaining_guids:
                                    remaining_guids.remove(ref[2])
                            logger.warning('Not all guids referenced by TAG file %s were found in available sites - ignoring this file.' % tag_file)
                        elif num > 0:
                            remaining_tags.append( tag_file )
                            
                while remaining_guids and len(subjobs)<config['MaxJobsDQ2JobSplitter']:
                    num_remaining_guids = len(remaining_guids)
                    j = Job()
                    j.name = job.name
                    j.inputdata = job.inputdata
                    j.inputdata.dataset = dataset
                    j.inputdata.sizes = []
                    j.inputdata.guids = []
                    j.inputdata.names = []
                    j.application   = job.application
                    j.application.run_event   = []

                    if self.numevtsperjob > 0:
                        if  unused_events == 0:
                            nevtstoskip = 0
                            id_lower = id_upper
                        else:
                            previous_guid = guids[id_lower] 
                            nevtstoskip = allcontent[previous_guid][2]- left_events
                        
                        #add events: first loop 
                        while (remaining_guids and not last_loop)  and  self.numevtsperjob > unused_events:
                            next_guid = guids[counter]
                            unused_events += allcontent[next_guid][2]
                            if counter < len(guids):
                                id_upper +=1
                            if counter < (len(guids) -1) :
                                remaining_guids.remove(next_guid)
                            if counter == (len(guids) -1) :
                                last_loop = True
                            counter += 1
                            first_loop = 1
                        
                        #use events: second loop
                        while (self.numevtsperjob <= unused_events) or last_loop:
                            if nsubjob == 0:
                                nevtstoskip = 0
                            elif first_loop != 1:
                                previous_guid = guids[id_lower] 
                                nevtstoskip = allcontent[previous_guid][2]- unused_events
                            first_loop = 0
                            if (nsubjob != nrjob -1):
                                unused_events -= self.numevtsperjob
                                left_events = unused_events
                            second_loop = 1
                            num_of_events = self.numevtsperjob
                            if ( nsubjob == nrjob - 1 ):
                                last_loop = False
                                last_guid = guids[counter - 1]
                                remaining_guids.remove(last_guid)
                                num_of_events = unused_events
                                unused_events = 0
                            break
                        
                        j.inputdata.guids = list(guids[id_lower:id_upper])
                        j.inputdata.names = [allcontent[g][0] for g in j.inputdata.guids]
                        j.inputdata.sizes = [allcontent[g][1] for g in j.inputdata.guids]
                        j.application.skip_events = nevtstoskip
                        j.application.max_events = num_of_events
                        events_processed += num_of_events
                        nsubjob += 1
                        
                        """
                        print "%10s %20s %10s %10s %10s %10s %10s %10s %10s "  %("nrjob", "guid", "nevents", "skip_events", "max_events", "unused_evts", "id_lower", "id_upper", "counter")
                        print "\n%10s %20s %10s %10s %10s %10s %10s %10s %10s "  %( nsubjob, (j.inputdata.names[0])[-18:], allcontent[j.inputdata.guids[0]][2] - j.application.skip_events, j.application.skip_events, j.application.max_events, unused_events, id_lower, id_upper, counter - 1)
                        
                        kcnt = 0
                        kevent = 0
                        for guid in j.inputdata.guids:
                            if (kcnt != 0):
                                print "%10s %20s %10s"  %("", allcontent[guid][0][-18:], allcontent[guid][2]) 
                                kevent += allcontent[guid][2]
                            else:
                                kevent += allcontent[guid][2] - j.application.skip_events
                            kcnt +=1
                        if len(j.inputdata.guids) > 1:
                            print "%10s %20s %10s" %("", "total_events", kevent)

                        """

                        #Remove previously added files from the subjobs         
                        id_lower = id_upper -1
                    
                    else:
                        # change the splitting based on local tag files
                        if job.inputdata.tag_info and local_tag:
                            while remaining_tags and len(j.inputdata.guids)<max_subjob_numfiles and sum(j.inputdata.sizes)<max_subjob_filesize:
                                for next_tag in remaining_tags:
                                    
                                    remaining_tags.remove(next_tag)
                                    
                                    for ref in job.inputdata.tag_info[next_tag]['refs']:
                                        if ref[2] in remaining_guids:
                                            remaining_guids.remove(ref[2])
                                        else:
                                            logger.warning("Multiple TAG files referenced GUID %s - this could lead to unexpected results!" % ref[2])
                                                                                                
                                        j.inputdata.guids.append(ref[2])
                                        j.inputdata.names.append(allcontent[ref[2]][0])
                                        j.inputdata.sizes.append(allcontent[ref[2]][1])

                                    break
                            
                                else:
                                    break
                        else:
                            # default splitting
                            while remaining_guids and len(j.inputdata.guids)<max_subjob_numfiles and sum(j.inputdata.sizes)<max_subjob_filesize:
                                for next_guid in remaining_guids:
                                    if sum(j.inputdata.sizes)+allcontent[next_guid][1] < max_subjob_filesize:
                                        remaining_guids.remove(next_guid)
                                        j.inputdata.guids.append(next_guid)
                                        j.inputdata.names.append(allcontent[next_guid][0])
                                        j.inputdata.sizes.append(allcontent[next_guid][1])
                                        if job.inputdata._name != 'EventPicking':
                                            break
                                    
                                    if job.inputdata._name == 'EventPicking':
                                        for runevent in guid_run_evt_map[next_guid]:
                                            revt = "(%s,%s)" %(long(runevent[0]),long(runevent[1]))     
                                            j.application.run_event.append([long(runevent[0]),long(runevent[1])])
                                        
                                        if job.backend._name == 'Panda':
                                            app = job.application
                                            # copy run/event list in a file
                                            eventPickFile = '%s/%s/ep_%s.dat' % (test_area,app.atlas_run_dir,commands.getoutput('uuidgen'))
                                            evFH = open(eventPickFile,'w') 
                                            tlist = j.application.run_event
                                            j.application.run_event_file = os.path.basename(eventPickFile)

                                            for k in range(len(tlist)):
                                                evFH.write('%s %s\n' % (tlist[k][0], tlist[k][1]))
                                            # close        
                                            evFH.close()
                                            #write in file 
                                            evFileList.write('%s%s\n' %(app.atlas_run_dir, os.path.basename(eventPickFile)))

                                else:
                                    break

                    
                    j.inputdata.number_of_files = len(j.inputdata.guids)
                    if (self.numevtsperjob == 0):
                        if not (job.inputdata.tag_info and local_tag) and num_remaining_guids == len(remaining_guids):
                            logger.warning('Filesize constraint blocked the assignment of %d files having guids: %s'%(len(remaining_guids),remaining_guids))
                            break
                    #print j.inputdata.names
                    #print j.inputdata.sizes
                    #print sum(j.inputdata.sizes)
                    j.outputdata    = job.outputdata
                    j.backend       = job.backend
                    if j.backend._name in [ 'LCG', 'CREAM']:
                        j.backend.requirements.sites = sites.split(':')
                    if j.backend._name == 'Panda':
                        j.backend.site=pandaSite
                    if j.backend._name == 'SGE':
                        for site in sites.split(':'):
                            if site.startswith('DESY-HH'):
                                j.backend.extraopts+=' -l site=hh '
                                break
                            elif site.startswith('DESY-ZN'):
                                j.backend.extraopts+=' -l site=zn '
                                break

                    j.inputsandbox  = job.inputsandbox
                    j.outputsandbox = job.outputsandbox 

                    # job inputdata type
                    #j.inputdata.type = job.inputdata.type

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

        
        if job.inputdata._name == 'EventPicking' and job.backend._name == 'Panda':
            evFileList.close()
            #append list of files from picking to source archive
            appendFile(eventPickFileList, job.application.user_area.name)

        if not subjobs:
            logger.error('DQ2JobSplitter did not produce any subjobs! Either the dataset is not present in the cloud or at the site or all chosen sites are black-listed for the moment.')
        
        if self.numevtsperjob > 0:
            logger.info('Total events assigned to subjobs is %d'%events_processed)
            if not (allevents == events_processed) :
                logger.error('DQ2JobSplitter was only able to process %s out of %s events to the subjobs ! Please check your job configuration if this is intended and possibly change to a different cloud or choose different sites!' %(events_processed, allevents) )
        else:
            logger.info('Total files assigned to subjobs is %d'%totalfiles)
            if not (totalfiles == allfiles):
                logger.error('DQ2JobSplitter was only able to assign %s out of %s files to the subjobs ! Please check your job configuration if this is intended and possibly change to a different cloud or choose different sites!', totalfiles, allfiles)
                
                self.missing_files = []
                for name in allnames:
                    found=False
                    for sj in subjobs:
                        if name in sj.inputdata.names:
                            found=True
                            break
                    if not found:
                        self.missing_files.append(name)
                logger.info('The files not assigned to jobs have been stored in job.splitter.missing_files')

        return subjobs
    
config = getConfig('Athena')
config.addOption('MaxJobsDQ2JobSplitter', 5000, 'Maximum number of allowed subjobs of DQ2JobSplitter')
config.addOption('MaxFilesPandaDQ2JobSplitter', 5000, 'Maximum number of allowed subjobs of DQ2JobSplitter')
config.addOption('MaxJobsDQ2JobSplitterLCGCompile', 500, 'Maximum number of allowed subjobs of DQ2JobSplitter on LCG/Cream with compile option switched on')
config.addOption('MaxFileSizeNGDQ2JobSplitter', 14336, 'Maximum total sum of filesizes per subjob of DQ2JobSplitter at the NG backend (in MB)')
config.addOption('MaxFileSizePandaDQ2JobSplitter', 13336, 'Maximum total sum of filesizes per subjob of DQ2JobSplitter at the Panda backend (in MB)')
config.addOption('DefaultNumFilesPandaDirectDQ2JobSplitter', 50, 'Default number of input files per subjob used at a direct access site in Panda in DQ2JobSplitter')
config.addOption('AllowedSitesNGDQ2JobSplitter', [ 'NDGF-T1_DATADISK', 'NDGF-T1_MCDISK', 'NDGF-T1_PRODDISK', 'NDGF-T1_SCRATCHDISK' ], 'Allowed space tokens/sites for DQ2JobSplitter on NG backend' )
