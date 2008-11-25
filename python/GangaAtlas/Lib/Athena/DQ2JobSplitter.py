###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DQ2JobSplitter.py,v 1.14 2008-11-25 20:24:43 elmsheus Exp $
###############################################################################
# Athena DQ2JobSplitter

import math, socket

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
    })

    _GUIPrefs = [ { 'attribute' : 'numfiles',        'widget' : 'Int' },
                  { 'attribute' : 'numsubjobs',      'widget' : 'Int' },
                  { 'attribute' : 'use_lfc',         'widget' : 'Bool' }
                  ]


    def split(self,job):

        logger.debug('DQ2JobSplitter called')

        if job.inputdata._name <> 'DQ2Dataset':
            raise ApplicationConfigurationError(None,'DQ2 Job Splitter requires a DQ2Dataset as input')

        if job.backend._name <> 'LCG' and job.backend._name <> 'Panda':
            raise ApplicationConfigurationError(None,'DQ2JobSplitter requires an LCG or Panda backend')

        if self.numfiles <= 0: 
            self.numfiles = 1

        allowed_sites = []
        if job.backend._name == 'LCG':
            if job.backend.requirements._name == 'AtlasLCGRequirements':
                if job.backend.requirements.cloud:
                    allowed_sites = job.backend.requirements.list_sites_cloud()
                elif job.backend.requirements.sites:
                    allowed_sites = job.backend.requirements.sites
                else: 
                    raise ApplicationConfigurationError(None,'DQ2JobSplitter requires a cloud or a site to be set - please use the --cloud option, j.backend.requirements.cloud=CLOUDNAME (T0, IT, ES, FR, UK, DE, NL, TW, CA, US, NG) or j.backend.requirements.sites=SITENAME')
                #allowed_sites = job.backend.requirements.list_sites(True,True)
        elif job.backend._name == 'Panda':
            from GangaPanda.Lib.Panda.Panda import runPandaBrokerage,queueToAllowedSites
            runPandaBrokerage(job)
            allowed_sites = queueToAllowedSites(job.backend.site)

        if not allowed_sites:
            raise ApplicationConfigurationError(None,'DQ2JobSplitter found no allowed_sites for dataset')

        contents = dict(job.inputdata.get_contents(overlap=False))

        locations = job.inputdata.get_locations(overlap=False)

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
                siteinfo = dq2_siteinfo( dataset, allowed_sites, locations[dataset], udays )
            siteinfos[dataset]=siteinfo
            allcontents[dataset]=content
        
        subjobs = []
        for dataset, siteinfo in siteinfos.iteritems():
            if self.numfiles <= 0: 
                self.numfiles = 1

            if siteinfo.keys():
                sites = siteinfo.keys()[0]
            else:
                sites = []
            if siteinfo.values():
                guids = siteinfo.values()[0]
            else:
                guids = []

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

            nrjob = int(math.ceil(len(guids)/float(self.numfiles)))
            if nrjob > self.numsubjobs and self.numsubjobs!=0:
                self.numfiles = int(math.ceil(len(guids)/float(self.numsubjobs)))
                nrjob = int(math.ceil(len(guids)/float(self.numfiles)))
            
            if nrjob > config['MaxJobsDQ2JobSplitter']:
                self.numfiles = int(math.ceil(len(guids)/float(config['MaxJobsDQ2JobSplitter'])))
                nrjob = int(math.ceil(len(guids)/float(self.numfiles)))
        
            if self.numfiles > len(guids):
                self.numfiles = len(guids)

            for i in xrange(0,nrjob):
          
                j = Job()

                j.inputdata       = job.inputdata
                j.inputdata.dataset = dataset
                j.inputdata.guids = guids[i*self.numfiles:(i+1)*self.numfiles]
                j.inputdata.names = [ allcontent[guid] for guid in j.inputdata.guids ]
                j.inputdata.number_of_files = len(j.inputdata.guids)
            
                j.outputdata    = job.outputdata
                j.application   = job.application
                j.backend       = job.backend
                if j.backend._name == 'LCG':
                    j.backend.requirements.sites = sites.split(':')
                j.inputsandbox  = job.inputsandbox
                j.outputsandbox = job.outputsandbox 

                subjobs.append(j)
                
        return subjobs
    
config = getConfig('Athena')
config.addOption('MaxJobsDQ2JobSplitter', 1000, 'Maximum number of allowed subjobs of DQ2JobSplitter')
