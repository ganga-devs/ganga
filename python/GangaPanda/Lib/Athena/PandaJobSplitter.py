import math, socket, exceptions

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Schema import *

from Ganga.Utility.logging import getLogger

from GangaAtlas.Lib.ATLASDataset import DQ2Dataset
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import *
from Ganga.Utility.Config import getConfig, makeConfig, ConfigError

logger = getLogger()

from GangaAtlas.Lib.Athena.DQ2JobSplitter import dq2_siteinfo
import Client

def queueToAllowedSites(queue):
    ddm = Client.PandaSites[queue]['ddm']
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
                    print '%s has alternateName %s'%(ddm,alternate_names)
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
    return allowed_sites

class PandaJobSplitter(ISplitter):
    '''Dataset driven job brokerage and splitting for the Panda backend'''

    _name = 'PandaJobSplitter'
    _schema = Schema(Version(1,0), {
        'numfiles' : SimpleItem(defvalue=0,doc='Number of files per subjob'),
        'numsubjobs': SimpleItem(defvalue=0,sequence=0, doc="Number of subjobs")
    })

    _GUIPrefs = [ { 'attribute' : 'numfiles',        'widget' : 'Int' },
                  { 'attribute' : 'numsubjobs',      'widget' : 'Int' },
                  ]


    def split(self,job):

        logger.debug('PandaJobSplitter called')

        if job.inputdata._name <> 'DQ2Dataset':
            raise ApplicationConfigurationError(None,'PandaJobSplitter requires inputdata to be a DQ2Dataset')

        if job.backend._name <> 'Panda':
            raise ApplicationConfigurationError(None,'PandaJobSplitter requires the backend to be Panda')

        if self.numfiles > 1 and self.numsubjobs > 1:
            raise ApplicationConfigurationError(None,'At least one of numfiles and numsubjobs must be 0')

        if self.numfiles <= 0: 
            self.numfiles = 1

        # Do brokering

        # get locations when site==AUTO
        fileList = []
        if job.backend.site == "AUTO":
            try:
                fileList  = Client.queryFilesInDataset(job.inputdata.dataset[0],False)
            except exceptions.SystemExit:
                raise ApplicationConfigurationError(None,'Error in Client.queryFilesInDataset')
            try:
                dsLocationMap = Client.getLocations(job.inputdata.dataset[0],fileList,job.backend.cloud,False,False,expCloud=True)
            except exceptions.SystemExit:
                raise ApplicationConfigurationError(None,'Error in Client.getLocations')
            # no location
            if dsLocationMap == {}:
                raise ApplicationConfigurationError(None,"ERROR : could not find supported locations in the %s cloud for %s" % (job.backend.cloud,job.inputdata.dataset[0]))
            # run brorage
            tmpSites = []
            for tmpItem in dsLocationMap.values():
                tmpSites += tmpItem
            try:
                status,out = Client.runBrokerage(tmpSites,'Atlas-%s' % job.application.atlas_release,verbose=False)
            except exceptions.SystemExit:
                raise ApplicationConfigurationError(None,'Error in Client.runBrokerage')
            if status != 0:
                raise ApplicationConfigurationError(None,'failed to run brokerage for automatic assignment: %s' % out)
            if not Client.PandaSites.has_key(out):
                raise ApplicationConfigurationError(None,'brokerage gave wrong PandaSiteID:%s' % out)
            # set site
            site = out
        
        # patch for BNL
        if site == "ANALY_BNL":
            site = "ANALY_BNL_ATLAS_1"
        # long queue
        if job.backend.long:
            site = re.sub('ANALY_','ANALY_LONG_',site)
            site = re.sub('_\d+$','',site)

        job.backend.site = site
        job.backend.actualCE = site
        # correct the cloud in case site was not AUTO
        job.backend.cloud = Client.PandaSites[job.backend.site]['cloud']
        cloud = job.backend.cloud
        logger.warning('Panda runBrokerage results: cloud %s, site %s'%(job.backend.cloud,job.backend.site))

        # Do splitting
 
        # translate site (analysis queue) to storage elements
        allowed_sites = queueToAllowedSites(job.backend.site)

        # lookup contents at sites
        contents = dict(job.inputdata.get_contents(overlap=False))
        locations = job.inputdata.get_locations(overlap=False)
        siteinfos = {}
        allcontent = {}
        for dataset, content in contents.iteritems():
            content = dict(content)
            allcontent.update(content)
            siteinfo = dq2_siteinfo(dataset, allowed_sites, locations[dataset])
            siteinfos.update(siteinfo)
        logger.warning('Input DS has %d files in total.'%len(allcontent))

        # get the SE holding the data
        se = siteinfos.keys()[0]
        guids = siteinfos[se]       
        logger.warning('Input DS has %d files at %s'%(len(guids),se))
        if len(allcontent) > len(guids):
            logger.warning('%s has incomplete copy of the input DS'%se)

        nrjob = int(math.ceil(len(guids)/float(self.numfiles)))
        if nrjob > self.numsubjobs and self.numsubjobs!=0:
            self.numfiles = int(math.ceil(len(guids)/float(self.numsubjobs)))
        if nrjob > 2000:
            self.numfiles = int(math.ceil(len(guids)/float(2000)))
        nrjob = int(math.ceil(len(guids)/float(self.numfiles)))
        
        subjobs = []
        for i in xrange(0,nrjob):
            j = Job()
            j.inputdata       = job.inputdata
            j.inputdata.guids = guids[i*self.numfiles:(i+1)*self.numfiles]
            j.inputdata.names = [allcontent[guid] for guid in j.inputdata.guids ]
            j.inputdata.number_of_files = len(j.inputdata.guids)
            j.outputdata    = job.outputdata
            j.application   = job.application
            j.backend       = job.backend
            j.inputsandbox  = job.inputsandbox
            j.outputsandbox = job.outputsandbox 
            subjobs.append(j)
                
        return subjobs
