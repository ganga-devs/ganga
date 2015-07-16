###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AthenaMCDatasets.py,v 1.47 2009-07-14 09:11:16 fbrochu Exp $
###############################################################################
# A DQ2 dataset

import sys, os, re, urllib, commands, imp, threading,random

from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.Utility.files import expandfilename

from dq2.common.DQException import *
from dq2.info.TiersOfATLAS import _refreshToACache, ToACache, getSites, getLocalCatalog, whichCloud,getRemoteCatalogs

from dq2.repository.DQRepositoryException import DQUnknownDatasetException
from dq2.location.DQLocationException import DQLocationExistsException
from dq2.common.DQException import DQInvalidRequestException
from dq2.common.client.DQClientException import DQInternalServerException
from dq2.content.DQContentException import DQFileExistsInDatasetException

from Ganga.GPIDev.Credentials import GridProxy

from Ganga.Utility.GridShell import getShell

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname

_refreshToACache()
gridshell = getShell("EDG")
_writeTokens=["SCRATCHDISK","LOCALGROUPDISK"] # have to include LOCALGROUPDISK because direct subscription does not work.


_subscriptionTokens=[]

def getLFCmap():
    lfcstrings,lfccloud={},{}
    for lfctag, sites in ToACache.catalogsTopology.iteritems():
        for site in sites:
            lfccloud[site]=lfctag
    for cloud, localids in ToACache.topology.iteritems():
        if cloud not in lfccloud.keys():
            #                print cloud,localids
            for loctag in localids:
                if loctag in ToACache.topology.keys():
                    for s in ToACache.topology[loctag]:
                        if s not in lfcstrings and s in lfccloud:
                            # including US lrcs...
                            # print site, lfccloud[site]
                            lfcstrings[s]=lfccloud[s]
    
            continue
        for loctag in localids:
            if loctag in ToACache.topology.keys():
                for s in ToACache.topology[loctag]:
                    lfcstrings[s]=lfccloud[cloud]
            else:
                lfcstrings[loctag]=lfccloud[cloud]
    return lfcstrings


def checkpath(path,prefix):
    if prefix=="castor":
        cmd="rfdir %s" % path
    else:
        cmd="ls %s" % path
    status,output=commands.getstatusoutput(cmd)
    return not status



def mapSitesToCloud():
    # create dictionnary cloud[site]
    clouds={}
    # clouds are mapped in dbcloud
    # invert map for convenience:
    cloud1={}
    for region,cloud in ToACache.dbcloud.iteritems():
        cloud1[cloud]=region
    # now convert cloud(s) into list of related sites, effectively mapping sites to regions.
    for cloud, localids in ToACache.topology.iteritems():
        if cloud in cloud1.keys():
            for loctag in localids:
                if loctag in ToACache.topology.keys(): # loctag is a subcloud like UKTIER2S, need to iterate again
                    for s in ToACache.topology[loctag]:
                        clouds[s]=cloud1[cloud]
                else:
                    clouds[loctag]=cloud1[cloud]
    logger.debug("got mapping regions to sites: %s" % str(clouds)) 
    return clouds

def extractFileNumber(filename):
    """ Returns the file number contained in the filename (file._00001.root),
        Returns None and prints a WARNING if unsuccessful"""
    if isinstance(filename,int):
        return filename
    if filename.find("DBRelease")>-1:
        return 
    if filename.find("._") != -1:
        num = filename.split("._")[-1].split(".")[0]
    else:
        num = filename.split("_")[-1].split(".")[0]
    try:
       return int(num)
    except ValueError:
       logger.warning("could not find partition number on %s" % filename)
       return

def matchFile(matchrange, filename):
    """ Returns True if the filename matches an entry in matchrange = (matchlist, openrange)
        or, if openrange == True, if it is above the last entry in matchlist in numbering.
        If (not matchrange) is true, then the matchrange matches all files. """
    if not matchrange:
        return True
    for match in matchrange[0]:
        if match in filename:
            return True
    if matchrange[1]: # in case of an open range
        lastnum = extractFileNumber(matchrange[0][-1])
        num = extractFileNumber(filename)
        if num > lastnum:
            return True
    return False

def expandList(partnumbers):
    openrange = 0
    if isinstance(partnumbers, list):
        return (partnumbers, False)
    protolist=string.split(partnumbers,",")
    result=[]
    for block in protolist:
        block=string.strip(block)
        if string.find(block,"-")>-1:
            [begin,end]=string.split(block,"-")
            begin=string.strip(begin)
            if begin=="":
                begin=1 # easy case for open ranges...
            end=string.strip(end)
            if end=="":
                logger.debug("Detected open range towards the end of dataset. Setting flag for future resolution")
                openrange=string.atoi(begin)
            if not begin.isdigit():
                logger.error("Non digit entered in partition list: %s. Invalid list, returning empty handed" %block)
                return ([],False)
            if end.isdigit():
                for i in range(string.atoi(begin),string.atoi(end)+1):
                    result.append(i)
        else:
            if not block.isdigit() :
                logger.error("Non digit entered in partition list: %s. Invalid list, returning empty handed" %block)
                return ([],False)
            result.append(string.atoi(block))
    result = dict([(i,1)for i in result]).keys() # make results unique
    result.sort()
    if not openrange:
       return (result, False)
    else:
       return ([r for r in result if r < openrange] + [openrange], True)

class AthenaMCInputDatasets(Dataset):
    '''AthenaMC Input Datasets class'''

    _schema = Schema(Version(2,0), {
        'DQ2dataset' : SimpleItem(defvalue = '', doc = 'DQ2 Dataset Name'),
        'LFCpath' : SimpleItem(defvalue = '', doc = 'LFC path of directory to find inputfiles on the grid, or local directory path for input datasets (datasetType=local). For all non-DQ2 datasets.'),
        'datasetType' : SimpleItem(defvalue = 'unknown', doc = 'Type of dataset(DQ2,private,unknown or local). DQ2 means the requested dataset is registered in DQ2 catalogs, private is for input datasets registered in a non-DQ2 storage (Tier3) and known to CERN local LFC. local is for local datasets on Local backend only'),
        'number_events_file' : SimpleItem(defvalue=1,sequence=0,doc='Number of "events" per input file. This is used together with application.number_events_job to calculate the number of input files per job, or the number of jobs to inputfiles, respectively. This replaces "n_infiles_job".'),
        'skip_files' : SimpleItem(defvalue=0,doc='File numbers to skip in the input dataset. This shifts the numbering of the output files: If skip_files = 10 the processing starts with output file number one at input file 11.'),
        'skip_events' : SimpleItem(defvalue=0,doc='Number of events to skip in the first input file (after skip_files). This shifts the numbering of the output files, so the output file number one will contain the first set of events after skip_files and skip_events.'),
        'use_partition_numbers'  : SimpleItem(defvalue=False,doc='If True, use the partition numbers "_00001" of the input files. This MUST be enabled if you work with input datasets that keep changing, since only then are you guaranteed not to process the same file twice. If your input dataset is frozen or you only process the dataset once, you can leave this on False.', typelist=["bool"]),
        'redefine_partitions'  : SimpleItem(defvalue ="",doc='FOR EXPERTS: Redefine the input partitions. There are three possibilities to specify the new input partitions: 1) String of input file numbers to be used (each block separated by a comma). A block can be a single number or a closed subrange (x-y). Subranges are defined with a dash. 2) List of input file numbers as integers 3) List of LFNs of input files as strings. This replaces the "inputfiles" property. To only process some events, it is recommended not to use "redefine_partitions" but rather use an AthenaMCSplitter and j.splitter.input_partitions.', typelist=["str","list"]),
        'cavern' : SimpleItem(defvalue = '', doc = 'Name of the dataset to be used for cavern noise (pileup jobs) or extra input dataset (other transforms). This dataset must be a DQ2 dataset'),
        'n_cavern_files_job': SimpleItem(defvalue =1,doc='Number of input cavern files processed by one job or subjob. Minimum 1'),
        'minbias' : SimpleItem(defvalue = '', doc = 'Name of the dataset to be used for minimum bias (pileup jobs) or extra input dataset (other transforms). This dataset must be a DQ2 dataset'),
        'n_minbias_files_job': SimpleItem(defvalue =1,doc='Number of input cavern files processed by one job or subjob. Minimum 1'),
    })

    _category = 'datasets'
    _name = 'AthenaMCInputDatasets'
    _exportmethods = [ 'get_dataset', 'get_cavern_dataset', 'get_minbias_dataset','get_DBRelease','matchSite','trimSites' ]
    _GUIPrefs= [ { 'attribute' : 'datasetType', 'widget' : 'String_Choice', 'choices' : ['DQ2','private','unknown','local']}]

    # content = [ ]
    # content_tag = [ ]
    redefined_partitions = None
    
    def __init__(self):
        super( AthenaMCInputDatasets, self ).__init__()
##         # Extract username from certificate
##         proxy = GridProxy()
##         username = proxy.identity()
##         if 'ALLOW_MISSING_NICKNAME_DQ2OUTPUTDATASET' in configDQ2 and configDQ2['ALLOW_MISSING_NICKNAME_DQ2OUTPUTDATASET']:
##             nickname = getNickname(allowMissingNickname=True)
##         else:
##             nickname = getNickname(allowMissingNickname=False)
            
##         if nickname and 'USE_NICKNAME_DQ2OUTPUTDATASET' in configDQ2 and configDQ2['USE_NICKNAME_DQ2OUTPUTDATASET']:
##             username = nickname
##         #self.initDQ2hashes()
##         #logger.debug(self.baseURLDQ2)

## additional class methods to support migration ################################
    def getMigrationClass(cls, version):
        """This class method returns a (stub) class compatible with the schema <version>.
        Alternatively, it may return a (stub) class with a schema more recent than schema <version>,
        but in this case the returned class must have "getMigrationClass" and "getMigrationObject"
        methods implemented, so that a chain of convertions can be applied."""
        return AthenaMCInputDatasetsMigration12
    getMigrationClass = classmethod(getMigrationClass)

    def getMigrationObject(cls, obj):
        """This method takes as input an object of the class returned by the "getMigrationClass" method,
        performs object transformation and returns migrated object of this class (cls)."""
        converted_obj = cls()
        for attr, item in converted_obj._schema.allItems():
            # specific convertion stuff
            if attr == 'option_file':
                setattr(converted_obj, attr, [getattr(obj, attr)]) # correction: []
            else:
                setattr(converted_obj, attr, getattr(obj, attr))
            return converted_obj
    getMigrationObject = classmethod(getMigrationObject)
## end of additional class methods to support migration ################################


    def numbersToMatcharray(self,partitions):
        """ Transform a list of input file numbers into a matcharray that can be used on a list of filenames.
            This is a trivial operation if the user did not redefine the input partitions via redefine_partitions """
        # redefine partitions if necessary 

        if self.redefine_partitions:
           if not self.redefined_partitions:
              self.redefined_partitions = expandList(self.redefine_partitions)
        else:
           self.redefined_partitions = []

        if not self.redefined_partitions:
            return ["_"+string.zfill(f, 5) for f in partitions]
        else:
            files = []
            for p in partitions:
                if p <= len(self.redefined_partitions[0]):
                    f = self.redefined_partitions[0][p-1]
                    if isinstance(f,int): # if f is an integer...
                        f = "_"+string.zfill(f, 5)
                    files.append(f)
                elif self.redefined_partitions[1]: # if the partitions redefined had an open range, continue after last element...
                    files.append("_"+string.zfill(p - len(self.redefined_partitions[0]) + self.redefined_partitions[0][-1], 5))
                else:
                    logger.error("Only %i input partitions defined in inputdata.redefine_partitions, but partition %i was requested!", len(self.redefined_partitions[0]), p)
                    raise Exception()
            return files

    def filesToNumbers(self,files):
        """ Transform a list of input file names into a list of input partition numbers.
            This is a trivial operation if the user did not redefine the input partitions via redefine_partitions """
        if self.redefine_partitions:
           if not self.redefined_partitions:
              self.redefined_partitions = expandList(self.redefine_partitions)
        else:
           self.redefined_partitions = []

        if not self.redefined_partitions:
            return [extractFileNumber(fn) for fn in files]
        else:
            num = []
            file_numbers = [extractFileNumber(fn) for fn in files]
            part_numbers = [extractFileNumber(fn) for fn in self.redefined_partitions[0]]
            numbers = []
            for i in file_numbers:
                 try:
                     numbers.append(1+part_numbers.index(i))
                 except ValueError:
                     if self.redefined_partitions[1] and part_numbers[-1] < i:
                         lastpart = part_numbers[-1]
                         lastnumber = len(part_numbers)
                         numbers.append(lastnumber + i - lastpart)
            return numbers

    def get_dataset(self, app,backend):
        '''seek dataset informations and returns (hopefully) a formatted set of information for all processing jobs (turls, catalog servers, dataset location for each lfn). Called by master_submit'''

        # first of all, check that _usertag is set properly:
        try:
            assert _usertag
        except:
            logger.error("config['DQ2']['usertag'] is not set properly! Please exit ganga, do: export GANGA_CONFIG_PATH=GangaAtlas/Atlas.ini and restart ganga")
            raise
        
        dataset=self.DQ2dataset
        path=self.LFCpath
        datasetType=self.datasetType
        # consistency checks first:
        if datasetType=="DQ2":
            try:
                assert dataset != ""
            except:
                logger.error("datasetType set to DQ2 but no DQ2 dataset declared...Aborting")
                raise
        elif datasetType=="local":
            try:
                assert path != ""
            except:
                logger.error("datasetType set to local but no local path declared in LFCpath. Aborting")
                raise
        
        try:
            assert app.number_events_job != 0
        except:
            logger.error("application.number_events_job is zero! Aborting.")
            raise

        if not dataset and not path:
            try:
                assert app.production_name
            except:
                logger.error("Input dataset must be set if production_name is not") 
                raise
            # set up default values: DQ2 dataset with automatic naming conventions
            if app.mode=='simul':
                dataset = "%s.%s.ganga.%s.evgen.EVNT" % (_usertag,username,app.production_name)
                if app.run_number and app.process_name and app.production_name:
                    dataset = "%s.%s.ganga.%s.%6.6d.%s.evgen.EVNT" % (_usertag,username,app.production_name,int(app.run_number),app.process_name)
                    
            elif app.mode=="recon":
                if app.transform_script=="csc_recoAOD_trf.py":
                   dataset = "%s.%s.ganga.%s.recon.ESD" % (_usertag,username,app.production_name)
                   if app.run_number and app.process_name and app.production_name:
                       dataset = "%s.%s.ganga.%s.%6.6d.%s.recon.ESD" % (_usertag,username,app.production_name,int(app.run_number),app.process_name)    
                else:
                    dataset = "%s.%s.ganga.%s.simul.RDO" % (_usertag,username,app.production_name)
                    if app.run_number and app.process_name and app.production_name:
                        dataset = "%s.%s.ganga.%s.%6.6d.%s.simul.RDO" % (_usertag,username,app.production_name,int(app.run_number),app.process_name)   
            if app.version:
                dataset+="."+str(app.version)
            datasetType="DQ2" # force datasetType to be DQ2 as this is the default mode.
        # extract app.production_name from input dataset if needed. Needed for output data processing
        if not app.production_name:
            #print dataset
            imin=string.find(dataset,"ganga.")
            #print imin, dataset[imin+6:imin+12]
            if imin>-1 and dataset[imin+6:imin+12].isdigit():
                imin=imin+6
            else:
                imin=string.find(dataset,".")
            match='.evgen.EVNT'
            if app.mode=='recon':
                if app.transform_script=="csc_recoAOD_trf.py":
                    match='.recon.ESD'
                else:
                    match='.simul.RDO'
            imax=string.find(dataset,match)
            if imin>-1 and imax>-1:
                app.production_name=dataset[imin+1:imax]
            #print "production name:",app.production_name
        # get tuple (list, openrange) of partitions to process 
        partitions = app.getPartitionList()
        inputfiles = app.getInputsForPartitions(partitions[0], self)
        matchrange = (self.numbersToMatcharray(inputfiles), partitions[1])
        logger.info("Matchrange: %s (open: %s)" % matchrange)
        if matchrange[1] or ( self.redefine_partitions=="" and backend != "Local"): # We must not limit dataset collection if we have an open range...
            matchrange = ([],True)

        
        new_backend = backend
        
        if (datasetType=="DQ2" or datasetType=="unknown") and dataset:
            logger.debug("looking for dataset in DQ2, input data is : %s %s" % (dataset,inputfiles))
            new_backend = self.getdq2data(dataset,matchrange,backend,update=True)
                
        if (datasetType=="private"  or datasetType=="unknown") and path != "":
            logger.debug("scanning CERN LFC for data in Tier 3, input data is : %s %s " % (path,inputfiles))
            self.getlfcdata(path,matchrange,"prod-lfc-atlas-local.cern.ch",backend)
            
        if datasetType=="local" and path != "":
            logger.warning("getting data from local source: %s %s  " % (path,str(matchrange)))
            self.getlocaldata(path,matchrange,backend)

        try:
            assert backend == new_backend
        except:
            logger.error("Dataset %s not found on backend %s. Please change the backend  to %s" % ( dataset, backend, new_backend))
            raise
        app.turls=self.turls
        app.lfcs=self.lfcs
        app.sites=self.sites
        # now getting the rest of the dataset block
        # cavern datasets
        logger.info("Signal dataset: %s done." % dataset)
        if self.cavern:
            self.get_cavern_dataset(app)
            logger.info("Cavern dataset: %s done" % self.cavern)
        # minbias datasets
        if self.minbias:
            self.get_minbias_dataset(app)
            logger.info("MinBias dataset: %s done" % self.minbias)
        # DB release if any
        if app.dbrelease:
            self.get_DBRelease(app)
            logger.info("DB Release done")
        # got everything. now need to trim down app.(minbias,cavern,db)sites to
        # match app.sites (at least sharing the same cloud?)
        
        return

    def matchSite(self,site,siteShortList):
        result=""
        imax=site.find("_")
        matchSite=site[:imax]
        # first, look for exact site match
        siteShorts=[]
        for spaceToken in siteShortList:
            imax=spaceToken.find("_")
            if imax>-1:
                siteShorts.append(spaceToken[:imax])
        #print matchSite, siteShorts
        if matchSite in siteShorts:
            return site
        # next try to see if they belong to the same cloud...
        # later perhaps?
        return result

        
    def trimSites(self,siteList,siteShortList):
        '''Reduce input siteList to loosely match the contents of siteShortList'''
        result=[]
        for site in siteList:
 #           print "finding match for", site, siteShortList
            selsite=self.matchSite(site,siteShortList)
            if selsite:
#                print "got match, keeping", selsite,siteShortList
                result.append(selsite)
        return result
    

    def get_cavern_dataset(self, app):
        '''seek dataset informations based on job.inputdata information and returns (hopefully) a formatted set of information for all processing jobs (turls, catalog servers, dataset location for each lfn). Called by master_submit'''

        job = app.getJobObject()
        dataset=self.cavern
        backend=job.backend._name
        
        backend = self.getdq2data(dataset,None,backend,update=False)
#        try:
#            assert backend == job.backend._name
#        except:
#            logger.error("Dataset %s not found on backend %s. Please change the backend  to %s" % ( dataset,job.backend._name,backend))
#            raise
        app.cavern_turls=self.turls
        app.cavern_lfcs=self.lfcs
        app.cavern_sites=self.trimSites(self.sites,app.sites)
        try:
            assert len(app.cavern_sites)>0
        except:
            logger.error("Could not find a site match between input dataset locations: %s and existing cavern dataset locations: %s. Aborting the job" % (app.sites,self.sites))
            raise       
        return 
    
    def get_minbias_dataset(self, app):
        '''seek dataset informations based on job.inputdata information and returns (hopefully) a formatted set of information for all processing jobs (turls, catalog servers, dataset location for each lfn). Called by master_submit'''

        job = app.getJobObject()
        dataset=self.minbias
        backend=job.backend._name
        
        backend = self.getdq2data(dataset,None,backend,update=False)
#        try:
#            assert backend == job.backend._name
#        except:
#            logger.error("Dataset %s not found on backend %s. Please change the backend  to %s" % ( dataset,job.backend._name,backend))
#            raise        
        app.minbias_turls=self.turls
        app.minbias_lfcs=self.lfcs
        app.minbias_sites=self.trimSites(self.sites,app.sites)
        try:
            assert len(app.minbias_sites)>0
        except:
            logger.error("Could not find a site match between input dataset locations: %s and existing minbias dataset locations: %s. Aborting the job" % (app.sites,self.sites))
            raise
        return 


    def get_DBRelease(self, app):
        '''Get macthing DBrelease dataset from DQ2 database and useful information like guid for downloads'''

        relary=string.split(app.dbrelease,".")
        release_string=""
        for bits in relary:
            release_string+=string.zfill(bits,2)
        dataset="ddo.000001.Atlas.Ideal.DBRelease.v"+release_string

        allturls={}
        try:
            dq2_lock.acquire()
            datasets = dq2.listDatasets('%s' % dataset)
        finally:
            dq2_lock.release()
        if len(datasets.values())==0:
            logger.error('Dataset %s is not defined in DQ2 database!',dataset)
            raise Exception()
        dsetlist=datasets.keys()

        dsetname=dsetlist[0]
        # got exact dataset name from DQ2 catalog, now getting the files:
        job = app.getJobObject()
        backend=job.backend._name
        backend = self.getdq2data(dsetname,None,backend,update=False)
#        try:
#            assert backend == job.backend._name
#        except:
#            logger.error("Dataset %s not found on backend %s. Please change the backend  to %s or subscribe the DB release dataset to the desired site" % ( dataset,job.backend._name,backend))
#            raise
        app.dbturls=self.turls
        app.dblfcs=self.lfcs
        app.dbsites=self.trimSites(self.sites,app.sites)
        try:
            assert len(app.dbsites)>0
        except:
            logger.error("Could not find a site match between input dataset locations: %s and DB release locations: %s. Aborting the job" % (app.sites,self.sites))
            raise
        return 

    def getdq2data(self,dataset,matchrange,backend,update):
        allturls={}
        self.turls={}
        self.lfcs={}
        self.sites=[]

        dsetname=""
        dsetmatch=dataset
        logger.debug( "matching input dataset: %s" % str(dsetmatch))
        try:
            dq2_lock.acquire()
            datasets = dq2.listDatasets(dsetmatch)
        finally:
            dq2_lock.release()
        logger.debug( "results of the match: %s" % str(datasets))
        
        if len(datasets.values())==0:
            logger.warning('could not find exact match for %s, attempting loose matching. This will take considerably longer...'% dataset)
            dsetmatch='%s*' % dataset
            if dataset[-1]=="/":
                dsetmatch='%s*' % dataset[:-1] # turning container name into dataset root for matching
            try:
                dq2_lock.acquire()
                datasets = dq2.listDatasets(dsetmatch)
            finally:
                dq2_lock.release()
            logger.debug( "results of the match: %s" % str(datasets))
        
        if len(datasets.values())==0:            
            logger.error('no Dataset matching %s is registered in DQ2 database! Aborting',dataset)
            raise Exception()

        dsetlist=datasets.keys()
        dsetlist.sort()
        dsetname=dsetlist[0]
        containers=[]
        container=""
        inputdsets=[]
        for dset in dsetlist:
            if string.find(dset,"/")>-1:
                containers.append(dset)
            else:
                inputdsets.append(dset)
        try:
            assert len(containers)<2
        except:
            logger.error("dataset search has returned more than one physics dataset: %s: Please refine" % str(containers))
            raise Exception()
        if len(containers)==1:
            container=containers[0]

        logger.debug("Selected dataset %s" % dsetname)    
        if update:
            self.DQ2dataset=dsetname # update job with result of dataset search
            self.datasetType="DQ2"

        # get list of files from dataset list
        contents_new = {}

        if len(inputdsets) == 0:
            inputdsets = containers
        for dset in inputdsets:
            try:
                dq2_lock.acquire()
                contents=dq2.listFilesInDataset(dset)
            finally:
                dq2_lock.release()
            if not contents:
                if dset.find("_sub")<0:
                    logger.warning("Empty DQ2 dataset %s" % dset)
                continue
            data = contents[0]
            for guid, info in data.iteritems():
                contents_new[guid]=info['lfn']
        contents=contents_new
#                contents = contents_new            
##        # get list of files in selected dataset.
##        try:
##            dq2_lock.acquire()
##            contents = dq2.listFilesInDataset(dsetname)
##        finally:
##            dq2_lock.release()
        # Convert 0.3 output to 0.2 style

##        if not contents:
##            logger.error("Empty DQ2 dataset %s." % dsetname)
##            raise Exception
##        contents = contents[0]
##        contents_new = {}
##        for guid, info in contents.iteritems():
##            contents_new[guid]=info['lfn']
##        contents = contents_new
        
        # sort lfns alphabetically, then get the largest partition number to close the openrange.
        all_lfns=contents.values()
        all_files = [(extractFileNumber(fn), fn) for fn in all_lfns]
        all_files.sort()
        logger.debug("All lfns: %s " % str(all_files))
        numbers = []
        inputdata=self.redefine_partitions
        if len(inputdata)==0 :
            inputdata=[]
        (inputlist,isPartRange)=expandList(inputdata)
        #print inputlist,isPartRange
        for guid, lfn in contents.iteritems():
            if matchrange and matchrange[0] and not matchFile(matchrange, lfn):
                continue
            num = extractFileNumber(lfn)
            if num and num in numbers and isPartRange: # extra protection to cover the case where extractFileNumber returns "". Must not be used if inputdata.redefine_partitions is a list of lfns instead of a list of numbers.
                logger.warning("In dataset %s there is more than one file with the number %i!" % (dsetname, int(num)))
                logger.warning("File '%s' ignored!" % (lfn))
                continue
            numbers.append(num)
            allturls[lfn]="guid:"+guid
         
        # now get associated lfcs... by getting list of host sites first...
        # problem here: the container has no associated locations, so one has to loop over the constituent datasets...
        locations={0:[],1:[]}
        dsetlist=[dsetname]
        if container:
            dsetlist=dq2.listDatasetsInContainer(container)
            
        for dset in dsetlist:
            try:
                dq2_lock.acquire()
                data = dq2.listDatasetReplicas(dset)
                locs=data.values()
                # Avoid crashes if empty datasets are in containers
                if (len(locs) == 0) or (len(locs[0]) < 2):
                    continue
                locations[0]+=locs[0][0]
                locations[1]+=locs[0][1]
            finally:
                dq2_lock.release()
        
        datasetType="complete"
        allSites=[]
        for site in locations[1]:
            if site not in allSites:
                allSites.append(site)

        if len(allSites)==0:
            # add "incomplete" sites only if there is no "complete" one
            for site in locations[0]:
                if site not in allSites:
                    allSites.append(site)
            datasetType="incomplete"
## F.M.B: listFileReplicas does not work on incomplete dataset. Commenting out the following as it is not going to work.
##            # promote site with most complete set
##            selSites=[]
##            max=0
##            for sources in allSites:
##                data=dq2.listFileReplicas(sources,dset)
##                if "found" not in data[0]:
##                    continue
##                if data[0]["found"]>max:
##                    logger.info("Found %d files in %s" % (data[0]["found"],sources))
##                    selSites.insert(0,sources)
##                    max=data[0]["found"]
##                else:
##                    selSites.append(sources)
##            allSites=selSites
        try:
            assert len(allSites)>0
        except:
            logger.error("dataset %s has no registered locations. Aborting" % dsetname)
            raise Exception()


                
        # using the site list allSites, map to clouds and reject forbidden sites depending on backend.
        USsites=getSites('USASITES')
        NGsites=getSites('NDGF')
        selectedSites=[]
        for site in allSites:
            if backend=="Panda":
                selectedSites.append(site)
                continue
            if backend=="LCG" and site not in USsites and site not in NGsites:
                selectedSites.append(site)
                continue
            # all other backends (should perhaps restrict to Local ones...)
            selectedSites.append(site)
            #if backend=="NG" and site in NGSites:
            #   selectedSites.append(site)
        if len(selectedSites)==0:
            logger.error("Dataset not registered in %s, aborting. Please subscribe your dataset from one of these sites %s to a target site from the %s grid or change your backend" % (backend,str(allSites),backend))
            raise Exception()

        allSites=selectedSites

        self.lfcs[dsetname]=self.getLFCs(allSites)
        
        self.sites=allSites # collecting all sites from now on, doing the selection externally.
                
        # Now filling up self.turls...
        self.turls=allturls # as easy as that....
        #logger.warning("final lfc list:%s" % str(self.lfcs))
        return backend
    
    def getlfcdata(self,path,matchrange,lfc,backend):
        if path[-1]=="/":
            path=path[:-1] # pruning path .
        # check that path does not already contain lfc. If yes, then reallocate path and lfc accordingly
        imin=string.find(path,"lfc:")
        if imin>-1:
            lfc=path[imin+4:]
            imax=string.find(lfc,":")
            path=lfc[imax+1:]
            lfc=lfc[:imax]
            
        status,output,m=gridshell.cmd1("lfc-ls %s:%s" % (lfc,path),allowed_exit=[0,255])
        if status !=0:
            logger.error("Error accessing LFC %s: %s" %  (lfc,status))
            raise Exception()
        # sort lfns alphabetically, then get the largest partition number to close the openrange.
        inputfiles=output.split()
        try:
            assert len(inputfiles)>0
        except AssertionError:
            logger.error("No input files found at specified location %s:%s. Giving up" % (lfc,path))
            raise Exception()
                
        numbers = []
        for fn in inputfiles:
            if not matchFile(matchrange, fn):
                continue
            status,turl,m=gridshell.cmd1("export LFC_HOST=%s; lcg-lg --vo atlas lfn:%s/%s" % (lfc,path,lfn),allowed_exit=[0,1,255])
            num = extractFileNumber(lfn)
            if status==0:
                if num and num in numbers:
                    logger.warning("In directory %s there is more than one file with the number %i!" % (path, num))
                    logger.warning("File '%s' ignored!" % (lfn))
                    continue
                numbers.append(num)
                self.turls[lfn]=turl
        self.lfcs[path]=lfc
            
    def getlocaldata(self,path,matchrange,backend):
        allturls={}
        self.turls={}
        self.lfcs={}
        self.sites=[]

        if backend not in ["LSF","Local","PBS","SGE"]:
            logger.error("Attempt to use a local file on a job due to be submitted remotely.")
            raise Exception()
        
        if path[-1]=="/":
            path=path[:-2]
        prefix="file"
        readcmd="ls"
        # castor case:
        if string.find(path,"castor")>-1:
            prefix="castor"
            readcmd="rfdir"

        if not checkpath(path,prefix):
            logger.error("Non existent input path %s" % path)
            raise Exception()


        output=commands.getoutput("%s %s" % (readcmd,path))
        # sort lfns alphabetically, then get the largest partition number to close the openrange.
        inputfiles=output.split()
        try:
            assert len(inputfiles)>0
        except AssertionError:
            logger.error("No input files found at specified location %s. Giving up" % path)
            raise
        all_files = [(extractFileNumber(fn), fn) for fn in inputfiles]
        all_files.sort()
        logger.warning("All lfns: %s " % str(all_files))
        inputfiles.sort()
                
        numbers = []
        for (num, fn) in all_files:
            print fn,matchrange,path
            if not matchFile(matchrange, fn) :
                continue
            fullpath=path+"/"+fn
            if checkpath(fullpath,prefix):
                num = extractFileNumber(fn)
                if num and num in numbers:
                    logger.warning("In directory %s there is more than one file with the number %i!" % (path, num))
                    logger.warning("File '%s' ignored!" % (fn))
                    continue
                numbers.append(num)
                self.turls[fn]="%s:%s/%s "% (prefix,path,fn)
        self.lfcs[path]=''
    def getLFCs(self,allSites):
        result=[]
        allLFCs={}
        for cl in ToACache.dbcloud:
            id=ToACache.dbcloud[cl]
            l = getLocalCatalog(id)
            r=  getRemoteCatalogs(id)
            if l:
                allLFCs[id]=l
            elif len(r)>0:
                allLFCs[id]=r[0]
            
#        print allLFCs
        for site in allSites:
            cloud=whichCloud(site)
 #           print cloud,site
            if cloud=="USASITES":
                continue # no lfc in US.
            lfc=allLFCs[cloud]
            if lfc not in result:
                result.append(lfc)        
        return result

    def whichCloud (site):
        from dq2.info.TiersOfATLASValidator import is_site
        is_site(site)
        for cloudID, eachCloud in ToACache.dbcloud.iteritems():
            sites = getSites(eachCloud)
            if site in sites:
                return cloudID
            
        info = { 'CERN' : 'TO', 'CNAF' : 'IT', 'PIC': 'ES', 'LYON': 'FR',
                     'RAL' : 'UK', 'FZK': 'DE', 'SARA' : 'NL', 'ASGC' : 'TW',
                     'TRIUMF' : 'CA', 'BNL' : 'US', 'NDGF' : 'NG' }
        for sitename, cloud in info.iteritems():
            if site == sitename:
                return cloud
                
        return None

class AthenaMCOutputDatasets(Dataset):
    """AthenaMC Output Dataset class """
    
    _schema = Schema(Version(2,0), {
        'outdirectory'     : SimpleItem(defvalue = '', doc='path of output directory tree for storage. Used for both LFC and physical file locations.'), 
        'output_dataset'   : SimpleItem(defvalue = '', doc = 'dataset suffix for combined output dataset. If set, it will collect all expected output files for the job. If not set, every output type (histo, HITS, EVGEN...) will have its own output dataset.'),
        'output_firstfile'   : SimpleItem(defvalue=1,doc='EXPERT: Number of first output file. The job processing the first partition will generate the file with the number output_firstfile, the second will generate output_firstfile+1, and so on...'),
        'logfile'          : SimpleItem(defvalue='',doc='file prefix and dataset suffix for logfiles.'),
        'outrootfiles'     : SimpleItem(defvalue={},typelist=["dict","str"], doc='file prefixes and dataset suffixes for other output root files. To set for example the evgen file prefix, type: j.outputdata.outrootfiles["EVNT"] = "file.prefix". The keys used are EVNT, HIST, HITS, RDO, ESD, AOD and NTUP. To reset a value to default, type "del j.outputdata.outrootfiles["EVNT"]. To disable creation of a file, type j.outputdata.outrootfiles["EVNT"] = "NONE"'),
        'expected_output'         : SimpleItem(defvalue = [], typelist=['list'], sequence = 1, protected=1,doc = 'List of output files expected to be produced by the job. Should not be visible nor modified by the user.'),
        'actual_output'         : SimpleItem(defvalue = [], typelist=['list'], sequence = 1, protected=1,doc = 'List of output files actually produced by the job followed by their locations. Should not be visible nor modified by the user.'),
         'store_datasets'        : SimpleItem(defvalue = [], typelist=['list'], sequence = 1, protected=1,doc = 'List of output datasets to be frozen once filled up. Should not be visible nor modified by the user.')
        
        })
    
    _category = 'datasets'
    _name = 'AthenaMCOutputDatasets'

    _exportmethods = [ 'prep_data', 'getDQ2Locations', 'getSEs', 'create_dataset','fill','retrieve','recover','validate' ]

    def __init__(self):
        super(AthenaMCOutputDatasets, self).__init__()
        #       self.initDQ2hashes()
        #       logger.debug(self.baseURLDQ2)
        #        self.baseURLDQ2 = 'http://atlddmpro.cern.ch:8000/dq2/'

## additional class methods to support migration ################################
    def getMigrationClass(cls, version):
        """This class method returns a (stub) class compatible with the schema <version>.
        Alternatively, it may return a (stub) class with a schema more recent than schema <version>,
        but in this case the returned class must have "getMigrationClass" and "getMigrationObject"
        methods implemented, so that a chain of convertions can be applied."""
        return AthenaMCOutputDatasetsMigration12
    getMigrationClass = classmethod(getMigrationClass)

    def getMigrationObject(cls, obj):
        """This method takes as input an object of the class returned by the "getMigrationClass" method,
        performs object transformation and returns migrated object of this class (cls)."""
        converted_obj = cls()
        for attr, item in converted_obj._schema.allItems():
            # specific convertion stuff
            if attr == 'option_file':
                setattr(converted_obj, attr, [getattr(obj, attr)]) # correction: []
            else:
                setattr(converted_obj, attr, getattr(obj, attr))
            return converted_obj
    getMigrationObject = classmethod(getMigrationObject)
## end of additional class methods to support migration ################################

    def prep_data(self,app):
        ''' generate output paths and file prefixes based on app and outputdata information. Generate corresponding entries in DQ2. '''
        fileprefixes,outputpaths=self.outrootfiles.copy(),{}
        # first of all, check that _usertag is set properly:
        try:
            assert _usertag 
        except:
            logger.error("config['DQ2']['usertag'] is not set properly! Please exit ganga, do: export GANGA_CONFIG_PATH=GangaAtlas/Atlas.ini and restart ganga")
            raise
        # The common prefix production.00042.physics.
        app_prefix = "%s" % app.production_name
        # backward compatibility
        if app.production_name and app.run_number and app.process_name:
            app_prefix = "%s.%6.6d.%s" % (app.production_name,int(app.run_number),app.process_name)

        if self.output_dataset and string.find(self.output_dataset,",")<0:
            app_prefix=self.output_dataset


        job=app._getParent()# prep_data called from master_prepare(), so it should be the master job
        jid=job.id
        # The Logfile must be set
        if not "LOG" in fileprefixes:
            fileprefixes["LOG"]="%s.%s.LOG" % (app_prefix,app.mode)

        # Add missing output file names.
        if app.mode == "evgen":
            if not "EVNT" in fileprefixes:
                fileprefixes["EVNT"] = app_prefix + ".evgen.EVNT"
        elif app.mode == "simul":
            if not "HITS" in fileprefixes:
                fileprefixes["HITS"] = app_prefix + ".simul.HITS"
            if not "RDO" in fileprefixes:
                fileprefixes["RDO"]  = app_prefix + ".simul.RDO"
        elif app.mode == "recon":
            if not "ESD" in fileprefixes:
                fileprefixes["ESD"]  = app_prefix + ".recon.ESD"
            if not "AOD" in fileprefixes:
                fileprefixes["AOD"]  = app_prefix + ".recon.AOD"
            #if not "NTUP" in fileprefixes: # rarely use, should be set on demand only
            #    fileprefixes["NTUP"] = app_prefix + ".recon.NTUP"

        if app.version:
            for key in fileprefixes.keys():
                fileprefixes[key]+="."+str(app.version)
                
        # now generating output paths.
        # 2) otherwise it is the conversion of outputdata.output_dataset
        # 3) finally, it is the conversion of pre-generated outputfiles.
        for type in fileprefixes.keys():
            # get dataset suffix now.
            protodataset="%s.%s.ganga.%s" % (_usertag,username,fileprefixes[type])
            suffix=self.get_dataset_suffix(protodataset,jid)
            outputpaths[type]="/%s/%s/ganga/%s.%s"% (_usertag,username,fileprefixes[type],suffix)
            try:
                assert len(outputpaths[type])<=133
            except:
                overflow=len(outputpaths[type])-133
                dsetstr=outputpaths[type][1:]
                dsetstr=string.replace(dsetstr,"/",".")
                #print dsetstr
                logger.error("dataset name: %s too long by %d characters. Please reduce the size of any of the following fields (if set): job.outputdata.output_dataset, job.application.production_name, job.application.process_name,job.application.version " % (dsetstr,overflow))
                raise Exception()
            
        return fileprefixes,outputpaths
        
    def getDQ2Locations(self,se_name):
        ''' Provides the triplet: LFC, site and srm path from input se'''
        lfcstrings=getLFCmap()
        outputlocation={}
        default_site="CERN-PROD_SCRATCHDISK"
        job=self._getParent()
        app=job.application
        
        # first of all, check that _usertag is set properly:
        try:
            assert _usertag 
        except:
            logger.error("config['DQ2']['usertag'] is not set properly! Please exit ganga, do: export GANGA_CONFIG_PATH=GangaAtlas/Atlas.ini and restart ganga")
            raise
        for site, desc in ToACache.sites.iteritems():
            try:
                outloc = desc['srm'].strip()
                imax=outloc.rfind(":")
                imin2=outloc.find("=")
                token=outloc.find("token:")
                if imin2>0 and token>=0:
                    # srmv2 site. Save token information as coded in ToA: token:SPACETOKEN+srm path, removing the junk bit between the port number (after the last : in the string) and the equal sign.
                    outputlocation[site]= outloc[:imax]+outloc[imin2+1:]
                else:
                    # srmv1 site
                    outputlocation[site]= outloc
            except KeyError:
                continue
            
        sites=lfcstrings.keys()
        if se_name not in sites:
            logger.warning("%s not found in DQ2 site list. This must be a Storage Element server name. If not, please kill the job as the output will not be saved anywhere." % se_name)
            if se_name != "none":
                return ["",se_name,""]
            else:
                return [lfcstrings[default_site],default_site,outputlocation[default_site]]
                
        else:
            selsite=se_name
            makeSites=[]
            forbiddenSE="true"
            if "LOCALGROUPDISK" in se_name:
                # check that the user's proxy is compatible with the cloud.
                print "LOCALGROUPDISK used for output staging, checking authorizations"
                siteList=self.checkSites(selsite)
                if len(siteList)==0:
                    forbiddenSE=="true"
            else:
                for token in _writeTokens:
                    if token in se_name:
                        forbiddenSE="false"
            #print se_name, forbiddenSE
            if forbiddenSE=="true" :
                #print app.sites
                selsite=""
                logger.warning("Detected forbidden output space token in input: %s. Changing to SCRATCHDISK" % se_name)
                imax=se_name.find("_")
                sitename=se_name[:imax]
                
                # build all possible alternative, check that they are in DQ2 site list.
                for token in _writeTokens:
                    makeSites.append(sitename+"_"+token)

                for site in makeSites:
                    if site in sites:
                        selsite=site
                        logger.info("Found alternative: %s" % selsite)
                        break
                if not selsite:
                    logger.error("Could not come up with alternative to %s" % se_name)
                    return ["","",""]
            imin=string.find(lfcstrings[selsite],"lfc://") # lfc:// notation from ToA
            if imin>-1:
                imax=string.rfind(lfcstrings[selsite],":/")
                return [lfcstrings[selsite][imin+6:imax],selsite,outputlocation[selsite]]
            else:
                return [lfcstrings[default_site],default_site,outputlocation[default_site]] # does not make sense yet to write data out of LCG as we are not supporting other grid backend yet.
            
        
    def makeLCGmatch(self,lfc,site):
        ''' reverse of the previous: forms LCG requirements line based on SEs matching either the provided site or lfc name.'''
        
        lfcstrings=getLFCmap()
        sites=lfcstrings.keys()
        cloud=mapSitesToCloud()
        result=""
        if site and site != "none":
            if site not in sites:
                # "site" must be a SE name...
                logger.debug("%s not found in DQ2 site list. Must be a private production" % site)
                return 'anyMatch(other.storage.closeSEs,target.GlueSEUniqueID=="%s")' % site
            else:
                result= ' Member("VO-atlas-cloud-%s",other.GlueHostApplicationSoftwareRunTimeEnvironment)' % cloud[site]
                return result
                
        else:
            # invert lfc map to get sites
            for site in lfcstrings.keys():
                if string.find(lfcstrings[site],lfc)>-1 :
                    result=' Member("VO-atlas-cloud-%s",other.GlueHostApplicationSoftwareRunTimeEnvironment)' % cloud[site]
                    return result
        return result
        


    def checkDataset(self,dataset):
        """check that dataset is not already frozen, and if yes, then create a new instance with an extra suffix (vX) """
        # stupidity check
        dsexisted="true"
        try:
            dq2_lock.acquire()
            dsetlist = dq2.listDatasets('%s' % dataset)
            if len(dsetlist)==0:
                dsexisted="false"
                logger.info("Dataset %s does not exist, creating it" % dataset)
                logger.info("Opening new dataset: %s" % dataset)
                dq2.registerNewDataset(dataset)
        finally:
            dq2_lock.release()
        if dsexisted=="false":
            return dataset
        
        try:
            dq2_lock.acquire()
            frozen=dq2.getMetaDataAttribute(dataset,["frozendate"])
        finally:
            dq2_lock.release()
        if frozen.values()[0]=="None":
            logger.info("Dataset %s is still open, using this for file and location registration" % dataset)
            return dataset
        else:
            logger.warning("Dataset %s is already frozen, will create alternative" % dataset)
            
        # now generating the vX suffix.
        dsetlist=[]
        try:
            dq2_lock.acquire()
            dsetlist = dq2.listDatasets('%sv*' % dataset)
        finally:
            dq2_lock.release()            
        for dset in dsetlist:
            try:
                dq2_lock.acquire()
                frozen=dq2.getMetaDataAttribute(dset,["frozendate"])
            finally:
                dq2_lock.release()
            if frozen.values()[0]=="None":
                logger.warning("Dataset %s is still open, using this for file and location registration" % dset)
                return dset
        # if we are still there, then we need to create another opus...
        suffix="v"+str(len(dsetlist)+1)
        newdataset=dataset+suffix
        logger.info("Opening new dataset: %s" % newdataset)
        try:
            dq2_lock.acquire()
            dq2.registerNewDataset(newdataset)
        finally:
            dq2_lock.release()             
        return newdataset
        
    def get_dataset_suffix(self, dataset,jobid):
        """generate final output dataset name using dataset and jobid. Returns the created suffix"""
        # first, create the jid suffix. First part is the job id, zfill to make it 6 digits.

        suffix="jid"+string.zfill(jobid,6)
        #print "job id retrieved is %s",suffix
        datasetname=dataset+"."+suffix
        
        # then the timestamp: timestamp will be vX, where X is the  number of already existing subdatasets with the same jid
        dsetlist=[]
        try:
            dq2_lock.acquire()
            dsetlist = dq2.listDatasets('%s' % datasetname)
        finally:
            dq2_lock.release()
        if len(dsetlist)>0:
            # need to work out exactly the number of subdatasets:
            try:
                dq2_lock.acquire()
                dsetlist = dq2.listDatasets('%s*' % datasetname)
            finally:
                dq2_lock.release()
            
            suffix+="v%d" % len(dsetlist)
        datasetname=dataset+"."+suffix
        #print"final dataset name is",datasetname

        return suffix
    
    def register_dataset_location(self, datasetname, siteID):
        """Register location of dataset into DQ2 database"""
        
        try:
            dq2_lock.acquire()
            content = dq2.listDatasets(datasetname)
        finally:
            dq2_lock.release()

        if content=={}:
            logger.error('Dataset %s is not defined in DQ2 database !',datasetname)
            return

        locations={}
        try:
            dq2_lock.acquire()
            locations=dq2.listDatasetReplicas(datasetname)
        finally:
            dq2_lock.release()

        try:
            dq2_lock.acquire()
            datasetinfo = dq2.listDatasets(datasetname)
        finally:
            dq2_lock.release()
        datasetvuid = datasetinfo[datasetname]['vuids'][0]
    
        sitelist=[]
        if locations:
            sitelist=locations[datasetvuid][1] + locations[datasetvuid][0]

        logger.debug("%s %s" % (siteID,str(sitelist)))
        if siteID not in sitelist:
            try:
                dq2_lock.acquire()
                dq2.registerDatasetLocation(datasetname, siteID)
            finally:
                dq2_lock.release()

        return

    def register_file_in_dataset(self,datasetname,lfn,guid, size, checksum):
        """Add file to dataset into DQ2 database"""
                # Check if dataset really exists

        try:
            dq2_lock.acquire()
            content = dq2.listDatasets(datasetname)
        finally:
            dq2_lock.release()

        if content=={}:
            logger.error('Dataset %s is not defined in DQ2 database !',datasetname)
            return
        # Add file to DQ2 dataset
        ret = []
        try:
            dq2_lock.acquire()
            try:
                ret = dq2.registerFilesInDataset(datasetname, lfn, guid, size, checksum ) 
            except (DQFileExistsInDatasetException, DQInvalidRequestException):
                logger.debug('Warning, some files already in dataset')
                pass
        finally:
            dq2_lock.release()

        return 

    def register_datasets_details(self,outdata):
        reglines=[]
        datasets=[]
        for line in outdata:
            try:
                [dataset,lfn,guid,size,md5sum,siteID]=line.split(",")
            except ValueError:
                logger.warning("Missing data information from job, skipping: %s" % line)
                continue
            size = long(size)
            #            md5sum = 'md5:'+md5sum
            adler32='ad:'+md5sum
            if len(md5sum)==32:
                adler32='md5:'+md5sum
            try:
                assert len(md5sum)<=32
            except:
                logger.warning("Wrong checksum information, skipping registration %s"%  md5sum)
                continue
            siteID=siteID.strip() # remove \n from last component
            # Must check whether or not the dataset is frozen. If this is the case (job resubmission), then one should create a new one with a vX suffix, and use this new dataset to store the data.
            dataset=self.checkDataset(dataset)
            datasets.append(dataset)
            regline=dataset+","+siteID
            if regline in reglines:
                logger.warning("Registration of %s in %s already done, skipping" % (dataset,siteID))
                #continue
            else:
                reglines.append(regline)
                logger.debug("Registering dataset %s in %s" % (dataset,siteID))
                self.actual_output.append("%s %s" % (lfn,siteID))
                self.register_dataset_location(dataset,siteID)
            self.register_file_in_dataset(dataset,[lfn],[guid], [size],[adler32])
        return datasets
    def recover(self,freeze="no"):
        from Ganga.GPIDev.Lib.Job import Job
        from GangaAtlas.Lib.ATLASDataset import filecheck
        job = self._getParent()
        if job.master:
            logger.error("recover() can only be called from the master job")
            return
        try:
            assert job.backend._name=="LCG"
        except:
            logger.error("recover() only works for LCG() jobs")
            return
            
        ntot=len(job.subjobs)
        for sj in job.subjobs:
            if sj.status=='failed' or sj.status=='completing':
                logger.warning("Recovering output data from subjob %i" % sj.id)
                sj.outputdata.fill()
                continue
            if sj.status!='completed' :
                logger.warning("subjob %i is not finished yet" % sj.id)
                freeze="no"
                continue
                
        if freeze=="yes" or not job.subjobs:
            if job.subjobs:
                logger.warning("Now freezing output datasets")
            else:
                logger.warning("Recovering output data ")
            self.fill()

        # now time for a summary
        dsets=self.store_datasets
        contents_new = {}

        for dset in dsets:
            try:
                dq2_lock.acquire()
                nfiles=dq2.getNumberOfFiles(dset)
            finally:
                dq2_lock.release()
            print "got %d out of %d files for dataset %s" % (nfiles,ntot,dset)
            if freeze=="yes":
                logger.warning("Dataset %s is now frozen and cannot be updated further. Next attempt to run recover() will fail." % dset)
            else:
                logger.info("Dataset %s is still open as some subjobs have not completed." % dset)
        return
        
    def fill(self, type=None, name=None, **options ):
        """Determine outputdata and outputsandbox locations of finished jobs
        and fill output variable"""
        from Ganga.GPIDev.Lib.Job import Job
        from GangaAtlas.Lib.ATLASDataset import filecheck
        job = self._getParent()
##        jid=job.id
##        if job.master:
##            jid=job.master.id
#        print "JOB ID",jid
        pfn = job.outputdir + "output_data"
        fsize = filecheck(pfn)
        outdata=""
        # first check grid proxy validity:
        proxy = GridProxy()
        timeleft=proxy.timeleft().split(":")
        totaltime=string.atoi(timeleft[0])*3600+string.atoi(timeleft[1])*60+string.atoi(timeleft[2])
        maxtime=600
        njobs=1
        if job.master:
            masterjob=job.master
            njobs=masterjob.splitter.numsubjobs
        elif job.splitter:
            njobs=job.splitter.numsubjobs
        maxtime*=njobs
        if maxtime>80000:
            maxtime=80000
        try:
            assert totaltime>maxtime
        except:
            logger.error("Grid proxy lifetime not enough to allow dq2 registration. Aborting. Please refresh proxy and run job.outputdata.recover() ")
            raise Exception()
        
        if fsize and fsize != -1:
            logger.debug("file size %d" % fsize)
            f=open(pfn)
            outdata=f.readlines() 
            f.close()
            
        logger.debug("outdata: %s" % outdata)
        # Register all files and dataset location into DQ2
        if outdata: # outdata is only filled by subjobs...
#            self.store_datasets=self.register_datasets_details(outdata)
            self.store_datasets=self.register_datasets_details(outdata)
            logger.debug("output job contents: %s" % str(self.actual_output))
            logger.debug("expected: %s" % str(self.expected_output))

            # compare actual_output and rest of members of AthenaMCOutputDatasets (declared/expected)
            # first, extract list of lfns from actual_output
            expected_lfns=self.expected_output[:] # forces deep copy. Otherwise, we just copy a pointer and self.expected_output is emptied which is not what we want...
                
            for line in self.actual_output:
                lfn,dest=string.split(line)
                # must strip lfn from final numbers (timestamp and job) for comparison!
                imax=string.rfind(lfn,".")
                imax=string.rfind(lfn[:imax],".")
                lfn=lfn[:imax]
                if lfn in expected_lfns:
                    expected_lfns.remove(lfn)
            for missing in expected_lfns:
                logger.warning("Missing output file: %s" % missing)
            
        # closing a job: freeze the jid dataset, create container if needed and add to container.
        if not job.master:
            # loop over list of datasets from self.register_datasets_details
            logger.info("finalizing master job: %s" % str(job.id))
            for subjob in job.subjobs:
                dsets=subjob.outputdata.store_datasets
                for dset in dsets:
                    if dset not in self.store_datasets:
                        self.store_datasets.append(dset)
            logger.info("list of datasets: %s" % str(self.store_datasets))
            logger.warning("Master job is now completed, but created datasets are still opened and left on a temporary space token.")
            logger.warning("Please run job.outputdata.validate() to close the datasets and associate them to their physics container.")
            
#       Output files in the sandbox 
        outputsandboxfiles = job.outputsandbox
        output=[]
        for file in outputsandboxfiles:
            pfn = job.outputdir+"/"+file
            fsize = filecheck(pfn)
            if (fsize>0):
                output.append(pfn)

        return

    def retrieve(self, type=None, name=None, **options ):
        """Retrieve files listed in outputdata and registered in output from
        remote SE to local filesystem in background thread"""
        
        logger.error("Nothing to download")
        return
    
    def validate(self,freeze="yes",subscribeTo=[],force="false",subscribedDatasets=["all"]):
        """validate(freeze='yes',subscribeTo=['site1','site2',...],force='false',subscribedDatasets=['all'])
        Possible options:
        1) job.outputdata.validate() as is will just freeze your output datasets and add them to the relevant container.
        2) job.outputdata.validate(subscribeTo=['site1','site2',...]) will subscribe all the output datasets of job to site1 and site2 (after checking that you are allowed to write into site1 and site2).
        3) job.outputdata.validate(subscribedDatasets=['dataset1','dataset3'],subscribeTo=['site1','site2']) will subscribe only dataset1 and dataset3 to site1 and site2. This is useful to screen out unwanted datasets (like NTUP or LOG)
        4) force='yes' should be used when one wants to force freeze datasets from a master job which is never going to finish (subjobs stuck in completing/running/submitted states)
        """
        from Ganga.GPIDev.Lib.Job import Job
        from GangaAtlas.Lib.ATLASDataset import filecheck
        job = self._getParent()
        if job.master:
            logger.error("validate() can only be called from the master job")
            return
        # check master and subjobs status. Refuse cowardly to validate the dataset if force left to false.
        if job.subjobs:
            stats = [s.status for s in job.subjobs]
        else:
            stats=[job.status]
        logger.debug("job(s) status: %s" % str(stats))
        if "completing" in stats or "running" in stats or "submitted" in stats:
            logger.error("Found that some job or subjobs are not completed!")
            if force=="false":
                logger.warning("Refusing cowardly to validate incomplete datasets")
                return
            
        # validate() works on all backends, since it only does ddm stuff .
        # first, collect datasets from subjobs. if no subjobs, then self.store_datasets is already set from fill()
        
        for subjob in job.subjobs:
            dsets=subjob.outputdata.store_datasets
            for dset in dsets:
                if dset not in self.store_datasets:
                    self.store_datasets.append(dset)
        if len(self.store_datasets)==0: # Panda jobs leave this field unfilled.
            self.store_datasets=string.split(self.output_dataset,",")
            
        logger.info("list of datasets: %s" % str(self.store_datasets))                
        if freeze=="yes":
            # freeze output dataset, create container if needed then add to container.
            for dset in self.store_datasets:
                self.freeze_dataset(dset)

        # subscribe output data if requested.
        if len(subscribeTo)==0 or len(subscribedDatasets)==0:
            logger.info("No subscription requested. Finishing now")
            return
##        else:
##            logger.error("Direct DQ2 subscriptions are not working at the moment. Please use the web interface: http://panda.cern.ch:25880/server/pandamon/query?mode=reqsubs0")
##            return
        
        # check subscribeTo contents.
        allowed_Sites=self.checkSites(subscribeTo)
        if len(allowed_Sites)==0:
            logger.error("No allowed sites to subscribe to. Please check your proxy rights and the requested space tokens")
            return
        # check that subscribedDatasets are all frozen
        if subscribedDatasets[0]=="all":
            subscribedDatasets=self.store_datasets
        subDatasets=self.checkFrozen(subscribedDatasets)
        if len(subDatasets)==0:
            logger.error("None of the requested datasets are frozen. Aborting")
            return

        for dset in subDatasets:
            locations={}
            try:
                dq2_lock.acquire()
                locations=dq2.listDatasetReplicas(dset)
                datasetinfo = dq2.listDatasets(dset)
            finally:
                dq2_lock.release()
            if not datasetinfo or not locations:
                break
            datasetvuid = datasetinfo[dset]['vuids'][0]
            sitelist=locations[datasetvuid][1] + locations[datasetvuid][0]

            for site in allowed_Sites:
                if site in sitelist:
                    continue
                logger.warning("Subscribing %s to %s" % (dset, site))
                try:
                    dq2_lock.acquire()
                    dq2.registerDatasetSubscription(dset,site)
                finally:
                    dq2_lock.release()            
        return

    def freeze_dataset(self,dset):
        testdset=self.checkFrozen([dset])
        if len(testdset)==0:
            logger.info("proceeding with freezing the dataset %s" % dset)
            try:
                dq2_lock.acquire()
                dq2.freezeDataset(dset)
            finally:
                dq2_lock.release()
        # Once frozen, the dataset can be added to a container.
        imax=string.find(dset,".jid")
        containername=dset[:imax]+"/"
        logger.info("attempting to create container %s from %s" % (containername,dset))
        dsetlist=[]
        try:
            dq2_lock.acquire()
            dsetlist = dq2.listDatasets('%s' % containername)
        finally:
            dq2_lock.release()
        logger.debug("found %s" % str(dsetlist))
        if len(dsetlist)==0:
            logger.debug("creating container: %s", containername)
            try:
                dq2_lock.acquire()
                containerClient.create(containername)
            finally:
                dq2_lock.release()
        # at this point, one should have a container. Updating dsetlist with contents of container:
        try:
            dq2_lock.acquire()
            dsetlist = dq2.listDatasetsInContainer('%s' % containername)
        finally:
            dq2_lock.release()
                
        if dset in dsetlist:
            logger.warning("dataset %s already registered in container %s, skipping" % (dset,containername))
        if len(dsetlist)==0 or dset not in dsetlist:
            try:
                containerClient.register(containername,[dset])
            except:
                logger.error("Error registering the dataset %s in its container %s. Please discard if this job was resubmitted." % (dset,containername))
                pass
        return

    def checkFrozen(self,subscribedDatasets):
        result=[]
        for dset in subscribedDatasets:
            try:
                dq2_lock.acquire()
                frozen=dq2.getMetaDataAttribute(dset,["frozendate"])
                if frozen.values()[0]!="None":
                    logger.info("Dataset %s is frozen" % dset)
                    result.append(dset)
            finally:
                dq2_lock.release()
        return result

    def checkSites(self,siteList):
        # check tokens from siteList, compare with allowed tokens.
        #print _subscriptionTokens
        checkTokens=list(_subscriptionTokens)
        result=[]
        cloud=""
        cloudSites=[]
        for token in checkTokens:
            if "LOCALGROUPDISK" in token:
                imin=token.find("_")+1
                cloud=token[imin:]
                index=checkTokens.index(token)
                checkTokens[index]="LOCALGROUPDISK"
                print cloud.upper(),ToACache.dbcloud[cloud.upper()]
                cloudSites=getSites(ToACache.dbcloud[cloud.upper()])
                print "test",cloudSites
        for site in siteList:
            for token in checkTokens:
                #print site, token
                if token in site and token!="LOCALGROUPDISK":
                    result.append(site)
                    continue
                if len(cloudSites)>0 and site in cloudSites and token in site:
                    result.append(site)
            
        return result

###### migration class #############################################################
class AthenaMCInputDatasetsMigration12(AthenaMCInputDatasets):
    """This is a migration class for Athena Job Handler with schema version 1.2.
    There is no need to implement any methods in this class, because they will not be used.
    However, the class may have "getMigrationClass" and "getMigrationObject" class 
    methods, so that a chain of convertions can be applied."""

    _schema = Schema(Version(1,0), {
        'DQ2dataset'    : SimpleItem(defvalue = '', doc = 'DQ2 Dataset Name'),
        'LFCpath' : SimpleItem(defvalue = '', doc = 'LFC path of directory to find inputfiles on the grid, or local directory path for input datasets (datasetType=local). For all non-DQ2 datasets.'),
        'inputfiles'      : SimpleItem(defvalue = [], typelist=['str'], sequence = 1, doc = 'Logical File Names of subset of files to be processed. Must be used in conjunction of either DQ2dataset or LFCpath.'),
        'inputpartitions'  : SimpleItem(defvalue ="",doc='String of input file numbers to be used (each block separated by a coma).A block can be a single number or a closed subrange (x-y). Subranges are defined with a dash. Must be used in conjunction of either DQ2dataset or LFCpath. Alternative to inputfiles.'),
        'number_inputfiles'  : SimpleItem(defvalue="",sequence=0,doc='Number of inputfiles to process.'),
        'n_infiles_job'    : SimpleItem(defvalue=1,doc='Number of input files processed by one job or subjob. Minimum 1'),
        'datasetType'      : SimpleItem(defvalue = 'unknown', doc = 'Type of dataset(DQ2,private,unknown or local). DQ2 means the requested dataset is registered in DQ2 catalogs, private is for input datasets registered in a non-DQ2 storage (Tier3) and known to CERN local LFC. local is for local datasets on Local backend only'),
        'cavern' : SimpleItem(defvalue = '', doc = 'Name of the dataset to be used for cavern noise (pileup jobs) or extra input dataset (other transforms). This dataset must be a DQ2 dataset'),
        'n_cavern_files_job': SimpleItem(defvalue =1,doc='Number of input cavern files processed by one job or subjob. Minimum 1'),
        'minbias' : SimpleItem(defvalue = '', doc = 'Name of the dataset to be used for minimum bias (pileup jobs) or extra input dataset (other transforms). This dataset must be a DQ2 dataset'),
        'n_minbias_files_job': SimpleItem(defvalue =1,doc='Number of input cavern files processed by one job or subjob. Minimum 1')
        })

    _category = 'application_converters' # put this class in different category
    _name = 'AthenaMCInputDatasetsMigration12'
###### end of migration class #######################################################


###### migration class #############################################################
class AthenaMCOutputDatasetsMigration12(AthenaMCOutputDatasets):
    """This is a migration class for Athena Job Handler with schema version 1.2.
    There is no need to implement any methods in this class, because they will not be used.
    However, the class may have "getMigrationClass" and "getMigrationObject" class 
    methods, so that a chain of convertions can be applied."""

    _schema = Schema(Version(1,0), {
        'outdirectory'     : SimpleItem(defvalue = '', doc='path of output directory tree for storage. Used for both LFC and physical file locations.'), 
        'output_dataset'         : SimpleItem(defvalue = '', doc = 'dataset suffix for combined output dataset. If set, it will collect all expected output files for the job. If not set, every output type (histo, HITS, EVGEN...) will have its own output dataset.'),
        'output_firstfile'   : SimpleItem(defvalue=1,doc='offset for output file partition numbers. First job will generate the partition number output_firstfile, second will generate output_firstfile+1, and so on...'),
        'logfile'            : SimpleItem(defvalue='',doc='file prefix and dataset suffix for logfiles.'),
        'outrootfile'        : SimpleItem(defvalue='',doc='file prefix and dataset suffix for primary output root file (EVGEN for evgen jobs, HITS for simul jobs). Placeholder for any type of output file in template mode.'),
        'outhistfile'          : SimpleItem(defvalue='',doc='file prefix and dataset suffix for histogram files. Placeholder for any type of output file in template mode.'),
        'outntuplefile'          : SimpleItem(defvalue='',doc='file prefix and dataset suffix for ntuple files. Placeholder for any type of output file in template mode.'),
        'outrdofile'         : SimpleItem(defvalue='',doc='file prefix and dataset suffix for RDO files. Placeholder for any type of output file in template mode.'),
       'outesdfile'         : SimpleItem(defvalue='',doc='file prefix and dataset suffix for ESD files. Placeholder for any type of output file in template mode.'),
        'outaodfile'         : SimpleItem(defvalue='',doc='file prefix and dataset suffix for AOD files. Placeholder for any type of output file in template mode.'),
        'expected_output'         : SimpleItem(defvalue = [], typelist=['list'], sequence = 1, protected=1,doc = 'List of output files expected to be produced by the job. Should not be visible nor modified by the user.'),
        'actual_output'         : SimpleItem(defvalue = [], typelist=['list'], sequence = 1, protected=1,doc = 'List of output files actually produced by the job followed by their locations. Should not be visible nor modified by the user.')
        })

    _category = 'application_converters' # put this class in different category
    _name = 'AthenaMCOutputDatasetsMigration12'
###### end of migration class #######################################################


from Ganga.Utility.logging import getLogger
logger = getLogger()

##from Ganga.Utility.Config import makeConfig, ConfigError
##config = makeConfig('AthenaMCDatasets', 'AthenaMCDatasets configuration options')
##config.addOption('usertag','user','user tag for a given data taking period')
##_usertag=config['usertag']
from dq2.container.client import ContainerClient
containerClient = ContainerClient()       

from dq2.clientapi.DQ2 import DQ2
dq2=DQ2()

from threading import Lock
dq2_lock = Lock()

from Ganga.Utility.logging import getLogger
logger = getLogger()

from Ganga.Utility.Config import getConfig, ConfigError
configDQ2 = getConfig('DQ2')
    
_usertag=configDQ2['usertag']

baseURLDQ2 = configDQ2['DQ2_URL_SERVER']
baseURLDQ2SSL = configDQ2['DQ2_URL_SERVER_SSL']

# Extract username from certificate
proxy = GridProxy()
username = proxy.identity()
if 'ALLOW_MISSING_NICKNAME_DQ2OUTPUTDATASET' in configDQ2 and configDQ2['ALLOW_MISSING_NICKNAME_DQ2OUTPUTDATASET']:
    nickname = getNickname(allowMissingNickname=True)
else:
    nickname = getNickname(allowMissingNickname=False)
    
if nickname and 'USE_NICKNAME_DQ2OUTPUTDATASET' in configDQ2 and configDQ2['USE_NICKNAME_DQ2OUTPUTDATASET']:
    username = nickname

if proxy.init_opts:
    proxytag=proxy.init_opts
    #print "found proxy role:",proxyrole
    imin=string.find(proxytag,"atlas/")
    imax=string.rfind(proxytag,"/")
    imin2=string.find(proxytag,"Role=")
    role,group="",""
    if imin>-1:
        group=proxytag[imin+6:]
    if imax-imin>6:
        group=proxytag[imin+6:imax]
    if imin2>-1:
        role=proxytag[imin2+5:]
        
    if string.find(group,"phys")>-1 or string.find(group,"perf") >-1 or string.find(group,"trig") >-1 and role=="production":
        logger.warning("Detected physics/performance group: %s with production role in .gangarc, using this group for dataset naming and storage" % role)
        username=group
        _subscriptionTokens.append(group.upper())
        _usertag=_usertag.replace("user","group")
        logger.info("user data for data management: group: %s, username: %s" % (_usertag,username))
    elif len(group)==2:
        # need to handle clouds as well, for localgroupdisk...
        _subscriptionTokens.append("LOCALGROUPDISK_"+group.upper())

        

