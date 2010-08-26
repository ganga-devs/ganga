################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Panda.py,v 1.47 2009-07-23 23:46:50 dvanders Exp $
################################################################################
                                                                                                              

import os, sys, time, commands, re, tempfile, exceptions, urllib
import cPickle as pickle

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.Core import BackendError, Sandbox
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core import FileWorkspace
from Ganga.Utility.Shell import Shell
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
from Ganga.Utility.logging import getLogger

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import ToACache
from GangaAtlas.Lib.ATLASDataset.ATLASDataset import Download

logger = getLogger()
config = makeConfig('Panda','Panda backend configuration parameters')
config.addOption( 'prodSourceLabelBuild', 'panda', 'prodSourceLabelBuild')
config.addOption( 'prodSourceLabelRun', 'user', 'prodSourceLabelRun')
config.addOption( 'assignedPriorityBuild', 2000, 'assignedPriorityBuild' )
config.addOption( 'assignedPriorityRun', 1000, 'assignedPriorityRun' )
config.addOption( 'processingType', 'ganga', 'processingType' )
config.addOption( 'enableDownloadLogs', False , 'enableDownloadLogs' )  
config.addOption( 'trustIS', True , 'Trust the Information System' )  
config.addOption( 'serverMaxJobs', 5000 , 'Maximum number of subjobs to send to the Panda server' )  
config.addOption( 'chirpconfig', '' , 'Configuration string for chirp data output, e.g. "chirp^etpgrid01.garching.physik.uni-muenchen.de^/tanyasandoval^-d chirp" ' )  

def queueToAllowedSites(queue):
    from pandatools import Client
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

    disallowed_sites = ['CERN-PROD_TZERO']
    allowed_allowed_sites = []
    for site in allowed_sites:
        if site not in disallowed_sites:
            allowed_allowed_sites.append(site)
    return allowed_allowed_sites

def runPandaBrokerage(job):
    from pandatools import Client

    tmpSites = []
    # get locations when site==AUTO
    if job.backend.site == "AUTO":
        libdslocation = []
        if job.backend.libds:
            try:
                libdslocation = Client.getLocations(job.backend.libds,[],job.backend.requirements.cloud,False,False)
            except exceptions.SystemExit:
                raise BackendError('Panda','Error in Client.getLocations for libDS')
            if not libdslocation:
                raise ApplicationConfigurationError(None,'Could not locate libDS %s'%job.backend.libds)
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
                    raise ApplicationConfigurationError(None,'Could not determine input datasetname for Panda brokerage')
            if not dataset:
                raise ApplicationConfigurationError(None,'Could not determine input datasetname for Panda brokerage')

            fileList = []
            try:
                fileList  = Client.queryFilesInDataset(dataset,False)
            except exceptions.SystemExit:
                raise BackendError('Panda','Error in Client.queryFilesInDataset')
            try:
                dsLocationMap = Client.getLocations(dataset,fileList,job.backend.requirements.cloud,False,False,expCloud=True)
                if not dsLocationMap:
                    logger.info('Dataset not found in cloud %s, searching all clouds...'%job.backend.requirements.cloud)
                    dsLocationMap = Client.getLocations(dataset,fileList,job.backend.requirements.cloud,False,False,expCloud=False)
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
            logger.warning("Could not determine athena tag for Panda brokering")
    try:
        status,out = Client.runBrokerage(tmpSites,tag,verbose=False,trustIS=config['trustIS'],processingType=config['processingType'])
    except exceptions.SystemExit:
        job.backend.reason = 'Exception in Client.runBrokerage: %s %s'%(sys.exc_info()[0],sys.exc_info()[1])
        raise BackendError('Panda','Exception in Client.runBrokerage: %s %s'%(sys.exc_info()[0],sys.exc_info()[1]))
    if status != 0:
        job.backend.reason = 'Non-zero to run brokerage for automatic assignment: %s' % out
        raise BackendError('Panda','Non-zero to run brokerage for automatic assignment: %s' % out)
    if not Client.PandaSites.has_key(out):
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


def uploadSources(path,sources):
    from pandatools import Client

    logger.info('Uploading source tarball %s in %s to Panda...'%(sources,path))
    try:
        cwd = os.getcwd()
        os.chdir(path)
        rc, output = Client.putFile(sources)
        os.chdir(cwd)
        if output != 'True':
            logger.error('Uploading sources %s/%s from failed. Status = %d', path, sources, rc)
            logger.error(output)
            raise BackendError('Panda','Uploading sources to Panda failed')
    except:
        raise BackendError('Panda','Exception while uploading archive: %s %s'%(sys.exc_info()[0],sys.exc_info()[1]))

def getLibFileSpecFromLibDS(libDS):
    from pandatools import Client
    from taskbuffer.FileSpec import FileSpec

    # query files in lib dataset to reuse libraries
    logger.info("query files in %s" % libDS)
    tmpList = Client.queryFilesInDataset(libDS,False)
    tmpFileList = []
    tmpGUIDmap = {}
    for fileName in tmpList.keys():
        # ignore log file
        if len(re.findall('.log.tgz.\d+$',fileName)) or len(re.findall('.log.tgz$',fileName)):
            continue
        tmpFileList.append(fileName)
        tmpGUIDmap[fileName] = tmpList[fileName]['guid'] 
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
    return fileBO



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

class Panda(IBackend):
    '''Panda backend: submission to the PanDA workload management system
    '''

    _schema = Schema(Version(2,4), {
        'site'          : SimpleItem(defvalue='AUTO',protected=0,copyable=1,doc='Require the job to run at a specific site'),
        'requirements'  : ComponentItem('PandaRequirements',doc='Requirements for the resource selection'),
        'extOutFile'    : SimpleItem(defvalue=[],typelist=['str'],sequence=1,protected=0,copyable=1,doc='define extra output files, e.g. [\'output1.txt\',\'output2.dat\']'),        
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=1,copyable=0,doc='PandaID of the job'),
        'url'           : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Web URL for monitoring the job.'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Panda job status'),
        'actualCE'      : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Actual CE where the job is run'),
        'libds'         : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=1,doc='Existing Library dataset to use (disables buildjob)'),
        'buildjob'      : ComponentItem('PandaBuildJob',load_default=0,optional=1,protected=1,copyable=0,doc='Panda Build Job'),
        'jobSpec'       : SimpleItem(defvalue={},optional=1,protected=1,copyable=0,doc='Panda JobSpec'),
        'exitcode'      : SimpleItem(defvalue='',protected=1,copyable=0,doc='Application exit code (transExitCode)'),
        'piloterrorcode': SimpleItem(defvalue='',protected=1,copyable=0,doc='Pilot Error Code'),
        'reason'        : SimpleItem(defvalue='',protected=1,copyable=0,doc='Error Code Diagnostics'),
        'accessmode'    : SimpleItem(defvalue='',protected=0,copyable=1,doc='EXPERT ONLY'),
        'individualOutDS': SimpleItem(defvalue=False,protected=0,copyable=1,doc='Create individual output dataset for each data-type. By default, all output files are added to one output dataset'),
        'bexec'         : SimpleItem(defvalue='',protected=0,copyable=1,doc='String for Executable make command - if filled triggers a build job for the Execuatble'),
    })

    _category = 'backends'
    _name = 'Panda'
    _exportmethods = ['list_sites','get_stats']
  
    def __init__(self):
        super(Panda,self).__init__()

    def master_submit(self,rjobs,subjobspecs,buildjobspec):
        '''Submit jobs'''
        
        from pandatools import Client
        from Ganga.Core import IncompleteJobSubmissionError
        from Ganga.Utility.logging import log_user_exception

        assert(implies(rjobs,len(subjobspecs)==len(rjobs))) 
        
        if self.libds:
            buildjobspec = None

        for subjob in rjobs:
            subjob.updateStatus('submitting')

        job = self.getJobObject()

        if buildjobspec:
            jobspecs = [buildjobspec] + subjobspecs
        else:
            jobspecs = subjobspecs

        if len(jobspecs) > config['serverMaxJobs']:
            raise BackendError('Panda','Cannot submit %d subjobs. Server limits to %d.' % (len(jobspecs),config['serverMaxJobs']))

        configSys = getConfig('System')
        for js in jobspecs:
            js.lockedby = configSys['GANGA_VERSION']

        verbose = logger.isEnabledFor(10)
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

    def master_kill(self):
        '''Kill jobs'''  

        from pandatools import Client

        job = self.getJobObject()
        logger.debug('Killing job %s' % job.getFQID('.'))

        active_status = [ None, 'defined', 'unknown', 'assigned', 'waiting', 'activated', 'sent', 'starting', 'running', 'holding', 'transferring' ]

        jobids = []
        if self.buildjob and self.buildjob.id and self.buildjob.status in active_status: 
            jobids.append(self.buildjob.id)
        if self.id and self.status in active_status: 
            jobids.append(self.id)

#       subjobs cannot have buildjobs
                
        jobids += [subjob.backend.id for subjob in job.subjobs if subjob.backend.id and subjob.backend.status in active_status]

        status, output = Client.killJobs(jobids)
        if status:
             logger.error('Failed killing job (status = %d)',status)
             return False
        return True

    def master_resubmit(self,jobs):
        '''Resubmit failed subjobs'''
        from pandatools import Client

        if self._getParent()._getParent(): # if has a parent then this is a subjob
            raise BackendError('Panda','Resubmit on subjobs is not supported for Panda backend. \nUse j.resubmit() (i.e. resubmit the master job) and your failed subjobs \nwill be automatically selected and retried.')

        jobIDs = {}
        for job in jobs: 
            jobIDs[job.backend.id] = job

        rc,jspecs = Client.getFullJobStatus(jobIDs.keys(),False)
        if rc:
            logger.error('Return code %d retrieving job status information.',rc)
            raise BackendError('Panda','Return code %d retrieving job status information.' % rc)

        retryJobs = [] # jspecs
        retrySite    = None
        retryElement = None
        retryDestSE  = None
        resubmittedJobs = [] # ganga jobs
        for job in jspecs:
            if job.jobStatus in ['failed', 'killed']:
                oldID = job.PandaID
                # unify sitename
                if retrySite == None:
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
                job.dispatchDBlock = None
                job.jobExecutionID = job.jobDefinitionID
                for file in job.Files:
                    file.rowID = None
                    if file.type == 'input':
                        file.status = 'ready'
                    elif file.type in ('output','log'):
                        file.destinationSE = retryDestSE
                        file.destinationDBlock=file.dataset
                        # add attempt nr
                        oldName  = file.lfn
                        file.lfn = re.sub("\.\d+$","",file.lfn)
                        file.lfn = "%s.%d" % (file.lfn,job.attemptNr)
                        newName  = file.lfn
                        # modify jobParameters
                        job.jobParameters = re.sub("'%s'" % oldName ,"'%s'" % newName, job.jobParameters)
                retryJobs.append(job)
                resubmittedJobs.append(jobIDs[oldID])
            elif job.jobStatus == 'finished':
                pass
            else:
                logger.warning("Cannot resubmit. Some jobs are still running.")
                return False

        # submit
        if len(retryJobs)==0:
            logger.warning("No failed jobs to resubmit")
            return False

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

        logger.info('Resubmission successful')
        return True

    def master_updateMonitoringInformation(jobs):
        '''Monitor jobs'''       
        from pandatools import Client

        active_status = [ None, 'defined', 'unknown', 'assigned', 'waiting', 'activated', 'sent', 'starting', 'running', 'holding', 'transferring' ]

        jobdict = {}

        for job in jobs:

            buildjob = job.backend.buildjob
            if buildjob and buildjob.id and buildjob.status in active_status:
                jobdict[buildjob.id] = job

            if job.backend.id and job.backend.status in active_status:
                jobdict[job.backend.id] = job 

            for subjob in job.subjobs:
                if subjob.backend.status in active_status:
                    jobdict[subjob.backend.id] = subjob

        # split into 2000-job pieces
        allJobIDs = jobdict.keys()
        jIDPieces = [allJobIDs[i:i+2000] for i in range(0,len(allJobIDs),2000)]

        for jIDs in jIDPieces:
            rc, jobsStatus = Client.getFullJobStatus(jIDs,False)
            if rc:
                logger.error('Return code %d retrieving job status information.',rc)
                raise BackendError('Panda','Return code %d retrieving job status information.' % rc)
         
            for status in jobsStatus:

                if not status: continue

                job = jobdict[status.PandaID]
                if job.backend.id == status.PandaID:

                    if job.backend.status != status.jobStatus:
                        job.backend.jobSpec = dict(zip(status._attributes,status.values()))

                        for k in job.backend.jobSpec.keys():
                            if type(job.backend.jobSpec[k]) not in [type(''),type(1)]:
                                job.backend.jobSpec[k]=str(job.backend.jobSpec[k])

                        logger.debug('Job %s has changed status from %s to %s',job.getFQID('.'),job.backend.status,status.jobStatus)
                        job.backend.status = status.jobStatus
                        job.backend.exitcode = str(status.transExitCode)
                        job.backend.piloterrorcode = str(status.pilotErrorCode)
                        job.backend.reason = ''
                        for k in job.backend.jobSpec.keys():
                            if k.endswith('ErrorDiag') and job.backend.jobSpec[k]!='NULL':
                                job.backend.reason += '%s: %s, '%(k,str(job.backend.jobSpec[k]))
                        #if job.backend.jobSpec['transExitCode'] != 'NULL':
                        job.backend.reason += 'transExitCode: %s'%job.backend.jobSpec['transExitCode']

                        if status.jobStatus in ['defined','unknown','assigned','waiting','activated','sent']:
                            job.updateStatus('submitted')
                        elif status.jobStatus in ['starting','running','holding','transferring']:
                            job.updateStatus('running')
                        elif status.jobStatus == 'finished':
                            if not job.backend._name=='PandaBuildJob' and job.status != "completed":
                                job.backend.fillOutputData(job, status)
                                if config['enableDownloadLogs']:
                                    job.backend.getLogFiles(job.getOutputWorkspace().getPath(), status)
                            job.updateStatus('completed')
                        elif status.jobStatus == 'failed':
                            job.updateStatus('failed')
                        elif status.jobStatus == 'cancelled' and job.status not in ['completed','failed']: # bug 67716
                            job.updateStatus('killed')
                        else:
                            logger.warning('Unexpected job status %s',status.jobStatus)

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
                            job.updateStatus('failed')
                        elif status.jobStatus == 'cancelled':
                            job.updateStatus('killed')
                        else:
                            logger.warning('Unexpected job status %s',status.jobStatus)

                        #un = job.backend.buildjob.jobSpec['prodUserID'].split('/CN=')[-2]
                        #jdid = job.backend.buildjob.jobSpec['jobDefinitionID']
                        #job.backend.url = 'http://panda.cern.ch/?job=*&jobDefinitionID=%s&user=%s'%(jdid,un)
                else:
                    logger.warning('Unexpected Panda ID %s',status.PandaID)

        for job in jobs:
            if job.subjobs and job.status <> 'failed': job.updateMasterJobStatus()
        

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

    def list_sites(self):
        from pandatools import Client
        sites=Client.PandaSites.keys()
        sites.sort()
        return sites

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
