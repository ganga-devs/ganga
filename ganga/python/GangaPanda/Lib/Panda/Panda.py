################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Panda.py,v 1.13 2008-11-13 16:28:09 dvanders Exp $
################################################################################
                                                                                                              

import os, sys, time, commands, re, tempfile
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
from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

# Panda Client

import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import ToACache

def queueToAllowedSites(queue):
    try:
        ddm = Client.PandaSites[queue]['ddm']
    except KeyError:
        raise ApplicationConfigurationError(None,'Queue %s has no ddm field in SchedConfig'%queue)
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
                    #print '%s has alternateName %s'%(ddm,alternate_names)
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
    #print 'allowed_sites: %s'%allowed_sites

    disallowed_sites = ['CERN-PROD_TZERO']
    allowed_allowed_sites = []
    for site in allowed_sites:
        if site not in disallowed_sites:
            allowed_allowed_sites.append(site)
    return allowed_allowed_sites

def runPandaBrokerage(job):
    # get locations when site==AUTO
    if job.backend.site == "AUTO":
        fileList = []
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
        job.backend.site = out
    
    # patch for BNL
    if job.backend.site == "ANALY_BNL":
        job.backend.site = "ANALY_BNL_ATLAS_1"

    # long queue
    if job.backend.long:
        job.backend.site = re.sub('ANALY_','ANALY_LONG_',job.backend.site)
    job.backend.actualCE = job.backend.site
    # correct the cloud in case site was not AUTO
    job.backend.cloud = Client.PandaSites[job.backend.site]['cloud']
    logger.info('Panda brokerage results: cloud %s, site %s'%(job.backend.cloud,job.backend.site))

class PandaBuildJob(GangaObject):
    _schema = Schema(Version(1,0), {
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=0,copyable=0,doc='Panda Job id'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=0,doc='Panda Job status')
    })

    _category = 'PandaBuildJob'
    _name = 'PandaBuildJob'

    def __init__(self):
        super(PandaBuildJob,self).__init__()

class Panda(IBackend):
    '''Panda backend'''

    _schema = Schema(Version(1,2), {
        'site'          : SimpleItem(defvalue='AUTO',protected=0,copyable=1,doc='Require the job to run at a specific site'),
        'long'          : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Send job to a long queue'),
#        'blong'         : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Send build job to a long queue'),
#        'nFiles'        : SimpleItem(defvalue=0,protected=0,copyable=1,doc='Use an limited number of files in the input dataset'),
#        'SkipFiles'    : SimpleItem(defvalue=0,protected=0,copyable=1,doc='Skip N files in the input dataset'),
        'cloud'         : SimpleItem(defvalue='US',protected=0,copyable=1,doc='cloud where jobs are submitted (default:US)'),
#        'noBuild'       : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Skip buildJob'),
#        'memory'        : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required memory size'),
#        'fileList'      : SimpleItem(defvalue='',protected=0,copyable=1,doc='List of files in the input dataset to be run'),        
#        'shipinput'     : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Ship input files to remote WNs'),        
        'extOutFile'    : SimpleItem(defvalue=[],typelist=['str'],sequence=1,protected=0,copyable=1,doc='define extra output files, e.g. [\'output1.txt\',\'output2.dat\']'),        
#        'addPoolFC'     : SimpleItem(defvalue='',protected=0,copyable=1,doc='file names to be inserted into PoolFileCatalog.xml except input files. e.g., MyCalib1.root,MyGeom2.root'),        
        'corCheck'      : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Enable a checker to skip corrupted files'),        
        'notSkipMissing': SimpleItem(defvalue=False,protected=0,copyable=1,doc='If input files are not read from SE, they will be skipped by default. This option disables the functionality'),        
#        'pfnList'       : SimpleItem(defvalue='',protected=0,copyable=1,doc='Name of file which contains a list of input PFNs. Those files can be un-registered in DDM'),        
#        'mcData'        : SimpleItem(defvalue='',protected=0,copyable=1,doc='Create a symlink with linkName to .dat which is contained in input file'),
        'ara'           : SimpleItem(defvalue=False,protected=0,copyable=1,doc='use Athena ROOT Access'),
#        'araOutFile'    : SimpleItem(defvalue=[],protected=0,copyable=1,doc='define output files for ARA, e.g., [\'output1.root\',\'output2.root\']'),
#        'trf'           : SimpleItem(defvalue='',protected=0,copyable=1,doc='run transformation, e.g. .trf = "csc_atlfast_trf.py %IN %OUT.AOD.root %OUT.ntuple.root -1 0"'),
        'supStream'     : SimpleItem(defvalue=[],typelist=['str'],sequence=1,protected=0,copyable=1,doc='suppress some output streams. e.g., [\'ESD\',\'TAG\']'),
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=1,copyable=0,doc='Panda job id'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Panda job status'),
        'actualCE'      : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Actual CE where the job is run'),
        'buildjob'      : ComponentItem('PandaBuildJob',load_default=0,optional=1,protected=1,copyable=0,doc='Panda Build Job')
    })

    _category = 'backends'
    _name = 'Panda'
    _exportmethods = ['list_sites']
  
    def __init__(self):
        super(Panda,self).__init__()

    def master_submit(self,rjobs,subjobspecs,buildjobspec):
        '''Submit jobs'''
 
        from Ganga.Core import IncompleteJobSubmissionError
        from Ganga.Utility.logging import log_user_exception

        assert(implies(rjobs,len(subjobspecs)==len(rjobs))) 

        for subjob in rjobs:
            subjob.updateStatus('submitting')

        job = self.getJobObject()

        if buildjobspec:
            jobspecs = [buildjobspec] + subjobspecs
        else:
            jobspecs = subjobspecs

        verbose = logger.isEnabledFor(10)
        status, jobids = Client.submitJobs(jobspecs,verbose)
        if status:
            logger.error('Status %d from Panda submit',status)
            return False
       
        if buildjobspec:
            job.backend.buildjob = PandaBuildJob() 
            job.backend.buildjob.id = jobids[0][0]
            del jobids[0]

        for subjob, jobid in zip(rjobs,jobids):
            subjob.backend.id = jobid[0]
            subjob.updateStatus('submitted')

        return True

    def master_kill(self):
        '''Kill jobs'''  
                                                                                                            
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
        jobIDs = {}
        for job in jobs: 
            jobIDs[job.backend.id] = job

        rc,jspecs = Client.getJobStatus(jobIDs.keys())
        if rc:
            logger.error('Return code %d retrieving job status information.',rc)
            raise BackendError('Panda','Return code %d retrieving job status information.' % rc)

        retryJobs = [] # jspecs
        resubmittedJobs = [] # ganga jobs
        for job in jspecs:
            if job.jobStatus == 'failed':
                oldID = job.PandaID
                # reset
                job.jobStatus = None
                job.commandToPilot = None
                job.startTime = None
                job.endTime = None
                job.attemptNr = 1+job.attemptNr
                job.transExitCode = None
                job.pilotErrorCode = None
                job.exeErrorCode = None
                job.ddmErrorCode = None
                job.taskBufferErrorCode = None
                job.dispatchDBlock = None
                for file in job.Files:
                    file.rowID = None
                    if file.type in ('output','log'):
                        file.destinationDBlock=file.dataset
                        # add attempt nr
                        oldName  = file.lfn
                        file.lfn = re.sub("\.\d+$","",file.lfn)
                        file.lfn = "%s.%d" % (file.lfn,job.attemptNr)
                        newName  = file.lfn
                        # modify jobParameters
                        job.jobParameters = re.sub("'%s'" % oldName ,"'%s'" % newName, job.jobParameters)
                    elif file.type == 'input' and re.search('\.lib\.tgz',file.lfn)==None:
                        # reset dispatchDBlock
                        if file.status != 'ready':
                            file.dispatchDBlock = None
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
            job.backend.status = None
            job.updateStatus('submitted')

        logger.info('Resubmission successful',status)
        return True

    def master_updateMonitoringInformation(jobs):
        '''Monitor jobs'''       

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

        rc, jobsStatus = Client.getJobStatus(jobdict.keys())
        if rc:
            logger.error('Return code %d retrieving job status information.',rc)
            raise BackendError('Panda','Return code %d retrieving job status information.' % rc)
      
        for status in jobsStatus:

            if not status: continue

            job = jobdict[status.PandaID]
            if job.backend.id == status.PandaID:

                if job.backend.status != status.jobStatus:
                    logger.debug('Job %s has changed status from %s to %s',job.getFQID('.'),job.backend.status,status.jobStatus)
                    job.backend.status = status.jobStatus

                    if status.computingElement != 'NULL':
                        job.backend.CE = status.computingElement
                    else:
                        job.backend.CE = None
               
                    if status.jobStatus in ['defined','unknown','assigned','waiting','activated','sent']:
                        pass
                    elif status.jobStatus in ['starting','running','holding','transferring']:
                        job.updateStatus('running')
                    elif status.jobStatus == 'finished':
                        job.updateStatus('completed')
                    elif status.jobStatus == 'failed':
                        job.updateStatus('failed')
                    else:
                        logger.warning('Unexpected job status %s',status.jobStatus)

            elif job.backend.buildjob and job.backend.buildjob.id == status.PandaID:
                if job.backend.buildjob.status != status.jobStatus:
                    logger.debug('Buildjob %s has changed status from %s to %s',job.getFQID('.'),job.backend.buildjob.status,status.jobStatus)
                    job.backend.buildjob.status = status.jobStatus

                    if status.jobStatus in ['defined','unknown','assigned','waiting','activated','sent','finished']:
                        pass
                    elif status.jobStatus in ['starting','running','holding','transferring']:
                        job.updateStatus('running')
                    elif status.jobStatus == 'failed':
                        job.updateStatus('failed')
                    else:
                        logger.warning('Unexpected job status %s',status.jobStatus)
            else:
                logger.warning('Unexpected Panda ID %s',status.PandaID)

        for job in jobs:
            if job.subjobs and job.status <> 'failed': job.updateMasterJobStatus()

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

    def list_sites(self):
        sites=Client.PandaSites.keys()
        sites.sort()
        return sites

logger = getLogger()
config = makeConfig('Panda','Panda backend configuration parameters')
config.addOption( 'prodSourceLabel', 'user', 'FIXME')
config.addOption( 'assignedPriority', 1000, 'FIXME' )


#
#
# $Log: not supported by cvs2svn $
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
