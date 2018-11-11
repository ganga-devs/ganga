################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id$
################################################################################
                                                                                                              

import os, sys, time, commands, re, tempfile, exceptions, urllib, datetime
import cPickle as pickle

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Adapters.IBackend import IBackend
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

from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname

logger = getLogger()
config = getConfig('Jedi')

def retrieveMergeJobs(job, pandaJobDefId):
    '''
    methods for retrieving panda job ids of merging jobs given a jobDefId
    '''
    from pandatools import Client

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
                job.backend.mergejobs = []

            elif status == 'generating':
                logger.debug('merging jobs are generating')
                job.backend.mergejobs = []

            elif status == 'standby':
                logger.debug('merging jobs to be created')
                job.backend.mergejobs = []

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

                            if mjobj not in job.backend.mergejobs:
                                job.backend.mergejobs.append(mjobj)
                            else:
                                logger.debug("merging job %s already exists locally" % mjobj.id)

                            num_mjobs += 1
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

def retrievePandaJobs(job, jIDs):
    '''
    methods for retrieving panda job ids of panda jobs given a jobDefId
    '''
    from pandatools import Client

    ick       = False
    jstatus    = ''
    num_pjobs = 0

    logger.debug("retrievePandaJobs jIDs=%s" %jIDs)

    # get status from Panda server
    rc, jobsStatus = Client.getFullJobStatus(jIDs,False)
    if rc:
        logger.error('Return code %d retrieving job status information.',rc)
        raise BackendError('Jedi','Return code %d retrieving job status information.' % rc)

    for status in jobsStatus:
        if not status: continue

        jstatus = status.jobStatus
        if status.jobStatus is None:
            logger.warning('No panda jobs expected')
            job.backend.pandajobs = []

        elif status.jobStatus in [ "defined", "activated", "running", "failed", "finished", "holding", "assigned"]:
            logger.debug('Panda jobs are running')
            logger.debug("PandaID: %d" % status.PandaID)

            pjobj         = JediPandaJob()
            pjobj.id      = status.PandaID
            pjobj.url     = 'http://panda.cern.ch/?job=%d' % status.PandaID
            pjobj.jopSpec = dict(zip(status._attributes,status.values()))
            for k in pjobj.jobSpec.keys():
                if type(pjobj.jobSpec[k]) not in [type(''),type(1)]:
                    pjobj.jobSpec[k]=str(pjobj.jobSpec[k])

            if pjobj not in job.backend.pandajobs:
                job.backend.pandajobs.append(pjobj)
            else:
                logger.debug("Panda job %s already exists locally" % pjobj.id)
                
            num_pjobs += 1
        else:
            logger.warning("getFullJobStatus returned unsupported status %s for Panda job %s " %(status.jobStatus, status.PandaID) )
            
        ick = True

    return (ick, jstatus, num_pjobs)

def checkForRebrokerage(string):
    import re
    matchObj = re.match('reassigned to another site by rebrokerage. new PandaID=(\d+) JobsetID=(\d+) JobID=(\d+)', string)
    if matchObj:
        newPandaID = long(matchObj.group(1))
        newJobsetID = long(matchObj.group(2))
        newJobID = long(matchObj.group(3))
        return newPandaID
    raise BackendError('Jedi','Error getting new PandaID for rebrokered job. Report to DA Help')

class JediPandaMergeJob(GangaObject):
    _schema = Schema(Version(1,0), {
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=0,copyable=0,doc='Panda Job id'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=0,doc='Panda Job status'),
        'jobSpec'       : SimpleItem(defvalue={},optional=1,protected=1,copyable=0,doc='Panda JobSpec'),
        'url'           : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Web URL for monitoring the job.')
    })

    _category = 'JediPandaMergeJob'
    _name = 'JediPandaMergeJob'

    def __init__(self):
        super(JediPandaMergeJob,self).__init__()

    def __eq__(self, other):
        return other.id == self.id

class JediPandaJob(GangaObject):
    _schema = Schema(Version(1,0), {
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=0,copyable=0,doc='Panda Job id'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=0,doc='Panda Job status'),
        'jobSpec'       : SimpleItem(defvalue={},optional=1,protected=1,copyable=0,doc='Panda JobSpec'),
        'url'           : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Web URL for monitoring the job.'),
        'exitcode'      : SimpleItem(defvalue='',protected=1,copyable=0,doc='Application exit code (transExitCode)'),
        'piloterrorcode': SimpleItem(defvalue='',protected=1,copyable=0,doc='Pilot Error Code'),
        'reason'        : SimpleItem(defvalue='',protected=1,copyable=0,doc='Error Code Diagnostics'),
    })

    _category = 'JediPandaJob'
    _name = 'JediPandaJob'
    _exportmethods = ['get_stats']

    def __init__(self):
        super(JediPandaJob,self).__init__()

    def __eq__(self, other):
        return other.id == self.id

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

class Jedi(IBackend):
    '''Jedi backend: submission to the Jedi/PanDA workload management system
    '''

    _schema = Schema(Version(1,0), {
        'site'          : SimpleItem(defvalue='AUTO',protected=0,copyable=1,doc='Require the job to run at a specific site'),
        'requirements'  : ComponentItem('JediRequirements',doc='Requirements for the resource selection'),
        'extOutFile'    : SimpleItem(defvalue=[],typelist=['str'],sequence=1,protected=0,copyable=1,doc='define extra output files, e.g. [\'output1.txt\',\'output2.dat\']'),        
        'id'            : SimpleItem(defvalue=None,typelist=['type(None)','int'],protected=1,copyable=0,doc='PandaID of the job'),
        'url'           : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Web URL for monitoring the job.'),
        'status'        : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Panda job status'),
        'actualCE'      : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=1,copyable=0,doc='Actual CE where the job is run'),
        'libds'         : SimpleItem(defvalue=None,typelist=['type(None)','str'],protected=0,copyable=1,doc='Existing Library dataset to use (disables buildjob)'),
        'buildjob'      : ComponentItem('PandaBuildJob',load_default=0,optional=1,protected=1,copyable=0,doc='Panda Build Job'),
        'buildjobs'     : ComponentItem('PandaBuildJob',sequence=1,defvalue=[],optional=1,protected=1,copyable=0,doc='Panda Build Job'),
        'mergejobs'     : ComponentItem('JediPandaMergeJob',sequence=1,defvalue=[],optional=1,protected=1,copyable=0,doc='Panda Output Merging Jobs'),
        'pandajobs'     : ComponentItem('JediPandaJob',sequence=1,defvalue=[],optional=1,protected=1,copyable=0,doc='Jedi Panda Jobs'),
        'jobSpec'       : SimpleItem(defvalue={},optional=1,protected=1,copyable=0,doc='Panda JobSpec'),
        'exitcode'      : SimpleItem(defvalue='',protected=1,copyable=0,doc='Application exit code (transExitCode)'),
        'piloterrorcode': SimpleItem(defvalue='',protected=1,copyable=0,doc='Pilot Error Code'),
        'reason'        : SimpleItem(defvalue='',protected=1,copyable=0,doc='Error Code Diagnostics'),
        'accessmode'    : SimpleItem(defvalue='',protected=0,copyable=1,doc='EXPERT ONLY'),
        'forcestaged'   : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Force staging of input DS'),
        'individualOutDS': SimpleItem(defvalue=False,protected=0,copyable=1,doc='Create individual output dataset for each data-type. By default, all output files are added to one output dataset'),
        'bexec'         : SimpleItem(defvalue='',protected=0,copyable=1,doc='String for Executable make command - if filled triggers a build job for the Execuatble'),
        'nobuild'       : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Boolean if no build job should be sent - use it together with Athena.athena_compile variable'),
    })

    _category = 'backends'
    _name = 'Jedi'
    _exportmethods = ['get_stats']
  
    def __init__(self):
        super(Jedi,self).__init__()

    def master_submit(self,rjobs,subjobspecs,buildjobspec):
        '''Submit jobs'''
       
        from pandatools import Client
        from pandatools import MiscUtils

        from GangaCore.Core.exceptions import IncompleteJobSubmissionError
        from GangaCore.Utility.logging import log_user_exception

        job = self.getJobObject()

        # job name
        jobName = 'ganga.%s' % MiscUtils.wrappedUuidGen()

        jobspecs = {}
        if buildjobspec:
            jobspecs = buildjobspec
        else:
            jobspecs = subjobspecs

        logger.debug(jobspecs)

        # submit task
        for subjob in rjobs:
            subjob.updateStatus('submitting')

        logger.info("Submitting to Jedi ...")
        verbose = logger.isEnabledFor(10)
        status, tmpOut = Client.insertTaskParams(jobspecs, verbose)

        logger.debug(tmpOut)

        if status != 0:
            logger.error("Task submission to Jedi failed with %s " %status)
            return False
        if tmpOut[0] == False:
            logger.error("Task submission to Jedi failed %s" %tmpOut[1])
            return False
        logger.info("Task submission to Jedi suceeded with new jediTaskID=%s" %tmpOut[1])

        #if buildjobspec:
        #    job.backend.buildjob = PandaBuildJob() 
        #    job.backend.buildjob.id = jobids[0][0]
        #    job.backend.buildjob.url = 'http://panda.cern.ch/?job=%d'%jobids[0][0]
        #    del jobids[0]

        for subjob in rjobs:
            subjob.backend.id = tmpOut[1]
            subjob.backend.url = 'http://pandamon.cern.ch/jedi/taskinfo?days=20&task=%d'%tmpOut[1]
            subjob.updateStatus('submitted')
            logger.info("Panda monitor url: %s" %subjob.backend.url)

        return True

    def master_kill(self):
        '''Kill jobs'''  

        from pandatools import Client

        job = self.getJobObject()
        logger.debug('Killing job %s' % job.getFQID('.'))

        active_status = [ None, 'registered', 'waiting', 'defined', 'pending', 'assigning', 'ready', 'scouting', 'running', 'holding', 'merging', 'prepared', 'aborting', 'finishing' ]
        #active_status = [ None, 'defined', 'unknown', 'assigned', 'waiting', 'activated', 'sent', 'starting', 'running', 'holding', 'transferring' ]

        if self.id and self.status in active_status: 
            status, output = Client.killTask(self.id)
            if status:
                logger.error('Failed killing job (status = %d)',status)
                return False
            else:
                logger.info('Killing Jedi task %s, Server returned: %s' %(self.id, output))
        else:
            logger.error('Cannot kill Jedi job %s since it is not in active status', self.id)

        return True

    @staticmethod
    def master_updateMonitoringInformation(jobs):
        '''Monitor jobs'''       
        from pandatools import Client

        #active_status = [ None, 'defined', 'unknown', 'assigned', 'waiting', 'activated', 'sent', 'starting', 'running', 'holding', 'transferring' ]

        submitting_status = [ ]
        active_status = [ None, 'registered', 'waiting', 'defined', 'pending', 'assigning', 'ready', 'scouting', 'running', 'holding', 'merging', 'prepared', 'aborting', 'finishing' ]
 
        inactive_status = [ 'finished', 'aborted', 'broken', 'failed', 'done' ]

        # Find jobs to be monitored
        jobdict = {}
        for job in jobs:
            # add a delay as Panda can be a little slow in sorting out a new Task
            if job.backend.id and job.backend.status in active_status and ( (datetime.datetime.utcnow() - job.time.timestamps["submitted"]).seconds > 120):
                jobdict[job.backend.id] = job 

        logger.debug("jobdict = %s" %jobdict)
        
        # Monitor active Jedi tasks
        allJobIDs = jobdict.keys()
        pandaJobIDs = {}
        for jID in allJobIDs:
            status, jediTaskDict = Client.getJediTaskDetails({'jediTaskID': jID},False,True,verbose=False)
            if status != 0:
                logger.error("Failed to get task details for %s" % jID)
                #raise BackendError('Jedi','Return code %d retrieving job status information.' % status)
                continue
            # Retrieve job
            job = jobdict[jediTaskDict['jediTaskID']]
            # Store associated Panda jobs
            if job.backend.pandajobs:
                pandaJobIDs[job.backend.id] = [pj.id for pj in job.backend.pandajobs]
            else:
                pandaJobIDs[jediTaskDict['jediTaskID']] = jediTaskDict['PandaID']
            logger.debug("jID = %s, pandaJobIDs = %s" % (jID, pandaJobIDs))

            # Fill the output data dataset list
            if 'outDS' in jediTaskDict and jediTaskDict['outDS'] != '':
                for ds in jediTaskDict['outDS'].split(','):
                    if not ds in job.outputdata.datasetList:
                        job.outputdata.datasetList.append(ds)

            # Jedi job status has changed
            if job.backend.status != jediTaskDict['status']:
                logger.debug('Job %s has changed status from %s to %s',job.getFQID('.'),job.backend.status, jediTaskDict['status'])
                job.backend.status = jediTaskDict['status']
                job.backend.reason = jediTaskDict['statistics']

                # Now update Jedi job status
                if jediTaskDict['status'] in ['registered', 'waiting', 'defined', 'pending', 'assigning', 'ready']:
                    job.updateStatus('submitted')
                elif jediTaskDict['status'] in ['scouting', 'running', 'holding', 'merging', 'prepared' ]:
                    job.updateStatus('running')
                elif jediTaskDict['status'] in ['done']:
                    job.updateStatus('completed')
                elif jediTaskDict['status'] in ['failed', 'finished']:
                    job.updateStatus('failed')
                elif jediTaskDict['status'] in [ 'aborted', 'broken', 'cancelled' ] and job.status not in ['completed','failed']:
                    job.updateStatus('killed')
                else:
                    logger.warning('Unexpected Jedi task status %s', jediTaskDict['status'])

            # Check if associated Panda job exist and monitor them
            if not job.backend.pandajobs:
                jdefids = pandaJobIDs[jID]
                # skip if there are no Panda jobs yet 
                if not jdefids:
                    continue
                tot_num_mjobs = 0

                do_master_update = True
                ick,status,num_mjobs = retrievePandaJobs(job, jdefids)
                logger.debug('retrievePandaJobs returns: %s %s' % (repr(ick),status))
                if not ick:
                    logger.debug('Panda job retrival failure for Jedi task %s with PandaIds %s' % (job.backend.id, jdefids))
                    do_master_update = False

                tot_num_mjobs += num_mjobs
                logger.debug('Job %s retrieved %d Panda jobs' % (job.getFQID('.'),tot_num_mjobs) )
            # Now monitor the already attached Panda jobs
            else:
                jdefids = [ pj.id for pj in job.backend.pandajobs ] 
                rc, jobsStatus = Client.getFullJobStatus(jdefids,False)
                if rc:
                    logger.error('Return code %d retrieving job status information.',rc)
                    raise BackendError('Jedi','Return code %d retrieving job status information.' % rc)

                for status in jobsStatus:
                    if not status: continue

                    for pjob in job.backend.pandajobs:
                        if pjob.id == status.PandaID:
                            # skip if no status change
                            if pjob.status == status.jobStatus:
                                continue 
                            # Else update job record
                            pjob.jobSpec = dict(zip(status._attributes,status.values()))

                            for k in pjob.jobSpec.keys():
                                if type(pjob.jobSpec[k]) not in [type(''),type(1)]:
                                    pjob.jobSpec[k]=str(pjob.jobSpec[k])

                            logger.debug('Job %s with Panda job %s has changed status from %s to %s',job.getFQID('.'),pjob.id, pjob.status,status.jobStatus)
                            pjob.status = status.jobStatus
                            pjob.exitcode = str(status.transExitCode)
                            pjob.piloterrorcode = str(status.pilotErrorCode)
                            pjob.reason = ''
                            for k in pjob.jobSpec.keys():
                                if k.endswith('ErrorDiag') and pjob.jobSpec[k]!='NULL':
                                    pjob.reason += '%s: %s, '%(k,str(pjob.jobSpec[k]))
                            #if job.backend.jobSpec['transExitCode'] != 'NULL':
                            pjob.reason += 'transExitCode: %s'%pjob.jobSpec['transExitCode']

                            if status.jobStatus in ['defined','unknown','assigned','waiting','activated','sent']:
                                logger.debug('Panda job %s %s' % (pjob.id, status.jobStatus))
                            elif status.jobStatus in ['starting','running','holding','transferring', 'merging']:
                                logger.debug('Panda job %s %s '% (pjob.id, status.jobStatus))
                            elif status.jobStatus in ['finished']:
                                logger.debug('Panda job %s %s '% (pjob.id, status.jobStatus))
                            elif status.jobStatus == 'failed':
                                logger.debug('Panda job %s %s '% (pjob.id, status.jobStatus))
                                # check for server side retry
                                if 'taskBufferErrorDiag' in pjob.jobSpec and pjob.jobSpec['taskBufferErrorDiag'].find("PandaID=") != -1:
                                    # grab the new panda ID
                                    newPandaID = long(pjob.jobSpec['taskBufferErrorDiag'].split("=")[1])
                                    pjob.id = newPandaID
                                    pjob.status = None
                                    pjob.url = 'http://panda.cern.ch/?job=%d'%newPandaID
                            elif status.jobStatus == 'cancelled' and pjob.status not in ['completed','failed']: # bug 67716
                                logger.debug('Panda job %s cancelled'%pjob.id)
                                if 'taskBufferErrorDiag' in pjob.jobSpec and "rebrokerage" in pjob.jobSpec['taskBufferErrorDiag']:
                                    newPandaID = checkForRebrokerage(pjob.jobSpec['taskBufferErrorDiag'])
                                    logger.warning("Subjob rebrokered by Panda server. Job %d moved to %d."%(pjob.id, newPandaID))
                                    pjob.id = newPandaID
                                    pjob.status = None
                            else:
                                logger.warning('Unexpected job status %s',status.jobStatus)

    def master_resubmit(self,jobs):
        '''Resubmit failed Jedi job'''
        from pandatools import Client

        jobIDs = {}
        for job in jobs: 
            jobIDs[job.backend.id] = job

        allJobIDs = jobIDs.keys()
        pandaJobIDs = {}
        for jID in allJobIDs:
            status, jediTaskDict = Client.getJediTaskDetails({'jediTaskID': jID},False,True,verbose=False)
            if status != 0:
                logger.error("Failed to get task details for %s" % jID)
                raise BackendError('Jedi','Return code %d retrieving job status information.' % status)

            # Retrieve job
            job = jobIDs[jediTaskDict['jediTaskID']]
       
            newJobsetID = -1 # get jobset
            retryJobs = [] # jspecs
            resubmittedJobs = [] # ganga jobs

            if jediTaskDict['status'] in ['failed', 'killed', 'cancelled', 'aborted', 'broken', 'finished' ]:
                retryJobs.append(job)
                resubmittedJobs.append(jID)
            #elif jediTaskDict['status'] == 'finished':
            #    pass
            else:
                logger.warning("Cannot resubmit. Jedi task %s is status %s." %(jID, jediTaskDict['status'] ))
                return False

            # submit
            if len(retryJobs)==0:
                logger.warning("No failed jobs to resubmit")
                return False
            
            status,out = Client.retryTask(jID, verbose=False)                                                               
            if status != 0:
                logger.error(status)
                logger.error(out)
                logger.error("Failed to retry JobID=%s" % jID)                                                                                         
                return False
            tmpStat,tmpDiag = out
            if not tmpStat:
                logger.error(tmpDiag)
                logger.error("Failed to retry JobID=%s" % jID)
                return False
            logger.info(tmpDiag)
       
            job.backend.status = None
            job.backend.jobSpec = {}
            job.updateStatus('submitted')

        logger.info('Resubmission successful')
        return True

    def master_setup_bulk_subjobs(self, jobs, jdefids):
           
        from GangaCore.GPIDev.Lib.Job.Job import Job
        master_job=self.getJobObject()
        for i in range(len(jdefids)):
            j=Job()
            j.copyFrom(master_job)
            j.splitter = None
            j.backend=Panda()
            j.backend.id = jdefids[i]
            j.id = i
            j.status = 'submitted'
            j.time.timenow('submitted')
            master_job.subjobs.append(j)
        return True

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


