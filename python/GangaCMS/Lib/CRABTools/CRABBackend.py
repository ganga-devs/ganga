from ConfigParser import ConfigParser
from Ganga.Core import BackendError
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.Utility import Config
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Shell import Shell
from GangaCMS.Lib.CRABTools.CRABServer import CRABServer
from GangaCMS.Lib.CRABTools.CRABServerError import CRABServerError
from xml.dom.minidom import parse

import datetime
import os


logger = getLogger()


class CRABBackend(IBackend):
    """Backend implementation for CRAB."""
    schemadic = {}
    schemadic['verbose'] = SimpleItem(defvalue=1,
                                      typelist=['int'],
                                      doc='Set to 0 to disable CRAB logging')
    schemadic['statusLog'] = SimpleItem(defvalue=0,
                                        typelist=['int'],
                                        doc='Set to 1 to keep -status logs')
    schemadic['report'] = SimpleItem(defvalue={})
    schemadic['fjr'] = SimpleItem(defvalue={})
    schemadic['crab_env'] = SimpleItem(defvalue={})
    _schema = Schema(Version(1, 0), schemadic)
    _category = 'backends'
    _name = 'CRABBackend'

    def __init__(self):
        super(CRABBackend, self).__init__()
        config = Config.getConfig('CMSSW')
        shell = Shell(os.path.join(config['CMSSW_SETUP'], 'CMSSW_generic.sh'),
                      [config['CMSSW_VERSION'], config['CRAB_VERSION']])
        self.crab_env = shell.env

    def master_submit(self, rjobs, subjobconfigs, masterjobconfig):
        """Perform de submission of the master job (the CRAB task)."""
        if rjobs[0]:
            job = rjobs[0].master
            for subjob in job.subjobs:
                subjob.updateStatus('submitting')

            try:
                CRABServer().submit(job)
            except CRABServerError:
                logger.error('Submission through CRAB failed.')
                for subjob in job.subjobs:
                    subjob.rollbackToNewState()
                job.updateMasterJobStatus()
                logger.info('All subjobs have been reverted to "new".')
                return False

            # This will perform a crab -status and parse the XML.
            self.master_updateMonitoringInformation((job,))

            # Forcing all the jobs to be submitted, so the monitoring loops
            # keeps issuing calls after to update.
            for subjob in job.subjobs:
                if subjob.status in ('submitting'):
                    subjob.updateStatus('submitted')

            job.updateMasterJobStatus()
        else:
            logger.warning('Not submitting job without subjobs.')
        return True

    def master_resubmit(self, rjobs):
        """Performs the resubmission of all the jobs in a jobset."""
        if rjobs[0]:
            job = rjobs[0].master
            for subjob in job.subjobs:
                subjob.updateStatus('submitting')

            try:
                CRABServer().resubmit(job)
            except CRABServerError:
                logger.error('Resubmission through CRAB failed.')
                for subjob in job.subjobs:
                    subjob.rollbackToNewState()
                job.updateMasterJobStatus()
                logger.info('All subjobs have been reverted to "new".')
                return False

            # This will perform a crab -status and parse the XML.
            self.master_updateMonitoringInformation((job,))

            # Forcing all the jobs to be submitted, so the monitoring loops
            # keeps issuing calls after to update.
            for subjob in job.subjobs:
                if subjob.status in ('submitting'):
                    subjob.updateStatus('submitted')

            job.updateMasterJobStatus()
        else:
            logger.warning('Not resubmitting job without subjobs.')
        return True

    def master_kill(self):

        #Kills a job & subjobs
        job = self.getJobObject()
        server = CRABServer()

        try:
            server.kill(job)    
        except:
            logger.warning('Killing the job using CRAB failed.')
            return 1

        if len(job.subjobs):
            for s in job.subjobs:
                if not s.status in ['completed','failed']:
                    s.updateStatus('killed')   
        else:
            if not job.status in ['completed','failed']:
                job.updateStatus('killed')

        job.updateMasterJobStatus()        

        try:
            server.status(job)
        except:
            logger.warning('Get job status from CRAB failed. Job may have not be killed.')

        return 1

    def postMortem(self,job):

        logger.info('postMortem')

        #Gets post Mortem imformation of failed job
        server = CRABServer()
        try:
            server.postMortem(job)
        except:
            logger.warning('PostMortem retrival with CRAB failed.')

        job.updateMasterJobStatus()
        return 1

    def parseResults(self):

        job = self.getJobObject()   

        server = CRABServer()
        try:
            server.status(job)
            server.getOutput(job) 
        except:
            logger.error('Could not get the output of the job.')
            # Let's not raise this yet (in case of a double call).
            # raise CRABServerError('Impossible to get the output of the job')

        workdir = job.inputdata.ui_working_dir
        index = int(job.id) + 1
        doc_path = '%s/res/crab_fjr_%d.xml'%(workdir,index)

        if not os.path.exists(doc_path):
            logger.error('FJR %s not found.'%(doc_path))
            return

        try:
            doc = parse(doc_path)   
        except:
            logger.error("Could not parse document. File not present?")
            return
        status = doc.firstChild.getAttribute("Status")

        if status in ["Failed"]:
            self.postMortem(job)
            job.updateStatus('failed')
        elif status in ["Success"]:
            if job.status == 'submitting':
                job.updateStatus('submitted')
            job.updateStatus('completed')
        else:
            logger.warning("UNKNOWN PARSE STATUS: "+str(status))

        config = Config.getConfig('Metrics')
        location = config['location']
        if not os.path.exists(location):
            raise BackendError(0,'Location %s file doesnt exist.'%(location))

        config = ConfigParser()
        config.read(location)      

        #Iterate over all them
        SECTIONS = config.sections()
        if 'report' in SECTIONS:
            SECTIONS.remove('report')

        # Only five sections work here...
        for section in SECTIONS:

            if section not in job.backend.fjr:
                job.backend.fjr[section] = {}

            performancereport = doc.getElementsByTagName("PerformanceReport")[0]
            performancesummary = performancereport.getElementsByTagName("PerformanceSummary")
            for pfs in performancesummary:
                if pfs.getAttribute("Metric") == section:
                    metrics = pfs.getElementsByTagName("Metric")
                    for metric in metrics:
                        name = metric.getAttribute("Name")
                        if config.has_option(section,name):
                            # Due to the names with minus intead of underscore, we have to do thiw walkarround
                            # to send them to the DB.
                            name = config.get(section,name)
                            if name:
                                job.backend.fjr[section][name] = metric.getAttribute("Value")


    def checkReport(self,jobDoc):

        job = self.getJobObject()     

        config = Config.getConfig('Metrics')
        location = config['location']
        if not os.path.exists(location):
            raise BackendError(0,'Location %s file doesnt exist.'%(location))

        config = ConfigParser()
        config.read(location)      

        PARAMS = [('status','status')]

        if config.has_section('report'):
            PARAMS += config.items('report')
        else:
            logger.warning('No report in metrics')

        for n,v in PARAMS:
            if v:
                job.backend.report[v] = jobDoc.getAttribute(v)

    def checkStatus(self):

        GANGA_S = ['completed','failed','killed','new','running','submitted','submitting']
        STATUS  = {'A':'aborted',
                   'C':'created',
                   'CS':'created on the server',
                   'DA':'failed',
                   'E':'cleared',
                   'K':'killed',
                   'R':'running',
                   'SD':'done',
                   'SR':'ready',
                   'S':'submitted on the server',
                   'SS':'scheduled',
                   'SU':'submitted',
                   'SW':'waiting',
                   'W':'declared',
                   'UN':'undefined',
                   }

        job = self.getJobObject()
        try:
            status = job.backend.report['status']      
        except:
            logger.warning('Missing the status for the job %d while checking' % job.id)
            return

        if status=='R' and job.status != 'killed':
            if job.status in ['submitting','new']:
                # The job has to pass by this status at least once.
                job.updateStatus('submitted')
            elif job.status != 'running':
                # A job could come from the died (i.e. completed -> running).
                job.updateStatus('running')
        elif status in ['UN','C','CS','W'] and job.status not in ['submitting','new','killed']:
            logger.warning('The job is an invalid status (%s - %s), it will  be reverted.' % (status, job.status))
            job.rollbackToNewState()
        elif status in ['SU','S','SR','SS','SW'] and job.status not in ['submitted','killed']:
            job.updateStatus('submitted')
        elif status == 'SD' and job.status not in ['completed','failed','killed']:
            logger.info('Retrieving %d.'%(job.id))
            job.backend.parseResults()
            # The job can be done, but failed...
            # So, let's update the status retrieved from the output file.
            if job.status not in ['completed','failed','killed']:
                logger.warning('Processing a done job ended in not final status. Parhaps the output getting failed?')
        elif status in ['A','DA'] and job.status not in ['failed','killed']:
            self.postMortem(job)
            job.updateStatus('failed')
        elif status == 'K' and job.status not in ['killed']:
            job.updateStatus('killed')
        elif status == 'E' and job.status not in ['completed','failed','killed']:
            logger.info('Job %d has been purged.'%(job.id))
            job.backend.parseResults()
            # We have to set this now (can be repeated) in case output retrieval fails.
            if job.status not in ['completed','failed','killed']:
                self.postMortem(job)
                job.updateStatus('failed')
        else:
            if status not in STATUS:  
                logger.warning('UNKNOWN STATUS: ' + str(status))

        # Check the CRAB created jobs that are set as submitted... for a timeout
        if status in ['C','CS','W'] and job.status in ['submitted']:
            try:
                # If submission time is more than one hour ago, a problem happened...
                if datetime.datetime.utcnow() - job.time.timestamps['submitted'] > datetime.timedelta(hours=0.5):
                    logger.info('Submission for job %d failed (timeout).' % job.id)
                    job.updateStatus('failed')
            except:
                logger.warning('Error while retrieving submit time for job %d.' % job.id)

    @staticmethod
    def master_updateMonitoringInformation(jobs):
        """Updates the statuses of the list of jobs provided by issuing crab -status."""
        logger.debug('Updating the monitoring information of ' + str(len(jobs)) + ' jobs')
        server = CRABServer()
 
        for j in jobs:
            logger.debug('Updating monitoring information for job %d (%s)' % (j.id, j.status))

            try:
                server.status(j)
            except:
                logger.error('Get status for job %d failed, skipping.' % j.id)
                continue

            doc_path = '%s/share/%s' % (j.inputdata.ui_working_dir, j.inputdata.xml_report)

            if os.path.isfile(doc_path):
                logger.debug('Found XML report for the job %d' % j.id)
                jobDoc = parse(doc_path).getElementsByTagName("RunningJob")

                for subjobDoc in jobDoc:
                    index  = subjobDoc.getAttribute("jobId")
                    j.subjobs[int(index) - 1].backend.checkReport(subjobDoc)                   
                    j.subjobs[int(index) - 1].backend.checkStatus()

                j.updateMasterJobStatus()
            else:
                logger.info('No results.xml for %s' % (j.id))

