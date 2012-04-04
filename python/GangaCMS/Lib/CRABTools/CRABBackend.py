#
# CRAB Backend
#
# 08/06/10 @ ubeda
#


from Ganga.Core import BackendError
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import *

from ConfigParser import ConfigParser
import Ganga.Utility.Config

from GangaCMS.Lib.CRABTools.CRABServer import *
from GangaCMS.Lib.Utils import Timeout, TimeoutError

import os,os.path,datetime
import subprocess
import xml.dom.minidom
from xml.dom.minidom import parse,Node

########################################################################################
#
#  CRAB LOWEST INDEX IS 1   |
#  GANGA LOWEST INDEX IS 0  |  be careful...
#
########################################################################################

class CRABBackend(IBackend):

    comments = []
    comments.append('Set this variable to 0 if you dont want to see crab logs in the screen.')
    comments.append('Set this variable to 1 if you want to keep crab -status logs.')         

    schemadic={}
    schemadic['verbose']                   = SimpleItem(defvalue=1, typelist=['int'], doc=comments[0])
    schemadic['statusLog']                 = SimpleItem(defvalue=0, typelist=['int'], doc=comments[1])  
    schemadic['report']                    = SimpleItem(defvalue={})
    schemadic['fjr']                       = SimpleItem(defvalue={})
    schemadic['crab_env']                  = SimpleItem(defvalue={})

    _schema = Schema(Version(1,0), schemadic)
    _category = 'backends'
    _name  = 'CRABBackend'

    def __init__(self):

        super(CRABBackend, self).__init__()

#        try:
        config = Ganga.Utility.Config.getConfig('CMSSW')
        cmssw_setup = config['CMSSW_SETUP'] # the directory
        cmssw_version = config['CMSSW_VERSION'] # e.g. CMSSW_3_7_0, CMSSW_generic
        crab_version = config['CRAB_VERSION']
        cmssw_setup_script = os.path.join(cmssw_setup,'CMSSW_generic.sh')
        from Ganga.Utility.Shell import Shell
        shell = Shell(cmssw_setup_script,[cmssw_version,crab_version])
        self.crab_env = shell.env
#        except:
#          pass    

    def master_submit(self,rjobs,subjobconfigs,masterjobconfig):

        if rjobs[0]:

            job = rjobs[0].master

            server = CRABServer()
            for subjob in job.subjobs:
                subjob.updateStatus('submitting')

            try:
                server.submit(job)
            except:
                for subjob in job.subjobs:
                    subjob.rollbackToNewState()
                job.updateMasterJobStatus()
                logger.error('Submission through CRAB failed. All subjobs have been reverted to new.')
                return 1
#            job.backend.server.submit(job)

            self.master_updateMonitoringInformation((job,))

            # Forcing all the jobs to be submitted, so the monitoring loops keeps issuing calls.
            for subjob in job.subjobs:
                if subjob.status in ('submitting'):
                    subjob.updateStatus('submitted')

#            try:
#                server.status(job)
#            except:
#                logger.warning('CRAB status failed. Submission could have been failed too.')
#            job.backend.server.status(job)

            return 1        
        #j.updateMasterJobStatus()
        logger.error('Not submitting job without subjobs.')
        return 1           

    def master_resubmit(self,rjobs):             

        logger.error('master_resubmit() called. THIS METHOD IS BUGGY, CHECK.')
        server = CRABServer()
        server.resubmit(job)
#        self.server.resubmit(job)

        #If first raises exception, all are caput
        #Controll that.
        for job in rjobs:

            subjob.updateStatus('submitting')  
            server.resubmit(job)
            subjob.updateStatus('submitted')  
        #j.updateMasterJobStatus()
        return 1         

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
#        job.backend.server.getOutput(job)

        workdir = job.inputdata.ui_working_dir
        index = int(job.id) + 1
        doc_path = '%s/res/crab_fjr_%d.xml'%(workdir,index)

        if not os.path.exists(doc_path):
            #raise CRABServerError.CRABServerError('FJR %s not found.'%(doc_path))
            logger.error('FJR %s not found.'%(doc_path))
            return

        try:
            doc = parse(doc_path)   
        except:
            logger.error("Could not parse document. File not present?")
            return
        status = doc.firstChild.getAttribute("Status")

        if status in ["Failed"]:
            #logger.warning('Failed job detected in parsing.')
            self.postMortem(job)
            job.updateStatus('failed')
        elif status in ["Success"]:
            if job.status == 'submitting':
                job.updateStatus('submitted')
            job.updateStatus('completed')
        else:
            logger.warning("UNKNOWN PARSE STATUS: "+str(status))

        #exitcode = ''
        #line = doc.getElementsByTagName("ExitCode")[0]
        #try:       
        #    exitcode = line.getAttribute("Value")
        #except:
        #    pass
        #
        #job.backend.fjr['ExitCode'] = {'exitcode':exitcode}

        #frameworkerrors = doc.getElementsByTagName("FrameworkError")

        #if not job.backend.fjr.has_key('FrameworkError'):
        #    job.backend.fjr['FrameworkError'] = {}

        #for fwe in frameworkerrors:
        #    name = fwe.getAttribute("Type")
        #    job.backend.fjr['FrameworkError'][name] = fwe.getAttribute("ExitStatus")

        config = Ganga.Utility.Config.getConfig('Metrics')
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

            if not job.backend.fjr.has_key(section):
                job.backend.fjr[section] = {}

            performancereport = doc.getElementsByTagName("PerformanceReport")[0]
            performancesummary = performancereport.getElementsByTagName("PerformanceSummary")
            for pfs in performancesummary:
                if pfs.getAttribute("Metric") == section:
#                    METRIC_NAMES = config.options(section)
                    metrics = pfs.getElementsByTagName("Metric")
                    for metric in metrics:
                        name = metric.getAttribute("Name")
                        if config.has_option(section,name):
                            # Due to the names with minus intead of underscore, we have to do thiw walkarround
                            # to send them to the DB.
                            name = config.get(section,name)
                            if name:
                                job.backend.fjr[section][name] = metric.getAttribute("Value")

        ## BE CAREFUL WITH NEGATIVE VALUES: eg StageoutTime ##   
        # more than 1 exitcode?

    def checkReport(self,jobDoc):

        job = self.getJobObject()     

        config = Ganga.Utility.Config.getConfig('Metrics')
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
        STATUS  = {'A':'submitted, not known to scheduler / aborted anyway',
                   'C':'created, not submitted yet',
                   'CS':'created on the server, not submitted yet',
                   'DA':'done and failed',
                   'E':'ended,output retrieved and DB updated from journal',
                   'K':'killed by user',
                   'R':'submitted,started, the scheduler reports running',
                   'SD':'',
                   'SR':'',
                   'S':'submitted on the server',
                   'SS':'',
                   'SU':'',
                   'SW':'',
                   'W':'declared, not submitted'
                   }

        job = self.getJobObject()
        try:
            status = job.backend.report['status']      
        except:
            logger.warning('Missing the status for the job %d while checking' % job.id)
            return

        if (status=='R' and not (job.status in ['killed'])):
            if (job.status in ['submitting','new']):
                job.updateStatus('submitted')
            if not (job.status in ['running'] ):
                job.updateStatus('running')
        elif (status == 'C' or status == 'CS') and not (job.status in ['submitting','new']):
            logger.warning('The job is an invalid status (%s - %s), it will  be reverted.' % (status, job.status))
            job.rollbackToNewState()
        elif (status == 'SS' or status == 'W') and not (job.status in ['submitting','submitted','killed']):
            job.updateStatus("submitting")
        elif (status == 'SU' or status == 'S' or status=='SR') and not (job.status in ['submitted','killed']):
            job.updateStatus('submitted')
        elif (status == 'SD') and not (job.status in ['completed','failed','killed']):
            logger.info('Retrieving %d.'%(job.id))
            job.backend.parseResults()
            # The job can be done, but failed...
            # So, let's update the status retrieved from the output file.
            if job.status not in (['completed','failed','killed']):
                logger.warning('Processing a done job ended in not final status. Parhaps the output getting failed?')
        elif (status == 'A' or status == 'DA') and not (job.status in ['failed','killed']):
            self.postMortem(job)
            job.updateStatus('failed')
        elif (status == 'k') and not (job.status in ['killed']):
            job.updateStatus('killed')
        elif (status in ['E']) and not (job.status in ['completed','killed']):
            logger.info('Job %d has been purged.'%(job.id))
            job.backend.parseResults()
            # We have to set this now (can be repeated) in case output retrieval fails.
            if job.status not in ['completed','failed','killed']:
                self.postMortem(job)
                job.updateStatus('failed')
        else:
            if not STATUS.has_key(status):  
                logger.warning('UNKNOWN STATUS: '+str(status)+' ')

    def master_updateMonitoringInformation(jobs):

        logger.info('Going to update the monitoring information of ' + str(len(jobs)) + ' jobs')
 
        for j in jobs:

            if not j.status in ['submitting','submitted','running']:
              logger.info('Skipping job in monitring loop: %s - %s'%(j.id,j.status))
              continue 

            server = CRABServer()

            try:
                server.status(j)
            except:
                logger.error('Get status for job %d failed, skipping.' % j.id)
                continue

            workdir = j.inputdata.ui_working_dir

            doc_path = '%s/share/%s'%(workdir,j.inputdata.xml_report)
            res = os.path.isfile(doc_path)
            if res:

                doc = parse(doc_path)
                jobDoc = doc.getElementsByTagName("RunningJob")
                subjobs = j.subjobs    

                for subjobDoc in jobDoc:
                    index  = subjobDoc.getAttribute("jobId")

                    subjobs[int(index)-1].backend.checkReport(subjobDoc)                   
                    subjobs[int(index)-1].backend.checkStatus()

                j.updateMasterJobStatus()

            else:

                logger.info('No results.xml for %s'%(j.id))

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)


