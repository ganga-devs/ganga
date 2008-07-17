################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Panda.py,v 1.1 2008-07-17 16:41:31 moscicki Exp $
################################################################################
                                                                                                              

import os, sys, time, commands, re, tempfile
import cPickle as pickle

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.Core import BackendError, Sandbox
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core import FileWorkspace
from Ganga.Utility.Shell import Shell
from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

# Panda Client

from GangaPanda.Lib.Client import Client

# JobSpec and FileSpec have to be imported as taskbuffer.JobSpec and taskbuffer.FileSpec

tmpdir = tempfile.mkdtemp()
src = os.path.join(os.path.dirname(__file__),'../Client')
dst = os.path.join(tmpdir,'taskbuffer')
os.symlink(src,dst)

sys.path.insert(0,tmpdir)
from taskbuffer.JobSpec  import JobSpec 
from taskbuffer.FileSpec import FileSpec
del sys.path[0]

os.remove(dst)
os.rmdir(tmpdir)

def dq2_get(dataset,lfns,directory):
    '''small wrapper to call dq2_get'''

    cmd = os.path.join(os.path.dirname(__file__),'dq2_get')
    rc,output,m = shell.cmd1('%s -d %s -r %s %s' % (cmd,directory,dataset,' '.join(lfns)))
    return rc

def extract_stdout(directory,tarball):
    '''Extract stdout and stderr'''

    savedir = os.getcwd()
    os.chdir(directory)

    re_child = re.compile('^(.*)/pilot_child\.(.*)$')
    for line in os.popen('tar tzf %s' % tarball):
        match = re_child.match(line)
        if match:
            filename = line[:-1]
            os.system('tar xzf %s %s' % (tarball,filename))
            os.rename(filename,match.group(2))
            os.rmdir(match.group(1))

    os.chdir(savedir)

class PandaBuildJob(GangaObject):

    _schema = Schema(Version(1,0), {
        'id'            : SimpleItem(defvalue=None,protected=0,copyable=0,doc='Panda Job id'),
        'status'        : SimpleItem(defvalue=None,protected=0,copyable=0,doc='Panda Job status')
    })

    _category = 'PandaBuildJob'
    _name = 'PandaBuildJob'

    def __init__(self):

        super(PandaBuildJob,self).__init__()

class Panda(IBackend):
    '''Panda backend'''

    _schema = Schema(Version(1,0), {
        'site'          : SimpleItem(defvalue='',protected=0,copyable=1,doc='Require the job to run at a specific site'),
        'long'          : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Send job to a long queue'),
#        'blong'         : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Send build job to a long queue'),
#        'nFiles'        : SimpleItem(defvalue=0,protected=0,copyable=1,doc='Use an limited number of files in the input dataset'),
#        'SkipFiles'    : SimpleItem(defvalue=0,protected=0,copyable=1,doc='Skip N files in the input dataset'),
#        'cloud'         : SimpleItem(defvalue='US',protected=0,copyable=1,doc='cloud where jobs are submitted (default:US)'),
#        'noBuild'       : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Skip buildJob'),
#        'memory'        : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required memory size'),
#        'fileList'      : SimpleItem(defvalue='',protected=0,copyable=1,doc='List of files in the input dataset to be run'),        
#        'shipinput'     : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Ship input files to remote WNs'),        
#        'extOutFile'    : SimpleItem(defvalue=[],protected=0,copyable=1,doc='define extra output files, e.g. [\'output1.txt\',\'output2.dat\']'),        
#        'addPoolFC'     : SimpleItem(defvalue='',protected=0,copyable=1,doc='file names to be inserted into PoolFileCatalog.xml except input files. e.g., MyCalib1.root,MyGeom2.root'),        
        'corCheck'      : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Enable a checker to skip corrupted files'),        
        'notSkipMissing': SimpleItem(defvalue=False,protected=0,copyable=1,doc='If input files are not read from SE, they will be skipped by default. This option disables the functionality'),        
#        'pfnList'       : SimpleItem(defvalue='',protected=0,copyable=1,doc='Name of file which contains a list of input PFNs. Those files can be un-registered in DDM'),        
#        'mcData'        : SimpleItem(defvalue='',protected=0,copyable=1,doc='Create a symlink with linkName to .dat which is contained in input file'),
        'ara'           : SimpleItem(defvalue=False,protected=0,copyable=1,doc='use Athena ROOT Access'),
#        'araOutFile'    : SimpleItem(defvalue=[],protected=0,copyable=1,doc='define output files for ARA, e.g., [\'output1.root\',\'output2.root\']'),
#        'trf'           : SimpleItem(defvalue='',protected=0,copyable=1,doc='run transformation, e.g. .trf = "csc_atlfast_trf.py %IN %OUT.AOD.root %OUT.ntuple.root -1 0"'),
        'id'            : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Panda job id'),
        'status'        : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Panda job status'),
        'actualCE'      : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Actual CE where the job is run'),
        'buildjob'      : ComponentItem('PandaBuildJob',load_default=0,optional=1,protected=1,copyable=0,doc='Panda Build Job')
    })

    _category = 'backends'
    _name = 'Panda'
    _exportmethods = ['list_sites']
  
    def __init__(self):

        super(Panda,self).__init__()

    def getQueue(self):

        if not self.site:
            return 'ANALY_BNL_ATLAS_1' 

        site = self.site.upper()

        if site.startswith('ANALY_'):
            return site

        if self.long:
            return 'ANALY_LONG_%s' % site, 
        else:
            return 'ANALY_%s' % site

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
#
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
        logger.info('Killing job %s' % job.getFQID('.'))

        active_status = [ None, 'activated', 'defined', 'holding', 'running' ]

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

    def master_updateMonitoringInformation(jobs):
        '''Monitor jobs'''       

        active_status = [ None, 'activated', 'defined', 'holding', 'running' ]

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
                    logger.info('Job %s has changed status to %s',job.getFQID('.'),status.jobStatus)

                job.backend.status = status.jobStatus
                if status.computingElement != 'NULL':
                    job.backend.CE = status.computingElement
                else:
                    job.backend.CE = None
               
                if status.jobStatus == 'finished':

                    if job.status == 'submitted':
                        job.updateStatus('running')

                    if job.status == 'running':
                        job.updateStatus('completing')
                        logger.info('Retrieving output for job %s ...',job.getFQID('.'))
                        job.updateStatus('completed')
   
                elif status.jobStatus == 'running':

                    if job.status == 'submitted':
                        job.updateStatus('running')

                elif status.jobStatus == 'failed':
                    
                    if job.status in [ 'submitted', 'running' ]:
                        job.updateStatus('failed')

                elif status.jobStatus in ['activated','defined','holding']:
                    pass

                else:
                    logger.warning('Unexpected job status %s',status.jobStatus)

            elif job.backend.buildjob and job.backend.buildjob.id == status.PandaID:

                 if job.backend.buildjob.status != status.jobStatus:
                     logger.info('Buildjob %s has changed status to %s',job.getFQID('.'),status.jobStatus)

                 job.backend.buildjob.status = status.jobStatus

                 if status.jobStatus == 'running':
                     job.updateStatus('running')

                 elif status.jobStatus == 'failed':
                     job.updateStatus('failed')

                 elif status.jobStatus in ['activated', 'defined', 'holding','finished']:
                     pass

                 else:
                     logger.warning('Unexpected job status %s',status.jobStatus)

            else:
                logger.warning('Unexpected Panda ID %s',status.PandaID)

        for job in jobs:
            if job.subjobs and job.status <> 'failed': job.updateMasterJobStatus()

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

    def list_sites(self):

        return [ 'AGLT2', 'ALBERTA', 'BNL', 'BNL_ATLAS_1', 'BNL_ATLAS_2', 'CERN', 'CNAF', 
                 'CPPM', 'FZK', 'LAPP', 'LPC', 'LYON', 'SACLAY', 'SHEF', 'SLAC', 'TAIWAN',
                 'TOKYO', 'TORONTO', 'TRIUMF', 'UBC', 'UK', 'UTA', 'UTA-DPCC', 'VICTORIA' ] 



logger = getLogger()
config = makeConfig('Panda','Panda backend configuration parameters')

config.addOption( 'SETUP', '/afs/cern.ch/project/gd/LCG-share/sl3/etc/profile.d/grid_env.sh', 'FIXME')  
config.addOption( 'prodSourceLabel', 'user', 'FIXME')
config.addOption( 'assignedPriority', 1000, 'FIXME' )


shell = Shell(config['SETUP'])
shell.env['DQ2_COPY_COMMAND'] = 'lcg-cp -v --vo atlas'
shell.env['DQ2_URL_SERVER'] = 'http://atlddmpro.cern.ch:8000/dq2/'
shell.env['DQ2_LOCAL_ID'] = 'CERN'
#
#
# $Log: not supported by cvs2svn $
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
