###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LCG.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
###############################################################################
#
# LCG backend
#
# ATLAS/ARDA
#
# Date:   August 2005

import os, sys, md5, re, tempfile, time, errno, socket
from types import *
from urlparse import urlparse

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Credentials import getCredential 
from Ganga.GPIDev.Adapters.IBackend import IBackend 
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core import BackendError, GangaException
from Ganga.Utility.Shell import Shell
from Ganga.Utility.Config import makeConfig,getConfig, ConfigError
from Ganga.Utility.logging import getLogger, log_user_exception
from Ganga.Utility.util import isStringLike
from Ganga.Utility.GridShell import getShell
from Ganga.Lib.LCG.GridCache import GridCache
from Ganga.Lib.LCG.ElapsedTimeProfiler import ElapsedTimeProfiler

# for runtime stdout/stderr inspection
from Ganga.Lib.MonitoringServices.Octopus import Octopus,ProtocolException

try:
    simulator_enabled = os.environ['GANGA_GRID_SIMULATOR']
except KeyError:
    simulator_enabled = False

if simulator_enabled:
    from GridSimulator import GridSimulator as Grid
else:
    from Grid import Grid
    
class LCG(IBackend):
    '''LCG backend - submit jobs to the EGEE/LCG Grid using gLite/EDG middleware.

    The middleware type (EDG/gLite) may be selected with the middleware
    attribute. The specific middleware type must be enabled in ganga
    configuration. See [LCG] section of ~/.gangarc file.

    If the input sandbox exceeds the limit specified in the ganga
    configuration, it is automatically uploaded to a storage element.  This
    overcomes sandbox size limits on the resource broker. If iocache attribute
    is unspecified then the DefaultSE is used as defined in ganga
    configuration.

    For gLite middleware bulk (faster) submission is supported so splitting
    jobs may be more efficient than submitting bunches of individual jobs.

   For more options see help on LCGRequirements.

    See also: http://cern.ch/glite/documentation
    '''

    # internal usage of the flag:
    #  - 0: job without the need of special control
    #  - 1: job (normally a subjob) resubmitted individually. The monitoring of those jobs should be separated.
    _schema = Schema(Version(1,7), {
        'CE'                  : SimpleItem(defvalue=None,doc='Request a specific Computing Element'),
        'iocache'             : SimpleItem(defvalue='',protected=0,copyable=1,doc='Storage element used as a cache for oversized input sandbox'),
        'jobtype'             : SimpleItem(defvalue='Normal',doc='Job type: Normal, MPICH'),
        'requirements'        : ComponentItem('LCGRequirements',doc='Requirements for the resource selection'),
        'id'                  : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Middleware job identifier'),
        'status'              : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Middleware job status'),
        'middleware'          : SimpleItem(defvalue='EDG',protected=0,copyable=1,doc='Middleware type',checkset='_checkset_middleware'),
        'exitcode'            : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Application exit code'),
        'exitcode_lcg'        : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Middleware exit code'),
        'reason'              : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Reason of causing the job status'),
        'perusable'           : SimpleItem(defvalue=False,protected=0,copyable=1,checkset='_checkset_perusable',doc='Enable the job perusal feature of GLITE'),
        'actualCE'            : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Computing Element where the job actually runs.'),
        'monInfo'             : SimpleItem(defvalue=None,protected=1,copyable=0,hidden=1,doc='Hidden information of the monitoring service.'),
        'octopus'             : SimpleItem(defvalue=None,protected=1,copyable=0,transient=1,hidden=1,doc='Hidden transient object for Octopus connection.'),
        'flag'                : SimpleItem(defvalue=0,protected=1,copyable=0,hidden=1,doc='Hidden flag for internal control.')
    })

    _category = 'backends'
    _name =  'LCG'
    _exportmethods = ['check_proxy','loginfo','cleanup_iocache','inspect']

    _GUIPrefs = [ { 'attribute' : 'CE', 'widget' : 'String' },
                  { 'attribute' : 'iocache', 'widget' : 'String'},
                  { 'attribute' : 'jobtype', 'widget' : 'String_Choice', 'choices' : ['Normal', 'MPICH'] },
                  { 'attribute' : 'middleware', 'widget' : 'String_Choice', 'choices' : [ 'EDG', 'GLITE' ] } ]

    def __init__(self):
        super(LCG,self).__init__()
        if not self.middleware:
            self.middleware = 'EDG'

        # Disable GLITE perusal by default, since it can be dangerous
        self.perusable=False
        
        # dynamic requirement object loading 
        try:
            reqName1   = config['Requirements']
            reqName   = config['Requirements'].split('.').pop()
            reqModule = __import__(reqName1, globals(), locals(), [reqName1])
            reqClass  = vars(reqModule)[reqName]
            self.requirements = reqClass()

            logger.debug('load %s as LCGRequirements' % reqName)
        except:
            logger.debug('load default LCGRequirements')
            pass

    def _checkset_middleware(self, value):
        if value and not value.upper() in ['GLITE','EDG']:
            raise AttributeError('middleware value must be either \'GLITE\' or \'EDG\'')

    def _checkset_perusable(self, value):
        if value!=False and self.middleware.upper()!='GLITE':
            raise AttributeError("perusable can only be set for GLITE jobs")

    def __make_collection_jdl(self,nodeJDLFiles=[]):
        '''Compose the collection JDL for the master job'''

        nodes = ',\n'.join(map(lambda x:'[file = "%s";]' % x, nodeJDLFiles))

        jdl = {
            'Type'  : 'collection',
            'VirtualOrganisation'  : config['VirtualOrganisation'],
            'Nodes' : ''
        }

        # specification of the node jobs
        node_cnt = 0
        node_str = ''
        jdl ['Nodes'] = '{\n';
        for f in nodeJDLFiles:
            node_str += '[NodeName = "gsj_%d"; file="%s";],\n' % (node_cnt, f)
            node_cnt += 1
        if node_str:
            jdl['Nodes'] += node_str.strip()[:-1]
        jdl['Nodes'] += '\n}';

        jdlText = Grid.expandjdl(jdl)
        logger.debug('master job JDL: %s' % jdlText)
        return jdlText

    def __refresh_jobinfo__(self,job):
        '''Refresh the lcg jobinfo. It will be called after resubmission.'''
        job.backend.status   = None
        job.backend.reason   = None
        job.backend.actualCE = None
        job.backend.exitcode = None
        job.backend.exitcode_lcg = None
        job.backend.flag     = 0

    def master_submit(self,rjobs,subjobconfigs,masterjobconfig):
        '''Submit the master job to the grid'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        mt = self.middleware.upper()

        job = self.getJobObject()

        ick = False
        if not config['%s_ENABLE' % mt]:
            #logger.warning('Operations of %s middleware are disabled.' % mt)
            #ick = False
            raise GangaException('Operations of %s middleware not enabled' % mt)
        else:
            if mt == 'EDG' or len(job.subjobs) == 0:
                ick = IBackend.master_submit(self,rjobs,subjobconfigs,masterjobconfig)
            else:
                ick = self.master_bulk_submit(rjobs,subjobconfigs,masterjobconfig)
                if not ick:
                    raise GangaException('GLITE bulk submission failure')

        profiler.check('==> master_submit() elapsed time')
        return ick

    def master_resubmit(self,rjobs):
        '''Resubmit the master job to the grid'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        mt = self.middleware.upper()

        job = self.getJobObject()

        ick = False
        if not config['%s_ENABLE' % mt]:
            #logger.warning('Operations of %s middleware are disabled.' % mt)
            #ick = False
            raise GangaException('Operations of %s middleware not enabled' % mt)
        else:
            if mt == 'EDG':
                ick = IBackend.master_resubmit(self,rjobs)

            if mt == 'GLITE':
                if not job.master and len(job.subjobs) == 0:
                    # case 1: master job normal resubmission
                    logger.debug('rjobs: %s' % str(rjobs))
                    logger.debug('mode: master job normal resubmission')
                    ick = IBackend.master_resubmit(self,rjobs)

                elif job.master:
                    # case 2: individual subjob resubmission
                    logger.debug('mode: individual subjob resubmission')
                    status = IBackend.master_resubmit(self,rjobs)
                    if status:
                        # set the backend flag to 1 if the job is individually submitted
                        # the monitoring loop on the master job shouldn't taken into account this job
                        job.backend.flag = 1
                    ick = status

                else:
                    # case 3: master job bulk resubmission
                    logger.debug('mode: master job bulk resubmission')
                    ick = self.master_bulk_resubmit(rjobs)
                    if not ick:
                        raise GangaException('GLITE bulk submission failure')

        profiler.check('job re-submission elapsed time')

        return ick

    def master_kill(self):
        '''kill the master job to the grid'''
        mt = self.middleware.upper()

        job = self.getJobObject()

        if mt == 'EDG':
            return IBackend.master_kill(self)

        if mt == 'GLITE':
            if not job.master and len(job.subjobs) == 0:
                return IBackend.master_kill(self)
            elif job.master:
                #logger.warning('Killing individual subjob in GLITE middleware is an experimental function.')
                return IBackend.master_kill(self)
            else:
                return self.master_bulk_kill()

    def master_bulk_submit(self,rjobs,subjobconfigs,masterjobconfig):
        '''GLITE bulk submission'''

        from Ganga.Core import IncompleteJobSubmissionError
        from Ganga.Utility.logging import log_user_exception

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        assert(implies(rjobs,len(subjobconfigs)==len(rjobs)))

        mt = self.middleware.upper()

        # prepare the master job (i.e. create shared inputsandbox, etc.)
        master_input_sandbox=IBackend.master_prepare(self,masterjobconfig)

        job = self.getJobObject()

        # create jdl repository in master job's inputdir for storing subjobs' jdls
        #jdlrepos = os.path.join(job.inputdir,'jdlrepos')
        #if os.path.exists(jdlrepos):
        #    grids[mt].shell.system('rm -rf %s' % jdlrepos)
        #os.makedirs(jdlrepos)

        # prepare the subjobs, jdl repository before bulk submission
        node_jdls = []
        for sc,sj in zip(subjobconfigs,rjobs):
            try:
                logger.debug("preparing subjob %s" % sj.getFQID('.'))
                jdlpath = sj.backend.preparejob(sc,master_input_sandbox)
                node_jdls.append(jdlpath)
                #os.symlink(jdlpath,os.path.join(jdlrepos,os.path.basename(jdlpath)+'.'+str(sj.id)))
            except Exception,x:
                log_user_exception()
                raise IncompleteJobSubmissionError(sj.id,str(x))

        # use --collection to submit jobs 
        #master_jid = grids[mt].native_master_submit(jdlrepos,self.CE)

        # compose master JDL for collection job
        inpw = job.getInputWorkspace()
        master_jdl = inpw.writefile(FileBuffer('__jdlfile__', self.__make_collection_jdl(node_jdls)))

        profiler.checkAndStart('job preparation elapsed time')

        # set all subjobs to submitting status
        for sj in rjobs:
            sj.updateStatus('submitting')

        profiler.checkAndStart('job state transition (submitting) elapsed time')

        master_jid = grids[mt].submit(master_jdl,ce=None)

        if not master_jid:
            logger.error('Job submission failed')
            return False

        self.id = master_jid

        # set all subjobs to submitted status (a temporary workaround for Ganga 4)
        # NOTE: this is just a workaround to avoid the unexpected transition
        #       that turns the master job's status from 'submitted' to 'submitting'.
        #       As this transition should be allowed to simulate a lock mechanism in Ganga 4, the workaround
        #       is to set all subjob's status to 'submitted' so that the transition can be avoided.
        #       A more clear solution should be implemented with the lock mechanism introduced in Ganga 5.  
        for sj in rjobs:
            sj.updateStatus('submitted')
            sj.info.submit_counter += 1

        profiler.checkAndStart('job state transition (submitting) elapsed time')

        # update the subjob information right after the submission
        #self.master_bulk_updateMonitoringInformation([job],True)

        #profiler.checkAndStart('job info update elapsed time')
        
        #for sj in rjobs:
        #    sj.updateStatus('submitted')
        #    sj.info.submit_counter += 1
        #profiler.checkAndStart('job state transition (submitted) elapsed time')

        return True

    def master_bulk_resubmit(self,rjobs):
        '''GLITE bulk resubmission'''

        from Ganga.Core import IncompleteJobSubmissionError
        from Ganga.Utility.logging import log_user_exception
        mt  = self.middleware.upper()

        job = self.getJobObject()

        # use --collection to submit jobs 
        #jdlrepos = os.path.join(job.inputdir,'jdlrepos')
        #master_jid = grids[mt].native_master_submit(jdlrepos,self.CE)

        # compose master JDL for collection job
        node_jdls = []
        for sj in rjobs:
            jdlpath = os.path.join(sj.inputdir,'__jdlfile__')
            node_jdls.append(jdlpath)

        inpw = job.getInputWorkspace()
        master_jdl = inpw.writefile(FileBuffer('__jdlfile__', self.__make_collection_jdl(node_jdls)))
        master_jid = grids[mt].submit(master_jdl,ce=None)

        if not master_jid:
            logger.error('Job resubmission failed')
            return False
            #raise IncompleteJobSubmissionError(job.id,'native master job submission failed.')

        self.id = master_jid
        self.__refresh_jobinfo__(job)
        for sj in rjobs:
            sj.backend.id = None
            self.__refresh_jobinfo__(sj)
            sj.updateStatus('submitting')

        # set all subjobs to submitted status
        # NOTE: this is just a workaround to avoid the unexpected transition
        #       that turns the master job's status from 'submitted' to 'submitting'.
        #       As this transition should be allowed to simulate a lock mechanism in Ganga 4, the workaround
        #       is to set all subjobs' status to 'submitted' so that the transition can be avoided.
        #       A more clear solution should be implemented with the lock mechanism introduced in Ganga 5.  
        for sj in rjobs:
            sj.updateStatus('submitted')
            sj.info.submit_counter += 1

        return True

    def master_bulk_kill(self):
        '''GLITE bulk resubmission'''

        job = self.getJobObject()
        mt  = self.middleware.upper()

        ## killing the individually re-submitted subjobs
        logger.debug('cancelling individually resubmitted subjobs.')

        ## 1. collect job ids 
        ids = []
        for sj in job.subjobs:
            if sj.backend.flag == 1 and sj.status in ['submitted','running']:
                ids.append(sj.backend.id)

        ## 2. cancel the collected jobs
        ck = grids[mt].cancelMultiple(ids)
        if not ck:
            logger.warning('Job cancellation failed')
            return False
        else:
            for sj in job.subjobs:
                if sj.backend.flag == 1 and sj.status in ['submitted','running']:
                    sj.updateStatus('killed')
        
        ## killing the master job
        logger.debug('cancelling the master job.')
        ck = grids[mt].native_master_cancel(self.id)

        if not ck:
            logger.warning('Job cancellation failed: %s' % self.id)
            return False
        else:
            for sj in job.subjobs:
                if sj.backend.flag != 1 and sj.status in ['submitted','running']:
                    sj.updateStatus('killed')
            return True

    def fupload(self,localFilePath,maxRetry=3):
        '''Upload a localfile to the Grid'''

        # make sure the localFilePath is represented as file://...
        localFilePath = os.path.abspath(localFilePath.replace('file://',''))
        if not os.path.exists(localFilePath):
            return None

        localFilePath = 'file://' + localFilePath

        return grids[self.middleware.upper()].lcg_file_upload(self.iocache,localFilePath,maxRetry)

    def fdownload(self,src,localFilePath,maxRetry=3):
        '''Download a file on the Grid to the localfile storage'''

        # make sure the localFilePath is represented as file://...
        localFilePath = os.path.abspath(localFilePath.replace('file://',''))
        if os.path.exists(localFilePath):
            os.rename(localFilePath,localFilePath+'.bak')

        localFilePath = 'file://' + os.path.abspath(localFilePath.replace('file://',''))

        return grids[self.middleware.upper()].lcg_file_download(self.iocache,src,localFilePath,maxRetry)

    def fdelete(self,uri,lfc,maxRetry=3):
        '''Delete all replicas of a Grid file corresponding to the given guid'''

        # cache the current setting of LFC_HOST
        lfc_back = grids[self.middleware.upper()].shell.env['LFC_HOST']

        # set to the given lfc 
        grids[self.middleware.upper()].shell.env['LFC_HOST'] = lfc

        # perform job deletion 
        ick = grids[self.middleware.upper()].lcg_file_delete(self.iocache,uri,maxRetry)

        # roll back the LFC_HOST setting 
        grids[self.middleware.upper()].shell.env['LFC_HOST'] = lfc_back

        return ick

    def loginfo(self,verbosity=1):
        """Get the job's logging info"""

        job = self.getJobObject()

        logger.debug('Getting logging info of job %s' % job.getFQID('.'))

        mt = self.middleware.upper()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return None 

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return None 

        # successful logging info fetching returns a file path to the information
        loginfo_output = grids[self.middleware.upper()].get_loginfo(self.id,job.outputdir,verbosity)

        if loginfo_output:

            # returns the name of the file where the logging info is saved
            return loginfo_output

            #f = open(loginfo_output,'r')
            #info = map(lambda x:x.strip(),f.readlines())
            #f.close()
            # returns the logging info as an array of strings
            #return info 
        else:
            logger.debug('Getting logging info of job %s failed.' % job.getFQID('.'))
            return None 

    def submit(self,subjobconfig,master_job_sandbox):
        '''Submit the job to the grid'''

        mt = self.middleware.upper()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return None

        jdlpath = self.preparejob(subjobconfig,master_job_sandbox)
        # If GLITE, tell it whether to enable perusal
        if mt=="GLITE":
            grids[mt].perusable=self.perusable
        
        self.id = grids[mt].submit(jdlpath,self.CE)
        return not self.id is None

    def resubmit(self):
        '''Resubmit the job'''
        job = self.getJobObject()
      
        mt = self.middleware.upper()
        self.id = grids[mt].submit(job.getInputWorkspace().getPath("__jdlfile__"),self.CE)

        if self.id:
            # refresh the lcg job information
            self.__refresh_jobinfo__(job)

        return not self.id is None

    def kill(self):
        '''Kill the job'''

        job   = self.getJobObject()

        logger.info('Killing job %s' % job.getFQID('.'))

        mt = self.middleware.upper()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return False 

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return False

        return grids[self.middleware.upper()].cancel(self.id)

    def __jobWrapperTemplate__(self):
        '''Create job wrapper'''

        script = """#!/usr/bin/env python
#-----------------------------------------------------
# This job wrapper script is automatically created by
# GANGA LCG backend handler.
#
# It controls:
# 1. unpack input sandbox
# 2. invoke application executable
# 3. invoke monitoring client
#-----------------------------------------------------
import os,os.path,shutil,tempfile
import sys,popen2,time,traceback

#bugfix #36178: subprocess.py crashes if python 2.5 is used
#try to import subprocess from local python installation before an
#import from PYTHON_DIR is attempted some time later
try:
    import subprocess 
except ImportError:
    pass

## Utility functions ##
def timeString():
    return time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time()))

def printInfo(s):
    out.write(timeString() + '  [Info]' +  ' ' + str(s) + os.linesep)
    out.flush()

def printError(s):
    out.write(timeString() + ' [Error]' +  ' ' + str(s) + os.linesep)
    out.flush()

def lcg_file_download(vo,guid,localFilePath,timeout=60,maxRetry=3):
    cmd = 'lcg-cp -t %d --vo %s %s file://%s' % (timeout,vo,guid,localFilePath)

    printInfo('LFC_HOST set to %s' % os.environ['LFC_HOST'])

    i         = 0
    rc        = 0
    isDone    = False
    try_again = True

    while try_again:
        i = i + 1
        try:
            ps = os.popen(cmd)
            ps.close()
            isDone = True
        except IOError:
            printError("Download file %s from iocache failed ... trial %d." % (os.path.basename(localFilePath),i))
            isDone = False

        if isDone:
            try_again = False
        elif i == maxRetry:
            try_again = False
        else:
            try_again = True

    return isDone


## system command executor with subprocess
def execSyscmdSubprocess(cmd, wdir=os.getcwd()):

    import os, subprocess

    global exitcode

    outfile   = file('stdout','w') 
    errorfile = file('stderr','w') 

    try:
        child = subprocess.Popen(cmd, cwd=wdir, shell=True, stdout=outfile, stderr=errorfile)

        while 1:
            exitcode = child.poll()
            if exitcode is not None:
                break
            else:
                outfile.flush()
                errorfile.flush()
                monitor.progress()
                time.sleep(0.3)
    finally:
        monitor.progress()

    outfile.flush()
    errorfile.flush()
    outfile.close()
    errorfile.close()

    return True

## system command executor with multi-thread
## stderr/stdout handler
def execSyscmdEnhanced(cmd, wdir=os.getcwd()):

    import os, threading

    cwd = os.getcwd()

    isDone = False

    try:
        ## change to the working directory
        os.chdir(wdir)

        child = popen2.Popen3(cmd,1)
        child.tochild.close() # don't need stdin
 
        class PipeThread(threading.Thread):
 
            def __init__(self,infile,outfile,stopcb):
                self.outfile = outfile
                self.infile = infile
                self.stopcb = stopcb
                self.finished = 0
                threading.Thread.__init__(self)
 
            def run(self):
                stop = False
                while not stop:
                    buf = self.infile.read(10000)
                    self.outfile.write(buf)
                    self.outfile.flush()
                    time.sleep(0.01)
                    stop = self.stopcb()
                #FIXME: should we do here?: self.infile.read()
                #FIXME: this is to make sure that all the output is read (if more than buffer size of output was produced)
                self.finished = 1

        def stopcb(poll=False):
            global exitcode 
            if poll:
                exitcode = child.poll()
            return exitcode != -1

        out_thread = PipeThread(child.fromchild, sys.stdout, stopcb)
        err_thread = PipeThread(child.childerr, sys.stderr, stopcb)

        out_thread.start()
        err_thread.start()
        while not out_thread.finished and not err_thread.finished:
            stopcb(True)
            monitor.progress()
            time.sleep(0.3)
        monitor.progress()

        sys.stdout.flush()
        sys.stderr.flush()

        isDone = True

    except(Exception,e):
        isDone = False

    ## return to the original directory
    os.chdir(cwd)

    return isDone

############################################################################################

###INLINEMODULES###

############################################################################################

## Main program ##

outputsandbox = ###OUTPUTSANDBOX###
input_sandbox = ###INPUTSANDBOX###
wrapperlog = ###WRAPPERLOG###
appexec = ###APPLICATIONEXEC###
appargs = ###APPLICATIONARGS###

exitcode=-1

import sys, stat
wdir = os.getcwd()
sys.path.insert(0,os.path.join(wdir,PYTHON_DIR))
os.environ['PATH'] = '.:'+os.environ['PATH']

vo = os.environ['GANGA_LCG_VO']

out = open('%s' % wrapperlog,'w')

try:
    printInfo('Job Wrapper start.')

#   download inputsandbox from remote cache
    for f,guid in input_sandbox['remote'].iteritems():
        if not lcg_file_download(vo,guid,os.path.join(wdir,f)):
            raise Exception('Download remote input %s:%s failed.' % (guid,f) )
        else:
            getPackedInputSandbox(f)

    printInfo('Download inputsandbox from iocache passed.')

#   unpack inputsandbox from wdir
    for f in input_sandbox['local']:
        getPackedInputSandbox(f)

    printInfo('Unpack inputsandbox passed.')

    printInfo('Loading Python modules ...')

    # check the python library path 
    try: 
        printInfo(' ** PYTHON_DIR: %s' % os.environ['PYTHON_DIR'])
    except KeyError:
        pass

    try: 
        printInfo(' ** PYTHONPATH: %s' % os.environ['PYTHONPATH'])
    except KeyError:
        pass

    for lib_path in sys.path:
        printInfo(' ** sys.path: %s' % lib_path)

    ###MONITORING_SERVICE###
    monitor = createMonitoringObject()
    monitor.start()

#   execute application
    try: #try to make shipped executable executable
        os.chmod('%s/%s'% (wdir,appexec),stat.S_IXUSR|stat.S_IRUSR|stat.S_IWUSR)
    except:
        pass

    status = False
    try:
        # use subprocess to run the user's application if the module is available on the worker node
        import subprocess
        printInfo('Load application executable with subprocess module')
        status = execSyscmdSubprocess('%s %s' % (appexec,appargs))
    except ImportError,err:
        # otherwise, use separate threads to control process IO pipes 
        printInfo('Load application executable with separate threads')
        status = execSyscmdEnhanced('%s %s' % (appexec,appargs))

    printInfo('GZipping stdout and stderr...')
    os.system("gzip stdout stderr") 

    if not status:
        raise Exception('Application execution failed.')
    printInfo('Application execution passed with exit code %d.' % exitcode)

    createPackedOutputSandbox(outputsandbox,None,wdir)

#   pack outputsandbox
#    printInfo('== check output ==')
#    for line in os.popen('pwd; ls -l').readlines():
#        printInfo(line)

    printInfo('Pack outputsandbox passed.')
    monitor.stop(exitcode)
    
except Exception,e:
    printError(sys.exc_info()[0])
    printError(sys.exc_info()[1])
    str_traceback = traceback.format_tb(sys.exc_info()[2])
    for str_tb in str_traceback:
        printError(str_tb)

printInfo('Job Wrapper stop.')

out.close()

# always return exit code 0 so the in the case of application failure
# one can always get stdout and stderr back to the UI for debug. 
sys.exit(0)
"""
        return script;

    def peek(self, filename='', command=''):
        """
        Allow peeking of this job's stdout on the WN
        (i.e. while job is in 'running' state)

        Return value: None
        """
        if filename and filename != 'stdout':
            logger.warning('Arbitrary file peeking not supported for a running LCG job')
        else:
            self.inspect(command)

    def inspect( self, cmd=None):
        """
        Allow viewing of this job's stdout on the WN
        (i.e. while job is in 'running' state)
                                                                                  
        Return value: None
        """

        job = self.getJobObject()

        # Use GLITE's job perusal feature if enabled
        if self.middleware.upper()=="GLITE" and self.status=="Running" and self.perusable:
            fname = os.path.join(job.outputdir,'_peek.dat')
            #f    = open(fname,'w')

            sh=grids[self.middleware.upper()].shell
            re, output, m=sh.cmd("glite-wms-job-perusal --get --noint --all -f stdout %s" % self.id, fname)
            job.viewFile(fname,cmd)
        
            return None

        remoteFile = None
      
        ## create new connection if not connection is made
        checkOctopus = False

        if not self.octopus:
            s = ''
            p = ''
            c = 0L
            try:
                s = self.monInfo['octopus_server']
                p = self.monInfo['octopus_port']
                c = self.monInfo['channel']
             
                self.octopus = Octopus(s, p)
                self.octopus.join(c)
             
                checkOctopus = True 

            except (TypeError,KeyError):
                logger.warning('Octopus monitoring service not enabled at submission time')

            except ProtocolException, pe:
                logger.warning('Octopus connection error: %s' % pe.__str__())

        ## do nothing if skipOtcopus is True
        if not checkOctopus:
            return None
      
        ## detect which remote file is going to be inspected 
        if not remoteFile:
            remoteFile = self.monInfo['remotefile']

        ## send reset to octopus server if the remot file is changed 
        if remoteFile != self.monInfo['remotefile']:
            self.octopus.reset()
            self.monInfo['remotefile'] = remoteFile
            self.octopus.send('set ' + remoteFile + '\n')
            logger.debug('reset target file for inspection: %s',remoteFile)

        logger.debug('inspecting file: %s',remoteFile)

        ## pick up data from the server 
        size_picked = 0
        read_cnt    = 0
        data = ''

        ## temporary file for storing the picked data
        ## the given command will be operated on the temporary file
        #tmpf = tempfile.mktemp('.tmp', '_inspect_%s_' % remoteFile, job.outputdir)

        tmpf = os.path.join(job.outputdir,'_%s_peek.dat' % remoteFile)
        f    = open(tmpf,'w')

        if not self.octopus.eotFound:
            try:
                data = self.octopus.read()
                read_cnt += 1
                while len(data) > 0:
                    logger.debug('read count:%d\tlength of data:%d' % (read_cnt,len(data)))
                    f.write(data)
                    size_picked += len(data)

                    data = ''
                    if not self.octopus.eotFound:
                        data = self.octopus.read()
                        read_cnt += 1
                    else:
                        logger.debug('end of data read')
            
            except socket.error, e:
                if e[0] != errno.EAGAIN:
                    logger.warning('socket error: %s' % e)

            except IOError, (errno, strerror):
                logger.warning("I/O error(%s): %s" % (errno, strerror))
        else:
            logger.debug('end of data read')

        ## close the opened temporary file
        f.close()

        ## close the octopus connection
        if self.octopus:
            self.octopus.close()
            self.octopus = None

        ## performing the command on the temporary file
        if size_picked:
           if not cmd:
               cmd = ''

           job.viewFile(tmpf,cmd)

        return None

    def peek(self, filename='', command=''):
        """
        Allow peeking of this job's stdout on the WN
        (i.e. while job is in 'running' state)

        Return value: None
        """
        if filename and filename != 'stdout':
            logger.warning('Arbitrary file peeking not supported for a running LCG job')
        else:
            self.inspect(command)

    def inspect( self, cmd=None):
        """
        Allow viewing of this job's stdout on the WN
        (i.e. while job is in 'running' state)
                                                                                  
        Return value: None
        """

        job = self.getJobObject()

        # Use GLITE's job perusal feature if enabled
        if self.middleware.upper()=="GLITE" and self.status=="Running" and self.perusable:
            fname = os.path.join(job.outputdir,'_peek.dat')
            #f    = open(fname,'w')

            sh=grids[self.middleware.upper()].shell
            re, output, m=sh.cmd("glite-wms-job-perusal --get --noint --all -f stdout %s" % self.id, fname)
            job.viewFile(fname,cmd)
        
            return None

        remoteFile = None
      
        ## create new connection if not connection is made
        checkOctopus = False

        if not self.octopus:
            s = ''
            p = ''
            c = 0L
            try:
                s = self.monInfo['octopus_server']
                p = self.monInfo['octopus_port']
                c = self.monInfo['channel']
             
                self.octopus = Octopus(s, p)
                self.octopus.join(c)
             
                checkOctopus = True 

            except (TypeError,KeyError):
                logger.warning('Octopus monitoring service not enabled at submission time')

            except ProtocolException, pe:
                logger.warning('Octopus connection error: %s' % pe.__str__())

        ## do nothing if skipOtcopus is True
        if not checkOctopus:
            return None
      
        ## detect which remote file is going to be inspected 
        if not remoteFile:
            remoteFile = self.monInfo['remotefile']

        ## send reset to octopus server if the remot file is changed 
        if remoteFile != self.monInfo['remotefile']:
            self.octopus.reset()
            self.monInfo['remotefile'] = remoteFile
            self.octopus.send('set ' + remoteFile + '\n')
            logger.debug('reset target file for inspection: %s',remoteFile)

        logger.debug('inspecting file: %s',remoteFile)

        ## pick up data from the server 
        size_picked = 0
        read_cnt    = 0
        data = ''

        ## temporary file for storing the picked data
        ## the given command will be operated on the temporary file
        #tmpf = tempfile.mktemp('.tmp', '_inspect_%s_' % remoteFile, job.outputdir)

        tmpf = os.path.join(job.outputdir,'_%s_peek.dat' % remoteFile)
        f    = open(tmpf,'w')

        if not self.octopus.eotFound:
            try:
                data = self.octopus.read()
                read_cnt += 1
                while len(data) > 0:
                    logger.debug('read count:%d\tlength of data:%d' % (read_cnt,len(data)))
                    f.write(data)
                    size_picked += len(data)

                    data = ''
                    if not self.octopus.eotFound:
                        data = self.octopus.read()
                        read_cnt += 1
                    else:
                        logger.debug('end of data read')
            
            except socket.error, e:
                if e[0] != errno.EAGAIN:
                    logger.warning('socket error: %s' % e)

            except IOError, (e, strerror):
                logger.warning("I/O error(%s): %s" % (e, strerror))
        else:
            logger.debug('end of data read')

        ## close the opened temporary file
        f.close()

        ## close the octopus connection
        if self.octopus:
            self.octopus.close()
            self.octopus = None

        ## performing the command on the temporary file
        if size_picked:
           if not cmd:
               cmd = ''

           job.viewFile(tmpf,cmd)

        return None

    def cleanup_iocache(self):
        '''Clean up the uploaded files in remote cache'''
        job = self.getJobObject()

        if job.master is None:
            clog = os.path.join(job.inputdir,'__iocache__')
        else:
            clog = os.path.join(job.master.inputdir,'__iocache__')

        remoteFiles = {}
        if os.path.exists(clog):
            ## read-in the remote files on the iocache
            f = open(clog,'r')
            for line in f.readlines():
                myguid,mylfc,myfname,mymd5 = line.strip().split()
                remoteFiles[myguid] = [myfname,mymd5,mylfc]
            f.close()

            ## cleanup the remote cache when job is running is dangerous
            if len(remoteFiles.keys()) > 0 and not job.status in ['completed','failed']:
                logger.warning("Job haven't reached final status: removing files from remote cache may cause a failed job.")
                return False

            ## try to remove them one-by-one
            ## the files failed to be removed will be kept in the same filea 
            f = open(clog,'w')
            fct = 0
            for myguid in remoteFiles.keys():
                if not self.fdelete(myguid,remoteFiles[myguid][2]):
                    fct = fct + 1
                    f.write(myguid + '\t' + remoteFiles[myguid][2] + '\t' + remoteFiles[myguid][0] + '\t' + remoteFiles[myguid][1] + '\n')
            f.close()

            if fct > 0:
                logger.warning("Not all remote files were successfully removed from the iocache.")
                return False
            else:
                return True
        else:
            return True

    def preparejob(self,jobconfig,master_job_sandbox):
        '''Prepare the JDL'''

        ## inner method for creating md5sum of files
        def __md5sum__(fname):
            f = open(fname, 'rb')
            m = md5.new()
            while True:
                d = f.read(8096)
                if not d:
                    break
                m.update(d)
            f.close()
            return m.hexdigest()

        script = self.__jobWrapperTemplate__()

        job = self.getJobObject()
        inpw = job.getInputWorkspace()
        
        wrapperlog = '__jobscript__.log'

        import Ganga.Core.Sandbox as Sandbox
        
        script = script.replace('###OUTPUTSANDBOX###',repr(jobconfig.outputbox)) #FIXME: check what happens if 'stdout','stderr' are specified here

        script = script.replace('###APPLICATION_NAME###',job.application._name)
        script = script.replace('###APPLICATIONEXEC###',repr(jobconfig.getExeString()))
        script = script.replace('###APPLICATIONARGS###',repr(jobconfig.getArguments()))
        script = script.replace('###WRAPPERLOG###',repr(wrapperlog))
        import inspect
        script = script.replace('###INLINEMODULES###',inspect.getsource(Sandbox.WNSandbox))

        mon = job.getMonitoringService()

        # catch the monitoring service information of OctopusMS
        if mon.getJobInfo().has_key('Ganga.Lib.MonitoringServices.Octopus.OctopusMS.OctopusMS'):
            self.monInfo = mon.getJobInfo()['Ganga.Lib.MonitoringServices.Octopus.OctopusMS.OctopusMS']
        else:
            self.monInfo = None

        # set the monitoring file by default to the stdout
        if type(self.monInfo) is type({}):
            self.monInfo['remotefile'] = 'stdout'

        # try to print out the monitoring service information in debug mode
        try:
            logger.debug('job info of monitoring service: %s' % str(self.monInfo))
        except:
            pass

        script = script.replace('###MONITORING_SERVICE###',mon.getWrapperScriptConstructorText())

#       prepare input/output sandboxes
        packed_files = jobconfig.getSandboxFiles()+Sandbox.getGangaModulesAsSandboxFiles(Sandbox.getDefaultModules()) \
	+ Sandbox.getGangaModulesAsSandboxFiles(mon.getSandboxModules())
        sandbox_files = job.createPackedInputSandbox(packed_files)

        ## sandbox of child jobs should include master's sandbox
        sandbox_files.extend(master_job_sandbox)

        ## check the input file size and pre-upload larger inputs to the iocache
        inputs = {'remote':{},'local':[]}

        if job.master is None:
            clog = os.path.join(job.inputdir,'__iocache__')
        else:
            clog = os.path.join(job.master.inputdir,'__iocache__')

        ## read-in the previously uploaded files 
        uploadedFiles = {} 
        if os.path.exists(clog):
            c = open(clog,'r')
            for line in c.readlines():
                myguid,mylfc,myfname,mymd5 = line.strip().split()
                uploadedFiles[mymd5] = [myguid,myfname,mylfc]
            c.close()

        ## pre-upload oversized input sandbox to iocache
        ##  - if the file has been previously uploaded, 
        ##    just take the uploaded one and ignore the pre-upload

        ## get the environment variable LFC_HOST 
        lfc_host = grids[self.middleware.upper()].shell.env['LFC_HOST']

        c = open(clog,'a')
        for f in sandbox_files:
            abspath = os.path.abspath(f)
            if os.path.getsize(abspath) > config['BoundSandboxLimit']:
                md5sum  = __md5sum__(abspath)
                if not uploadedFiles.has_key(md5sum):
                    logger.warning('The size of %s is larger than the sandbox limit (%d byte). Please wait while pre-staging ...' % (f,config['BoundSandboxLimit']) )
                    guid = self.fupload(abspath)
                    if not guid: # file upload to iocache failed
                        return
                    else:
                        c.write(guid + '\t' + lfc_host  + '\t' + os.path.basename(f) + '\t' + md5sum + '\n')
                        inputs['remote'][os.path.basename(f)] = guid
                else:
                    # the same file has been uploaded to the iocache
                    # TODO: send the lfc_host as well
                    inputs['remote'][os.path.basename(f)] = uploadedFiles[md5sum][0]
            else:
                inputs['local'].append(abspath)
        c.close()
       
        ## update the job wrapper with the inputsandbox list
        script = script.replace('###INPUTSANDBOX###',repr({'remote':inputs['remote'],'local':[ os.path.basename(f) for f in inputs['local'] ]}))

        ## write out the job wrapper and put job wrapper into job's inputsandbox
        scriptPath = inpw.writefile(FileBuffer('__jobscript_%s__' % job.getFQID('.'),script),executable=1)
        input_sandbox  = inputs['local'] + [scriptPath]

        ## compose output sandbox to include by default the following files:
        ##  - stdout
        ##  - stderr
        ##  - __jobscript__.log (job wrapper's log)
        output_sandbox = ['stdout.gz','stderr.gz',wrapperlog]
        if len(jobconfig.outputbox):
            output_sandbox += [Sandbox.OUTPUT_TARBALL_NAME]

        ## compose LCG JDL
        jdl = {
            'VirtualOrganisation' : config['VirtualOrganisation'],
            'Executable' : os.path.basename(scriptPath),
            'Environment': {'GANGA_LCG_VO': config['VirtualOrganisation'], 'LFC_HOST': lfc_host},
            'StdOutput' : 'stdout',
            'StdError' : 'stderr',
            'InputSandbox' : input_sandbox,
            'OutputSandbox' : output_sandbox
        }

        if self.middleware.upper() == 'GLITE' and self.perusable:
            logger.debug("Adding persual info to JDL")
            # remove the ExpiryTime attribute as it's absolute timestamp that will cause the re-submitted job being
            # ignored by the WMS. TODO: fix it in a better way.
            # jdl['ExpiryTime'] = time.time() + config['JobExpiryTime']
            jdl['PerusalFileEnable']='true'
            jdl['PerusalTimeInterval']=120

        if self.CE:
            jdl['Requirements'] = ['other.GlueCEUniqueID=="%s"' % self.CE ]
            # send the CE name as an environmental variable of the job if CE is specified
            # this is basically for monitoring purpose
            jdl['Environment'].update({'GANGA_LCG_CE': self.CE})
        else:
            jdl['Requirements'] = self.requirements.merge(jobconfig.requirements).convert()
#           input data
            if jobconfig.inputdata:
                jdl['InputData'] = jobconfig.inputdata
                jdl['DataAccessProtocol'] = [ 'gsiftp' ]

        if self.jobtype.upper() in ['MPICH','NORMAL','INTERACTIVE']:
            jdl['JobType'] = self.jobtype.upper()
            if self.jobtype.upper() == 'MPICH':
                jdl['Requirements'].append('(other.GlueCEInfoTotalCPUs >= NodeNumber)')
                jdl['Requirements'].append('Member("MPICH",other.GlueHostApplicationSoftwareRunTimeEnvironment)')
                jdl['NodeNumber'] = self.requirements.nodenumber
        else:
            logger.warning('JobType "%s" not supported' % self.jobtype)
            return

#       additional settings from the job
        if jobconfig.env:
            jdl['Environment'].update(jobconfig.env)

#       the argument of JDL should be the argument for the wrapper script
#       application argument has been put into the wrapper script
#        if jobconfig.args: jdl['Arguments'] = jobconfig.getArguments()

#       additional settings from the configuration
        ## !!note!! StorageIndex is not defined in EDG middleware
        for name in [ 'ShallowRetryCount','RetryCount', 'Rank', 'ReplicaCatalog', 'StorageIndex', 'MyProxyServer' ]:
            if config[name]:
                jdl[name] = config[name]

        jdlText = Grid.expandjdl(jdl)
        logger.debug('subjob JDL: %s' % jdlText)
        return inpw.writefile(FileBuffer('__jdlfile__',jdlText))

    def updateGangaJobStatus(job,status):
        '''map backend job status to Ganga job status'''

        if status == 'Running':
            job.updateStatus('running')
     
        elif status == 'Done (Success)':
            job.updateStatus('completed')
      
        elif status in ['Aborted','Cancelled','Done (Exit Code !=0)']:
            job.updateStatus('failed') 
     
        elif status == 'Cleared':
            if job.status in ['completed','failed']:
                #logger.warning('Monitoring loop should not have been called for job %d as status is already %s',job.id,job.status)
                return 
            logger.warning('The job %d has reached unexpected the Cleared state and Ganga cannot retrieve the output.',job.id)
            job.updateStatus('failed')
     
        elif status in ['Submitted','Waiting','Scheduled','Ready','Done (Failed)']:
            pass
     
        else:
            logger.warning('Unexpected job status "%s"',info['status'])

    updateGangaJobStatus = staticmethod(updateGangaJobStatus)

    def master_updateMonitoringInformation(jobs):
        '''Main Monitoring loop'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        emulated_bulk_jobs = []
        native_bulk_jobs   = []

        for j in jobs:

            mt = j.backend.middleware.upper()

            if mt == 'EDG' or len(j.subjobs) == 0:
                emulated_bulk_jobs.append(j)
            else:
                native_bulk_jobs.append(j)
                # put the individually submitted subjobs into the emulated_bulk_jobs list
                # those jobs should be checked individually as a single job
                for sj in j.subjobs:
                    if sj.backend.flag == 1 and sj.status in ['submitted','running']:
                        logger.debug('job %s submitted individually. separate it in a different monitoring loop.' % sj.getFQID('.'))
                        emulated_bulk_jobs.append(sj)

        # involk normal monitoring method for normal jobs
        IBackend.master_updateMonitoringInformation(emulated_bulk_jobs)

        # involk special monitoring method for glite bulk jobs
        LCG.master_bulk_updateMonitoringInformation(native_bulk_jobs)

        profiler.check('==> master_updateMonitoringInformation() elapsed time')

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

    def updateMonitoringInformation(jobs):
        '''Monitoring loop for normal jobs'''
      
        jobdict   = dict([ [job.backend.id,job] for job in jobs if job.backend.id ])

        ## divide jobs into classes based on the middleware type
        jobclass  = {}
        for key in jobdict:
            mt = jobdict[key].backend.middleware.upper()
            if not jobclass.has_key(mt):
                jobclass[mt] = [key]
            else:
                jobclass[mt].append(key)

        ## loop over the job classes 
        for mt in jobclass.keys():

            if not config['%s_ENABLE' % mt]:
                continue 

            ## loop over the jobs in each class
            for info in grids[mt].status(jobclass[mt]):

                job = jobdict[info['id']]
         
                if job.backend.actualCE != info['destination']:
                    logger.info('job %s has been assigned to %s',job.getFQID('.'),info['destination'])
                    job.backend.actualCE = info['destination']
        
                if job.backend.status != info['status']:
                    logger.info('job %s has changed status to %s',job.getFQID('.'),info['status'])
                    job.backend.status = info['status']
                    job.backend.reason = info['reason']
                    job.backend.exitcode_lcg = info['exit']

                    pps_check = (True,None)

                    # postprocess of getting job output if the job is done
                    if info['status'] == 'Done (Success)' and job.status != 'completed':

                        # update to 'running' before changing to 'completing'
                        if job.status == 'submitted':
                            job.updateStatus('running')

                        job.updateStatus('completing')
                        outw = job.getOutputWorkspace()
                        pps_check = grids[mt].get_output(job.backend.id,outw.getPath(),wms_proxy=False)
                
                    if pps_check[0]:
                        LCG.updateGangaJobStatus(job,info['status'])
                        job.backend.exitcode = 0
                    else:
                        job.updateStatus("failed")
                        # update the backend's reason if the failure detected in the Ganga's pps 
                        if pps_check[1] != 0:
                            job.backend.reason = 'non-zero app. exit code: %s' % pps_check[1]
                            job.backend.exitcode = pps_check[1]

    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

    def master_bulk_updateMonitoringInformation(jobs,updateMasterStatus=True):
        '''Monitoring loop for glite bulk jobs'''

        grid = grids['GLITE']

        if not grid:
            return

        jobdict = dict([ [job.backend.id,job] for job in jobs if job.backend.id ])

        job        = None
        subjobdict = {}

        ## make sure all the status information is available
        ## if not ... wait for a while and fetch the status again
        def check_info(status):
            for info in status:
                if info['is_node'] and not info['name']:
                    return False
            return True

        status_info = grid.status(jobdict.keys(),is_collection=True)
        #while not check_info(status_info):
        #    logger.debug('waiting for node job information to be available')
        #    time.sleep(0.1)
        #    status_info = grid.status(jobdict.keys(),is_collection=True)

        ## update GANGA job repository according to the available job information 
        for info in status_info:
            if not info['is_node']: # this is the info for the master job

                job = jobdict[info['id']]

                subjobdict = dict([ [str(subjob.id),subjob] for subjob in job.subjobs ])

                if job.backend.status != info['status']:
                    logger.debug('job %s has changed status to %s',job.getFQID('.'),info['status'])

                job.backend.status = info['status']
                job.backend.reason = info['reason']
                job.backend.exitcode_lcg = info['exit']

            else: # this is the info for the node job

                # subjob's node name is not available 
                if not info['name']: continue
                subjob = subjobdict[info['name'].replace('gsj_','')]

                # skip updating the cleared jobs   
                if info['status'] == 'Cleared' and subjob.status in ['completed','failed']: continue

                # skip updating the jobs that are individually resubmitted after the original bulk submission
                if subjob.backend.flag == 1:
                    logger.debug('job %s was resubmitted individually. skip updating it from the monitoring of its master job.' % subjob.getFQID('.'))
                # skip updating the jobs that are individually killed
                elif subjob.status == 'killed':
                    logger.debug('job %s was killed individually. skip updating it from the monitoring of its master job.' % subjob.getFQID('.'))
                else:
                    if not subjob.backend.id:
                        # send out the subjob's id which is becoming available at the first time.
                        # (a temporary workaround for fixing the monitoring issue of getting the job id)
                        # Note: As the way of sending job id is implemented as an generic hook triggered
                        #       by the transition from 'submitting' to 'submitted'. For gLite bulk submission
                        #       the id is not available immediately right after the submission, therefore a late
                        #       job id transmission is needed.
                        #       This issue linked to the temporary workaround of setting subjob's status to 'submitted'
                        #       in the master_bulk_(re)submit() methods. In Ganga 5, a clear implementation should be
                        #       applied with the new lock mechanism.
                        logger.debug('job %s obtained backend id, transmit it to monitoring service.' % subjob.getFQID('.'))
                        subjob.backend.id = info['id']
                        subjob.getMonitoringService().submit()

                        # in the temporary workaround, there is no need to set job status to 'submitted'
                        #subjob.updateStatus('submitted')
                 
                    if subjob.backend.actualCE != info['destination']:
                        logger.info('subjob %s has been assigned to %s',subjob.getFQID('.'),info['destination'])
                        subjob.backend.actualCE = info['destination']
                 
                    if subjob.backend.status != info['status']:
                        logger.info('subjob %s has changed status to %s',subjob.getFQID('.'),info['status'])
                        subjob.backend.status = info['status']
                        subjob.backend.reason = info['reason']
                        subjob.backend.exitcode_lcg = info['exit']
                        pps_check = (True,None)
                        
                        if info['status'] == 'Done (Success)' and subjob.status != 'completed':
                 
                            # update to 'running' before changing to 'completing'
                            if subjob.status == 'submitted':
                                subjob.updateStatus('running')
                     
                            subjob.updateStatus('completing')
                            outw = subjob.getOutputWorkspace()
                            pps_check = grid.get_output(subjob.backend.id,outw.getPath(),wms_proxy=True)
                    
                        if pps_check[0]:
                            LCG.updateGangaJobStatus(subjob,info['status'])
                        else:
                            subjob.updateStatus('failed')
                            # update the backend's reason if the failure detected in the Ganga's pps 
                            if pps_check[1]:
                                job.backend.reason = 'warning from Ganga: %s' % pps_check[1]

        # update master job status
        if updateMasterStatus:
            for jid in jobdict.keys():
                jobdict[jid].updateMasterJobStatus()

    master_bulk_updateMonitoringInformation = staticmethod(master_bulk_updateMonitoringInformation)

    def check_proxy(self):
        '''Update the proxy'''

        mt = self.middleware.upper()
        return grids[mt].check_proxy()

class LCGJobConfig(StandardJobConfig):
    '''Extends the standard Job Configuration with additional attributes'''
   
    def __init__(self,exe=None,inputbox=[],args=[],outputbox=[],env={},inputdata=[],requirements=None):
   
        self.inputdata=inputdata
        self.requirements=requirements

        StandardJobConfig.__init__(self,exe,inputbox,args,outputbox,env)

    def getArguments(self):
    
        return ' '.join(self.getArgStrings())
    
    def getExecutable(self):
    
        exe=self.getExeString()
        if os.path.dirname(exe) == '.':
            return os.path.basename(exe)
        else:
            return exe

        
# initialisation

# function for parsing VirtualOrganisation from ConfigVO
def __getVOFromConfigVO__(file):
    re_vo = re.compile(r'.*VirtualOrganisation\s*=\s*"(.*)"')
    try:
        f = open(file)
        for l in f.readlines():
            m = re_vo.match(l.strip())
            if m:
                f.close()
                return m.groups()[0]
    except:
        raise Ganga.Utility.Config.ConfigError('ConfigVO %s does not exist.' % file )

# configuration preprocessor : avoid VO switching
def __avoidVOSwitch__(opt,val):

    if not opt in ['VirtualOrganisation','ConfigVO']:
        # bypass everything irrelevant to the VO 
        return val
    elif opt == 'ConfigVO' and val == '':
        # accepting '' to disable the ConfigVO
        return val
    else:
        # try to get current value of VO
        if config['ConfigVO']:
            vo_1 = __getVOFromConfigVO__(config['ConfigVO'])
        else:
            vo_1 = config['VirtualOrganisation']

        # get the VO that the user trying to switch to
        if opt == 'ConfigVO':
            vo_2 = __getVOFromConfigVO__(val)
        else:
            vo_2 = val
 
        # if the new VO is not the same as the existing one, raise ConfigError
        if vo_2 != vo_1:
            raise Ganga.Utility.Config.ConfigError('Changing VirtualOrganisation is not allowed in GANGA session.')

    return val

# configuration preprocessor : enabling middleware 
def __enableMiddleware__(opt,val):

    if opt in ['EDG_ENABLE','GLITE_ENABLE'] and val:
        mt = opt.split('_')[0]
        try:
            if config[opt]:
                logger.info('LCG-%s was already enabled.' % mt)
            else:
                grids[mt] = Grid(mt)
                return grids[mt].active
        except:
            raise Ganga.Utility.Config.ConfigError('Failed to enable LCG-%s.' % mt)

    return val

# configuration preprocessor : disabling middleware 
def __disableMiddleware__(opt,val):

    if opt in ['EDG_ENABLE','GLITE_ENABLE'] and not val:
        mt = opt.split('_')[0]
        grids[mt] = None
        if not config['EDG_ENABLE'] and not config['GLITE_ENABLE']:
            logger.warning('No middleware is enabled. LCG handler is disabled.')

    return

# configuration postprocessor : updating the configuration of the cached Grid objects 
def __updateGridObjects__(opt,val):

    ## update the config binded with the grid objects
    for mt in grids.keys():
        grids[mt].config = getConfig('LCG')
        logger.debug('update grid configuration for %s' % mt)

    ## when user changes the 'DefaultLFC', change the env. variable, LFC_HOST, of the cached grid shells
    if opt == 'DefaultLFC' and val:
        for mt in grids.keys():
            try:
                grids[mt].shell.env['LFC_HOST'] = val
                logger.debug('set env. variable LFC_HOST to %s' % val)
            except:
                pass
    return

# configuration preprocessor 
def __preConfigHandler__(opt,val):
    val = __avoidVOSwitch__(opt,val)
    val = __enableMiddleware__(opt,val)
    return val

# configuration postprocessor 
def __postConfigHandler__(opt,val):
    logger.info('%s has been set to %s' % (opt,val))
    __disableMiddleware__(opt,val)
    __updateGridObjects__(opt,val)
    return

# global variables
logger = getLogger()
config = makeConfig('LCG','LCG/gLite/EGEE configuration parameters')
#gproxy_config = getConfig('GridProxy_Properties')

# set default values for the configuration parameters
config.addOption('EDG_ENABLE',True,'enables/disables the support of the EDG middleware')
config.addOption('EDG_SETUP', '/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh','sets the LCG-UI environment setup script for the EDG middleware')
config.addOption('GLITE_ENABLE', False, 'Enables/disables the support of the GLITE middleware')
config.addOption('GLITE_SETUP', '/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh','sets the LCG-UI environment setup script for the GLITE middleware')

config.addOption('VirtualOrganisation','dteam','sets the name of the grid virtual organisation')
config.addOption('ConfigVO','','sets the VO-specific LCG-UI configuration script for the EDG resource broker')
config.addOption('Config','','sets the generic LCG-UI configuration script for the GLITE workload management system')

config.addOption('AllowedCEs','','sets allowed computing elements by a regular expression')
config.addOption('ExcludedCEs','','sets excluded computing elements by a regular expression')

config.addOption('MyProxyServer','myproxy.cern.ch','sets the myproxy server')
config.addOption('RetryCount',3,'sets maximum number of job retry')
config.addOption('ShallowRetryCount',10,'sets maximum number of job shallow retry')

config.addOption('Rank','','sets the ranking rule for picking up computing element')
config.addOption('ReplicaCatalog','','sets the replica catalogue server')
config.addOption('StorageIndex','','sets the storage index')

config.addOption('DefaultSE','srm.cern.ch','sets the default storage element')
config.addOption('DefaultSRMToken','','sets the space token for storing temporary files (e.g. oversized input sandbox)')
config.addOption('DefaultLFC','prod-lfc-shared-central.cern.ch','sets the file catalogue server')
config.addOption('BoundSandboxLimit',10 * 1024 * 1024,'sets the size limitation of the input sandbox, oversized input sandbox will be pre-uploaded to the storage element specified by \'DefaultSE\' in the area specified by \'DefaultSRMToken\'')

config.addOption('Requirements','Ganga.Lib.LCG.LCGRequirements','sets the full qualified class name for other specific LCG job requirements')
#config.addOption('JobExpiryTime', 30 * 60, 'sets the job\'s expiry time')

# apply preconfig and postconfig handlers
config.attachUserHandler(__preConfigHandler__,__postConfigHandler__)

# startup two independent middleware environments for LCG
grids = {'EDG':None,'GLITE':None}

if config['GLITE_ENABLE']:
    grids['GLITE'] = Grid('GLITE')
    if grids['GLITE'].shell:
        config.setSessionValue('DefaultLFC',grids['GLITE'].shell.env['LFC_HOST'])
    config.setSessionValue('GLITE_ENABLE',grids['GLITE'].active)

if config['EDG_ENABLE']:
    grids['EDG'] = Grid('EDG')
    if grids['EDG'].shell:
	config.setSessionValue('DefaultLFC', grids['EDG'].shell.env['LFC_HOST'])
    config.setSessionValue('EDG_ENABLE', grids['EDG'].active)

##if config['GLITE_ENABLE']:
##    grids['GLITE'] = Grid('GLITE')
##    if grids['GLITE'].shell:
##        config['DefaultLFC'] = grids['GLITE'].shell.env['LFC_HOST']
##    config['GLITE_ENABLE'] = grids['GLITE'].active
##
##if config['EDG_ENABLE']:
##    grids['EDG'] = Grid('EDG')
##    if grids['EDG'].shell:
##        config['DefaultLFC'] = grids['EDG'].shell.env['LFC_HOST']
##    config['EDG_ENABLE'] = grids['EDG'].active

# $Log: not supported by cvs2svn $
# Revision 1.95.4.9  2008/05/15 16:01:08  hclee
# - bugfix #36178 (subprocess in python2.5)
#
# Revision 1.95.4.8  2008/05/08 13:28:06  hclee
# gzipped stdout stderr
#
# Revision 1.95.4.7  2008/03/31 15:56:27  hclee
# merge the srmv2 space token support made in Ganga4 branch
#
# Revision 1.95.4.6  2008/03/07 12:27:31  hclee
# distinguish application exitcode and middleware exitcode in schema
#  - exitcode: application exitcode
#  - exitcode_lcg: middleware exitcode
#
# Revision 1.95.4.5  2008/02/06 17:05:01  hclee
# add descriptions of configuration attributes
#
# Revision 1.95.4.4  2008/02/06 11:21:20  hclee
# merge 4.4 and 5.0 and fix few issues
#
# Revision 1.95.4.3  2007/12/11 09:54:30  amuraru
# moved GLITE_SETUP and EDG_SETUP to LCG module
#
# Revision 1.95.4.2  2007/12/10 18:05:13  amuraru
# merged the 4.4.4 changes
#
# Revision 1.95.4.1  2007/10/12 13:56:25  moscicki
# merged with the new configuration subsystem
#
# Revision 1.95.6.3  2007/10/12 08:16:50  roma
# Migration to new Config
#
# Revision 1.95.6.2  2007/10/09 15:06:47  roma
# Migration to new Config
#
# Revision 1.95.6.1  2007/09/25 09:45:12  moscicki
# merged from old config branch
#
# Revision 1.111  2007/12/04 17:26:19  hclee
# fix small typo
#
# Revision 1.110  2007/12/04 17:19:42  hclee
# - fix bugs in updating bulk job's status
# - fix status parser for gLite 3.1
#
# Revision 1.109  2007/12/04 15:53:49  moscicki
# sparated Grid class into another module
# added optional import of GridSimulator class
#
# Revision 1.108  2007/11/30 11:31:12  hclee
# - improve the job id parser in the submit method
# - remove the warning message for individual subjob submission/killing
#
# Revision 1.107  2007/11/29 13:57:40  hclee
# fill up subjob ids in the monitoring loop
#
# Revision 1.106  2007/11/23 15:22:52  hclee
# add performance profiler
#
# Revision 1.105  2007/11/09 03:12:39  hclee
# bug fix on job id parser for edg-job-submit command
#
# Revision 1.104  2007/11/08 02:40:31  hclee
# fix the bug of parsing job id of edg-job-submit, remove the heading white spaces before parsing
#
# Revision 1.103  2007/10/23 12:18:43  hclee
# fix the subjob ordering issue of the glite collective job
#
# Revision 1.102  2007/10/19 14:43:14  hclee
# use -i in LCG command to kill multiple subjobs which are individually resubmitted
#
# Revision 1.101  2007/10/19 14:32:39  hclee
# bug fix for resubmission and kill on individual subjob
#
# Revision 1.100  2007/10/19 12:34:21  hclee
#  - improving the control of the resubmission of each individual subjob submitted through glite-bulk job
#  - enabling kill() on each individual subjob submitted through glite-bulk job
#  - updating job.info.submit_count on subjobs in submit and resubmit methods
#
# Revision 1.99  2007/10/11 12:00:16  hclee
# support job resubmission on the glite subjobs
#
# Revision 1.98  2007/10/08 16:21:01  hclee
#  - introduce "ShallowRetryCount" JDL attribute and set default to 10
#  - use the subprocess module to launch the application executable in the job wrapper
#
# Revision 1.97  2007/09/25 13:22:19  hclee
# implement the peek method with Octopus monitoring service
#
# Revision 1.114  2008/01/18 15:24:16  hclee
#  - integrate job perusal feature implemented by Philip
#  - fix bugs in backend.loginfo() and backend.inspect()
#
# Revision 1.113  2008/01/10 11:46:54  hclee
#  - disable the JDL attribute "ExpiryTime" to avoid the immediate crash of the resubmitted jobs
#  - merge the modification for enabling glite job perusal feature (contributed by Philip Rodrigues)
#
# Revision 1.112  2007/12/14 11:32:58  hclee
# fix the broken bulk submission - add temporary workaround to avoid the master job's state transition from 'submitted' to 'submitting'
#
# Revision 1.111  2007/12/04 17:26:19  hclee
# fix small typo
#
# Revision 1.110  2007/12/04 17:19:42  hclee
# - fix bugs in updating bulk job's status
# - fix status parser for gLite 3.1
#
# Revision 1.109  2007/12/04 15:53:49  moscicki
# sparated Grid class into another module
# added optional import of GridSimulator class
#
# Revision 1.108  2007/11/30 11:31:12  hclee
# - improve the job id parser in the submit method
# - remove the warning message for individual subjob submission/killing
#
# Revision 1.107  2007/11/29 13:57:40  hclee
# fill up subjob ids in the monitoring loop
#
# Revision 1.106  2007/11/23 15:22:52  hclee
# add performance profiler
#
# Revision 1.105  2007/11/09 03:12:39  hclee
# bug fix on job id parser for edg-job-submit command
#
# Revision 1.104  2007/11/08 02:40:31  hclee
# fix the bug of parsing job id of edg-job-submit, remove the heading white spaces before parsing
#
# Revision 1.103  2007/10/23 12:18:43  hclee
# fix the subjob ordering issue of the glite collective job
#
# Revision 1.102  2007/10/19 14:43:14  hclee
# use -i in LCG command to kill multiple subjobs which are individually resubmitted
#
# Revision 1.101  2007/10/19 14:32:39  hclee
# bug fix for resubmission and kill on individual subjob
#
# Revision 1.100  2007/10/19 12:34:21  hclee
#  - improving the control of the resubmission of each individual subjob submitted through glite-bulk job
#  - enabling kill() on each individual subjob submitted through glite-bulk job
#  - updating job.info.submit_count on subjobs in submit and resubmit methods
#
# Revision 1.99  2007/10/11 12:00:16  hclee
# support job resubmission on the glite subjobs
#
# Revision 1.98  2007/10/08 16:21:01  hclee
#  - introduce "ShallowRetryCount" JDL attribute and set default to 10
#  - use the subprocess module to launch the application executable in the job wrapper
#
# Revision 1.97  2007/09/25 13:22:19  hclee
# implement the peek method with Octopus monitoring service
#
# Revision 1.95  2007/08/09 14:01:45  kuba
# fixed the logic of dynamic requirements loading (fix from Johannes)
#
# Revision 1.94  2007/08/09 11:03:45  kuba
# protection for passing non-strings to printError and printWarning functions
#
# Revision 1.93  2007/08/01 13:39:27  hclee
# replace old glite-job-* commands with glite-wms-job-* commands
#
# Revision 1.92  2007/07/27 15:13:39  moscicki
# merged the monitoring services branch from kuba
#
# Revision 1.91  2007/07/25 14:08:07  hclee
#  - combine the query for glite subjob id (right after the job submission) with the hook of sending monitoring information to Dashboard
#  - improve the debug message in the job wrapper
#
# Revision 1.90  2007/07/24 13:53:11  hclee
# query for subjob ids right after the glite bulk submission
#
# Revision 1.89  2007/07/16 15:42:16  hclee
#  - move LCGRequirements out from LCG class
#  - add config['LCG']['Requirements'] attribute, default to the LCGRequirements class
#  - dynamic loading of the requirements module, allowing applications to override merge() and convert() methods for app specific requirement based on the GLUE schema
#
# Revision 1.88  2007/07/10 13:08:32  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.87  2007/07/03 10:05:10  hclee
# pass the GridShell instance to GridCache for pre-staging oversized inputsandbox
#
# Revision 1.86.2.1  2007/06/21 15:04:24  moscicki
# improvement of the monitoring services interface
#
# Revision 1.86  2007/06/15 08:42:59  hclee
#  - adopt the Credential plugin to get the voname from the voms proxy
#  - modify the logic of the Grid.check_proxy() method
#
# Revision 1.85  2007/06/06 18:56:38  hclee
# bug fix
#
# Revision 1.84  2007/06/06 15:21:52  hclee
# fix the issue that if the grids['EDG'] and grids['GLITE'] not properly created on the machine without UI installation
#
# Revision 1.83  2007/06/05 16:43:06  hclee
# get default lfc_host from lcg-infosites utility
#
# Revision 1.82  2007/06/05 15:06:22  hclee
# add a post-config hook for setting corresponding env. variables of the cached GridShells
#  - for instance, only config['LCG']['DefaultLFC'] affects GridShell.env['LFC_HOST']
#
# Revision 1.81  2007/05/30 16:17:26  hclee
# check the exit code of the real executable (bug #26290)
#
# Revision 1.80  2007/05/23 15:43:24  hclee
#  - introduce 'DefaultLFC' configuration property
#  - check the exit code from real executable (bug #26290)
#  - pass local 'LFC_HOST' environment variable to grid WNs (bug #26443)
#
# Revision 1.79  2007/05/10 10:05:14  liko
# Use srm.cern.ch for big sandbox and do not overwrite X509_USER_PROXY
#
# Revision 1.78.4.1  2007/06/18 07:44:56  moscicki
# config prototype
#
# Revision 1.78  2007/04/05 14:30:19  hclee
# - fix the bug in distinguishing master and node jobs of the glite bulk submission
# - add logic for handling master_resubmit and master_cancel for glite bulk jobs
#
# Revision 1.77  2007/04/05 07:13:01  hclee
# allow users to call the 'cleanup_iocache()' method when job is in 'completed' and 'failed' status
#
# Revision 1.76  2007/03/23 03:45:02  hclee
# remove CVS confliction marks
#
# Revision 1.75  2007/03/23 03:41:24  hclee
# merge modifications in 4.2.2-bugfix-branch
#
# Revision 1.74  2007/01/31 11:13:52  hclee
# remove the python path prepending when calling edg or glite UI commands
#
# Revision 1.73  2007/01/23 17:32:44  hclee
# input sandbox pre-upload is workable for gLite bulk submission
#
# Revision 1.72  2007/01/23 11:45:58  hclee
# the inputsandbox pre-upload takes into account the shared inputsandbox
#  - the shared inputsandbox will not be uploaded again if it has been existing on the remote iocache
# add and export cleanup_iocache() method for deleting the pre-uploaded input sandboxes
#  - if the job is not "completed", the operation will be simply ignored with some warning message
#
# Revision 1.71  2007/01/22 16:22:10  hclee
# the workable version for remote file cache using lcg-utils
#
# Revision 1.70  2007/01/17 17:54:36  hclee
# working for file upload
#
# Revision 1.69  2007/01/16 16:58:37  hclee
# In the middle of implementing large inputsandbox support
#
# Revision 1.68  2007/01/16 15:31:11  hclee
# Adopt the GridCache object for remote file I/O
#
# Revision 1.67  2006/12/14 08:53:03  hclee
# add file upload/download/delete methods
#
# Revision 1.66  2006/12/13 13:17:19  hclee
# merge the modifications in the 4-2-2 bugfix branch
#
# Revision 1.65  2006/11/02 13:35:49  hclee
# add resubmission implementations
#
# Revision 1.63.2.8  2006/12/13 12:52:40  hclee
# add _GPI_Prefs
#
# Revision 1.63.2.7  2006/11/22 20:39:10  hclee
# make sure the numerical values of requirements are correctly converted into string
#
# Revision 1.63.2.6  2006/11/22 15:40:16  hclee
# Make a more clear instruction for calling check_proxy method
#
# Revision 1.63.2.5  2006/11/03 15:57:18  hclee
# introduce the environmental variable, GANGA_LCG_CE, for monitoring purpose
# if the backend.CE is specified by the user
#
# Revision 1.63.2.4  2006/11/03 13:19:09  hclee
# rollback unintentional commit to exclude the resubmission feature
#
# Revision 1.63.2.3  2006/11/02 13:25:27  hclee
# implements the resubmit methods for both EDG and GLITE modes
#
# Revision 1.63.2.2  2006/10/26 14:14:46  hclee
# include the monitoring component
#
# Revision 1.63.2.1  2006/10/26 13:33:36  hclee
# - accept the verbosity argument when use backend.loginfo()
# - the backend.loginfo() method returns a filename of the saved logging info instead of printing out of the plain text of the logging info
#
# Revision 1.63  2006/10/24 12:53:48  hclee
# skip taking VO name from the voms proxy if using EDG middleware
#
# Revision 1.62  2006/10/12 13:00:27  hclee
#  - for subjobs, change to status 'submitting' before changing to 'submitted'
#
# Revision 1.61  2006/10/09 10:38:39  hclee
# Simplify the usage of the "Grid" objects
#
# Revision 1.60  2006/10/09 09:37:43  hclee
# voms attributes in the proxy takes precedence for VO detection in composing job submission command
#
# Revision 1.59  2006/10/09 09:14:43  hclee
# Appending "MPICH" requirements instead of overriding
#
# Revision 1.58  2006/10/06 08:05:08  hclee
# Add supports for multiple job types (Normal, MPICH, Interactive)
#
# Revision 1.57  2006/10/05 09:12:42  hclee
# - add default value of the configurable parameters
# - simplify the code accordingly by removing the checking of the existence of the configurable parameters
# - expose the exitcode of the real executable inside the job wrapper
#
# Revision 1.56  2006/09/28 14:36:56  hclee
# remove the redundant __credential_validity__ method
# change some message in the submit function to debug level info
#
# Revision 1.55  2006/09/18 09:48:46  hclee
# add "-r" option in job submission command if CE is specified (bypassing the RB match-making)
# change the name of some private method: Grid.proxy_voname() -> Grid.__get_proxy_voname__()
# change the argument of the Grid.__credential_validity__() method. Replace "value" with "type".
#
# Revision 1.54  2006/09/11 12:33:29  hclee
# job status rolls back to "failed" if output fetching fails.
#
# Revision 1.53  2006/09/06 15:08:54  hclee
# Catch and print the log file of grid commands
#
# Revision 1.52  2006/08/28 15:20:32  hclee
# - integrate shared inputsandbox for glite bulk submission
# - small fixes in job wrapper
#
# Revision 1.51  2006/08/24 16:48:24  moscicki
# - master/subjob sandbox support
# - fixes in the config for setting VO
#
# Revision 1.50  2006/08/22 12:06:30  hclee
# unpack the output sandbox tarball after getting output
#
# Revision 1.49  2006/08/21 10:31:55  hclee
# set PATH environment to search current working directory in the job wrapper
#
# Revision 1.48  2006/08/18 16:08:05  hclee
# small fix for vo switching
#
# Revision 1.47  2006/08/18 13:46:00  hclee
# update for the bugs:
#  - #19122: use jobconfig.getExeString() to get correct path of exeutable
#  - #19067: use an enhanced system call handler implemented in the Local handler to better control the stdout/stderr
#  - #19155: job submission/cancelling/monitoring will be just failed if no proxy is available
#
# Revision 1.46  2006/08/16 15:15:33  hclee
# fix the path problem of the actual executable in job wrapper
#
# Revision 1.45  2006/08/15 11:10:01  hclee
# - reduce verbosity
# - correct the way to specify default configuration attributes
#
# Revision 1.44  2006/08/10 13:39:50  moscicki
# using Sandbox mechanism
#
# Revision 1.43  2006/08/09 14:36:10  hclee
#  - use ProxyTimeLeft and ProxyTimeValid in proxy creating and checking
#  - in submit and cancel methods, check_proxy is called if no valid proxy available
#
# Revision 1.42  2006/08/09 11:07:32  hclee
#  - use getCredential method to create a credential
#  - enhancement in get_output() method
#
# Revision 1.41  2006/08/08 21:44:23  hclee
# change wrapper log format
#
# Revision 1.40  2006/08/08 21:23:40  hclee
# Change format of the wrapper log
#
# Revision 1.39  2006/08/08 20:02:36  hclee
#  - Add job wrapper
#  - modify the loop of backend status update
#  - use GridShell module to create Shell object
#
# Revision 1.38  2006/08/08 14:23:49  hclee
#  - Integrate with Credential module
#  - Add method for getting Shell objects
#  - In the middle of the job wrapper implementation
#
# Revision 1.37  2006/07/31 13:25:55  hclee
# replace the code of master job update with the factored out method: updateMasterJobStatus()
#
# Revision 1.36  2006/07/31 13:06:21  hclee
# Integration with state machine
# few bug fixes
#
# Revision 1.35  2006/07/20 21:06:15  hclee
#  - remove existing "jdlrepos" directory of bulk job
#
# Revision 1.34  2006/07/20 20:51:59  hclee
#  - return False if bulk submission failed
#
# Revision 1.33  2006/07/19 17:06:20  hclee
# initial implementation for gLite bulk submission
#
# Revision 1.32  2006/07/18 15:09:59  hclee
# Supporting both EDG and GLITE middlewares in LCG handler
#
# Revision 1.31  2006/07/17 10:14:29  hclee
# merge Alvin's patch for the version (Ganga-LCG-1-1) in Ganga release 4-2-0-beta2
#
# Revision 1.30  2006/07/10 13:12:59  moscicki
# changes from Johannes: outputdata handling and a bugfix
#
# Revision 1.29  2006/07/07 14:27:01  hclee
# Fix the scenario of VO check in the __avoidVOSwitch__ function
#
# Revision 1.28  2006/07/07 12:04:11  hclee
# Avoid VO switching in GANGA session
#
# Revision 1.27  2006/07/04 11:41:36  hclee
# Add internal function in Grid object for setting up the edg-job-submit options
#  - effective configurations are used in composing the options
#  - more virtual organisation checks
#  - the function will be called everytime the submit() function is called
#
# Revision 1.27  2006/07/03 13:55:30  hclee 
# Add internal function in Grid object for setting up the edg-job-submit options
#  - effective configurations are used in composing the options
#  - more virtual organisation checks
#  - the function will be called everytime the submit() function is called
#
# Revision 1.26  2006/06/07 17:16:02  liko
# Additional logic for the cleared state
#
# Revision 1.25  2006/06/07 17:15:44  liko
# Additional logic for the cleared state
#
# Revision 1.24  2006/05/31 10:12:17  liko
# Add Cleared
#
# Revision 1.23  2006/05/19 22:11:59  liko
# Add status Submitted
#
# Revision 1.22  2006/05/18 15:38:31  liko
# :
#
# Revision 1.21  2006/05/15 16:39:30  liko
# Done (Failed) is not final state ...
#
# Revision 1.20  2006/05/08 11:50:53  liko
# Include changes by Johannes
#
# Revision 1.19  2006/04/27 09:13:25  moscicki
#
# PREFIX_HACK:
# work around inconsistency of LCG setup script and commands:
# LCG commands require python2.2 but the setup script does not set this version of python. If another version of python is used (like in GUI), then python2.2 runs against wrong python libraries possibly should be fixed in LCG: either remove python2.2 from command scripts or make setup script force correct version of python
#
# Revision 1.18  2006/04/24 17:30:02  liko
# Several bug fixes
#
# Revision 1.17  2006/03/20 10:01:53  liko
# Fix retry count
#
# Revision 1.16  2006/03/17 00:55:19  liko
# Fix problem with replica catalog
#
# Revision 1.15  2006/03/17 00:06:55  liko
# defaults for config attributes ReplicaCatalog
#
# Revision 1.14  2006/03/16 23:53:12  liko
# Fix stupid proxy message
#
# Revision 1.13  2006/02/10 14:38:37  moscicki
# replaced KeyError by ConfigError
#
# fixed: bug #13462 overview: stdin and stdout are unconditionally added to OutputSandbox
#
# fixed: edg-job-cancel with the new release of LCG asks an interactive questions which made Ganga to "hang" on it, --noint option added wherever possible
#
# Revision 1.12  2006/02/07 13:02:33  liko
#
# 1) Fix problem with conflicting requirements definitions
# 2) Fix problem with AllowedCEs in configuration
# 3) Support for LFC in Athena handler
#
# Revision 1.11  2005/11/08 09:15:05  liko
# Fix a bug in the handling of the environment
#
# Revision 1.10  2005/10/21 13:19:09  moscicki
# fixed: kill should return the boolean sucess code
#
# Revision 1.9  2005/10/11 11:56:37  liko
# Default values for new configuration file
#
# Revision 1.8  2005/09/22 21:41:15  liko
# Add Cleared status
#
# Revision 1.7  2005/09/21 09:05:58  andrew
# Added a retry mechanism to the 'proxy-init' call. Now the user has
# 3 retries before giving up.
#
# Revision 1.6  2005/09/06 11:37:13  liko
# Mainly the Athena handler
#
# Revision 1.5  2005/09/02 12:46:10  liko
# Extensively updated version
#
