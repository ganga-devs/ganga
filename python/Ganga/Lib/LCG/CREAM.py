# CREAM backend
from LCG.Grid import cream_proxy_delegation
import os
import os.path
import math
import re

from urlparse import urlparse

from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm
from Ganga.Core import GangaException

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Lib.LCG.Utility import *
from Ganga.Lib.LCG.ElapsedTimeProfiler import ElapsedTimeProfiler

from Ganga.Lib.LCG.Grid import Grid
from Ganga.Lib.LCG.LCG import grids

def __cream_resolveOSBList__(job, jdl):

    osbURIList = []

    re_osb = re.compile('^.*OutputSandbox\s+\=\s+\{(.*)\}$')

    for l in jdl.split(';'):
        m = re_osb.match( l )
        if m:
            osb = m.group(1)
            osb = re.sub(r'\s?\"\s?', '', osb)

            for f in osb.split(','):
                if not urlparse(f)[0]:
                    osbURIList.append('%s/%s' % ( job.backend.osbURI, os.path.basename(f)) )
                else:
                    osbURIList.append(f)
            break

    return osbURIList

class CREAM(IBackend):
    '''CREAM backend - direct job submission to gLite CREAM CE'''
    _schema = Schema(Version(1,0), {
        'CE'                  : SimpleItem(defvalue='',doc='CREAM CE endpoint'),
        'jobtype'             : SimpleItem(defvalue='Normal',doc='Job type: Normal, MPICH'),
        'requirements'        : ComponentItem('LCGRequirements',doc='Requirements for the resource selection'),
        'sandboxcache'        : ComponentItem('LCGSandboxCache',copyable=1,doc='Interface for handling oversized input sandbox'),
        'id'                  : SimpleItem(defvalue='',typelist=['str','list'],protected=1,copyable=0,doc='Middleware job identifier'),
        'status'              : SimpleItem(defvalue='',typelist=['str','dict'], protected=1,copyable=0,doc='Middleware job status'),
        'exitcode'            : SimpleItem(defvalue='',protected=1,copyable=0,doc='Application exit code'),
        'exitcode_cream'      : SimpleItem(defvalue='',protected=1,copyable=0,doc='Middleware exit code'),
        'actualCE'            : SimpleItem(defvalue='',protected=1,copyable=0,doc='The CREAM CE where the job actually runs.'),
        'reason'              : SimpleItem(defvalue='',protected=1,copyable=0,doc='Reason of causing the job status'),
        'workernode'          : SimpleItem(defvalue='',protected=1,copyable=0,doc='The worker node on which the job actually runs.'),
        'isbURI'              : SimpleItem(defvalue='',protected=1,copyable=0,doc='The input sandbox URI on CREAM CE'),
        'osbURI'              : SimpleItem(defvalue='',protected=1,copyable=0,doc='The output sandbox URI on CREAM CE')
    })

    _category = 'backends'

    _name =  'CREAM'

    def __init__(self):
        super(CREAM, self).__init__()

        # dynamic requirement object loading
        try:
            reqName1  = config['Requirements']
            reqName   = config['Requirements'].split('.').pop()
            reqModule = __import__(reqName1, globals(), locals(), [reqName1])
            reqClass  = vars(reqModule)[reqName]
            self.requirements = reqClass()

            logger.debug('load %s as LCGRequirements' % reqName)
        except:
            logger.debug('load default LCGRequirements')
            pass

        # dynamic sandbox cache object loading
        try:
            scName1  = config['SandboxCache']
            scName   = config['SandboxCache'].split('.').pop()
            scModule = __import__(scName1, globals(), locals(), [scName1])
            scClass  = vars(scModule)[scName]
            self.sandboxcache = scClass()
            logger.debug('load %s as SandboxCache' % scName)
        except:
            logger.debug('load default LCGSandboxCAche')
            pass

    def __setup_sandboxcache__(self, job):
        '''Sets up the sandbox cache object to adopt the runtime configuration of the LCG backend'''

        re_token = re.compile('^token:(.*):(.*)$')

        self.sandboxcache.vo = config['VirtualOrganisation']
        self.sandboxcache.middleware = 'GLITE'
        self.sandboxcache.timeout    = config['SandboxTransferTimeout']

        if self.sandboxcache._name == 'LCGSandboxCache':
            if not self.sandboxcache.lfc_host:
                self.sandboxcache.lfc_host = grids[self.middleware.upper()].__get_lfc_host__()

            if not self.sandboxcache.se:

                token   = ''
                se_host = config['DefaultSE']
                m = re_token.match(se_host)
                if m:
                    token   = m.group(1)
                    se_host = m.group(2)

                self.sandboxcache.se = se_host

                if token:
                    self.sandboxcache.srm_token = token

            if (self.sandboxcache.se_type in ['srmv2']) and (not self.sandboxcache.srm_token):
                self.sandboxcache.srm_token = config['DefaultSRMToken']

        elif self.sandboxcache._name == 'DQ2SandboxCache':

            ## generate a new dataset name if not given
            if not self.sandboxcache.dataset_name:
                from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname
                self.sandboxcache.dataset_name,unused = dq2outputdatasetname("%s.input"%get_uuid(), 0, False, '')

            ## subjobs inherits the dataset name from the master job
            for sj in job.subjobs:
                sj.backend.sandboxcache.dataset_name = self.sandboxcache.dataset_name

        return True

    def __check_and_prestage_inputfile__(self, file):
        '''Checks the given input file size and if it's size is
           over "BoundSandboxLimit", prestage it to a grid SE.

           The argument is a path of the local file.

           It returns a dictionary containing information to refer to the file:

               idx = {'lfc_host': lfc_host,
                      'local': [the local file pathes],
                      'remote': {'fname1': 'remote index1', 'fname2': 'remote index2', ... }
                     }

           If prestaging failed, None object is returned.

           If the file has been previously uploaded (according to md5sum),
           the prestaging is ignored and index to the previously uploaded file
           is returned.
           '''

        idx = {'lfc_host':'', 'local':[], 'remote':{}}

        job = self.getJobObject()

        ## read-in the previously uploaded files
        uploadedFiles = []

        ## getting the uploaded file list from the master job
        if job.master:
            uploadedFiles += job.master.backend.sandboxcache.get_cached_files()

        ## set and get the $LFC_HOST for uploading oversized sandbox
        self.__setup_sandboxcache__(job)

        uploadedFiles += self.sandboxcache.get_cached_files()

        lfc_host = None

        ## for LCGSandboxCache, take the one specified in the sansboxcache object.
        ## the value is exactly the same as the one from the local grid shell env. if
        ## it is not specified exclusively.
        if self.sandboxcache._name == 'LCGSandboxCache':
            lfc_host = self.sandboxcache.lfc_host

        ## or in general, query it from the Grid object
        if not lfc_host:
            lfc_host = grids[self.sandboxcache.middleware.upper()].__get_lfc_host__()

        idx['lfc_host'] = lfc_host

        abspath = os.path.abspath(file)
        fsize   = os.path.getsize(abspath)

        print fsize
        print config['BoundSandboxLimit']

        if fsize > config['BoundSandboxLimit']:

            md5sum  = get_md5sum(abspath, ignoreGzipTimestamp=True)

            print md5sum

            doUpload = True
            for uf in uploadedFiles:
                if uf.md5sum == md5sum:
                    # the same file has been uploaded to the iocache
                    idx['remote'][os.path.basename(file)] = uf.id
                    doUpload = False
                    break

            if doUpload:

                logger.warning('The size of %s is larger than the sandbox limit (%d byte). Please wait while pre-staging ...' % (file,config['BoundSandboxLimit']) )

                if self.sandboxcache.upload( [abspath] ):
                    remote_sandbox = self.sandboxcache.get_cached_files()[-1]
                    idx['remote'][remote_sandbox.name] = remote_sandbox.id
                else:
                    logger.error('Oversized sandbox not successfully pre-staged')
                    return None
        else:
            idx['local'].append(abspath)

        return idx

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
    printInfo('lcg-cp timeout: %d' % timeout)

    i         = 0
    rc        = 0
    isDone    = False
    try_again = True

    while try_again:
        i = i + 1
        try:
            ps = os.popen(cmd)
            status = ps.close()

            if not status:
                isDone = True
                printInfo('File %s download from iocache' % os.path.basename(localFilePath))
            else:
                raise IOError("Download file %s from iocache failed with error code: %d, trial %d." % (os.path.basename(localFilePath), status, i))

        except IOError, e:
            isDone = False
            printError(str(e))

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
timeout = ###TRANSFERTIMEOUT###

exitcode=-1

import sys, stat, os, os.path, commands

# Change to scratch directory if provided
scratchdir = ''
tmpdir = ''

orig_wdir = os.getcwd()

# prepare log file for job wrapper
out = open(os.path.join(orig_wdir, wrapperlog),'w')

if os.getenv('EDG_WL_SCRATCH'):
    scratchdir = os.getenv('EDG_WL_SCRATCH')
elif os.getenv('TMPDIR'):
    scratchdir = os.getenv('TMPDIR')

if scratchdir:
    (status, tmpdir) = commands.getstatusoutput('mktemp -d %s/gangajob_XXXXXXXX' % (scratchdir))
    if status == 0:
        os.chdir(tmpdir)
    else:
        ## if status != 0, tmpdir should contains error message so print it to stderr
        printError('Error making ganga job scratch dir: %s' % tmpdir)
        printInfo('Unable to create ganga job scratch dir in %s. Run directly in: %s' % ( scratchdir, os.getcwd() ) )

        ## reset scratchdir and tmpdir to disable the usage of Ganga scratch dir
        scratchdir = ''
        tmpdir = ''

wdir = os.getcwd()

if scratchdir:
    printInfo('Changed working directory to scratch directory %s' % tmpdir)
    try:
        os.system("ln -s %s %s" % (os.path.join(orig_wdir, 'stdout'), os.path.join(wdir, 'stdout')))
        os.system("ln -s %s %s" % (os.path.join(orig_wdir, 'stderr'), os.path.join(wdir, 'stderr')))
    except Exception,e:
        printError(sys.exc_info()[0])
        printError(sys.exc_info()[1])
        str_traceback = traceback.format_tb(sys.exc_info()[2])
        for str_tb in str_traceback:
            printError(str_tb)
        printInfo('Linking stdout & stderr to original directory failed. Looking at stdout during job run may not be possible')

sys.path.insert(0,os.path.join(wdir,PYTHON_DIR))
os.environ['PATH'] = '.:'+os.environ['PATH']

vo = os.environ['GANGA_LCG_VO']

try:
    printInfo('Job Wrapper start.')

#   download inputsandbox from remote cache
    for f,guid in input_sandbox['remote'].iteritems():
        if not lcg_file_download(vo, guid, os.path.join(wdir,f), timeout=int(timeout)):
            raise Exception('Download remote input %s:%s failed.' % (guid,f) )
        else:
            getPackedInputSandbox(f)

    printInfo('Download inputsandbox from iocache passed.')

#   unpack inputsandbox from wdir
    for f in input_sandbox['local']:
        getPackedInputSandbox(os.path.join(orig_wdir,f))

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
        status = execSyscmdSubprocess('%s %s' % (appexec,appargs), wdir)
    except ImportError,err:
        # otherwise, use separate threads to control process IO pipes
        printInfo('Load application executable with separate threads')
        status = execSyscmdEnhanced('%s %s' % (appexec,appargs), wdir)

    os.system("cp %s/stdout stdout.1" % orig_wdir)
    os.system("cp %s/stderr stderr.1" % orig_wdir)

    printInfo('GZipping stdout and stderr...')

    os.system("gzip stdout.1 stderr.1")

    # move them to the original wdir so they can be picked up
    os.system("mv stdout.1.gz %s/stdout.gz" % orig_wdir)
    os.system("mv stderr.1.gz %s/stderr.gz" % orig_wdir)

    if not status:
        raise Exception('Application execution failed.')
    printInfo('Application execution passed with exit code %d.' % exitcode)

    createPackedOutputSandbox(outputsandbox,None,orig_wdir)

#   pack outputsandbox
#    printInfo('== check output ==')
#    for line in os.popen('pwd; ls -l').readlines():
#        printInfo(line)

    printInfo('Pack outputsandbox passed.')
    monitor.stop(exitcode)

    # Clean up after us - All log files and packed outputsandbox should be in "wdir"
    if scratchdir:
        os.chdir(orig_wdir)
        os.system("rm %s -rf" % wdir)
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
        return script

    def preparejob(self,jobconfig,master_job_sandbox):
        '''Prepare the JDL'''

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
        packed_files = jobconfig.getSandboxFiles() + Sandbox.getGangaModulesAsSandboxFiles(Sandbox.getDefaultModules()) + Sandbox.getGangaModulesAsSandboxFiles(mon.getSandboxModules())
        sandbox_files = job.createPackedInputSandbox(packed_files)

        ## sandbox of child jobs should include master's sandbox
        sandbox_files.extend(master_job_sandbox)

        ## check the input file size and pre-upload larger inputs to the iocache
        lfc_host = ''

        input_sandbox_uris  = []
        input_sandbox_names = []

        ick = True

        max_prestaged_fsize = 0
        for f in sandbox_files:

            idx = self.__check_and_prestage_inputfile__(f)

            if not idx:
                logger.error('input sandbox preparation failed: %s' % f)
                ick = False
                break
            else:

                if idx['lfc_host']:
                    lfc_host = idx['lfc_host']

                if idx['remote']:
                    abspath = os.path.abspath(f)
                    fsize   = os.path.getsize(abspath)

                    if fsize > max_prestaged_fsize:
                        max_prestaged_fsize = fsize

                    input_sandbox_uris.append( idx['remote'][ os.path.basename(f) ] )

                    input_sandbox_names.append( os.path.basename( urlparse(f)[2] ) )

                if idx['local']:
                    input_sandbox_uris += idx['local']
                    input_sandbox_names.append( os.path.basename(f) )

        if not ick:
            logger.error('stop job submission')
            return None

        ## determin the lcg-cp timeout according to the max_prestaged_fsize
        ##  - using the assumption of 1 MB/sec.
        max_prestaged_fsize = 0
        lfc_host = ''
        transfer_timeout = config['SandboxTransferTimeout']
        predict_timeout  = int( math.ceil( max_prestaged_fsize/1000000.0 ) )

        if predict_timeout > transfer_timeout:
            transfer_timeout = predict_timeout

        if transfer_timeout < 60:
            transfer_timeout = 60

        script = script.replace('###TRANSFERTIMEOUT###', '%d' % transfer_timeout)

        ## update the job wrapper with the inputsandbox list
        script = script.replace('###INPUTSANDBOX###',repr({'remote':{}, 'local': input_sandbox_names }))

        ## write out the job wrapper and put job wrapper into job's inputsandbox
        scriptPath = inpw.writefile(FileBuffer('__jobscript_%s__' % job.getFQID('.'),script),executable=1)
        input_sandbox  = input_sandbox_uris + [scriptPath]

        for isb in input_sandbox:
            logger.debug('ISB URI: %s' % isb)

        ## compose output sandbox to include by default the following files:
        ##  - gzipped stdout (transferred only when the JobLogHandler is WMS)
        ##  - gzipped stderr (transferred only when the JobLogHandler is WMS)
        ##  - __jobscript__.log (job wrapper's log)
        output_sandbox = [wrapperlog]

        if config['JobLogHandler'] in ['WMS']:
            output_sandbox += ['stdout.gz','stderr.gz']

        if len(jobconfig.outputbox):
            output_sandbox += [Sandbox.OUTPUT_TARBALL_NAME]

        ## compose LCG JDL
        jdl = {
            'VirtualOrganisation' : config['VirtualOrganisation'],
            'Executable' : os.path.basename(scriptPath),
            'Environment': {'GANGA_LCG_VO': config['VirtualOrganisation'], 'GANGA_LOG_HANDLER': config['JobLogHandler'], 'LFC_HOST': lfc_host},
            'StdOutput'               : 'stdout',
            'StdError'                : 'stderr',
            'InputSandbox'            : input_sandbox,
            'OutputSandbox'           : output_sandbox,
            'OutputSandboxBaseDestURI': 'gsiftp://localhost'
        }

        jdl['Environment'].update({'GANGA_LCG_CE': self.CE})
        jdl['Requirements'] = self.requirements.merge(jobconfig.requirements).convert()

        if self.jobtype.upper() in ['NORMAL','MPICH']:
            jdl['JobType'] = self.jobtype.upper()
            if self.jobtype.upper() == 'MPICH':
                #jdl['Requirements'].append('(other.GlueCEInfoTotalCPUs >= NodeNumber)')
                jdl['Requirements'].append('Member("MPICH",other.GlueHostApplicationSoftwareRunTimeEnvironment)')
                jdl['NodeNumber'] = self.requirements.nodenumber
        else:
            logger.warning('JobType "%s" not supported' % self.jobtype)
            return

#       additional settings from the job
        if jobconfig.env:
            jdl['Environment'].update(jobconfig.env)

        jdlText = Grid.expandjdl(jdl)
        logger.debug('subjob JDL: %s' % jdlText)
        return inpw.writefile(FileBuffer('__jdlfile__',jdlText))

    def kill(self):
        '''Kill the job'''
        job   = self.getJobObject()

        logger.info('Killing job %s' % job.getFQID('.'))

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return False

        return grids['GLITE'].cream_cancelMultiple([self.id])

    def master_submit(self,rjobs,subjobconfigs,masterjobconfig):
        '''Submit the master job to the grid'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        ## delegate proxy to CREAM CE
        if not grids['GLITE'].cream_proxy_delegation(self.CE):
            logger.warning('proxy delegation to %s failed' % self.CE)

        ick = IBackend.master_submit(self,rjobs,subjobconfigs,masterjobconfig)

        profiler.check('==> master_submit() elapsed time')

        return ick

    def submit(self,subjobconfig,master_job_sandbox):
        '''Submit the job to the grid'''

        jdlpath = self.preparejob(subjobconfig,master_job_sandbox)

        self.id = grids['GLITE'].cream_submit(jdlpath,self.CE)

        if self.id:
            self.actualCE = self.CE

        return not self.id is None

    def updateMonitoringInformation(jobs):
        '''Monitoring loop for normal jobs'''

        jobdict   = dict([ [job.backend.id,job] for job in jobs if job.backend.id ])

        jobInfoDict = grids['GLITE'].cream_status(jobdict.keys())

        ## update job information for those available in jobInfoDict
        for id, info in jobInfoDict.items():

            if info:

                job = jobdict[id]

                if job.backend.status != info['Current Status']:

                    if info.has_key('Worker Node'):
                        job.backend.workernode = info['Worker Node']

                    if info.has_key('CREAM ISB URI'):
                        job.backend.isbURI = info['CREAM ISB URI']

                    if info.has_key('CREAM OSB URI'):
                        job.backend.osbURI = info['CREAM OSB URI']

                    doStatusUpdate = True

                    ## download output sandboxes if final status is reached
                    if info['Current Status'] in ['DONE-OK','DONE-FAILED']:

                        ## resolve output sandbox URIs based on the JDL information
                        osbURIList = __cream_resolveOSBList__(job, info['JDL'])
                        
                        logger.debug('OSB list:')
                        for f in osbURIList:
                            logger.debug(f)

                        if osbURIList:
                            doStatusUpdate = grids['GLITE'].cream_get_output( osbURIList, job.outputdir )

                            if doStatusUpdate:
                                (ick, app_exitcode)  = grids['GLITE'].__get_app_exitcode__(job.outputdir)
                                job.backend.exitcode = app_exitcode

                        if not doStatusUpdate:
                            logger.error('fail to download job output: %s' % jobdict[id].getFQID('.'))

                    if doStatusUpdate:
                        job.backend.status = info['Current Status']
                        if info.has_key('ExitCode'):
                            job.backend.exitcode_cream = info['ExitCode']

                        job.backend.updateGangaJobStatus()
            else:
                logger.warning('fail to retrieve job informaton: %s' % jobdict[id].getFQID('.'))
                
    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

    def updateGangaJobStatus(self):
        '''map backend job status to Ganga job status'''

        job = self.getJobObject()

        if self.status in ['RUNNING','REALLY-RUNNING']:
            job.updateStatus('running')

        elif self.status == 'DONE-OK':
            if job.backend.exitcode and job.backend.exitcode != 0:
                job.backend.reason = 'non-zero app. exit code: %s' % repr(job.backend.exitcode)
                job.updateStatus('failed')
            else:
                job.updateStatus('completed')

        elif self.status in ['DONE-FAILED','ABORTED','UNKNOWN']:
            job.updateStatus('failed')

        elif self.status in ['CANCELLED']:
            job.updateStatus('killed')

        elif self.status in ['REGISTERED','PENDING','IDLE','HELD']:
            pass

        else:
            logger.warning('Unexpected job status "%s"', self.status)

logger = getLogger()

config = getConfig('LCG')

## add CREAM specific configuration options
config.addOption('CreamInputSandboxBaseURI', '', 'sets the baseURI for getting the input sandboxes for the job')
config.addOption('CreamOutputSandboxBaseURI', '', 'sets the baseURI for putting the output sandboxes for the job')
#config.addOption('CreamPrologue','','sets the prologue script')
#config.addOption('CreamEpilogue','','sets the epilogue script')