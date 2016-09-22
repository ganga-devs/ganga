# ARC backend
import os
import os.path
import math
import re
import mimetypes
import shutil

from urlparse import urlparse

from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm
from Ganga.Core import GangaException

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger, log_user_exception
from Ganga.Lib.LCG.Utility import get_md5sum
from Ganga.Lib.LCG.ElapsedTimeProfiler import ElapsedTimeProfiler

from Ganga.Lib.LCG import Grid
from Ganga.Lib.LCG.GridftpSandboxCache import GridftpSandboxCache

from Ganga.GPIDev.Base.Proxy import getName

config = getConfig('LCG')

class ARC(IBackend):

    '''ARC backend - direct job submission to an ARC CE'''
    _schema = Schema(Version(1, 0), {
        'CE': SimpleItem(defvalue='', doc='ARC CE endpoint'),
        'jobtype': SimpleItem(defvalue='Normal', doc='Job type: Normal, MPICH'),
        'requirements': ComponentItem('LCGRequirements', doc='Requirements for the resource selection'),
        'sandboxcache': ComponentItem('GridSandboxCache', copyable=1, doc='Interface for handling oversized input sandbox'),
        'id': SimpleItem(defvalue='', typelist=[str, list], protected=1, copyable=0, doc='Middleware job identifier'),
        'status': SimpleItem(defvalue='', typelist=[str, dict], protected=1, copyable=0, doc='Middleware job status'),
        'exitcode': SimpleItem(defvalue='', protected=1, copyable=0, doc='Application exit code'),
        'exitcode_arc': SimpleItem(defvalue='', protected=1, copyable=0, doc='Middleware exit code'),
        'actualCE': SimpleItem(defvalue='', protected=1, copyable=0, doc='The ARC CE where the job actually runs.'),
        'reason': SimpleItem(defvalue='', protected=1, copyable=0, doc='Reason of causing the job status'),
        'workernode': SimpleItem(defvalue='', protected=1, copyable=0, doc='The worker node on which the job actually runs.'),
        'isbURI': SimpleItem(defvalue='', protected=1, copyable=0, doc='The input sandbox URI on ARC CE'),
        'osbURI': SimpleItem(defvalue='', protected=1, copyable=0, doc='The output sandbox URI on ARC CE'),
        'verbose': SimpleItem(defvalue=False, doc='Use verbose options for ARC commands')
    })

    _category = 'backends'

    _name = 'ARC'

    def __init__(self):
        super(ARC, self).__init__()

        # dynamic requirement object loading
        try:
            reqName1 = config['Requirements']
            reqName = config['Requirements'].split('.').pop()
            reqModule = __import__(reqName1, globals(), locals(), [reqName1])
            reqClass = vars(reqModule)[reqName]
            self.requirements = reqClass()

            logger.debug('load %s as LCGRequirements' % reqName)
        except:
            logger.debug('load default LCGRequirements')
            pass

        # dynamic sandbox cache object loading
        # force to use GridftpSandboxCache
        self.sandboxcache = GridftpSandboxCache()
        try:
            scName1 = config['SandboxCache']
            scName = config['SandboxCache'].split('.').pop()
            scModule = __import__(scName1, globals(), locals(), [scName1])
            scClass = vars(scModule)[scName]
            self.sandboxcache = scClass()
            logger.debug('load %s as SandboxCache' % scName)
        except:
            logger.debug('load default SandboxCache')
            pass

    def __refresh_jobinfo__(self, job):
        '''Refresh the lcg jobinfo. It will be called after resubmission.'''
        job.backend.status = ''
        job.backend.reason = ''
        job.backend.actualCE = ''
        job.backend.exitcode = ''
        job.backend.exitcode_arc = ''
        job.backend.workernode = ''
        job.backend.isbURI = ''
        job.backend.osbURI = ''

    def __setup_sandboxcache__(self, job):
        '''Sets up the sandbox cache object to adopt the runtime configuration of the LCG backend'''

        re_token = re.compile('^token:(.*):(.*)$')

        self.sandboxcache.vo = config['VirtualOrganisation']
        self.sandboxcache.timeout = config['SandboxTransferTimeout']

        if self.sandboxcache._name == 'LCGSandboxCache':
            if not self.sandboxcache.lfc_host:
                self.sandboxcache.lfc_host = Grid.__get_lfc_host__()

            if not self.sandboxcache.se:

                token = ''
                se_host = config['DefaultSE']
                m = re_token.match(se_host)
                if m:
                    token = m.group(1)
                    se_host = m.group(2)

                self.sandboxcache.se = se_host

                if token:
                    self.sandboxcache.srm_token = token

            if (self.sandboxcache.se_type in ['srmv2']) and (not self.sandboxcache.srm_token):
                self.sandboxcache.srm_token = config['DefaultSRMToken']

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

        idx = {'lfc_host': '', 'local': [], 'remote': {}}

        job = self.getJobObject()

        # read-in the previously uploaded files
        uploadedFiles = []

        # getting the uploaded file list from the master job
        if job.master:
            uploadedFiles += job.master.backend.sandboxcache.get_cached_files()

        # set and get the $LFC_HOST for uploading oversized sandbox
        self.__setup_sandboxcache__(job)

        uploadedFiles += self.sandboxcache.get_cached_files()

        lfc_host = None

        # for LCGSandboxCache, take the one specified in the sansboxcache object.
        # the value is exactly the same as the one from the local grid shell env. if
        # it is not specified exclusively.
        if self.sandboxcache._name == 'LCGSandboxCache':
            lfc_host = self.sandboxcache.lfc_host

        # or in general, query it from the Grid object
        if not lfc_host:
            lfc_host = Grid.__get_lfc_host__()

        idx['lfc_host'] = lfc_host

        abspath = os.path.abspath(file)
        fsize = os.path.getsize(abspath)

        if fsize > config['BoundSandboxLimit']:

            md5sum = get_md5sum(abspath, ignoreGzipTimestamp=True)

            doUpload = True
            for uf in uploadedFiles:
                if uf.md5sum == md5sum:
                    # the same file has been uploaded to the iocache
                    idx['remote'][os.path.basename(file)] = uf.id
                    doUpload = False
                    break

            if doUpload:

                logger.warning(
                    'The size of %s is larger than the sandbox limit (%d byte). Please wait while pre-staging ...' % (file, config['BoundSandboxLimit']))

                if self.sandboxcache.upload([abspath]):
                    remote_sandbox = self.sandboxcache.get_cached_files()[-1]
                    idx['remote'][remote_sandbox.name] = remote_sandbox.id
                else:
                    logger.error(
                        'Oversized sandbox not successfully pre-staged')
                    return None
        else:
            idx['local'].append(abspath)

        return idx

    def __mt_job_prepare__(self, rjobs, subjobconfigs, masterjobconfig):
        '''preparing jobs in multiple threads'''

        logger.warning(
            'preparing %d subjobs ... it may take a while' % len(rjobs))

        # prepare the master job (i.e. create shared inputsandbox, etc.)
        master_input_sandbox = IBackend.master_prepare(self, masterjobconfig)

        # uploading the master job if it's over the WMS sandbox limitation
        for f in master_input_sandbox:
            master_input_idx = self.__check_and_prestage_inputfile__(f)

            if not master_input_idx:
                logger.error('master input sandbox perparation failed: %s' % f)
                return None

        # the algorithm for preparing a single bulk job
        class MyAlgorithm(Algorithm):

            def __init__(self):
                Algorithm.__init__(self)

            def process(self, sj_info):
                my_sc = sj_info[0]
                my_sj = sj_info[1]

                try:
                    logger.debug("preparing job %s" % my_sj.getFQID('.'))
                    jdlpath = my_sj.backend.preparejob(
                        my_sc, master_input_sandbox)

                    if (not jdlpath) or (not os.path.exists(jdlpath)):
                        raise GangaException(
                            'job %s not properly prepared' % my_sj.getFQID('.'))

                    self.__appendResult__(my_sj.id, jdlpath)
                    return True
                except Exception as x:
                    log_user_exception()
                    return False

        mt_data = []
        for sc, sj in zip(subjobconfigs, rjobs):
            mt_data.append([sc, sj])

        myAlg = MyAlgorithm()
        myData = Data(collection=mt_data)

        runner = MTRunner(
            name='lcg_jprepare', algorithm=myAlg, data=myData, numThread=10)
        runner.start()
        runner.join(-1)

        if len(runner.getDoneList()) < len(mt_data):
            return None
        else:
            # return a JDL file dictionary with subjob ids as keys, JDL file
            # paths as values
            return runner.getResults()

    def __mt_bulk_submit__(self, node_jdls):
        '''submitting jobs in multiple threads'''

        job = self.getJobObject()

        logger.warning(
            'submitting %d subjobs ... it may take a while' % len(node_jdls))

        # the algorithm for submitting a single bulk job
        class MyAlgorithm(Algorithm):

            def __init__(self, masterInputWorkspace, ce, arcverbose):
                Algorithm.__init__(self)
                self.inpw = masterInputWorkspace
                self.ce = ce
                self.arcverbose = arcverbose

            def process(self, jdl_info):
                my_sj_id = jdl_info[0]
                my_sj_jdl = jdl_info[1]

                #my_sj_jid = self.gridObj.arc_submit(my_sj_jdl, self.ce, self.verbose)
                my_sj_jid = Grid.arc_submit(
                    my_sj_jdl, self.ce, self.arcverbose)

                if not my_sj_jid:
                    return False
                else:
                    self.__appendResult__(my_sj_id, my_sj_jid)
                    return True

        mt_data = []
        for id, jdl in node_jdls.items():
            mt_data.append((id, jdl))

        myAlg = MyAlgorithm(masterInputWorkspace=job.getInputWorkspace(
        ), ce=self.CE, arcverbose=self.verbose)
        myData = Data(collection=mt_data)

        runner = MTRunner(name='arc_jsubmit', algorithm=myAlg,
                          data=myData, numThread=config['SubmissionThread'])
        runner.start()
        runner.join(timeout=-1)

        if len(runner.getDoneList()) < len(mt_data):
            # not all bulk jobs are successfully submitted. canceling the
            # submitted jobs on WMS immediately
            logger.error(
                'some bulk jobs not successfully (re)submitted, canceling submitted jobs on WMS')
            Grid.arc_cancelMultiple(runner.getResults().values())
            return None
        else:
            return runner.getResults()

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

        except IOError as e:
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
                time.sleep(0.3)
    finally:
        pass

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
            time.sleep(0.3)

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
appenvs = ###APPLICATIONENVS###
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
    except Exception as e:
        printError(sys.exc_info()[0])
        printError(sys.exc_info()[1])
        str_traceback = traceback.format_tb(sys.exc_info()[2])
        for str_tb in str_traceback:
            printError(str_tb)
        printInfo('Linking stdout & stderr to original directory failed. Looking at stdout during job run may not be possible')

os.environ['PATH'] = '.:'+os.environ['PATH']

vo = os.environ['GANGA_LCG_VO']

try:
    printInfo('Job Wrapper start.')

#   download inputsandbox from remote cache
    for f,guid in input_sandbox['remote'].iteritems():
        if not lcg_file_download(vo, guid, os.path.join(wdir,f), timeout=int(timeout)):
            raise IOError('Download remote input %s:%s failed.' % (guid,f) )
        else:
            if mimetypes.guess_type(f)[1] in ['gzip', 'bzip2']:
                getPackedInputSandbox(f)
            else:
                shutil.copy(f, os.path.join(os.getcwd(), os.path.basename(f)))

    printInfo('Download inputsandbox from iocache passed.')

#   unpack inputsandbox from wdir
    for f in input_sandbox['local']:
        if mimetypes.guess_type(f)[1] in ['gzip', 'bzip2']:
            getPackedInputSandbox(os.path.join(orig_wdir,f))

    printInfo('Unpack inputsandbox passed.')

    #get input files
    ###DOWNLOADINPUTFILES###

    printInfo('Loading Python modules ...')

    sys.path.insert(0,os.path.join(wdir,PYTHON_DIR))

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

#   execute application

    ## convern appenvs into environment setup script to be 'sourced' before executing the user executable

    printInfo('Prepare environment variables for application executable')

    env_setup_script = os.path.join(os.getcwd(), '__ganga_lcg_env__.sh')

    f = open( env_setup_script, 'w')
    f.write('#!/bin/sh' + os.linesep )
    f.write('##user application environmet setup script generated by Ganga job wrapper' + os.linesep)
    for k,v in appenvs.items():

        str_env = 'export %s="%s"' % (k, v)

        printInfo(' ** ' + str_env)
        
        f.write(str_env + os.linesep)
    f.close()

    try: #try to make shipped executable executable
        os.chmod('%s/%s'% (wdir,appexec),stat.S_IXUSR|stat.S_IRUSR|stat.S_IWUSR)
    except:
        pass

    status = False
    try:
        # use subprocess to run the user's application if the module is available on the worker node
        import subprocess
        printInfo('Load application executable with subprocess module')
        status = execSyscmdSubprocess('source %s; %s %s' % (env_setup_script, appexec, appargs), wdir)
    except ImportError as err:
        # otherwise, use separate threads to control process IO pipes
        printInfo('Load application executable with separate threads')
        status = execSyscmdEnhanced('source %s; %s %s' % (env_setup_script, appexec, appargs), wdir)

    os.system("cp %s/stdout stdout.1" % orig_wdir)
    os.system("cp %s/stderr stderr.1" % orig_wdir)

    printInfo('GZipping stdout and stderr...')

    os.system("gzip stdout.1 stderr.1")

    # move them to the original wdir so they can be picked up
    os.system("mv stdout.1.gz %s/stdout.gz" % orig_wdir)
    os.system("mv stderr.1.gz %s/stderr.gz" % orig_wdir)

    if not status:
        raise OSError('Application execution failed.')
    printInfo('Application execution passed with exit code %d.' % exitcode)      

    ###OUTPUTUPLOADSPOSTPROCESSING###

    for f in os.listdir(os.getcwd()):
        command = "cp %s %s" % (os.path.join(os.getcwd(),f), os.path.join(orig_wdir,f))
        os.system(command)            

    createPackedOutputSandbox(outputsandbox,None,orig_wdir)

#   pack outputsandbox
#    printInfo('== check output ==')
#    for line in os.popen('pwd; ls -l').readlines():
#        printInfo(line)

    printInfo('Pack outputsandbox passed.')

    # Clean up after us - All log files and packed outputsandbox should be in "wdir"
    if scratchdir:
        os.chdir(orig_wdir)
        os.system("rm %s -rf" % wdir)
except Exception as e:
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

    def preparejob(self, jobconfig, master_job_sandbox):
        '''Prepare the JDL'''

        script = self.__jobWrapperTemplate__()

        job = self.getJobObject()
        inpw = job.getInputWorkspace()

        wrapperlog = '__jobscript__.log'

        import Ganga.Core.Sandbox as Sandbox

        # FIXME: check what happens if 'stdout','stderr' are specified here
        script = script.replace(
            '###OUTPUTSANDBOX###', repr(jobconfig.outputbox))

        script = script.replace(
            '###APPLICATION_NAME###', getName(job.application))
        script = script.replace(
            '###APPLICATIONEXEC###', repr(jobconfig.getExeString()))
        script = script.replace(
            '###APPLICATIONARGS###', repr(jobconfig.getArguments()))

        from Ganga.GPIDev.Lib.File.OutputFileManager import getWNCodeForOutputPostprocessing, getWNCodeForDownloadingInputFiles

        script = script.replace(
            '###OUTPUTUPLOADSPOSTPROCESSING###', getWNCodeForOutputPostprocessing(job, '    '))

        script = script.replace(
            '###DOWNLOADINPUTFILES###', getWNCodeForDownloadingInputFiles(job, '    '))

        if jobconfig.env:
            script = script.replace(
                '###APPLICATIONENVS###', repr(jobconfig.env))
        else:
            script = script.replace('###APPLICATIONENVS###', repr({}))

        script = script.replace('###WRAPPERLOG###', repr(wrapperlog))
        import inspect
        script = script.replace(
            '###INLINEMODULES###', inspect.getsource(Sandbox.WNSandbox))

        mon = job.getMonitoringService()

        self.monInfo = None

        # set the monitoring file by default to the stdout
        if isinstance(self.monInfo, dict):
            self.monInfo['remotefile'] = 'stdout'

        # try to print out the monitoring service information in debug mode
        try:
            logger.debug('job info of monitoring service: %s' %
                         str(self.monInfo))
        except:
            pass

#       prepare input/output sandboxes
        import Ganga.Utility.files
        from Ganga.GPIDev.Lib.File import File
        from Ganga.Core.Sandbox.WNSandbox import PYTHON_DIR
        import inspect

        fileutils = File( inspect.getsourcefile(Ganga.Utility.files), subdir=PYTHON_DIR )
        packed_files = jobconfig.getSandboxFiles() + [ fileutils ]
        sandbox_files = job.createPackedInputSandbox(packed_files)

        # sandbox of child jobs should include master's sandbox
        sandbox_files.extend(master_job_sandbox)

        # check the input file size and pre-upload larger inputs to the iocache
        lfc_host = ''

        input_sandbox_uris = []
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
                    fsize = os.path.getsize(abspath)

                    if fsize > max_prestaged_fsize:
                        max_prestaged_fsize = fsize

                    input_sandbox_uris.append(
                        idx['remote'][os.path.basename(f)])

                    input_sandbox_names.append(
                        os.path.basename(urlparse(f)[2]))

                if idx['local']:
                    input_sandbox_uris += idx['local']
                    input_sandbox_names.append(os.path.basename(f))

        if not ick:
            logger.error('stop job submission')
            return None

        # determin the lcg-cp timeout according to the max_prestaged_fsize
        # - using the assumption of 1 MB/sec.
        max_prestaged_fsize = 0
        lfc_host = ''
        transfer_timeout = config['SandboxTransferTimeout']
        predict_timeout = int(math.ceil(max_prestaged_fsize / 1000000.0))

        if predict_timeout > transfer_timeout:
            transfer_timeout = predict_timeout

        if transfer_timeout < 60:
            transfer_timeout = 60

        script = script.replace(
            '###TRANSFERTIMEOUT###', '%d' % transfer_timeout)

        # update the job wrapper with the inputsandbox list
        script = script.replace(
            '###INPUTSANDBOX###', repr({'remote': {}, 'local': input_sandbox_names}))

        # write out the job wrapper and put job wrapper into job's inputsandbox
        scriptPath = inpw.writefile(
            FileBuffer('__jobscript_%s__' % job.getFQID('.'), script), executable=1)
        input_sandbox = input_sandbox_uris + [scriptPath]

        for isb in input_sandbox:
            logger.debug('ISB URI: %s' % isb)

        # compose output sandbox to include by default the following files:
        # - gzipped stdout (transferred only when the JobLogHandler is WMS)
        # - gzipped stderr (transferred only when the JobLogHandler is WMS)
        # - __jobscript__.log (job wrapper's log)
        output_sandbox = [wrapperlog]

        from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns
        for outputSandboxPattern in getOutputSandboxPatterns(job):
            output_sandbox.append(outputSandboxPattern)

        if config['JobLogHandler'] in ['WMS']:
            output_sandbox += ['stdout.gz', 'stderr.gz']

        if len(jobconfig.outputbox):
            output_sandbox += [Sandbox.OUTPUT_TARBALL_NAME]

        # compose ARC XRSL
        xrsl = {
            #'VirtualOrganisation' : config['VirtualOrganisation'],
            'executable': os.path.basename(scriptPath),
            'environment': {'GANGA_LCG_VO': config['VirtualOrganisation'], 'GANGA_LOG_HANDLER': config['JobLogHandler'], 'LFC_HOST': lfc_host},
            #'stdout'                : 'stdout',
            #'stderr'                : 'stderr',
            'inputFiles': input_sandbox,
            'outputFiles': output_sandbox,
            #'OutputSandboxBaseDestURI': 'gsiftp://localhost'
        }

        xrsl['environment'].update({'GANGA_LCG_CE': self.CE})
        #xrsl['Requirements'] = self.requirements.merge(jobconfig.requirements).convert()

        # if self.jobtype.upper() in ['NORMAL','MPICH']:
        #xrsl['JobType'] = self.jobtype.upper()
        # if self.jobtype.upper() == 'MPICH':
        #xrsl['Requirements'].append('(other.GlueCEInfoTotalCPUs >= NodeNumber)')
        # xrsl['Requirements'].append('Member("MPICH",other.GlueHostApplicationSoftwareRunTimeEnvironment)')
        #xrsl['NodeNumber'] = self.requirements.nodenumber
        # else:
        #    logger.warning('JobType "%s" not supported' % self.jobtype)
        #    return

#       additional settings from the job
        if jobconfig.env:
            xrsl['environment'].update(jobconfig.env)

        xrslText = Grid.expandxrsl(xrsl)
        
        # append any additional requirements from the requirements object
        xrslText += '\n'.join(self.requirements.other)

        logger.debug('subjob XRSL: %s' % xrslText)
        return inpw.writefile(FileBuffer('__xrslfile__', xrslText))

    def kill(self):
        '''Kill the job'''
        job = self.getJobObject()

        logger.info('Killing job %s' % job.getFQID('.'))

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return False

        return Grid.arc_cancel([self.id])

    def master_kill(self):
        '''kill the master job to the grid'''

        job = self.getJobObject()

        if not job.master and len(job.subjobs) == 0:
            return IBackend.master_kill(self)
        elif job.master:
            return IBackend.master_kill(self)
        else:
            return self.master_bulk_kill()

    def master_bulk_kill(self):
        '''GLITE bulk resubmission'''

        job = self.getJobObject()

        # killing the individually re-submitted subjobs
        logger.debug('cancelling running/submitted subjobs.')

        # 1. collect job ids
        ids = []
        for sj in job.subjobs:
            if sj.status in ['submitted', 'running'] and sj.backend.id:
                ids.append(sj.backend.id)

        # 2. cancel the collected jobs
        ck = Grid.arc_cancelMultiple(ids)
        if not ck:
            logger.warning('Job cancellation failed')
            return False
        else:
            for sj in job.subjobs:
                if sj.backend.id in ids:
                    sj.updateStatus('killed')

            return True

    def master_bulk_submit(self, rjobs, subjobconfigs, masterjobconfig):
        '''submit multiple subjobs in parallel, by default using 10 concurrent threads'''

        from Ganga.Utility.logic import implies
        assert(implies(rjobs, len(subjobconfigs) == len(rjobs)))

        # prepare the subjobs, jdl repository before bulk submission
        node_jdls = self.__mt_job_prepare__(
            rjobs, subjobconfigs, masterjobconfig)

        if not node_jdls:
            logger.error('Some jobs not successfully prepared')
            return False

        # set all subjobs to submitting status
        for sj in rjobs:
            sj.updateStatus('submitting')

        node_jids = self.__mt_bulk_submit__(node_jdls)

        status = False

        if node_jids:
            for sj in rjobs:
                if sj.id in node_jids.keys():
                    sj.backend.id = node_jids[sj.id]
                    sj.backend.CE = self.CE
                    sj.backend.actualCE = sj.backend.CE
                    sj.updateStatus('submitted')
                    sj.info.submit_counter += 1
                else:
                    logger.warning(
                        'subjob %s not successfully submitted' % sj.getFQID('.'))

            status = True

        return status

    def master_bulk_resubmit(self, rjobs):
        '''ARC bulk resubmission'''

        from Ganga.Utility.logging import log_user_exception

#        job = self.getJobObject()

        # compose master JDL for collection job
        node_jdls = {}
        for sj in rjobs:
            jdlpath = os.path.join(sj.inputdir, '__jdlfile__')
            node_jdls[sj.id] = jdlpath

        # set all subjobs to submitting status
        for sj in rjobs:
            sj.updateStatus('submitting')

        node_jids = self.__mt_bulk_submit__(node_jdls)

        status = False

        if node_jids:
            for sj in rjobs:
                if sj.id in node_jids.keys():
                    self.__refresh_jobinfo__(sj)
                    sj.backend.id = node_jids[sj.id]
                    sj.backend.CE = self.CE
                    sj.backend.actualCE = sj.backend.CE
                    sj.updateStatus('submitted')
                    sj.info.submit_counter += 1
                else:
                    logger.warning(
                        'subjob %s not successfully submitted' % sj.getFQID('.'))

            status = True

#            # set all subjobs to submitted status
#            # NOTE: this is just a workaround to avoid the unexpected transition
#            #       that turns the master job's status from 'submitted' to 'submitting'.
#            #       As this transition should be allowed to simulate a lock mechanism in Ganga 4, the workaround
#            #       is to set all subjobs' status to 'submitted' so that the transition can be avoided.
#            #       A more clear solution should be implemented with the lock mechanism introduced in Ganga 5.
#            for sj in rjobs:
#                sj.updateStatus('submitted')
#                sj.info.submit_counter += 1

        return status

    def master_submit(self, rjobs, subjobconfigs, masterjobconfig):
        '''Submit the master job to the grid'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        job = self.getJobObject()

        # finding ARC CE endpoint for job submission
        #allowed_celist = []
        # try:
        #    allowed_celist = self.requirements.getce()
        #    if not self.CE and allowed_celist:
        #        self.CE = allowed_celist[0]
        # except:
        #    logger.warning('ARC CE assigment from ARCRequirements failed.')

        # if self.CE and allowed_celist:
        #    if self.CE not in allowed_celist:
        #        logger.warning('submission to CE not allowed: %s, use %s instead' % ( self.CE, allowed_celist[0] ) )
        #        self.CE = allowed_celist[0]

        # use arc info to check for any endpoints recorded in the config file
        rc, output = Grid.arc_info()

        if not self.CE and rc != 0:
            raise GangaException(
                "ARC CE endpoint not set and no default settings in '%s'. " % config['ArcConfigFile'])
        elif self.CE:
            logger.info('ARC CE endpoint set to: ' + str(self.CE))
        else:
            logger.info("Using ARC CE endpoints defined in '%s'" %
                        config['ArcConfigFile'])

        # doing massive job preparation
        if len(job.subjobs) == 0:
            ick = IBackend.master_submit(
                self, rjobs, subjobconfigs, masterjobconfig)
        else:
            ick = self.master_bulk_submit(
                rjobs, subjobconfigs, masterjobconfig)

        profiler.check('==> master_submit() elapsed time')

        return ick

    def submit(self, subjobconfig, master_job_sandbox):
        '''Submit the job to the grid'''

        ick = False

        xrslpath = self.preparejob(subjobconfig, master_job_sandbox)

        if xrslpath:
            self.id = Grid.arc_submit(
                xrslpath, self.CE, self.verbose)

            if self.id:
                self.actualCE = self.CE
                ick = True

        return ick

    def master_auto_resubmit(self, rjobs):
        """
        Resubmit each subjob individually as bulk resubmission will overwrite
        previous master job statuses
        """

        # check for master failure - in which case bulk resubmit
        mj = self._getParent()
        if mj.status == 'failed':
            return self.master_resubmit(rjobs)

        for j in rjobs:
            if not j.backend.master_resubmit([j]):
                return False

        return True

    def master_resubmit(self, rjobs):
        '''Resubmit the master job to the grid'''

        profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
        profiler.start()

        job = self.getJobObject()

        ick = False

        if not job.master and len(job.subjobs) == 0:
            # case 1: master job normal resubmission
            logger.debug('rjobs: %s' % str(rjobs))
            logger.debug('mode: master job normal resubmission')
            ick = IBackend.master_resubmit(self, rjobs)

        elif job.master:
            # case 2: individual subjob resubmission
            logger.debug('mode: individual subjob resubmission')
            ick = IBackend.master_resubmit(self, rjobs)

        else:
            # case 3: master job bulk resubmission
            logger.debug('mode: master job resubmission')

            ick = self.master_bulk_resubmit(rjobs)
            if not ick:
                raise GangaException('ARC bulk submission failure')

        profiler.check('job re-submission elapsed time')

        return ick

    def resubmit(self):
        '''Resubmit the job'''

        ick = False

        job = self.getJobObject()

        jdlpath = job.getInputWorkspace().getPath("__jdlfile__")

        if jdlpath:
            self.id = Grid.arc_submit(jdlpath, self.CE, self.verbose)

            if self.id:
                # refresh the lcg job information
                self.__refresh_jobinfo__(job)
                self.actualCE = self.CE
                ick = True

        return ick

    @staticmethod
    def updateMonitoringInformation(jobs):
        '''Monitoring loop for normal jobs'''

        import datetime

        backenddict = {}
        jobdict = {}
        for j in jobs:
            if j.backend.id and ((datetime.datetime.utcnow() - j.time.timestamps["submitted"]).seconds > config["ArcWaitTimeBeforeStartingMonitoring"]):
                jobdict[j.backend.id] = j
                backenddict[j.backend.actualCE] = j

        if len(jobdict.keys()) == 0:
            return

        jobInfoDict = Grid.arc_status(
            jobdict.keys(), backenddict.keys())
        jidListForPurge = []

        # update job information for those available in jobInfoDict
        for id, info in jobInfoDict.items():

            if info:

                job = jobdict[id]

                if job.backend.actualCE != urlparse(id)[1].split(":")[0]:
                    job.backend.actualCE = urlparse(id)[1].split(":")[0]

                if job.backend.status != info['State']:

                    doStatusUpdate = True

                    # no need to update Ganga job status if backend status is
                    # not changed
                    if info['State'] == job.backend.status:
                        doStatusUpdate = False

                    # download output sandboxes if final status is reached
                    elif info['State'] in ['Finished', '(FINISHED)', 'Finished (FINISHED)']:

                        # grab output sandbox
                        if Grid.arc_get_output(job.backend.id, job.getOutputWorkspace(create=True).getPath()):
                            (ick, app_exitcode) = Grid.__get_app_exitcode__(
                                job.getOutputWorkspace(create=True).getPath())
                            job.backend.exitcode = app_exitcode

                            jidListForPurge.append(job.backend.id)

                        else:
                            logger.error(
                                'fail to download job output: %s' % jobdict[id].getFQID('.'))

                    if doStatusUpdate:
                        job.backend.status = info['State']
                        if 'Exit Code' in info:
                            try:
                                job.backend.exitcode_arc = int(
                                    info['Exit Code'])
                            except:
                                job.backend.exitcode_arc = 1

                        if 'Job Error' in info:
                            try:
                                job.backend.reason = info['Job Error']
                            except:
                                pass

                        job.backend.updateGangaJobStatus()
            else:
                logger.warning(
                    'fail to retrieve job informaton: %s' % jobdict[id].getFQID('.'))

        # purging the jobs the output has been fetched locally
        if jidListForPurge:
            if not Grid.arc_purgeMultiple(jidListForPurge):
                logger.warning("Failed to purge all ARC jobs.")


    def updateGangaJobStatus(self):
        '''map backend job status to Ganga job status'''

        job = self.getJobObject()

        if self.status.startswith('Running') or self.status.startswith('Finishing'):
            job.updateStatus('running')
        elif self.status.startswith('Finished'):
            if job.backend.exitcode and job.backend.exitcode != 0:
                job.backend.reason = 'non-zero app. exit code: %s' % repr(
                    job.backend.exitcode)
                job.updateStatus('failed')
            elif job.backend.exitcode_arc and job.backend.exitcode_arc != 0:
                job.backend.reason = 'non-zero ARC job exit code: %s' % repr(
                    job.backend.exitcode_arc)
                job.updateStatus('failed')
            else:
                job.updateStatus('completed')

        elif self.status in ['DONE-FAILED', 'ABORTED', 'UNKNOWN', 'Failed']:
            job.updateStatus('failed')

        elif self.status in ['CANCELLED']:
            job.updateStatus('killed')

        elif self.status.startswith('Queuing'):
            pass

        else:
            logger.warning('Unexpected job status "%s"', self.status)

logger = getLogger()


