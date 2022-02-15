import os
import re
import errno
import subprocess
import datetime
import time
import inspect
import multiprocessing
import uuid
import shutil
import sys

from pathlib import Path
from os.path import join, dirname, abspath, isdir, isfile

import GangaCore.Utility.logic
import GangaCore.Utility.util
import GangaCore.Utility.logging
import GangaCore.Utility.Config
import GangaCore.Utility.Virtualization

from GangaCore.GPIDev.Adapters.IBackend import IBackend
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.GPIDev.Base.Proxy import getName, stripProxy
from GangaCore.GPIDev.Lib.File import FileBuffer
from GangaCore.GPIDev.Lib.File import FileUtils

logger = GangaCore.Utility.logging.getLogger()
config = GangaCore.Utility.Config.getConfig('Local')

class Localhost(IBackend):

    """Run jobs in the background on local host.

    The job is run in the workdir (usually in /tmp).
    """
    _schema = Schema(Version(1, 2), {'id': SimpleItem(defvalue=-1, protected=1, copyable=0, doc='Process id.'),
                                     'status': SimpleItem(defvalue=None, typelist=[None, str], protected=1, copyable=0, hidden=1, doc='*NOT USED*'),
                                     'exitcode': SimpleItem(defvalue=None, typelist=[int, None], protected=1, copyable=0, doc='Process exit code.'),
                                     'workdir': SimpleItem(defvalue='', protected=1, copyable=0, doc='Working directory.'),
                                     'actualCE': SimpleItem(defvalue='', protected=1, copyable=0, doc='Hostname where the job was submitted.'),
                                     'wrapper_pid': SimpleItem(defvalue=-1, typelist=['int', 'list'], protected=1, copyable=0, hidden=1, doc='(internal) process id(s) of the execution wrapper(s)'),
                                     'nice': SimpleItem(defvalue=0, doc='adjust process priority using nice -n command'),
                                     'force_parallel': SimpleItem(defvalue=False, doc='should jobs really be submitted in parallel'),
                                     'batchsize': SimpleItem(defvalue=-1, typelist=[int], doc='Run a maximum of this number of subjobs in parallel. If value is negative use number of available CPUs')
                                     })
    _category = 'backends'
    _name = 'Local'

    def __init__(self):
        super(Localhost, self).__init__()


    def prepare_master_script(self, rjobs):
        job = self.getJobObject()
        wrkspace = job.getInputWorkspace()
        if not job.splitter is None:
            bs = self.batchsize
            if (bs < 0):
                bs = multiprocessing.cpu_count()
                logger.info(f'Will run up to {bs} subjobs in parallel')


            script_location = join(dirname(abspath(inspect.getfile(inspect.currentframe()))),
                                                            'LocalHostExec_batch.py.template')
            script = FileUtils.loadScript(script_location, '')
            script = script.replace('###BATCHSIZE###', str(bs))
            script = script.replace('###SUBJOBLIST###', str([str(sj.id) for sj in rjobs]))
            script = script.replace('###WORKDIR###', repr(dirname(dirname(wrkspace.getPath()))))

            scriptname = f'__jobscript__{uuid.uuid4()}' 
            wrkspace.writefile(FileBuffer(scriptname, script), executable=1)
        else:
            scriptname = '__jobscript__'
        return wrkspace.getPath(scriptname)

    
    def master_submit(self, rjobs, subjobconfigs, masterjobconfig,keep_going=False):

        rcode = super().master_submit(rjobs, subjobconfigs,
                                      masterjobconfig, keep_going, self.force_parallel)

        scriptPath = self.prepare_master_script(rjobs)
        self.run(scriptPath)
        
        return 1

    
    def submit(self, jobconfig, master_input_sandbox):
        job = self.getJobObject()
        prepared = self.preparejob(jobconfig, master_input_sandbox)
        self.actualCE = GangaCore.Utility.util.hostname()
        return 1

    def cleanworkdir(self):
        if self.workdir == '':
            import tempfile
            self.workdir = tempfile.mkdtemp(dir=config['location'])

        try:
            shutil.rmtree(self.workdir)
        except OSError as x:
            import errno
            if x.errno != errno.ENOENT:
                logger.error('problem cleaning the workdir %s, %s', self.workdir, str(x))
                return 0
        try:
            os.mkdir(self.workdir)
        except Exception as x:
            if not isdir(self.workdir):
                logger.error('cannot make the workdir %s, %s', self.workdir, str(x))
                return 0

    def master_resubmit(self, rjobs):
        scriptPath = self.prepare_master_script(rjobs)
        for sj in rjobs:
            sj.updateStatus('submitted')
        self.run(scriptPath)

        master = self.getJobObject().master
        if master is not None:
            master.updateMasterJobStatus()

        return 1
        
        
    def resubmit(self):
        self.cleanworkdir()
        job = self.getJobObject()
        return self.run(job.getInputWorkspace().getPath('__jobscript__'))

    def run(self, scriptpath):
        try:
            process = subprocess.Popen([sys.executable, scriptpath, 'subprocess'], stdin=subprocess.DEVNULL, start_new_session=True)
        except OSError as x:
            logger.error('cannot start a job process: %s', str(x))
            return 0
        oldid = self.wrapper_pid
        if type(oldid) == int:
            oldid = [oldid]
            if oldid[0] == -1:
                self.wrapper_pid = [process.pid]
                self.actualCE = GangaCore.Utility.util.hostname()
                return 1

        self.wrapper_pid = oldid+[process.pid]
        return 1

    def peek(self, filename="", command=""):
        """
        Allow viewing of output files in job's work directory
        (i.e. while job is in 'running' state)

        Arguments other than self:
        filename : name of file to be viewed
                  => Path specified relative to work directory
        command  : command to be used for file viewing

        Return value: None
        """
        job = self.getJobObject()
        topdir = self.workdir.rstrip(os.sep)
        path = join(topdir, filename).rstrip(os.sep)
        job.viewFile(path=path, command=command)
        return None

    def getStateTime(self, status):
        """Obtains the timestamps for the 'running', 'completed', and 'failed' states.

           The __jobstatus__ file in the job's output directory is read to obtain the start and stop times of the job.
           These are converted into datetime objects and returned to the user.
        """
        j = self.getJobObject()
        end_list = ['completed', 'failed']
        d = {}
        checkstr = ''

        if status == 'running':
            checkstr = 'START:'
        elif status == 'completed':
            checkstr = 'STOP:'
        elif status == 'failed':
            checkstr = 'FAILED:'
        else:
            checkstr = ''

        if checkstr == '':
            logger.debug("In getStateTime(): checkstr == ''")
            return None

        try:
            p = join(j.outputdir, '__jobstatus__')
            logger.debug("Opening output file at: %s", p)
            f = open(p)
        except IOError:
            logger.debug('unable to open file %s', p)
            return None

        for l in f:
            if checkstr in l:
                pos = l.find(checkstr)
                timestr = l[pos + len(checkstr) + 1:pos + len(checkstr) + 25]
                try:
                    t = datetime.datetime(
                        *(time.strptime(timestr, "%a %b %d %H:%M:%S %Y")[0:6]))
                except ValueError:
                    logger.debug(
                        "Value Error in file: '%s': string does not match required format.", p)
                    return None
                return t

        f.close()
        logger.debug(
            "Reached the end of getStateTime('%s'). Returning None.", status)
        return None

    def timedetails(self):
        """Return all available timestamps from this backend.
        """
        j = self.getJobObject()
        # check for file. if it's not there don't bother calling getSateTime
        # (twice!)
        p = join(j.outputdir, '__jobstatus__')
        if not isfile(p):
            logger.error('unable to open file %s', p)
            return None

        r = self.getStateTime('running')
        c = self.getStateTime('completed')
        d = {'START': r, 'STOP': c}

        return d

    def preparejob(self, jobconfig, master_input_sandbox):

        job = self.getJobObject()
        # print str(job.backend_output_postprocess)
        mon = job.getMonitoringService()
        import GangaCore.Core.Sandbox as Sandbox
        from GangaCore.GPIDev.Lib.File import File
        from GangaCore.Core.Sandbox.WNSandbox import PYTHON_DIR

        virtualization = job.virtualization

        utilFiles= []
        fileutils = File( inspect.getsourcefile(GangaCore.Utility.files), subdir=PYTHON_DIR )
        utilFiles.append(fileutils)
        if virtualization:
            virtualizationutils = File( inspect.getsourcefile(GangaCore.Utility.Virtualization), subdir=PYTHON_DIR )
            utilFiles.append(virtualizationutils)
        sharedfiles = jobconfig.getSharedFiles()

        subjob_input_sandbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles() + utilFiles)

        appscriptpath = [jobconfig.getExeString()] + jobconfig.getArgStrings()
        if self.nice:
            appscriptpath = ['nice', '-n %d' % self.nice] + appscriptpath
        if self.nice < 0:
            logger.warning('increasing process priority is often not allowed, your job may fail due to this')

        sharedoutputpath = job.getOutputWorkspace().getPath()
        ## FIXME DON'T just use the blind list here, request the list of files to be in the output from a method.
        outputpatterns = jobconfig.outputbox
        environment = dict() if jobconfig.env is None else jobconfig.env

        import tempfile
        workdir = tempfile.mkdtemp(dir=config['location'])

        script_location = join(dirname(abspath(inspect.getfile(inspect.currentframe()))),
                                                        'LocalHostExec.py.template')

        from GangaCore.GPIDev.Lib.File import FileUtils
        script = FileUtils.loadScript(script_location, '')

        script = script.replace('###INLINEMODULES###', inspect.getsource(Sandbox.WNSandbox))

        from GangaCore.GPIDev.Lib.File.OutputFileManager import getWNCodeForOutputSandbox, getWNCodeForOutputPostprocessing, getWNCodeForDownloadingInputFiles, getWNCodeForInputdataListCreation
        from GangaCore.Utility.Config import getConfig
        jobidRepr = repr(job.getFQID('.'))


        script = script.replace('###OUTPUTSANDBOXPOSTPROCESSING###', getWNCodeForOutputSandbox(job, ['stdout', 'stderr', '__syslog__'], jobidRepr))
        script = script.replace('###OUTPUTUPLOADSPOSTPROCESSING###', getWNCodeForOutputPostprocessing(job, ''))
        script = script.replace('###DOWNLOADINPUTFILES###', getWNCodeForDownloadingInputFiles(job, ''))
        script = script.replace('###CREATEINPUTDATALIST###', getWNCodeForInputdataListCreation(job, ''))

        script = script.replace('###APPLICATION_NAME###', repr(getName(job.application)))
        script = script.replace('###INPUT_SANDBOX###', repr(subjob_input_sandbox + master_input_sandbox + sharedfiles))
        script = script.replace('###SHAREDOUTPUTPATH###', repr(sharedoutputpath))
        script = script.replace('###APPSCRIPTPATH###', repr(appscriptpath))
        script = script.replace('###OUTPUTPATTERNS###', str(outputpatterns))
        script = script.replace('###JOBID###', jobidRepr)
        script = script.replace('###ENVIRONMENT###', repr(environment))
        script = script.replace('###WORKDIR###', repr(workdir))
        script = script.replace('###INPUT_DIR###', repr(job.getStringInputDir()))

        if virtualization:
            script = virtualization.modify_script(script)

        self.workdir = workdir

        script = script.replace('###GANGADIR###', repr(getConfig('System')['GANGA_PYTHONPATH']))

        wrkspace = job.getInputWorkspace()
        scriptPath = wrkspace.writefile(FileBuffer('__jobscript__', script), executable=1)

        return scriptPath


    def master_kill(self):
        job = self.getJobObject()

        # Kill the wrapper that manages the subjobs if it is a master job
        if len(job.subjobs):
            self.kill()

        return super().master_kill()
    
    def kill(self):
        job = self.getJobObject()

        pids = self.wrapper_pid
        if type(pids) == int:
            pids = [pids]

        if pids[0] < 0:
            return 1

        for wrapper_pid in pids:
            
            try:
                groupid = os.getpgid(wrapper_pid)
                logger.debug(f"Wrapper for {job.getFQID('.')} has gid {groupid} and pid {self.wrapper_pid}")
                subprocess.run(['kill', '-9', f'-{groupid}'])
            except OSError as x:
                logger.warning('While killing wrapper script for job %s: pid=%d, %s', job.getFQID('.'), self.wrapper_pid, str(x))

            # waitpid to avoid zombies. This always returns an error
            try:
                ws = os.waitpid(wrapper_pid, 0)
            except OSError:
                pass

        self.wrapper_pid = -1
            
        from GangaCore.Utility.files import recursive_copy

        if self.workdir:
            for fn in ['stdout', 'stderr', '__syslog__']:
                try:
                    recursive_copy(
                        join(self.workdir, fn), job.getOutputWorkspace().getPath())
                except Exception as x:
                    logger.info('problem retrieving %s: %s', fn, x)

            self.remove_workdir()
        return 1

    def remove_workdir(self):
        if config['remove_workdir'] and self.workdir:
            import shutil
            try:
                logger.debug("removing: %s" % self.workdir)
                shutil.rmtree(self.workdir)
            except OSError as x:
                logger.warning('problem removing the workdir %s: %s', str(self.id), str(x))
                shutil.rmtree(self.workdir, ignore_errors=True)

                
    @staticmethod
    def updateMonitoringInformation(jobs):

        def get_exit_code(f):
            import re
            with open(f) as statusfile:
                stat = statusfile.read()
            m = re.compile(
                r'^EXITCODE: (?P<exitcode>-?\d*)', re.M).search(stat)

            if m is None:
                return None
            else:
                return int(m.group('exitcode'))

            
        def get_pids(f):
            import re
            with open(f) as statusfile:
                stat = statusfile.read()
            m_pid = re.compile(r'^PID: (?P<pid>\d*)', re.M).search(stat)
            m_wrapper = re.compile(r'^WRAPPER: (?P<pid>\d*)', re.M).search(stat)

            pid = None
            wrapper = None
            
            if m_pid:
                pid = int(m_pid.group('pid'))
            if m_wrapper:
                wrapper = int(m_wrapper.group('pid'))
                
            return pid, wrapper

        
        logger.debug('local ping: %s', str(jobs))

        for j in jobs:
            outw = j.getOutputWorkspace()

            # try to get the application exit code from the status file
            try:
                statusfile = join(outw.getPath(), '__jobstatus__')
                if j.status == 'submitted':
                    pid, wrapper_pid = get_pids(statusfile)
                    if wrapper_pid:
                        j.backend.wrapper_pid = wrapper_pid
                    if pid:
                        j.backend.id = pid
                        #logger.info('Local job %s status changed to running, pid=%d',j.getFQID('.'),pid)
                        j.updateStatus('running')  # bugfix: 12194
                exitcode = get_exit_code(statusfile)
                with open(statusfile) as status_file:
                    logger.debug('status file: %s %s', statusfile, status_file.read())
            except IOError as x:
                logger.debug('problem reading status file: %s (%s)', statusfile, str(x))
                exitcode = None
            except Exception as x:
                logger.critical('problem during monitoring: %s', str(x))
                import traceback
                traceback.print_exc()
                raise x

            if not exitcode is None:
                # status file indicates that the application finished
                j.backend.exitcode = exitcode

                if exitcode == 0:
                    j.updateStatus('completed')
                else:
                    j.updateStatus('failed')

                #logger.info('Local job %s finished with exitcode %d',j.getFQID('.'),exitcode)

                # if j.outputdata:
                # j.outputdata.fill()

                j.backend.remove_workdir()

