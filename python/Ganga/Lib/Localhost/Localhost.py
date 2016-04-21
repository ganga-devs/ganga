from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

import Ganga.Utility.logic
import Ganga.Utility.util

from Ganga.GPIDev.Lib.File import FileBuffer

import os
import os.path
import re
import errno

import subprocess

import datetime
import time

import Ganga.Utility.logging

import Ganga.Utility.Config

from Ganga.GPIDev.Base.Proxy import getName, stripProxy

logger = Ganga.Utility.logging.getLogger()
config = Ganga.Utility.Config.getConfig('Local')

class Localhost(IBackend):

    """Run jobs in the background on local host.

    The job is run in the workdir (usually in /tmp).
    """
    _schema = Schema(Version(1, 2), {'id': SimpleItem(defvalue=-1, protected=1, copyable=0, doc='Process id.'),
                                     'status': SimpleItem(defvalue=None, typelist=[None, str], protected=1, copyable=0, hidden=1, doc='*NOT USED*'),
                                     'exitcode': SimpleItem(defvalue=None, typelist=[int, None], protected=1, copyable=0, doc='Process exit code.'),
                                     'workdir': SimpleItem(defvalue='', protected=1, copyable=0, doc='Working directory.'),
                                     'actualCE': SimpleItem(defvalue='', protected=1, copyable=0, doc='Hostname where the job was submitted.'),
                                     'wrapper_pid': SimpleItem(defvalue=-1, protected=1, copyable=0, hidden=1, doc='(internal) process id of the execution wrapper'),
                                     'nice': SimpleItem(defvalue=0, doc='adjust process priority using nice -n command')
                                     })
    _category = 'backends'
    _name = 'Local'

    def __init__(self):
        super(Localhost, self).__init__()

    def submit(self, jobconfig, master_input_sandbox):
        prepared = self.preparejob(jobconfig, master_input_sandbox)
        self.run(prepared)
        return 1

    def resubmit(self):
        job = self.getJobObject()
        import shutil

        try:
            shutil.rmtree(self.workdir)
        except OSError as x:
            import errno
            if x.errno != errno.ENOENT:
                logger.error(
                    'problem cleaning the workdir %s, %s', self.workdir, str(x))
                return 0
        try:
            os.mkdir(self.workdir)
        except Exception as x:
            logger.error(
                'cannot make the workdir %s, %s', self.workdir, str(x))
            return 0
        return self.run(job.getInputWorkspace().getPath('__jobscript__'))

    def run(self, scriptpath):
        try:
            process = subprocess.Popen(["python", scriptpath, 'subprocess'])
        except OSError as x:
            logger.error('cannot start a job process: %s', str(x))
            return 0
        self.wrapper_pid = process.pid
        self.actualCE = Ganga.Utility.util.hostname()
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
        path = os.path.join(topdir, filename).rstrip(os.sep)
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
            p = os.path.join(j.outputdir, '__jobstatus__')
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
        p = os.path.join(j.outputdir, '__jobstatus__')
        if not os.path.isfile(p):
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
        import Ganga.Core.Sandbox as Sandbox
        from Ganga.GPIDev.Lib.File import File
        from Ganga.Core.Sandbox.WNSandbox import PYTHON_DIR
        import inspect

        fileutils = File( inspect.getsourcefile(Ganga.Utility.files), subdir=PYTHON_DIR )
        subjob_input_sandbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles() + [ fileutils ] )

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

        import inspect
        script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                                        'LocalHostExec.py')

        from Ganga.GPIDev.Lib.File import FileUtils
        script = FileUtils.loadScript(script_location, '')

        script = script.replace('###INLINEMODULES###', inspect.getsource(Sandbox.WNSandbox))

        from Ganga.GPIDev.Lib.File.OutputFileManager import getWNCodeForOutputSandbox, getWNCodeForOutputPostprocessing, getWNCodeForDownloadingInputFiles, getWNCodeForInputdataListCreation
        from Ganga.Utility.Config import getConfig
        jobidRepr = repr(job.getFQID('.'))


        script = script.replace('###OUTPUTSANDBOXPOSTPROCESSING###', getWNCodeForOutputSandbox(job, ['stdout', 'stderr', '__syslog__'], jobidRepr))
        script = script.replace('###OUTPUTUPLOADSPOSTPROCESSING###', getWNCodeForOutputPostprocessing(job, ''))
        script = script.replace('###DOWNLOADINPUTFILES###', getWNCodeForDownloadingInputFiles(job, ''))
        script = script.replace('###CREATEINPUTDATALIST###', getWNCodeForInputdataListCreation(job, ''))

        script = script.replace('###APPLICATION_NAME###', repr(getName(job.application)))
        script = script.replace('###INPUT_SANDBOX###', repr(subjob_input_sandbox + master_input_sandbox))
        script = script.replace('###SHAREDOUTPUTPATH###', repr(sharedoutputpath))
        script = script.replace('###APPSCRIPTPATH###', repr(appscriptpath))
        script = script.replace('###OUTPUTPATTERNS###', str(outputpatterns))
        script = script.replace('###JOBID###', jobidRepr)
        script = script.replace('###ENVIRONMENT###', repr(environment))
        script = script.replace('###WORKDIR###', repr(workdir))
        script = script.replace('###INPUT_DIR###', repr(job.getStringInputDir()))

        self.workdir = workdir

        script = script.replace('###GANGADIR###', repr(getConfig('System')['GANGA_PYTHONPATH']))

        wrkspace = job.getInputWorkspace()
        scriptPath = wrkspace.writefile(FileBuffer('__jobscript__', script), executable=1)

        return scriptPath

    def kill(self):
        import os
        import signal

        job = self.getJobObject()

        ok = True
        try:
            # kill the wrapper script
            # bugfix: #18178 - since wrapper script sets a new session and new
            # group, we can use this to kill all processes in the group
            os.kill(-self.wrapper_pid, signal.SIGKILL)
        except OSError as x:
            logger.warning('while killing wrapper script for job %s: pid=%d, %s', job.getFQID('.'), self.wrapper_pid, str(x))
            ok = False

        # waitpid to avoid zombies
        try:
            ws = os.waitpid(self.wrapper_pid, 0)
        except OSError as x:
            logger.warning('problem while waitpid %s: %s', job.getFQID('.'), x)

        from Ganga.Utility.files import recursive_copy

        for fn in ['stdout', 'stderr', '__syslog__']:
            try:
                recursive_copy(
                    os.path.join(self.workdir, fn), job.getOutputWorkspace().getPath())
            except Exception as x:
                logger.info('problem retrieving %s: %s', fn, x)

        self.remove_workdir()
        return 1

    def remove_workdir(self):
        if config['remove_workdir']:
            import shutil
            try:
                shutil.rmtree(self.workdir)
            except OSError as x:
                logger.warning('problem removing the workdir %s: %s', str(self.id), str(x))

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

        def get_pid(f):
            import re
            with open(f) as statusfile:
                stat = statusfile.read()
            m = re.compile(r'^PID: (?P<pid>\d*)', re.M).search(stat)

            if m is None:
                return None
            else:
                return int(m.group('pid'))

        logger.debug('local ping: %s', str(jobs))

        for j in jobs:
            outw = j.getOutputWorkspace()

            # try to get the application exit code from the status file
            try:
                statusfile = os.path.join(outw.getPath(), '__jobstatus__')
                if j.status == 'submitted':
                    pid = get_pid(statusfile)
                    if pid:
                        stripProxy(j.backend).id = pid
                        #logger.info('Local job %s status changed to running, pid=%d',j.getFQID('.'),pid)
                        j.updateStatus('running')  # bugfix: 12194
                exitcode = get_exit_code(statusfile)
                with open(statusfile) as status_file:
                    logger.debug(
                        'status file: %s %s', statusfile, status_file.read())
            except IOError as x:
                logger.debug(
                    'problem reading status file: %s (%s)', statusfile, str(x))
                exitcode = None
            except Exception as x:
                logger.critical('problem during monitoring: %s', str(x))
                import traceback
                traceback.print_exc()
                raise x

            # check if the exit code of the wrapper script is available (non-blocking check)
            # if the wrapper script exited with non zero this is an error
            try:
                ws = os.waitpid(stripProxy(j.backend).wrapper_pid, os.WNOHANG)
                if not Ganga.Utility.logic.implies(ws[0] != 0, ws[1] == 0):
                    # FIXME: for some strange reason the logger DOES NOT LOG (checked in python 2.3 and 2.5)
                    # print 'logger problem', logger.name
                    # print 'logger',logger.getEffectiveLevel()
                    logger.critical('wrapper script for job %s exit with code %d', str(j.getFQID('.')), ws[1])
                    logger.critical('report this as a bug at https://github.com/ganga-devs/ganga/issues/')
                    j.updateStatus('failed')
            except OSError as x:
                if x.errno != errno.ECHILD:
                    logger.warning(
                        'cannot do waitpid for %d: %s', stripProxy(j.backend).wrapper_pid, str(x))

            # if the exit code was collected for the application get the exit
            # code back

            if not exitcode is None:
                # status file indicates that the application finished
                stripProxy(j.backend).exitcode = exitcode

                if exitcode == 0:
                    j.updateStatus('completed')
                else:
                    j.updateStatus('failed')

                #logger.info('Local job %s finished with exitcode %d',j.getFQID('.'),exitcode)

                # if j.outputdata:
                # j.outputdata.fill()

                stripProxy(j.backend).remove_workdir()

