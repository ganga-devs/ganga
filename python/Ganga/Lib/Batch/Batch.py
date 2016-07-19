import datetime
import time
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Base.Proxy import isType, getName, stripProxy
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.Core import BackendError
import os.path

import Ganga.Utility.logging

import Ganga.Utility.Config

import os

logger = Ganga.Utility.logging.getLogger()

# A trival implementation of shell command with stderr/stdout capture
# This is a self-contained function (with logging).
#
# return (exitcode,soutfile,exeflag)
# soutfile - path where the stdout/stderr is stored
# exeflag - 0 if the command failed to execute, 1 if it executed
def shell_cmd(cmd, soutfile=None, allowed_exit=[0]):

    if not soutfile:
        import tempfile
        soutfile = tempfile.mktemp()

    # FIXME: garbbing stdout is done by shell magic and probably should be
    # implemented in python directly
    cmd = "%s > %s 2>&1" % (cmd, soutfile)

    logger.debug("running shell command: %s", cmd)
    rc = os.system(cmd)

    if not rc in allowed_exit:
        logger.debug('exit status [%d] of command %s', rc, cmd)
        logger.debug('full output is in file: %s', soutfile)
        with open(soutfile) as sout_file:
            logger.debug('<first 255 bytes of output>\n%s', sout_file.read(255))
        logger.debug('<end of first 255 bytes of output>')

    m = None

    if rc != 0:
        logger.debug('non-zero [%d] exit status of command %s ', rc, cmd)
        import re
        with open(soutfile) as sout_file:
            m = re.compile(r"command not found$", re.M).search(sout_file.read())

    return rc, soutfile, m is None


class Batch(IBackend):

    """ Batch submission backend.

    It  is  assumed  that   Batch  commands  (bjobs,  bsub  etc.)  setup
    correctly. As  little assumptions as  possible are made  about the
    Batch configuration but  at certain sites it may  not work correctly
    due  to a  different  Batch setup.  Tested  with CERN  and CNAF  Batch
    installations.

    Each batch system supports an 'extraopts' field, which allows customisation
    of way the job is submitted.

    PBS:
    Take environment settings on submitting machine and export to batch job:
    backend.extraopts = "-V"

    Request minimum walltime of 24 hours and minimum memory of 2GByte:
    backend.extraopts = "-l walltime=24:00:00 mem=2gb"

    The above can be combined as:
    backend.extraopts = "-V -l walltime=24:00:00 mem=2gb" 

    LSF:
    Sends mail to you when the job is dispatched and begins execution.
    backend.extraopts = "-B"

    Assigns the Ganga job name to the batch job. The job name does not need to 
    be unique.
    backend.extraopts = "-J "+ j.name

    Run the job on a host that meets the specified resource requirements.
    A resource requirement string describes the resources a job needs.
    E.g request 2Gb of memory ans 1Gb of swap space
    backend.extraopts = '-R "mem=2048" -R "swp=1024"'

    Kill job if it has exceeded the deadline (i.e. for your presentation)
    backend.extraopts = '-t 07:14:12:59' #Killed if not finished by 14 July before 1 pm
    """
    _schema = Schema(Version(1, 0), {'queue': SimpleItem(defvalue='', doc='queue name as defomed in your local Batch installation'),
                                     'extraopts': SimpleItem(defvalue='', doc='extra options for Batch. See help(Batch) for more details'),
                                     'id': SimpleItem(defvalue='', protected=1, copyable=0, doc='Batch id of the job'),
                                     'exitcode': SimpleItem(defvalue=None, typelist=[int, None], protected=1, copyable=0, doc='Process exit code'),
                                     'status': SimpleItem(defvalue='', protected=1, hidden=1, copyable=0, doc='Batch status of the job'),
                                     'actualqueue': SimpleItem(defvalue='', protected=1, copyable=0, doc='queue name where the job was submitted.'),
                                     'actualCE': SimpleItem(defvalue='', protected=1, copyable=0, doc='hostname where the job is/was running.')
                                     })
    _category = 'backends'
    _name = 'Batch'
    _hidden = 1

    def __init__(self):
        super(Batch, self).__init__()

    def command(klass, cmd, soutfile=None, allowed_exit=None):
        if allowed_exit is None:
            allowed_exit = [0]
        rc, soutfile, ef = shell_cmd(cmd, soutfile, allowed_exit)
        if not ef:
            logger.error(
                'Problem submitting batch job. Maybe your chosen batch system is not available or you have configured it wrongly')
            with open(soutfile) as sout_file:
                logger.error(sout_file.read())
                raiseable = BackendError(klass._name, 'It seems that %s commands are not installed properly:%s' % (klass._name, sout_file.readline()))
        return rc, soutfile

    command = classmethod(command)

    def submit(self, jobconfig, master_input_sandbox):

        job = self.getJobObject()

        inw = job.getInputWorkspace()
        outw = job.getOutputWorkspace()

        #scriptpath = self.preparejob(jobconfig,inw,outw)
        scriptpath = self.preparejob(jobconfig, master_input_sandbox)

        # FIX from Angelo Carbone
        # stderr_option = '-e '+str(outw.getPath())+'stderr'
        # stdout_option = '-o '+str(outw.getPath())+'stdout'

        # FIX from Alex Richards - see Savannah #87477
        stdout_option = self.config['stdoutConfig'] % str(outw.getPath())
        stderr_option = self.config['stderrConfig'] % str(outw.getPath())

        queue_option = ''
        if self.queue:
            queue_option = '-q ' + str(self.queue)

        try:
            jobnameopt = "-" + self.config['jobnameopt']
        except Exception, err:
            logger.debug("Unknown error: %s" % str(err))
            jobnameopt = False

        if self.extraopts:
            import re
            for opt in re.compile(r'(-\w+)').findall(self.extraopts):
                if opt in ('-o', '-e', '-oo', '-eo'):
                    logger.warning("option %s is forbidden", opt)
                    return False
                if self.queue and opt == '-q':
                    logger.warning("option %s is forbidden if queue is defined ( queue = '%s')", opt, self.queue)
                    return False
                if jobnameopt and opt == jobnameopt:
                    jobnameopt = False

            queue_option = queue_option + " " + self.extraopts

        if jobnameopt and job.name != '':
            # PBS doesn't like names with spaces
            tmp_name = job.name
            if isType(self, PBS):
                tmp_name = tmp_name.replace(" ", "_")
            queue_option = queue_option + " " + \
                jobnameopt + " " + "'%s'" % (tmp_name)

        # bugfix #16646
        if self.config['shared_python_executable']:
            import sys
            script_cmd = "%s %s" % (sys.executable, scriptpath)
        else:
            script_cmd = scriptpath

        command_str = self.config['submit_str'] % (inw.getPath(), queue_option, stderr_option, stdout_option, script_cmd)
        self.command_string = command_str
        rc, soutfile = self.command(command_str)
        with open(soutfile) as sout_file:
            sout = sout_file.read()
        import re
        m = re.compile(self.config['submit_res_pattern'], re.M).search(sout)
        if m is None:
            logger.warning('could not match the output and extract the Batch job identifier!')
            logger.warning('command output \n %s ', sout)
        else:
            self.id = m.group('id')
            try:
                queue = m.group('queue')
                if self.queue != queue:
                    if self.queue:
                        logger.warning('you requested queue "%s" but the job was submitted to queue "%s"', self.queue, queue)
                        logger.warning('command output \n %s ', sout)
                    else:
                        logger.info('using default queue "%s"', queue)
                    self.actualqueue = queue
            except IndexError:
                logger.info('could not match the output and extract the Batch queue name')

        # clean up the tmp file
        if os.path.exists(soutfile):
            os.remove(soutfile)

        return rc == 0

    def resubmit(self):

        job = self.getJobObject()

        inw = job.getInputWorkspace()
        outw = job.getOutputWorkspace()

        statusfilename = outw.getPath('__jobstatus__')
        try:
            os.remove(statusfilename)
        except OSError as x:
            if x.errno != 2:
                logger.warning("OSError:" + str(x))

        scriptpath = inw.getPath('__jobscript__')
        #stderr_option = '-e '+str(outw.getPath())+'stderr'
        #stdout_option = '-o '+str(outw.getPath())+'stdout'

        # FIX from Alex Richards - see Savannah #87477
        stdout_option = self.config['stdoutConfig'] % str(outw.getPath())
        stderr_option = self.config['stderrConfig'] % str(outw.getPath())

        queue_option = ''
        if self.queue:
            queue_option = '-q ' + str(self.queue)

        try:
            jobnameopt = "-" + self.config['jobnameopt']
        except Exception as err:
            logger.debug("Err: %s" % str(err))
            jobnameopt = False

        if self.extraopts:
            import re
            for opt in re.compile(r'(-\w+)').findall(self.extraopts):
                if opt in ('-o', '-e', '-oo', '-eo'):
                    logger.warning("option %s is forbidden", opt)
                    return False
                if self.queue and opt == '-q':
                    logger.warning("option %s is forbidden if queue is defined ( queue = '%s')", opt, self.queue)
                    return False
                if jobnameopt and opt == jobnameopt:
                    jobnameopt = False

            queue_option = queue_option + " " + self.extraopts

        if jobnameopt and job.name != '':
            # PBS doesn't like names with spaces
            tmp_name = job.name
            if isType(self, PBS):
                tmp_name = tmp_name.replace(" ", "_")
            queue_option = queue_option + " " + \
                jobnameopt + " " + "'%s'" % (tmp_name)

        # bugfix #16646
        if self.config['shared_python_executable']:
            import sys
            script_cmd = "%s %s" % (sys.executable, scriptpath)
        else:
            script_cmd = scriptpath

        command_str = self.config['submit_str'] % (
            inw.getPath(), queue_option, stderr_option, stdout_option, script_cmd)
        self.command_string = command_str
        rc, soutfile = self.command(command_str)
        logger.debug('from command get rc: "%d"', rc)
        if rc == 0:
            with open(soutfile) as sout_file:
                sout = sout_file.read()
            import re
            m = re.compile(
                self.config['submit_res_pattern'], re.M).search(sout)
            if m is None:
                logger.warning('could not match the output and extract the Batch job identifier!')
                logger.warning('command output \n %s ', sout)
            else:
                self.id = m.group('id')
                try:
                    queue = m.group('queue')
                    if self.queue != queue:
                        if self.queue:
                            logger.warning('you requested queue "%s" but the job was submitted to queue "%s"', self.queue, queue)
                            logger.warning('command output \n %s ', sout)
                        else:
                            logger.info('using default queue "%s"', queue)
                        self.actualqueue = queue
                except IndexError:
                    logger.info('could not match the output and extract the Batch queue name')
        else:
            with open(soutfile) as sout_file:
                logger.warning(sout_file.read())

        return rc == 0

    def kill(self):
        rc, soutfile = self.command(self.config['kill_str'] % (self.id))

        with open(soutfile) as sout_file:
            sout = sout_file.read()
        logger.debug('while killing job %s: rc = %d', self.getJobObject().getFQID('.'), rc)
        if rc == 0:
            return True
        else:
            import re
            m = re.compile(self.config['kill_res_pattern'], re.M).search(sout)
            logger.warning('while killing job %s: %s', self.getJobObject().getFQID('.'), sout)

            return m is not None

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
                    logger.debug("Value Error in file: '%s': string does not match required format.", p)
                    return None
                return t

        f.close()
        logger.debug("Reached the end of getStateTime('%s'). Returning None.", status)
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
        mon = job.getMonitoringService()
        import Ganga.Core.Sandbox as Sandbox
        from Ganga.GPIDev.Lib.File import File
        from Ganga.Core.Sandbox.WNSandbox import PYTHON_DIR
        import inspect

        fileutils = File( inspect.getsourcefile(Ganga.Utility.files), subdir=PYTHON_DIR )
        subjob_input_sandbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles() + [ fileutils ] )

        appscriptpath = [jobconfig.getExeString()] + jobconfig.getArgStrings()
        sharedoutputpath = job.getOutputWorkspace().getPath()
        ## FIXME Check this isn't a GangaList
        outputpatterns = jobconfig.outputbox
        environment = jobconfig.env if not jobconfig.env is None else {}


        import inspect
        script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                                       'BatchScriptTemplate.py')

        from Ganga.GPIDev.Lib.File import FileUtils
        text = FileUtils.loadScript(script_location, '')

        import Ganga.Core.Sandbox as Sandbox
        import Ganga.Utility as Utility
        from Ganga.Utility.Config import getConfig
        from Ganga.GPIDev.Lib.File.OutputFileManager import getWNCodeForOutputSandbox, getWNCodeForOutputPostprocessing, getWNCodeForDownloadingInputFiles
        jobidRepr = repr(self.getJobObject().getFQID('.'))

        replace_dict = {

        '###OUTPUTSANDBOXPOSTPROCESSING###' : getWNCodeForOutputSandbox(job, ['__syslog__'], jobidRepr),

        '###OUTPUTUPLOADSPOSTPROCESSING###' : getWNCodeForOutputPostprocessing(job, ''),

        '###DOWNLOADINPUTFILES###' : getWNCodeForDownloadingInputFiles(job, ''),

        '###INLINEMODULES###' : inspect.getsource(Sandbox.WNSandbox),
        '###INLINEHOSTNAMEFUNCTION###' : inspect.getsource(Utility.util.hostname),
        '###APPSCRIPTPATH###' : repr(appscriptpath),
        #'###SHAREDINPUTPATH###' : repr(sharedinputpath)),

        '###INPUT_SANDBOX###' : repr(subjob_input_sandbox + master_input_sandbox),
        '###SHAREDOUTPUTPATH###' : repr(sharedoutputpath),

        '###OUTPUTPATTERNS###' : repr(outputpatterns),
        '###JOBID###' : jobidRepr,
        '###ENVIRONMENT###' : repr(environment),
        '###PREEXECUTE###' : self.config['preexecute'],
        '###POSTEXECUTE###' : self.config['postexecute'],
        '###JOBIDNAME###' : self.config['jobid_name'],
        '###QUEUENAME###' : self.config['queue_name'],
        '###HEARTBEATFREQUENCE###' : self.config['heartbeat_frequency'],
        '###INPUT_DIR###' : repr(job.getStringInputDir()),

        '###GANGADIR###' : repr(getConfig('System')['GANGA_PYTHONPATH'])
        }

        for k, v in replace_dict.iteritems():
            text = text.replace(str(k), str(v))

        logger.debug('subjob input sandbox %s ', subjob_input_sandbox)
        logger.debug('master input sandbox %s ', master_input_sandbox)

        from Ganga.GPIDev.Lib.File import FileBuffer

        return job.getInputWorkspace().writefile(FileBuffer('__jobscript__', text), executable=1)

    @staticmethod
    def updateMonitoringInformation(jobs):

        import re
        repid = re.compile(r'^PID: (?P<pid>\d+)', re.M)
        requeue = re.compile(r'^QUEUE: (?P<queue>\S+)', re.M)
        reactualCE = re.compile(r'^ACTUALCE: (?P<actualCE>\S+)', re.M)
        reexit = re.compile(r'^EXITCODE: (?P<exitcode>\d+)', re.M)

        def get_last_alive(f):
            """Time since the statusfile was last touched in seconds"""
            import os.path
            import time
            talive = 0
            try:
                talive = time.time() - os.path.getmtime(f)
            except OSError as x:
                logger.debug('Problem reading status file: %s (%s)', f, str(x))

            return talive

        def get_status(f):
            """Give (pid,queue,actualCE,exit code) for job"""

            pid, queue, actualCE, exitcode = None, None, None, None

            import re
            statusfile = None
            try:
                statusfile = open(f)
                stat = statusfile.read()
            except IOError as x:
                logger.debug('Problem reading status file: %s (%s)', f, str(x))
                return pid, queue, actualCE, exitcode
            finally:
                if statusfile:
                    statusfile.close()

            mpid = repid.search(stat)
            if mpid:
                pid = int(mpid.group('pid'))

            mqueue = requeue.search(stat)
            if mqueue:
                queue = str(mqueue.group('queue'))

            mactualCE = reactualCE.search(stat)
            if mactualCE:
                actualCE = str(mactualCE.group('actualCE'))

            mexit = reexit.search(stat)
            if mexit:
                exitcode = int(mexit.group('exitcode'))

            return pid, queue, actualCE, exitcode

        from Ganga.Utility.Config import getConfig
        for j in jobs:
            stripProxy(j)._getSessionLock()
            outw = j.getOutputWorkspace()

            statusfile = os.path.join(outw.getPath(), '__jobstatus__')
            heartbeatfile = os.path.join(outw.getPath(), '__heartbeat__')
            pid, queue, actualCE, exitcode = get_status(statusfile)

            if j.status == 'submitted':
                if pid or queue:
                    j.updateStatus('running')

                    if pid:
                        j.backend.id = pid

                    if queue and queue != j.backend.actualqueue:
                        j.backend.actualqueue = queue

                    if actualCE:
                        j.backend.actualCE = actualCE

            if j.status == 'running':
                if exitcode is not None:
                    # Job has finished
                    j.backend.exitcode = exitcode
                    if exitcode == 0:
                        j.updateStatus('completed')
                    else:
                        j.updateStatus('failed')
                else:
                    # Job is still running. Check if alive
                    time = get_last_alive(heartbeatfile)
                    config = getConfig(getName(j.backend))
                    if time > config['timeout']:
                        logger.warning(
                            'Job %s has disappeared from the batch system.', str(j.getFQID('.')))
                        j.updateStatus('failed')

#_________________________________________________________________________

class LSF(Batch):

    ''' LSF backend - submit jobs to Load Sharing Facility.'''
    _schema = Batch._schema.inherit_copy()
    _category = 'backends'
    _name = 'LSF'

    config = Ganga.Utility.Config.getConfig('LSF')

    def __init__(self):
        super(LSF, self).__init__()


#_________________________________________________________________________

class PBS(Batch):

    ''' PBS backend - submit jobs to Portable Batch System.
    '''
    _schema = Batch._schema.inherit_copy()
    _category = 'backends'
    _name = 'PBS'

    config = Ganga.Utility.Config.getConfig('PBS')

    def __init__(self):
        super(PBS, self).__init__()


#_________________________________________________________________________

class SGE(Batch):

    ''' SGE backend - submit jobs to Sun Grid Engine.
    '''
    _schema = Batch._schema.inherit_copy()
    _category = 'backends'
    _name = 'SGE'

    config = Ganga.Utility.Config.getConfig('SGE')

    def __init__(self):
        super(SGE, self).__init__()

