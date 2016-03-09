###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Interactive.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
###############################################################################
# File: Interactive.py
# Author: K. Harrison
# Created: 060720
# Version 1.0: 060728
#
# KH 060803 - Corrected _getJobObject to getJobObject
#
# KH 060829 - Updated to use Sandbox module
#
# KH 060901 - Updates in submit and preparejob methods, for core changes
#
# KH 061103 - Corrected to take into account master input sandbox
#
# KH 080306 - Corrections from VR

"""Module containing class for running jobs interactively"""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "6 February 2008"
__version__ = "1.4"

from Ganga.Core import Sandbox
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Base.Proxy import stripProxy, getName
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.Utility import util
from Ganga.Utility.Config import getConfig
from Ganga.Utility.Shell import expand_vars

import inspect
import os
import re
import shutil
import signal
import time
import tempfile

from Ganga.Utility.logging import getLogger

logger = getLogger()


class Interactive(IBackend):

    """Run jobs interactively on local host.

       Interactive job prints output directly on screen and takes the input from the keyboard.
       So it may be interupted with Ctrl-C
    """

    _schema = Schema(Version(1, 0), {
        "id": SimpleItem(defvalue=0, protected=1, copyable=0,
            doc="Process id"),
        "status": SimpleItem(defvalue="new", protected=1, copyable=0,
            doc="Backend status"),
        "exitcode": SimpleItem(defvalue=0, protected=1, copyable=0,
            doc="Process exit code"),
        "workdir": SimpleItem(defvalue="", protected=1, copyable=0,
            doc="Work directory"),
        "actualCE": SimpleItem(defvalue="", protected=1, copyable=0,
            doc="Name of machine where job is run")})

    _category = "backends"
    _name = 'Interactive'

    def __init__(self):
        super(Interactive, self).__init__()

    def _getIntFromOutfile(self, keyword="", outfileName=""):
        value = -999
        job = self.getJobObject()
        if keyword and outfileName and hasattr(job, "outputdir"):
            outfilePath = os.path.join(job.outputdir, outfileName)
            try:
                with open(outfilePath) as statfile:
                    statString = statfile.read()
                    testString = "".join(["^", keyword, " (?P<value>\\d*)"])
                    regexp = re.compile(testString, re.M)
                    match = regexp.search(statString)
                    if match:
                        value = int(match.group("value"))
            except IOError as err:
                logger.debug("IOError: %s" % str(err))
                pass
        return value

    def submit(self, jobconfig, master_input_sandbox):
        """Submit job to backend (i.e. run job interactively).

            Arguments other than self:
               subjobconfig         - Dictionary of subjob properties
               master_input_sandbox - Dictionary of properties of master job

            Return value: True always"""

        job = self.getJobObject()

        scriptpath = self.preparejob(jobconfig, master_input_sandbox)
        return self._submit(scriptpath, jobconfig.env)

    def resubmit(self):
        return self._submit(self.getJobObject().getInputWorkspace().getPath("__jobscript__"))

    def _submit(self, scriptpath, env=None):
        if env is None:
            env = expand_vars(os.environ)
        job = self.getJobObject()
        self.actualCE = util.hostname()
        logger.info('Starting job %s', job.getFQID('.'))

        try:
            job.updateStatus("submitted")
            self.status = "submitted"
            import subprocess
            subprocess.call(scriptpath)
            self.status = "completed"
        except KeyboardInterrupt:
            self.status = "killed"

        return True

    def kill(self):
        """Method for killing job running on backend

           No arguments other than self:

           Return value: True always"""

        job = self.getJobObject()

        if not self.id:
            time.sleep(0.2)
            self.id = self._getIntFromOutfile("PID:", "__id__")

        try:
            os.kill(self.id, signal.SIGKILL)
        except OSError as x:
            logger.warning("Problem killing process %d for job %d: %s" % (self.id, job.id, str(x)))

            self.status = "killed"
        self.remove_workdir()

        return True

    def remove_workdir(self):
        """Method for removing job's work directory

           No arguments other than self:

           Return value: None"""

        try:
            shutil.rmtree(self.workdir)
        except OSError as x:
            logger.warning("Problem removing workdir %s: %s", self.workdir,
                    str(x))

            return None

    def preparejob(self, jobconfig, master_input_sandbox):
        """Method for preparing job script"""

        job = self.getJobObject()

        from Ganga.GPIDev.Lib.File import File
        from Ganga.Core.Sandbox.WNSandbox import PYTHON_DIR
        import Ganga.Utility.files
        import inspect

        fileutils = File( inspect.getsourcefile(Ganga.Utility.files), subdir=PYTHON_DIR )
        inputfiles = jobconfig.getSandboxFiles() + [ fileutils ]
        inbox = job.createPackedInputSandbox(inputfiles)

        inbox.extend(master_input_sandbox)
        inpDir = job.getInputWorkspace(create=True).getPath()
        outDir = job.getOutputWorkspace(create=True).getPath()
        workdir = tempfile.mkdtemp()
        self.workdir = workdir
        exeString = jobconfig.getExeString()
        argList = jobconfig.getArgStrings()
        argString = " ".join(map(lambda x: " %s " % x, argList))

        outputSandboxPatterns = jobconfig.outputbox
        patternsToZip = []
        wnCodeForPostprocessing = ''
        wnCodeToDownloadInputFiles = ''

        if (len(job.outputfiles) > 0):

            from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatternsForInteractive, getWNCodeForOutputPostprocessing
            (outputSandboxPatterns,
                    patternsToZip) = getOutputSandboxPatternsForInteractive(job)

            wnCodeForPostprocessing = 'def printError(message):pass\ndef printInfo(message):pass' + \
                getWNCodeForOutputPostprocessing(job, '')
 
        all_inputfiles = [this_file for this_file in job.inputfiles]
        if job.master: all_inputfiles.extend([this_file for this_file in job.master.inputfiles])

        wnCodeToDownloadInputFiles = ''

        if(len(all_inputfiles) > 0):

            from Ganga.GPIDev.Lib.File.OutputFileManager import outputFilePostProcessingOnWN

            for inputFile in all_inputfiles:

                inputfileClassName = getName(inputFile)

                logger.debug("name: %s" % inputfileClassName)
                logger.debug("result: %s" % str(outputFilePostProcessingOnWN(job, inputfileClassName)))

                if outputFilePostProcessingOnWN(job, inputfileClassName):
                    inputFile.processWildcardMatches()
                    if inputFile.subfiles:
                        getfromFile = False
                        for subfile in inputFile.subfiles:
                            wnCodeToDownloadInputFiles += subfile.getWNScriptDownloadCommand('')
                        else:
                            getfromFile = True
                    else:
                        getFromFile = True

                    if getFromFile:
                        wnCodeToDownloadInputFiles += inputFile.getWNScriptDownloadCommand('')

        wnCodeToDownloadInputData = ''

        if job.inputdata and (len(job.inputdata) > 0):

            from Ganga.GPIDev.Lib.File.OutputFileManager import getWNCodeForDownloadingInputFiles

            wnCodeToDownloadInputData = getWNCodeForDownloadingInputFiles(job, '')

        

        import inspect

        replace_dict = {
        '###CONSTRUCT_TIME###' : (time.strftime("%c")),
        '###WNSANDBOX_SOURCE###' : inspect.getsource(Sandbox.WNSandbox),
        '###GANGA_PYTHONPATH###' : getConfig("System")["GANGA_PYTHONPATH"],
        '###OUTPUTDIR###' : outDir,
        '###WORKDIR###' : workdir,
        '###IN_BOX###' : inbox,
        '###WN_INPUTFILES###' : wnCodeToDownloadInputFiles,
        '###WN_INPUTDATA###' : wnCodeToDownloadInputData,
        '###JOBCONFIG_ENV###' : jobconfig.env if jobconfig.env is not None else dict(),
        '###EXE_STRING###' : exeString,
        '###ARG_STRING###' : argString,
        '###WN_POSTPROCESSING###' : wnCodeForPostprocessing,
        '###PATTERNS_TO_ZIP###' : patternsToZip,
        '###OUTPUT_SANDBOX_PATTERNS###' : outputSandboxPatterns
        }

        script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                'InteractiveScriptTemplate.py')

        from Ganga.GPIDev.Lib.File import FileUtils
        commandString = FileUtils.loadScript(script_location, '')

        for k, v in replace_dict.iteritems():
            commandString = commandString.replace(str(k), str(v))

        return job.getInputWorkspace().writefile(FileBuffer("__jobscript__", commandString), executable=1)

    @staticmethod
    def updateMonitoringInformation(jobs):

        for j in jobs:
            stripProxy(j)._getWriteAccess()

            raw_backend = stripProxy(j.backend)

            if not j.backend.id:
                id = raw_backend._getIntFromOutfile("PID:", "__id__")
                if id > 0:
                    raw_backend.id = id
                    if ("submitted" == j.backend.status):
                        raw_backend.status = "running"

              # Check that the process is still alive
            if j.backend.id:
                try:
                    os.kill(j.backend.id, 0)
                except Exception as err:
                    logger.debug("Err: %s" % str(err))
                    raw_backend.status = "completed"

            if j.backend.status in ["completed", "failed", "killed"]:
                raw_backend.exitcode = raw_backend._getIntFromOutfile("EXITCODE:", "__jobstatus__")
               # Set job status to failed for non-zero exit code
                if j.backend.exitcode:
                    if j.backend.exitcode in [2, 9, 256]:
                        raw_backend.status = "killed"
                    else:
                        raw_backend.status = "failed"
                if (j.backend.status != j.status):
                    j.updateStatus(j.backend.status)

        return None

