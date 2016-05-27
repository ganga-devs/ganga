###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Condor.py,v 1.8 2009/04/02 17:52:24 karl Exp $
###############################################################################
# File: Condor.py
# Author: K. Harrison
# Created: 051228
#
# KH - 060321: Additions from Johannes Elmsheuser, to allow use with Condor-G
#
# KH - 060728: Changes for framework migration
#
# KH - 061012: Updates in submit and preparejob methods, for core changes
#
# KH - 061027: Corrections for case of no shared filesystem
#
# KH - 080213: Changes made to use global Condor id for tracking job status
#
# KH - 080215: Correction for passing quoted strings as a command-line argument
#              to an executable
#
# KH - 080410: Corrections made to kill() method of Condor class
#
# KH - 080410: Implemented resubmit() method of Condor class
#
# KH - 080729: Updates for changes to JobConfig class in Ganga 5
#              Error message printed in case submit command fails
#
# KH - 081008 : Added typelist information for schema property "submit_options"
#
# KH - 081102 : Remove spurious print statement
#
# KH - 090128 : Added getenv property to Condor class, to allow environment of
#               submit machine to be passed to worker node
#
#               Added creation of bash job wrapper, to allow environment
#               setup by defining BASH_ENV to point to setup script
#
#               Set status to failed for job with non-zero exit code
#
# KH - 090307 : Modified kill() method to assume success even in case of
#               non-zero return code from condor_rm
#
# KH - 090308 : Modified updateMonitoringInformation() method
#               to deal with case where all queues are empty
#
# KH - 090402 : Corrected bug that meant application arguments were ignored
#
#               In script to be run on worker node, print warning if unable
#               to find startup script pointed to by BASH_ENV
#
# KH - 090809 : Changed logic for updating job status to final value -
#               Condor log file is now searched to check whether job
#               is marked as "aborted" or "terminated"

"""Module containing class for handling job submission to Condor backend"""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "09 August 2009"
__version__ = "2.5"

from Ganga.Core import Sandbox
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility.ColourText import Foreground, Effects

import Ganga.Utility.logging
from Ganga.Utility.Config import getConfig

import commands
import inspect
import os
import shutil
import time

logger = Ganga.Utility.logging.getLogger()


class Condor(IBackend):

    """Condor backend - submit jobs to a Condor pool.

    For more options see help on CondorRequirements.
    """

    _schema = Schema(Version(1, 0), {
        "requirements": ComponentItem(category="condor_requirements",
                                      defvalue="CondorRequirements",
                                      doc="Requirements for selecting execution host"),
        "env": SimpleItem(defvalue={},
                          doc='Environment settings for execution host'),
        "getenv": SimpleItem(defvalue="False",
                             doc='Flag to pass current envrionment to execution host'),
        "rank": SimpleItem(defvalue="Memory",
                           doc="Ranking scheme to be used when selecting execution host"),
        "submit_options": SimpleItem(defvalue=[], typelist=[str],
                                     sequence=1, doc="Options passed to Condor at submission time"),
        "id": SimpleItem(defvalue="", protected=1, copyable=0,
                         doc="Condor jobid"),
        "status": SimpleItem(defvalue="", protected=1, copyable=0,
                             doc="Condor status"),
        "cputime": SimpleItem(defvalue="", protected=1, copyable=0,
                              doc="CPU time used by job"),
        "actualCE": SimpleItem(defvalue="", protected=1, copyable=0,
                               doc="Machine where job has been submitted"),
        "shared_filesystem": SimpleItem(defvalue=True,
                                        doc="Flag indicating if Condor nodes have shared filesystem"),
        "universe": SimpleItem(defvalue="vanilla",
                               doc="Type of execution environment to be used by Condor"),
        "globusscheduler": SimpleItem(defvalue="", doc="Globus scheduler to be used (required for Condor-G submission)"),
        "globus_rsl": SimpleItem(defvalue="",
                                 doc="Globus RSL settings (for Condor-G submission)"),
        "accounting_group": SimpleItem(defvalue='', doc="Provide an accounting group for this job."),
        "cdf_options": SimpleItem(defvalue={}, doc="Additional options to set in the CDF file given by a dictionary")
    })

    _category = "backends"
    _name = "Condor"
    statusDict = \
        {
            "0": "Unexpanded",
            "1": "Idle",
            "2": "Running",
            "3": "Removed",
            "4": "Completed",
            "5": "Held"
        }

    def __init__(self):
        super(Condor, self).__init__()

    def submit(self, jobconfig, master_input_sandbox):
        """Submit job to backend.

            Return value: True if job is submitted successfully,
                          or False otherwise"""

        cdfpath = self.preparejob(jobconfig, master_input_sandbox)
        status = self.submit_cdf(cdfpath)
        return status

    def submit_cdf(self, cdfpath=""):
        """Submit Condor Description File.

            Argument other than self:
               cdfpath - path to Condor Description File to be submitted

            Return value: True if job is submitted successfully,
                          or False otherwise"""

        commandList = ["condor_submit -v"]
        commandList.extend(self.submit_options)
        commandList.append(cdfpath)
        commandString = " ".join(commandList)

        status, output = commands.getstatusoutput(commandString)

        self.id = ""
        if 0 != status:
            logger.error\
                ("Tried submitting job with command: '%s'" % commandString)
            logger.error("Return code: %s" % str(status))
            logger.error("Condor output:")
            logger.error(output)
        else:
            tmpList = output.split("\n")
            for item in tmpList:
                if 1 + item.find("** Proc"):
                    localId = item.strip(":").split()[2]
                    queryCommand = " ".join\
                        (["condor_q -format \"%s\" GlobalJobId", localId])
                    qstatus, qoutput = commands.getstatusoutput(queryCommand)
                    if 0 != status:
                        logger.warning\
                            ("Problem determining global id for Condor job '%s'" %
                             localId)
                        self.id = localId
                    else:
                        self.id = qoutput
                    break

        return not self.id is ""

    def resubmit(self):
        """Resubmit job that has already been configured.

            Return value: True if job is resubmitted successfully,
                          or False otherwise"""

        job = self.getJobObject()

        inpDir = job.getInputWorkspace().getPath()
        outDir = job.getOutputWorkspace().getPath()

        # Delete any existing output files, and recreate output directory
        if os.path.isdir(outDir):
            shutil.rmtree(outDir)
        if os.path.exists(outDir):
            os.remove(outDir)
        os.mkdir(outDir)

        # Determine path to job's Condor Description File
        cdfpath = os.path.join(inpDir, "__cdf__")

        # Resubmit job
        if os.path.exists(cdfpath):
            status = self.submit_cdf(cdfpath)
        else:
            logger.warning\
                ("No Condor Description File for job '%s' found in '%s'" %
                 (str(job.id), inpDir))
            logger.warning("Resubmission failed")
            status = False

        return status

    def kill(self):
        """Kill running job.

           No arguments other than self

           Return value: True if job killed successfully,
                         or False otherwise"""

        job = self.getJobObject()

        if not self.id:
            logger.warning("Job %s not running" % job.id)
            return False

        idElementList = job.backend.id.split("#")
        if 3 == len(idElementList):
            if idElementList[1].find(".") != -1:
                killCommand = "condor_rm -name %s %s" % \
                    (idElementList[0], idElementList[1])
            else:
                killCommand = "condor_rm -name %s %s" % \
                    (idElementList[0], idElementList[2])
        else:
            killCommand = "condor_rm %s" % (idElementList[0])

        status, output = commands.getstatusoutput(killCommand)

        if (status != 0):
            logger.warning\
                ("Return code '%s' killing job '%s' - Condor id '%s'" %
                 (str(status), job.id, job.backend.id))
            logger.warning("Tried command: '%s'" % killCommand)
            logger.warning("Command output: '%s'" % output)
            logger.warning("Anyway continuing with job removal")

        job.backend.status = "Removed"
        killStatus = True

        return killStatus

    def preparejob(self, jobconfig, master_input_sandbox):
        """Prepare Condor description file"""

        job = self.getJobObject()
        inbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles())
        inpDir = job.getInputWorkspace().getPath()
        outDir = job.getOutputWorkspace().getPath()

        infileList = []

        exeString = jobconfig.getExeString().strip()
        quotedArgList = []
        for arg in jobconfig.getArgStrings():
            quotedArgList.append("\\'%s\\'" % arg)
        exeCmdString = " ".join([exeString] + quotedArgList)

        for filePath in inbox:
            if not filePath in infileList:
                infileList.append(filePath)

        for filePath in master_input_sandbox:
            if not filePath in infileList:
                infileList.append(filePath)

        fileList = []
        for filePath in infileList:
            fileList.append(os.path.basename(filePath))

        if job.name:
            name = job.name
        else:
            name = job.application._name
        name = "_".join(name.split())
        wrapperName = "_".join(["Ganga", str(job.id), name])

        commandList = [
            "#!/usr/bin/env python",
            "from __future__ import print_function",
            "# Condor job wrapper created by Ganga",
            "# %s" % (time.strftime("%c")),
            "",
            inspect.getsource(Sandbox.WNSandbox),
            "",
            "import os",
            "import time",
            "import mimetypes",
            "",
            "startTime = time.strftime"
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
            "",
            "for inFile in %s:" % str(fileList),
            "   if mimetypes.guess_type(inFile)[1] in ['gzip', 'bzip2']:",
            "       getPackedInputSandbox( inFile )",
            "",
            "exePath = '%s'" % exeString,
            "if os.path.isfile( '%s' ):" % os.path.basename(exeString),
            "   os.chmod( '%s', 0755 )" % os.path.basename(exeString),
            "wrapperName = '%s_bash_wrapper.sh'" % wrapperName,
            "wrapperFile = open( wrapperName, 'w' )",
            "wrapperFile.write( '#!/bin/bash\\n' )",
            "wrapperFile.write( 'echo \"\"\\n' )",
            "wrapperFile.write( 'echo \"Hostname: $(hostname -f)\"\\n' )",
            "wrapperFile.write( 'echo \"\\${BASH_ENV}: ${BASH_ENV}\"\\n' )",
            "wrapperFile.write( 'if ! [ -z \"${BASH_ENV}\" ]; then\\n' )",
            "wrapperFile.write( '  if ! [ -f \"${BASH_ENV}\" ]; then\\n' )",
            "wrapperFile.write( '    echo \"*** Warning: "
            + "\\${BASH_ENV} file not found ***\"\\n' )",
            "wrapperFile.write( '  fi\\n' )",
            "wrapperFile.write( 'fi\\n' )",
            "wrapperFile.write( 'echo \"\"\\n' )",
            "wrapperFile.write( '%s\\n' )" % exeCmdString,
            "wrapperFile.write( 'exit ${?}\\n' )",
            "wrapperFile.close()",
            "os.chmod( wrapperName, 0755 )",
            "result = os.system( './%s' % wrapperName )",
            "os.remove( wrapperName )",
            "",
            "endTime = time.strftime"
              + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
            "print('\\nJob start: ' + startTime)",
            "print('Job end: ' + endTime)",
            "print('Exit code: %s' % str( result ))"
        ]

        commandString = "\n".join(commandList)
        wrapper = job.getInputWorkspace().writefile\
            (FileBuffer(wrapperName, commandString), executable=1)

        infileString = ",".join(infileList)
        outfileString = ",".join(jobconfig.outputbox)

        cdfDict = \
            {
                'universe': self.universe,
                'on_exit_remove': 'True',
                'should_transfer_files': 'YES',
                'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                'executable': wrapper,
                'transfer_executable': 'True',
                'notification': 'Never',
                'rank': self.rank,
                'initialdir': outDir,
                'error': 'stderr',
                'output': 'stdout',
                'log': 'condorLog',
                'stream_output': 'false',
                'stream_error': 'false',
                'getenv': self.getenv
            }

        # extend with additional cdf options
        cdfDict.update(self.cdf_options)

        # accounting group
        if self.accounting_group:
            cdfDict['accounting_group'] = self.accounting_group

        envList = []
        if self.env:
            for key in self.env.keys():
                value = self.env[key]
                if (isinstance(value, str)):
                    value = os.path.expandvars(value)
                else:
                    value = str(value)
                envList.append("=".join([key, value]))
        envString = ";".join(envList)
        if jobconfig.env:
            for key in jobconfig.env.keys():
                value = jobconfig.env[key]
                if (isinstance(value, str)):
                    value = os.path.expandvars(value)
                else:
                    value = str(value)
                envList.append("=".join([key, value]))
        envString = ";".join(envList)
        if envString:
            cdfDict['environment'] = envString

        if infileString:
            cdfDict['transfer_input_files'] = infileString

        if self.globusscheduler:
            cdfDict['globusscheduler'] = self.globusscheduler

        if self.globus_rsl:
            cdfDict['globus_rsl'] = self.globus_rsl

        if outfileString:
            cdfDict['transfer_output_files'] = outfileString

        cdfList = [
            "# Condor Description File created by Ganga",
            "# %s" % (time.strftime("%c")),
            ""]
        for key, value in cdfDict.iteritems():
            cdfList.append("%s = %s" % (key, value))
        cdfList.append(self.requirements.convert())
        cdfList.append("queue")
        cdfString = "\n".join(cdfList)

        return job.getInputWorkspace().writefile\
            (FileBuffer("__cdf__", cdfString))

    def updateMonitoringInformation(jobs):

        jobDict = {}
        for job in jobs:
            if job.backend.id:
                jobDict[job.backend.id] = job

        idList = jobDict.keys()

        if not idList:
            return

        queryCommand = " ".join\
            ([
                "condor_q -global" if getConfig(
                    "Condor")["query_global_queues"] else "condor_q",
                "-format \"%s \" GlobalJobId",
                "-format \"%s \" RemoteHost",
                "-format \"%d \" JobStatus",
                "-format \"%f\\n\" RemoteUserCpu"
            ])
        status, output = commands.getstatusoutput(queryCommand)
        if 0 != status:
            logger.error("Problem retrieving status for Condor jobs")
            return

        if ("All queues are empty" == output):
            infoList = []
        else:
            infoList = output.split("\n")

        allDict = {}
        for infoString in infoList:
            tmpList = infoString.split()
            id, host, status, cputime = ("", "", "", "")
            if 3 == len(tmpList):
                id, status, cputime = tmpList
            if 4 == len(tmpList):
                id, host, status, cputime = tmpList
            if id:
                allDict[id] = {}
                allDict[id]["status"] = Condor.statusDict[status]
                allDict[id]["cputime"] = cputime
                allDict[id]["host"] = host

        fg = Foreground()
        fx = Effects()
        status_colours = {'submitted': fg.orange,
                          'running': fg.green,
                          'completed': fg.blue}

        for id in idList:

            printStatus = False
            if jobDict[id].status == "killed":
                continue

            localId = id.split("#")[-1]
            globalId = id

            if globalId == localId:
                queryCommand = " ".join\
                    ([
                        "condor_q -global" if getConfig(
                            "Condor")["query_global_queues"] else "condor_q",
                        "-format \"%s\" GlobalJobId",
                        id
                    ])
                status, output = commands.getstatusoutput(queryCommand)
                if 0 == status:
                    globalId = output

            if globalId in allDict.keys():
                status = allDict[globalId]["status"]
                host = allDict[globalId]["host"]
                cputime = allDict[globalId]["cputime"]
                if status != jobDict[id].backend.status:
                    printStatus = True
                    stripProxy(jobDict[id])._getWriteAccess()
                    jobDict[id].backend.status = status
                    if jobDict[id].backend.status == "Running":
                        jobDict[id].updateStatus("running")

                if host:
                    if jobDict[id].backend.actualCE != host:
                        jobDict[id].backend.actualCE = host
                jobDict[id].backend.cputime = cputime
            else:
                jobDict[id].backend.status = ""
                outDir = jobDict[id].getOutputWorkspace().getPath()
                condorLogPath = "".join([outDir, "condorLog"])
                checkExit = True
                if os.path.isfile(condorLogPath):
                    checkExit = False
                    for line in open(condorLogPath):
                        if -1 != line.find("terminated"):
                            checkExit = True
                            break
                        if -1 != line.find("aborted"):
                            checkExit = True
                            break

                if checkExit:
                    printStatus = True
                    stdoutPath = "".join([outDir, "stdout"])
                    jobStatus = "failed"
                    if os.path.isfile(stdoutPath):
                        with open(stdoutPath) as stdout:
                            lineList = stdout.readlines()
                        try:
                            exitLine = lineList[-1]
                            exitCode = exitLine.strip().split()[-1]
                        except IndexError:
                            exitCode = '-1'

                        if exitCode.isdigit():
                            jobStatus = "completed"
                        else:
                            logger.error("Problem extracting exit code from job %s. Line found was '%s'." % (
                                jobDict[id].fqid, exitLine))

                    jobDict[id].updateStatus(jobStatus)

            if printStatus:
                if jobDict[id].backend.actualCE:
                    hostInfo = jobDict[id].backend.actualCE
                else:
                    hostInfo = "Condor"
                status = jobDict[id].status
                if status in status_colours:
                    colour = status_colours[status]
                else:
                    colour = fg.magenta
                if "submitted" == status:
                    preposition = "to"
                else:
                    preposition = "on"

                if jobDict[id].backend.status:
                    backendStatus = "".join\
                        ([" (", jobDict[id].backend.status, ") "])
                else:
                    backendStatus = ""

                logger.info(colour + 'Job %s %s%s %s %s - %s' + fx.normal,
                            jobDict[
                                id].fqid, status, backendStatus, preposition, hostInfo,
                            time.strftime('%c'))

        return None

    updateMonitoringInformation = \
        staticmethod(updateMonitoringInformation)

#_________________________________________________________________________


