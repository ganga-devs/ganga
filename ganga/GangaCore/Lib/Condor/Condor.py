###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Condor.py,v 1.8 2009/04/02 17:52:24 karl Exp $
###############################################################################
# File: Condor.py

"""Module containing class for handling job submission to Condor backend"""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "09 August 2009"
__version__ = "2.5"

import subprocess
import inspect
import os
import shutil
import time
import datetime
import re

import GangaCore.Utility.logging
import GangaCore.Utility.Virtualization

from GangaCore.Core import Sandbox
from GangaCore.GPIDev.Adapters.IBackend import IBackend
from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.GPIDev.Lib.File.FileBuffer import FileBuffer
from GangaCore.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from GangaCore.Utility.ColourText import Foreground, Effects
from GangaCore.Core.exceptions import BackendError
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Lib.File import File
from GangaCore.Core.Sandbox.WNSandbox import PYTHON_DIR
from GangaCore.GPIDev.Credentials import credential_store
from GangaCore.GPIDev.Credentials.AfsToken import AfsToken
from GangaCore.GPIDev.Lib.File.OutputFileManager import getWNCodeForInputdataListCreation

logger = GangaCore.Utility.logging.getLogger()

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    try:
        import htcondor
        import classad
    except ModuleNotFoundError:
        logger.debug("The htcondor module not available for import. This will cause use of the Condor backend to fail.")


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
        "spool" : SimpleItem(defvalue=True, doc="Spool all required input files, job event log, and proxy over the connection to the condor_schedd. Required for EOS, see: http://batchdocs.web.cern.ch/batchdocs/troubleshooting/eos_submission.html"),
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

        # Add a volatile variable for recording the first time a job's stdout is checked
        self._stdout_check_time = 0

        # Store the format of the date in the condorLog
        self._condorDateFormat = None

        super(Condor, self).__init__()

    def master_submit(self, rjobs, subjobconfigs, masterjobconfig, keep_going=False, parallel_submit=False):
        """Submit condor jobs"""
        logger.debug("SubJobConfigs: %s" % len(subjobconfigs))
        logger.debug("rjobs: %s" % len(rjobs))

        #Get the credential into the scheduler
        col = htcondor.Collector()
        credd = htcondor.Credd()
        if credential_store.matches(AfsToken()):
            credd.add_user_cred(htcondor.CredTypes.Kerberos, None)

        master_input_sandbox = self.master_prepare(masterjobconfig)
        #Get the dict of common things
        cdfDict = self.cdfPreamble(masterjobconfig)
        #Now set up the things for each job. It needs to be a list of dictionaries
        sjDict = []
        for sc, sj in zip(subjobconfigs, rjobs):
            sj.updateStatus('submitting')
            sjDict.append(self.prepareSubjob(sj, sc, master_input_sandbox))

        #Put the common job dictionary into the HTCondor Submit object
        this_job = htcondor.Submit(cdfDict)
        #Now setup the submission
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            stati = this_job.queue_with_itemdata(txn, itemdata = iter(sjDict))

        cluster_id = stati.cluster()
        process_id = stati.first_proc()
        if not len(rjobs) == stati.num_procs():
            raise BackendError('Condor', 'Some subjobs failed to submit! Check their status!')

        for sj in rjobs:
            sj.backend.id = '%s.%s' % (cluster_id, process_id)
            sj.updateStatus('submitted')
            sj.time.timenow('submitted')
            stripProxy(sj.info).increment()
            process_id = process_id + 1
        return 1

    def resubmit(self):
        """Resubmit job that has already been configured. We have to do something a little different if this is a subjob.

            Return value: True if job is resubmitted successfully,
                          or False otherwise"""

        job = self.getJobObject()

        # Is this a subjob?
        if job.master:
            inpDir = job.master.getInputWorkspace().getPath()

        else:
            inpDir = job.getInputWorkspace().getPath()

        outDir = job.getOutputWorkspace().getPath()

        # Delete any existing output files, and recreate output directory
        if os.path.isdir(outDir):
            shutil.rmtree(outDir)
        if os.path.exists(outDir):
            os.remove(outDir)
        os.mkdir(outDir)

        masterjobconfig = job._getJobMasterConfig()
        master_input_sandbox = job.inputsandbox
        rjobs = [job]
        subjobconfigs = job._getJobSubConfig(rjobs)

        #Get the dict of common things
        cdfDict = self.cdfPreamble(masterjobconfig)
        #Now set up the things for each job. It needs to be a list of dictionaries
        sjDict = []
        for sc, sj in zip(subjobconfigs, rjobs):
            sj.updateStatus('submitting')
            sjDict.append(self.prepareSubjob(sj, sc, master_input_sandbox))

        #Put the common job dictionary into the HTCondor Submit object
        this_job = htcondor.Submit(cdfDict)
        #Now setup the submission
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            stati = this_job.queue_with_itemdata(txn, itemdata = iter(sjDict))

        cluster_id = stati.cluster()
        process_id = stati.first_proc()
        if not len(rjobs) == stati.num_procs():
            raise BackendError('Condor', 'Some subjobs failed to submit! Check their status!')

        for sj in rjobs:
            sj.backend.id = '%s.%s' % (cluster_id, process_id)
            sj.updateStatus('submitted')
            sj.time.timenow('submitted')
            stripProxy(sj.info).increment()
            process_id = process_id + 1

        return 1

    def kill(self):
        """Kill running job.

           No arguments other than self

           Return value: True if job killed successfully,
                         or False otherwise"""

        job = self.getJobObject()
        idList = []
        is_master = False
        #Are we in the master job? If so kill everything.
        if job.subjobs:
            is_master = True
            idList = job.subjobs[0].backend.id.split(".")
        else:
            idList = self.id.split(".")

        cluster_id = idList[0]
        proc_id = idList[1]

        schedd = htcondor.Schedd()
        result = {}
        if is_master:
            result = schedd.act(htcondor.JobAction.Remove, "ClusterID=={}".format(cluster_id))
        else:
            result = schedd.act(htcondor.JobAction.Remove, "ClusterID=={} && ProcID=={}".format(cluster_id, proc_id))
        
        if result['TotalError'] > 0:
            return False

        if is_master:
            for _sj in job.subjobs:
                _sj.backend.status = 'Removed'
        else:
            self.status = 'Removed'

        return True

    def cdfPreamble(self, jobconfig):
        """Prepare the cdf arguments that are common to all jobs so go at the start. Returns a dict"""

        wrapperScriptStr = '#!/bin/sh\n./$1\n'
        self.getJobObject().getInputWorkspace().writefile(FileBuffer("condorWrapper", wrapperScriptStr))

        cdfDict = \
            {
                'universe': self.universe,
                'on_exit_remove': 'True',
                'should_transfer_files': 'YES',
                'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                'transfer_executable': 'True',
                'notification': 'Never',
                'rank': self.rank,
                'error': 'stderr',
                'output': 'stdout',
                'log': 'condorLog',
                'stream_output': 'false',
                'stream_error': 'false',
                'getenv': self.getenv,
                'executable': os.path.join(self.getJobObject().getInputWorkspace().getPath(),'condorWrapper')
            }
        #Send the credentials for shared filesystems
        if credential_store.matches(AfsToken()):
            cdfDict['MY.SendCredential'] = 'True'
        # extend with additional cdf options
        cdfDict.update(self.cdf_options)

        # accounting group
        if self.accounting_group:
            cdfDict['accounting_group'] = self.accounting_group

        if self.globusscheduler:
            cdfDict['globusscheduler'] = self.globusscheduler

        if self.globus_rsl:
            cdfDict['globus_rsl'] = self.globus_rsl

        cdfDict['Requirements'] =  self.requirements.convert()

        return cdfDict


    def prepareSubjob(self, job, jobconfig, master_input_sandbox):
        """
        Prepare a Condor description string for a subjob
        This returns a dictionary with info specific to the subjobs
        """

        virtualization = job.virtualization

        utilFiles= []
        if virtualization:
            virtualizationutils = File( inspect.getsourcefile(GangaCore.Utility.Virtualization), subdir=PYTHON_DIR )
            utilFiles.append(virtualizationutils)

        inbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles() + utilFiles)
        inpDir = job.getInputWorkspace().getPath()
        outDir = job.getOutputWorkspace().getPath()

        infileList = []

        exeString = jobconfig.getExeString().strip()
        quotedArgList = []
        for arg in jobconfig.getArgStrings():
            quotedArgList.append("%s" % arg)
        exeCmd = [exeString] + quotedArgList

        for filePath in inbox:
            if not filePath in infileList:
                infileList.append(filePath)

        for filePath in master_input_sandbox:
            if not filePath in infileList:
                infileList.append(filePath)

        fileList = []
        for filePath in infileList:
            fileList.append(filePath)

        if job.name:
            name = job.name
        else:
            name = job.application._name
        name = "_".join(name.split())
        wrapperName = "_".join(["Ganga", str(job.id), name])

        commandList = [
            "#!/usr/bin/env python3",
            "# Condor job wrapper created by Ganga",
            "# %s" % (time.strftime("%c")),
            "",
            inspect.getsource(Sandbox.WNSandbox),
            "",
            "import os",
            "import time",
            "import mimetypes",
            "import shutil",
            "",
            "startTime = time.strftime"
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
            "",
            getWNCodeForInputdataListCreation(job, ''),
            "",
            "workdir = os.getcwd()",
            "execmd = %s" % repr(exeCmd),
            "",
            "###VIRTUALIZATION###",
            "",
            "for inFile in %s:" % str(fileList),
            "   if mimetypes.guess_type(inFile)[1] in ['gzip', 'bzip2']:",
            "       getPackedInputSandbox( inFile )",
            "   else:",
            "       shutil.copy(inFile, os.path.join(os.getcwd(), os.path.basename(inFile)))",
            "",
            "exePath = '%s'" % exeString,
            "if os.path.isfile( '%s' ):" % os.path.basename(exeString),
            "   os.chmod( '%s', 0o755)" % os.path.basename(exeString),
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
            "wrapperFile.write( '%s\\n' % \' \'.join(execmd) )",
            "wrapperFile.write( 'exit ${?}\\n' )",
            "wrapperFile.close()",
            "os.chmod( wrapperName, 0o755 )",
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

        if virtualization:
            commandString = virtualization.modify_script(commandString)

        wrapper = job.getInputWorkspace().writefile\
            (FileBuffer(wrapperName, commandString), executable=1)

        infileString = ",".join(infileList)
        outfileString = ",".join(jobconfig.outputbox)

        cdfDict = \
            {
                'transfer_input_files': wrapper,
                'initialdir': outDir,
                'arguments': wrapperName
            }

        # extend with additional cdf options
        cdfDict.update(self.cdf_options)

        envList = []
        if self.env:
            for key in self.env:
                value = self.env[key]
                if (isinstance(value, str)):
                    value = os.path.expandvars(value)
                else:
                    value = str(value)
                envList.append("=".join([key, value]))
        envString = ";".join(envList)
        if jobconfig.env:
            for key in jobconfig.env:
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
            cdfDict['transfer_input_files'] = cdfDict['transfer_input_files'] + "," + infileString

        if outfileString:
            cdfDict['transfer_output_files'] = outfileString

        return cdfDict

    def updateMonitoringInformation(jobs):

        #First collate jobs and Condor IDs
        jobDict = {}
        for job in jobs:
            if job.backend.id:
                jobDict[job.backend.id] = job

        idList = list(jobDict.keys())

        if not idList:
            return

        #Now gather together cluster IDs and process IDs
        id_dict = {}
        for _j in jobs:
            idStringList = _j.backend.id.split(".")
            this_cluster_id = idStringList[0]
            this_proc_id = idStringList[1]
            if this_cluster_id in id_dict.keys():
                id_dict[this_cluster_id].append(this_proc_id)
            else:
                id_dict[this_cluster_id] = [this_proc_id]
    
        #Now make a filtering string for the scheduler
        expr_tree = classad.ExprTree("false")
        for _cl in id_dict.keys():
            cl_expr = classad.ExprTree("ClusterID == %s" % _cl)
            pr_expr_str = "ProcID == %s" % id_dict[_cl][0]
            for _proc in id_dict[_cl][1:]:
                pr_expr_str = pr_expr_str + " || ProcID == %s" % _proc
            pr_expr = classad.ExprTree(pr_expr_str)
            cl_expr = cl_expr.and_(pr_expr)
            expr_tree = expr_tree.or_(cl_expr)

        #Now query the scheduler with or job list
        schedd = htcondor.Schedd()
        stati = []
        try:
            stati = schedd.query(constraint = expr_tree, projection = ["ClusterId", "ProcId", "JobStatus", "RemoteUserCpu","RemoteHost"])
        except htcondor.HTCondorIOError as err:
            logger.debug(err)

        allDict = {}
        for _stat in stati:
            this_id = str(_stat['ClusterId']) + '.' + str(_stat['ProcId'])
            allDict[this_id] = {}
            allDict[this_id]["status"] = Condor.statusDict[str(_stat["JobStatus"])]
            allDict[this_id]["cputime"] = _stat["RemoteUserCpu"]
            if "RemoteHost" in _stat.keys():
                allDict[this_id]["host"] = _stat["RemoteHost"]
            else:
                allDict[this_id]["host"] = ""


        fg = Foreground()
        fx = Effects()
        status_colours = {'submitted': fg.orange,
                          'running': fg.green,
                          'completed': fg.blue}


        for id in idList:

            printStatus = False
            if id in  jobDict[id].status == "killed":
                continue
            #If the queried job is still in the queue system:
            if id in allDict.keys():
                status = allDict[id]["status"]
                host = allDict[id]["host"]
                cputime = allDict[id]["cputime"]
                if status != jobDict[id].backend.status:
                    printStatus = True
                    stripProxy(jobDict[id])._getSessionLock()
                    jobDict[id].backend.status = status
                    if jobDict[id].backend.status == "Running":
                        jobDict[id].updateStatus("running")

                if host:
                    if jobDict[id].backend.actualCE != host:
                        jobDict[id].backend.actualCE = host
                jobDict[id].backend.cputime = cputime
            #Otherwise look a bit harder at the logs to see where it ended up
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
                            if exitCode=='0':
                                jobStatus = "completed"
                            else:
                                jobStatus = 'failed'
                        else:
                            # Some filesystems/setups have the file created but empty - only worry if it's been 10mins
                            # since we first checked the file
                            if len(lineList) == 0:
                                if not jobDict[id].backend._stdout_check_time:
                                    jobDict[id].backend._stdout_check_time = time.time()

                                if (time.time() - jobDict[id].backend._stdout_check_time) < 10*60:
                                    continue
                                else:
                                    logger.error("Empty stdout file from job %s after waiting 10mins. Marking job as"
                                                 "failed." % jobDict[id].fqid)
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

    def setCondorDateFormat(self, dateString):
        """Helper function to set the _condorDateFormat for a given dateString.

           This is called on-the-fly while parsing the condorLog in getStateTime.
           Depending on the version of condor, the format of the date is different:
               Count the number of date elements.
               Check for the separation character between date elements.
        """
        dateBreakdown = re.findall(r"\d+\D", dateString)
        numberOfDateElements = len(dateBreakdown)+1
        if numberOfDateElements > 0:
            self._condorDateFormat=(numberOfDateElements,dateBreakdown[0][-1])
            if numberOfDateElements > 3 or numberOfDateElements == 1:
                logger.warning(
                    "setCondorDateFormat number of date elements does not match: '%s'", dateString)
        else:
            logger.warning(
                    "setCondorDateFormat cannot determine date format: '%s'", dateString)

    def getCondorDate(self, dateString, timeString):
        """Helper function to unify the condor date format according to the format obtained in setCondorDateFormat and stored in _condorDateFormat

           Depending on the version of condor, the format of the date is different:
               If there are only two date elements, the condorLog doesn't tell you the year so we guess the closest one to now.
               The separation character of the year/month/day is unified to the same character for easy parsing.
        """
        result=dateString
        if self._condorDateFormat:
            if self._condorDateFormat[1] != "/":
                result=result.replace(self._condorDateFormat[1],"/")
            if self._condorDateFormat[0] == 2:
                year = datetime.datetime.now().year
                if datetime.datetime.strptime(str(year)+"/"+result+' '+timeString, "%Y/%m/%d %H:%M:%S") > datetime.datetime.now():
                    year = year - 1
                result=str(year)+"/"+result
        return result

    def getStateTime(self, status):
        """Obtains the timestamps for the 'running', 'completed', and 'failed' states.

           The condorLog file in the job's output directory is read to obtain the start and stop times of the job.
           These are converted into datetime objects and returned to the user.
        """
        j = self.getJobObject()
        end_list = ['completed', 'failed']
        d = {}
        checkstr = ''

        if status == 'submitted':
            checkstr = '000'
        elif status == 'running':
            checkstr = '001'
        elif status == 'completed':
            checkstr = '005'
        elif status == 'failed':
            checkstr = '005'
        else:
            checkstr = ''

        if checkstr == '':
            logger.debug("In getStateTime(): checkstr == ''")
            return None

        try:
            p = os.path.join(j.outputdir, 'condorLog')
            logger.debug("Opening output file at: %s", p)
            f = open(p)
        except IOError:
            logger.debug('unable to open file %s', p)
            return None


        for l in f:
            splitLine = l.split()
            if len(splitLine)>0 and checkstr == splitLine[0]:
                if not self._condorDateFormat:
                    self.setCondorDateFormat(splitLine[2])
                condorDate=self.getCondorDate(splitLine[2], splitLine[3])
                timestr = condorDate+' '+splitLine[3]
                try:
                    t = datetime.datetime(
                        *(time.strptime(timestr, "%Y/%m/%d %H:%M:%S")[0:6]))
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
        # check for file. if it's not there don't bother calling getStateTime
        # (twice!)
        p = os.path.join(j.outputdir, 'condorLog')
        if not os.path.isfile(p):
            logger.error('unable to open file %s', p)
            return None
        s = self.getStateTime('submitted')
        r = self.getStateTime('running')
        c = self.getStateTime('completed')
        d = {'SUBMIT': s,'START': r, 'STOP': c}

        return d

    updateMonitoringInformation = \
        staticmethod(updateMonitoringInformation)

    def peek(self, filename=None, command=None):
        job = self.getJobObject()
        
        queryCommand= " ".join\
            (["-maxbytes", "%s" %  getConfig("Condor")["MaxBytes"]])
        if job.subjobs:
            logger.error("Master Job does not have a peek status.")
            peekStatus= False
        name_of_file=""
        if filename:
            name_of_file=filename
        job_id = job.backend.id
        peekCommand = f"condor_tail {queryCommand} {job_id} {name_of_file}"

        status, output = subprocess.getstatusoutput(peekCommand)

        if (status != 0):
            logger.warning \
                ("Return code '%s' peek job '%s' - Condor id '%s'" %
                 (str(status), job.id, job.backend.id))
            logger.warning("Tried command: '%s'" % peekCommand)
            logger.warning("Command output: '%s'" % output)
            peekStatus= False
        else:
            logger.info(output)
            peekStatus = True

        return peekStatus

#_________________________________________________________________________
