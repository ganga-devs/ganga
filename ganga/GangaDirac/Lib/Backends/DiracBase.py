#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""The Ganga backendhandler for the Dirac system."""

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import re
import fnmatch
import time
import datetime
import shutil
import tempfile
import math
from functools import wraps
from collections import defaultdict
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from GangaCore.GPIDev.Adapters.IBackend import IBackend, group_jobs_by_backend_credential
from GangaCore.GPIDev.Lib.Job.Job import Job
from GangaCore.Core.exceptions import GangaFileError, GangaKeyError, BackendError, IncompleteJobSubmissionError, GangaDiskSpaceError
from GangaDirac.Lib.Backends.DiracUtils import result_ok, get_job_ident, get_parametric_datasets, outputfiles_iterator, outputfiles_foreach, getAccessURLs
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError, execute
from GangaDirac.Lib.Credentials.DiracProxy import DiracProxy
from GangaCore.Utility.util import require_disk_space
from GangaCore.Utility.ColourText import getColour
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger, log_user_exception
from GangaCore.GPIDev.Credentials import require_credential, credential_store, needed_credentials
from GangaCore.GPIDev.Base.Proxy import stripProxy, isType, getName
from GangaCore.Core.GangaThread.WorkerThreads import getQueues
from GangaCore.Core import monitoring_component
from GangaCore.Runtime.GPIexport import exportToGPI
from subprocess import check_output, CalledProcessError
configDirac = getConfig('DIRAC')
default_finaliseOnMaster = configDirac['default_finaliseOnMaster']
default_downloadOutputSandbox = configDirac['default_downloadOutputSandbox']
default_unpackOutputSandbox = configDirac['default_unpackOutputSandbox']
logger = getLogger()
regex = re.compile(r'[*?\[\]]')

class DiracBase(IBackend):

    """The backend that submits jobs to the Grid via DIRAC.

    The backend for jobs to be submitted to the Grid. Jobs are
    submitted through the DIRAC WMS system and then in turn submitted to the
    Grid. A few examples of usage are given below

    # Create Dirac backend object
    b = Dirac()

    # Create and submit job.
    j = Job(application=app,backend=b)
    j.submit()

    # Run a Root job on the Grid if in LHCb VO

    # Create a Root application object. See Root help text for instructions
    # on how to configure this.
    app = Root()

    # Create and submit job to Dirac using default options
    j = Job(application=app,backend=Dirac())
    j.submit()

    # Using the 'settings' attribute
    j.backend.settings['BannedSites'] = ['LCG.CERN.ch']
    j.resubmit()

    # settings can be set at any time but are only 'respected' during
    # submit and resubmit.

    # Submitting a job for several processors:
    You can request multiple processors with the minProcessors and maxProcessors attributes
    These set the minimum and maximum allowed number of processors for your job
    A job such as:
    j.backend.minProcessors = 2
    j.backend.minProcessors = 3
    would request 2 processors.
    Note that setting minProcessors to be more than 1 greatly reduces the number
    of possible sites that your job can run at as multi core resources are rare.

    """

    dirac_monitoring_is_active = True

    _schema = Schema(Version(3, 2), {
        'id': SimpleItem(defvalue=None, protected=1, copyable=0,
                         typelist=['int', 'type(None)'],
                         doc='The id number assigned to the job by the DIRAC WMS. If seeking help'
                         ' on jobs with the Dirac backend, please always report this id '
                         'number in addition to a full description of your problem. The id '
                         'can also be used to further inspect the job at '
                         'https://lhcbweb.pic.es/DIRAC/info/general/diracOverview'),
        'status': SimpleItem(defvalue=None, protected=1, copyable=0,
                             typelist=['str', 'type(None)'],
                             doc='The detailed status as reported by the DIRAC WMS'),
        'actualCE': SimpleItem(defvalue=None, protected=1, copyable=0,
                               typelist=['str', 'type(None)'],
                               doc='The location where the job ran'),
        'normCPUTime': SimpleItem(defvalue=None, protected=1, copyable=0,
                                  typelist=['str', 'type(None)'],
                                  doc='The normalized CPU time reported by the DIRAC WMS'),
        'statusInfo': SimpleItem(defvalue='', protected=1, copyable=0,
                                 typelist=['str', 'type(None)'],
                                 doc='Minor status information from Dirac'),
        'extraInfo': SimpleItem(defvalue='', protected=1, copyable=0,
                                typelist=['str', 'type(None)'],
                                doc='Application status information from Dirac'),
        'diracOpts': SimpleItem(defvalue='',
                                doc='DIRAC API commands to add the job definition script. Only edit '
                                'if you *really* know what you are doing'),
        'settings': SimpleItem(defvalue={'CPUTime': 14 * 86400},
                               doc='Settings for DIRAC job (e.g. CPUTime, BannedSites, etc.)'),
        'minProcessors': SimpleItem(defvalue=1,
                                    typelist=[int],
                                    doc='Minimum number of processors the job needs'),
        'maxProcessors': SimpleItem(defvalue=1,
                                    typelist=[int],
                                    doc='Maximum number of processors the job needs'),
        'credential_requirements': ComponentItem('CredentialRequirement', defvalue=DiracProxy),
        'blockSubmit' : SimpleItem(defvalue=True, 
                               doc='Shall we use the block submission?'),
        'finaliseOnMaster' : SimpleItem(defvalue=default_finaliseOnMaster,
                               doc='Finalise the subjobs all in one go when they are all finished.'),
        'downloadSandbox' : SimpleItem(defvalue=default_downloadOutputSandbox,
                               doc='Do you want to download the output sandbox when the job finalises.'),
        'unpackOutputSandbox' : SimpleItem(defvalue=default_unpackOutputSandbox, hidden=True,
                                           doc='Should the output sandbox be unpacked when downloaded.'),

    })
    _exportmethods = ['getOutputData', 'getOutputSandbox', 'removeOutputData',
                      'getOutputDataLFNs', 'getOutputDataAccessURLs', 'peek', 'reset', 'debug', 'finaliseCompletingJobs']
    _packed_input_sandbox = True
    _category = "backends"
    _name = 'DiracBase'
    _hidden = True

    def _setup_subjob_dataset(self, dataset):
        """
        This method is used for constructing datasets on a per subjob basis when submitting parametric jobs
        Args:
            Dataset (Dataset): This is a GangaDataset object, todo check this isn't a list
        """
        return None

    def _setup_bulk_subjobs(self, dirac_ids, dirac_script):
        """
        This is the old bulk submit method which is used to construct the subjobs for a parametric job
        Args:
            dirac_ids (list): This is a list of the Dirac ids which have been created
            dirac_script (str): Name of the dirac script which contains the job jdl
        """
        f = open(dirac_script, 'r')
        parametric_datasets = get_parametric_datasets(f.read().split('\n'))
        f.close()
        if len(parametric_datasets) != len(dirac_ids):
            raise BackendError('Dirac', 'Missmatch between number of datasets defines in dirac API script and those returned by DIRAC')

        master_job = self.getJobObject()
        master_job.subjobs = []
        for i in range(len(dirac_ids)):
            j = Job()
            j.copyFrom(master_job)
            j.splitter = None
            j.backend.id = dirac_ids[i]
            j.id = i
            j.inputdata = self._setup_subjob_dataset(parametric_datasets[i])
            j.status = 'submitted'
            j.time.timenow('submitted')
            master_job.subjobs.append(j)
        return True

    @require_credential
    def _common_submit(self, dirac_script):
        '''Submit the job via the Dirac server.
        Args:
            dirac_script (str): filename of the JDL which is to be submitted to DIRAC
        '''
        j = self.getJobObject()
        self.id = None
        self.actualCE = None
        self.status = None
        self.extraInfo = None
        self.statusInfo = ''
        j.been_queued = False
        dirac_cmd = """execfile(\'%s\')""" % dirac_script

        try:
            result = execute(dirac_cmd, cred_req=self.credential_requirements)
        except GangaDiracError as err:

            err_msg = 'Error submitting job to Dirac: %s' % str(err)
            logger.error(err_msg)
            logger.error("\n\n===\n%s\n===\n" % dirac_script)
            logger.error("\n\n====\n")
            with open(dirac_script, 'r') as file_in:
                logger.error("%s" % file_in.read())
            logger.error("\n====\n")
            raise BackendError('Dirac', err_msg)

        idlist = result
        if type(idlist) is list:
            return self._setup_bulk_subjobs(idlist, dirac_script)

        self.id = idlist
        return type(self.id) == int

    def _addition_sandbox_content(self, subjobconfig):
        '''any additional files that should be sent to dirac
        Args:
            subjobcofig (unknown): This is the config for this subjob (I think)'''
        return []

    @require_credential
    def _block_submit(self, myscript, lenSubjobs, keep_going = False):
        '''Submit a block of jobs via the Dirac server in one go.
        Args:
            dirac_script (str): filename of the JDL which is to be submitted to DIRAC
        '''
        j = self.getJobObject()
        self.id = None
        self.actualCE = None
        self.status = None
        self.extraInfo = None
        self.statusInfo = ''
        j.been_queued = False
        dirac_cmd = """execfile(\'%s\')""" % myscript
        submitFailures = {}
        try:
            result = execute(dirac_cmd, cred_req=self.credential_requirements, return_raw_dict = True, new_subprocess = True)

        except GangaDiracError as err:
            err_msg = 'Error submitting job to Dirac: %s' % str(err)
            logger.error(err_msg)
            logger.error("\n\n===\n%s\n===\n" % myscript)
            logger.error("\n\n====\n")
            with open(myscript, 'r') as file_in:
                logger.error("%s" % file_in.read())
            logger.error("\n====\n")
            j.updateStatus('failed')
            raise BackendError('Dirac', err_msg)
            return 0

        #Now put the list of Dirac IDs into the subjobs and get them monitored:
        if len(j.subjobs)>0:
            for jobNo in result.keys():
                sjNo = jobNo.split('.')[1]
                #If we get an int we have a DIRAC ID so job submitted
                if isinstance(result[jobNo], int):
                    j.subjobs[int(sjNo)].backend.id = result[jobNo]
                    j.subjobs[int(sjNo)].updateStatus('submitted')
                    j.time.timenow('submitted')
                    stripProxy(j.subjobs[int(sjNo)].info).increment()
                #If we get a string we have an error message. Add to the list of failures
                elif isinstance(result[jobNo], str):
                    j.subjobs[int(sjNo)].updateStatus('failed')
                    submitFailures.update({jobNo : result[jobNo]})
                #If we have neither int or str then something disastrous happened
                else:
                    j.subjobs[int(sjNo)].updateStatus('failed')
                    submitFailures.update({jobNo : 'DIRAC error!'})
        else:
            result_id = list(result.keys())[0]
            if isinstance(result[result_id], int):
                j.backend.id = result[result_id]
                j.updateStatus('submitted')
                j.time.timenow('submitted')
                stripProxy(j.info).increment()
            elif isinstance(result[result_id], str):
                j.updateStatus('failed')
                submitFailures.update({result_id: result[result_id]})
            else:
                j.updateStatus('failed')
                submitFailures.update({result_id: 'DIRAC error!'})

        #Check that every subjob got submitted ok
        if len(submitFailures) > 0:
            for sjNo in submitFailures.keys():
                logger.error('Job submission failed for job %s : %s' % (sjNo, submitFailures[sjNo]))
            raise GangaDiracError("Some subjobs failed to submit! Check their status!")
            return 0

        return 1 

    def master_submit(self, rjobs, subjobconfigs, masterjobconfig, keep_going=False, parallel_submit=False):
        """  Submit the master job and all of its subjobs. To keep things speedy when talking to DIRAC
        we can submit several subjobs in the same process. Therefore for each subjob we collect the code for
       the dirac-script into one large file that we then execute.
        """
        #If you want to go slowly use the regular master_submit:
        if not self.blockSubmit:
            return IBackend.master_submit(self, rjobs, subjobconfigs, masterjobconfig, keep_going, parallel_submit)

        #Otherwise use the block submit. Much of this is copied from IBackend
        logger.debug("SubJobConfigs: %s" % len(subjobconfigs))
        logger.debug("rjobs: %s" % len(rjobs))

        if rjobs and len(subjobconfigs) != len(rjobs):
            raise BackendError("The number of subjob configurations does not match the number of subjobs!")

        incomplete = 0
        incomplete_subjobs = []

        master_input_sandbox = self.master_prepare(masterjobconfig)

        nPerProcess = configDirac['maxSubjobsPerProcess']
        nProcessToUse = math.ceil((len(rjobs)*1.0)/nPerProcess)

        from GangaCore.Core.GangaThread.WorkerThreads import getQueues
        # Must check for credentials here as we cannot handle missing credentials on Queues by design!
        try:
            cred = credential_store[self.credential_requirements]
        except GangaKeyError:
            credential_store.create(self.credential_requirements)

        tmp_dir = tempfile.mkdtemp()
        # Loop over the processes and create the master script for each one.
        for i in range(0,int(nProcessToUse)):
            nSubjobs = 0
            #The Dirac IDs are stored in a dict so create it at the start of the script
            masterScript = 'resultdict = {}\n'
            for sc, sj in zip(subjobconfigs[i*nPerProcess:(i+1)*nPerProcess], rjobs[i*nPerProcess:(i+1)*nPerProcess]):
                #Add in the script for each subjob
                sj.updateStatus('submitting')
                fqid = sj.getFQID('.')
                #Change the output of the job script for our own ends. This is a bit of a hack but it saves having to rewrite every RTHandler
                sjScript = sj.backend._job_script(sc, master_input_sandbox, tmp_dir)
                sjScript = sjScript.replace("output(result)", "if isinstance(result, dict) and 'Value' in result:\n\tresultdict.update({sjNo : result['Value']})\nelse:\n\tresultdict.update({sjNo : result['Message']})")
                if nSubjobs == 0:
                    sjScript = re.sub(r"(dirac = Dirac.*\(\))",r"\1\nsjNo='%s'\n" % fqid, sjScript)
                if nSubjobs !=0 :
                    sjScript = sjScript.replace("from DIRAC.Core.Base.Script import parseCommandLine\nparseCommandLine()\n", "\n")
                    sjScript = re.sub(r"from .*DIRAC\.Interfaces\.API.Dirac.* import Dirac.*","",sjScript)
                    sjScript = re.sub(r"from .*DIRAC\.Interfaces\.API\..*Job import .*Job","",sjScript)
                    sjScript = re.sub(r"dirac = Dirac.*\(\)","",sjScript)
                    masterScript += "\nsjNo=\'%s\'" % fqid
                masterScript += sjScript
                nSubjobs +=1
            #Return the dict of job numbers and Dirac IDs          
            masterScript += '\noutput(resultdict)\n'
            dirac_script_filename = os.path.join(self.getJobObject().getInputWorkspace().getPath(),'dirac-script-%s.py') % i
            with open(dirac_script_filename, 'w') as f:
                f.write(masterScript)
            upperlimit = (i+1)*nPerProcess
            if upperlimit > len(rjobs) :
                upperlimit = len(rjobs)

            if (upperlimit-1) == 0:
                logger.info("Submitting job")
            else:
                logger.info("Submitting subjobs %s to %s" % (i*nPerProcess, upperlimit-1))

            try:
                #Either do the submission in parallel with threads or sequentially
                if parallel_submit:
                    getQueues()._monitoring_threadpool.add_function(self._block_submit, (dirac_script_filename, nSubjobs, keep_going))
                else:
                    self._block_submit(dirac_script_filename, nSubjobs, keep_going)

                while not self._subjob_status_check(rjobs, nPerProcess, i):
                    time.sleep(1.)
            finally:
                shutil.rmtree(tmp_dir, ignore_errors = True)
                    
            if (upperlimit-1) == 0:
                logger.info("Submitted job")
            else:
                logger.info("Submitted subjobs %s to %s" % (i*nPerProcess, upperlimit-1))

        for i in rjobs:
            if i.status in ["new", "failed"]:
                return 0
    
        return 1

    def _subjob_status_check(self, rjobs, nPerProcess, i):
                has_submitted = True
                for sj in rjobs[i*nPerProcess:(i+1)*nPerProcess]:
                    if sj.status not in ["submitted","failed","completed","running","completing"]:
                        has_submitted = False
                        break
                return has_submitted

    def _job_script(self, subjobconfig, master_input_sandbox, tmp_dir):
        """Get the script to submit a single DIRAC job
        Args:
            subjobconfig (unknown):
            master_input_sandbox (list): file names which are in the master sandbox of the master sandbox (if any)
            tmp_dir: (str) This is the temp directory where files will be placed when needed
        """

        j = self.getJobObject()

        sboxname = j.createPackedInputSandbox(subjobconfig.getSandboxFiles())

        input_sandbox = master_input_sandbox[:]
        input_sandbox += sboxname

        input_sandbox += self._addition_sandbox_content(subjobconfig)

        lfns = []

        ## Add LFN to the inputfiles section of the file
        if j.master:
            for this_file in j.master.inputfiles:
                if isType(this_file, DiracFile):
                    lfns.append('LFN:'+str(this_file.lfn))
        for this_file in j.inputfiles:
            if isType(this_file, DiracFile):
                lfns.append('LFN:'+str(this_file.lfn))

        # Make sure we only add unique LFN
        input_sandbox += set(lfns)

        # Remove duplicates incase the LFN have also been added by any prior step
        input_sandbox = list(set(input_sandbox))

        logger.debug("dirac_script: %s" % str(subjobconfig.getExeString()))
        logger.debug("sandbox_cont:\n%s" % str(input_sandbox))


        # This is a workaroud for the fact DIRAC doesn't like whitespace in sandbox filenames
        ### START_WORKAROUND

        # Loop through all files and if the filename contains a ' ' copy it to a location which doesn't contain one.
        # This does have the limitation that all file basenames must not contain a ' ' character.
        # However we don't make any in Ganga as of 20/09/16
        sandbox_str = '['
        for file_ in input_sandbox:
            if ' ' in str(file_):
                new_name = os.path.join(tmp_dir, os.path.basename(file_))
                shutil.copy(file_, new_name)
                file_ = new_name
            sandbox_str += '\'' + str(file_) + '\', '
        sandbox_str += ']'
        logger.debug("sandbox_str: %s" % sandbox_str)
        ### FINISH_WORKAROUND

        dirac_script = subjobconfig.getExeString().replace('##INPUT_SANDBOX##', sandbox_str)

        return dirac_script
        
    def submit(self, subjobconfig, master_input_sandbox):
        """Submit a DIRAC job
        Args:
            subjobconfig (unknown):
            master_input_sandbox (list): file names which are in the master sandbox of the master sandbox (if any)
        """

        j = self.getJobObject()

        tmp_dir = tempfile.mkdtemp()
        dirac_script = self._job_script(subjobconfig, master_input_sandbox, tmp_dir)

        dirac_script_filename = os.path.join(j.getInputWorkspace().getPath(), 'dirac-script.py')
        with open(dirac_script_filename, 'w') as f:
            f.write(dirac_script)

        try:
            return self._common_submit(dirac_script_filename)
        finally:
            # CLEANUP after workaround
            shutil.rmtree(tmp_dir, ignore_errors = True)

    def master_auto_resubmit(self, rjobs):
        '''Duplicate of the IBackend.master_resubmit but hooked into auto resubmission
        such that the monitoring server is used rather than the user server
        Args:
            rjobs (list): This is a list of jobs which are to be auto-resubmitted'''

        incomplete = 0

        def handleError(x):
            if incomplete:
                raise x
            else:
                return 0
        try:
            for sj in rjobs:
                fqid = sj.getFQID('.')
                logger.info("resubmitting job %s to %s backend", fqid, getName(sj.backend))
                try:
                    b = sj.backend
                    sj.updateStatus('submitting')
                    if self.blockSubmit:
                        result = b._blockResubmit()
                    else:
                        result = b._resubmit()
                    if result:
                        sj.updateStatus('submitted')
                        # sj._commit() # PENDING: TEMPORARY DISABLED
                        incomplete = 1
                    else:
                        return handleError(IncompleteJobSubmissionError(fqid, 'resubmission failed'))
                except Exception as x:
                    log_user_exception(logger, debug=isType(x, GangaDiracError))
                    return handleError(IncompleteJobSubmissionError(fqid, str(x)))
        finally:
            master = self.getJobObject().master
            if master:
                master.updateMasterJobStatus()
        return 1

    def resubmit(self):
        """Resubmit a DIRAC job"""
        if self.blockSubmit:
            return self._blockResubmit()
        else:
            return self._resubmit()

    def _resubmit(self):
        """Resubmit a DIRAC job"""
        j = self.getJobObject()
        parametric = False
        script_path = os.path.join(j.getInputWorkspace().getPath(), 'dirac-script.py')
        # Check old script
        if j.master is None and not os.path.exists(script_path):
            raise BackendError('Dirac', 'No "dirac-script.py" found in j.inputdir')

        if j.master is not None and not os.path.exists(script_path):
            script_path = os.path.join(
                j.master.getInputWorkspace().getPath(), 'dirac-script.py')
            if not os.path.exists(script_path):
                raise BackendError('Dirac', 'No "dirac-script.py" found in j.inputdir or j.master.inputdir')
            parametric = True

        # Read old script
        f = open(script_path, 'r')
        script = f.read()
        f.close()

        # Create new script - ##note instead of using get_parametric_dataset
        # could just use j.inputdata.
        if parametric is True:
            parametric_datasets = get_parametric_datasets(script.split('\n'))
            if j.master:
                if len(parametric_datasets) != len(j.master.subjobs):
                    raise BackendError('Dirac', 'number of parametric datasets defined in API script doesn\'t match number of master.subjobs')
            if j.inputdata and len(j.inputdata) > 0:
                _input_files = [f for f in j.inputdata if not isType(f, DiracFile)]
            else:
                _input_files = []
            if set(parametric_datasets[j.id]).symmetric_difference(set([f.namePattern for f in _input_files])):
                raise BackendError(
                    'Dirac', 'Mismatch between dirac-script and job attributes.')
            script = script.replace('.setParametricInputData(%s)' % str(parametric_datasets),
                                    '.setInputData(%s)' % str(parametric_datasets[j.id]))
            script = script.replace('%n', str(j.id))  # name

        start_user_settings = '# <-- user settings\n'
        new_script = script[
            :script.find(start_user_settings) + len(start_user_settings)]

        job_ident = get_job_ident(script.split('\n'))
        for key, value in self.settings.items():
            if str(key).startswith('set'):
                _key = key[3:]
            else:
                _key = key
            if type(value) is str:
                template = '%s.set%s("%s")\n'
            else:
                template = '%s.set%s(%s)\n'
            new_script += template % (job_ident, str(_key), str(value))
        new_script += script[script.find('# user settings -->'):]

        # Save new script
        new_script_filename = os.path.join(j.getInputWorkspace().getPath(), 'dirac-script.py')
        f = open(new_script_filename, 'w')
        f.write(new_script)
        f.flush()
        f.close()
        return self._common_submit(new_script_filename)

    def _blockResubmit(self):
        """Resubmit a DIRAC job that was submitted with bulk submission. This requires writing a new dirac-script for the individual job."""
        j = self.getJobObject()
        parametric = False

        if j.master is None:
            scriptDir = j.getInputWorkspace().getPath()
        else:
            scriptDir = j.master.getInputWorkspace().getPath()

        diracScriptFiles = []
        for fileName in os.listdir(scriptDir):
            if fnmatch.fnmatch(fileName, 'dirac-script-*.py'):
                diracScriptFiles.append(fileName)

        #Did we find any of the new style dirac scripts? If not try the old way as the job may have been submitted with an old ganga version.
        if diracScriptFiles == []:
            return self._resubmit()

        new_script_filename = ''

        for diracScript in diracScriptFiles:
            script_path = os.path.join(scriptDir, diracScript)
            # Check old script
            if not os.path.exists(script_path):
                raise BackendError('Dirac', 'No "dirac-script.py" found in j.inputdir')

            # Read old script
            with open(script_path, 'r') as f:
                script = f.read()
            # Is the subjob we want in there?
            if not ("sjNo='%s'" % j.fqid) in script:
                continue

            #First pick out the imports etc at the start
            newScript =  re.compile(r'%s.*?%s' % (r'resultdict = {}',r"dirac = Dirac.*?\(\)\n"),re.S).search(script).group(0)
            newScript += '\n'
            #Now pick out the job part
            start = "sjNo='%s'" % j.fqid
            #Check if the original script included the check for the dirac output
            if "result[\'Message\']" in script:
                newScript += re.compile(r'%s.*?%s' % (start,r"resultdict.update\({sjNo : result\['Message'\]}\)"),re.S).search(script).group(0)
            else:
                newScript += re.compile(r'%s.*?%s' % (start,r"resultdict.update\({sjNo : result\['Value'\]}\)"),re.S).search(script).group(0)
            newScript += '\noutput(resultdict)'
            
            # Modify the new script with the user settings

            start_user_settings = '# <-- user settings\n'
            new_script = newScript[
                :newScript.find(start_user_settings) + len(start_user_settings)]

            job_ident = get_job_ident(newScript.split('\n'))
            for key, value in self.settings.items():
                if str(key).startswith('set'):
                    _key = key[3:]
                else:
                    _key = key
                if type(value) is str:
                    template = '%s.set%s("%s")\n'
                else:
                    template = '%s.set%s(%s)\n'
                new_script += template % (job_ident, str(_key), str(value))
            new_script += newScript[newScript.find('# user settings -->'):]

            # Save new script
            new_script_filename = os.path.join(j.getInputWorkspace().getPath(), 'dirac-script.py')
            with open(new_script_filename, 'w') as f:
                f.write(new_script)
            # Break the loop now we have written the new script
            break

        if new_script_filename == '':
            raise BackendError('Dirac', 'Script for job number %s not found. Resubmission failed.' % j.fqid)

        return self._block_submit(new_script_filename, 1)
 
    def reset(self, doSubjobs=False):
        """Resets the state of a job back to 'submitted' so that the
        monitoring will run on it again.
        Args:
            doSubjobs (bool): Should we rest the subjobs associated with this job or not"""
        j = self.getJobObject()

        disallowed = ['submitting', 'killed']
        if j.status in disallowed:
            logger.warning("Can not reset a job in status '%s'." % j.status)
        else:
            j.getOutputWorkspace().remove(preserve_top=True)
            self.extraInfo = None
            self.statusInfo = ''
            self.status = None
            self.actualCE = None
            j.been_queued = False
            j.updateStatus('submitted')
            if j.subjobs and not doSubjobs:
                logger.info('This job has subjobs, if you would like the backends '
                            'of all the subjobs that are in status=\'completing\' or '
                            'status=\'failed\' also reset then recall reset with the '
                            'arg \'True\' i.e. job(3).backend.reset(True)')
            elif j.subjobs and doSubjobs:
                logger.info('resetting the backends of \'completing\' and \'failed\' subjobs.')
                for sj in j.subjobs:
                    if sj.status == 'completing' or sj.status == 'failed':
                        sj.backend.reset()
            if j.master:
                j.master.updateMasterJobStatus()

    @require_credential
    def kill(self):
        """ Kill a Dirac jobs"""
        if not self.id:
            return None
        dirac_cmd = 'kill(%d)' % self.id
        try:
            result = execute(dirac_cmd, cred_req=self.credential_requirements)
        except GangaDiracError as err:
            raise BackendError('Dirac', 'Could not kill job: %s' % err)
        return True

    @require_credential
    def master_kill(self):
        """ A method for killing a job and all of its subjobs. Does it en masse
        for maximum speed.
        """
        j = self.getJobObject()
        if len(j.subjobs)==0:
            return self.kill()
        else:
            kill_list = []
            for sj in j.subjobs:
                if sj.status not in ['completed', 'failed']:
                    kill_list.append(sj.backend.id)
            dirac_cmd = 'kill(%s)' % kill_list
            try:
                result = execute(dirac_cmd, cred_req=self.credential_requirements)
            except GangaDiracError as err:
                raise BackendError('Dirac', 'Dirac failed to kill job %d.' % j.id)
        if not len(result)==len(kill_list):
            diff = list(set(kill_list) - set(result))
            raise BackendError('Dirac', 'Dirac failed to kill jobs %s' % diff)
        return True

    @require_credential
    def peek(self, filename=None, command=None):
        """Peek at the output of a job (Note: filename/command are ignored).
        Args:
            filename (str): Ignored but is filename of a file in the sandbox
            command (str): Ignored but is a command which could be executed"""
        dirac_cmd = 'peek(%d)' % self.id
        try:
            result = execute(dirac_cmd, cred_req=self.credential_requirements)
            logger.info(result)
        except GangaDiracError:
            logger.error("No peeking available for Dirac job '%i'.", self.id)

    @require_credential
    @require_disk_space
    def getOutputSandbox(self, outputDir=None, unpack=True):
        """Get the outputsandbox for the job object controlling this backend
        Args:
            outputDir (str): This string represents the output dir where the sandbox is to be placed
        """
        j = self.getJobObject()
        if j.subjobs:
            logger.error('Cannot download a sandbox for a master job.')
            return False
        if outputDir is None:
            outputDir = j.getOutputWorkspace().getPath()
        dirac_cmd = "getOutputSandbox(%d,'%s', %s)"  % (self.id, outputDir, unpack)
        try:
            result = execute(dirac_cmd, cred_req=self.credential_requirements)
        except GangaDiracError as err:
            msg = 'Problem retrieving output: %s' % str(err)
            logger.warning(msg)
            return False

        return True

    def removeOutputData(self):
        """
        Remove all the LFNs associated with this job.
        """
        # Note when the API can accept a list for removeFile I will change
        # this.
        j = self.getJobObject()
        logger.info("Removing all DiracFile output for job %s" % j.id)
        lfnsToRemove = []
        if j.subjobs:
            for sj in j.subjobs:
                outputfiles_foreach(sj, DiracFile, lambda x: lfnsToRemove.append(x.lfn))
        else:
            outputfiles_foreach(j, DiracFile, lambda x: lfnsToRemove.append(x.lfn))
        dirac_cmd = "removeFile(%s)" % lfnsToRemove
        try:
            result = execute(dirac_cmd, cred_req=self.credential_requirements)
        except GangaDiracError as err:
            msg = 'Problem removing files: %s' % str(err)
            logger.warning(msg)
            return False
        def clearFileInfo(f):
            f.lfn = ""
            f.locations = []
            f.guid = ''
        if j.subjobs:
            for sj in j.subjobs:
                outputfiles_foreach(sj, DiracFile, lambda x: clearFileInfo(x))
        else:
            outputfiles_foreach(j, DiracFile, lambda x: clearFileInfo(x))

    @require_disk_space
    def getOutputData(self, outputDir=None, names=None, force=False):
        """Retrieve data stored on SE to dir (default=job output workspace).
        If names=None, then all outputdata is downloaded otherwise names should
        be a list of files to download. If force=True then data will be redownloaded
        even if the file already exists.

        Note that if called on a master job then all subjobs outputwill be downloaded.
        If dir is None then the subjobs output goes into their individual
        outputworkspaces as expected. If however one specifies a dir then this is
        treated as a top dir and a subdir for each job will be created below it. This
        will avoid overwriting files with the same name from each subjob.
        Args:
            outputDir (str): This string represents the output dir where the sandbox is to be placed
            names (list): list of names which match namePatterns in the outputfiles
            force (bool): Force the download out data potentially overwriting existing objects
        """
        j = self.getJobObject()
        if outputDir is not None and not os.path.isdir(outputDir):
            raise GangaDiracError("Designated outupt path '%s' must exist and be a directory" % outputDir)

        def download(dirac_file, job, is_subjob=False):
            dirac_file.localDir = job.getOutputWorkspace().getPath()
            if outputDir is not None:
                output_dir = outputDir
                if is_subjob:
                    output_dir = os.path.join(outputDir, job.fqid)
                    if not os.path.isdir(output_dir):
                        os.mkdir(output_dir)
                dirac_file.localDir = output_dir
            if os.path.exists(os.path.join(dirac_file.localDir, os.path.basename(dirac_file.lfn))) and not force:
                return
            try:
                dirac_file.get()
                return dirac_file.lfn
            # should really make the get method throw if doesn't suceed. todo
            except (GangaDiracError, GangaFileError) as e:
                logger.warning(e)

        suceeded = []
        if j.subjobs:
            for sj in j.subjobs:
                suceeded.extend([download(f, sj, True) for f in outputfiles_iterator(sj, DiracFile) if f.lfn != '' and (names is None or f.namePattern in names)])
        else:
            suceeded.extend([download(f, j, False) for f in outputfiles_iterator(j, DiracFile) if f.lfn != '' and (names is None or f.namePattern in names)])

        return [x for x in suceeded if x is not None]

    def getOutputDataLFNs(self):
        """Retrieve the list of LFNs assigned to outputdata"""
        j = self.getJobObject()
        lfns = []

        if j.subjobs:
            for sj in j.subjobs:
                lfns.extend([f.lfn for f in outputfiles_iterator(sj, DiracFile) if f.lfn != ''])
        else:
            lfns.extend([f.lfn for f in outputfiles_iterator(j, DiracFile) if f.lfn != ''])
        return lfns

    def getOutputDataAccessURLs(self):
        """Retrieve the list of accessURLs assigned to outputdata for a job"""
        return getAccessURLs(self.getOutputDataLFNs())

    @require_credential
    def debug(self):
        '''Obtains some (possibly) useful DIRAC debug info. '''
        # check services
        cmd = 'getServicePorts()'
        try:
            result = execute(cmd, cred_req=self.credential_requirements)
        except GangaDiracError as err:
            logger.warning('Could not obtain services: %s' % str(err))
            return
        services = result
        for category in services:
            system, service = category.split('/')
            cmd = "ping('%s','%s')" % (system, service)
            try:
                result = execute(cmd, cred_req=self.credential_requirements)
                msg = 'OK.'
            except GangaDiracError as err:
                msg = '%s' % err
            logger.info('%s: %s' % (category, msg))

        # get pilot info for this job
        if not isinstance(self.id, int):
            return
        j = self.getJobObject()
        cwd = os.getcwd()
        debug_dir = j.getDebugWorkspace().getPath()
        cmd = "getJobPilotOutput(%d,'%s')" % (self.id, debug_dir)
        try:
            result = execute(cmd, cred_req=self.credential_requirements)
            logger.info('Pilot Info: %s/pilot_%d/std.out.' % (debug_dir, self.id))
        except GangaDiracError as err:
            logger.error("%s" % err)

    def finaliseCompletingJobs(self, downloadSandbox=True):
        """
         A function to finalise all the subjobs in the completing state, so they are ready, before all the subjobs complete.
        """
        j = self.getJobObject()
        if j.master:
            j = j.master
        if not j.subjobs:
            logger.warning("There are no subjobs - this will finalise in its own time.")
            return
        jobList = []
        for sj in j.subjobs:
            if sj.status == 'completing':
                jobList.append(sj)
        if len(jobList) == 0:
            logger.warning("No subjobs are ready to be finalised yet. Be more patient.")
            return
        else:
            DiracBase.finalise_jobs(jobList, downloadSandbox)

    @staticmethod
    def _bulk_updateStateTime(jobStateDict, bulk_time_lookup={} ):
        """ This performs the same as the _getStateTime method but loops over a list of job ids within the DIRAC namespace (much faster)
        Args:
            jobStateDict (dict): This is a dict of {job.backend.id : job_status, } elements
            bulk_time_lookup (dict): Dict of result of multiple calls to getBulkStateTime, performed in advance
        """
        for this_state, these_jobs in jobStateDict.items():
            if bulk_time_lookup == {} or this_state not in bulk_time_lookup:
                bulk_result = execute("getBulkStateTime(%s,\'%s\')" % (repr([j.backend.id for j in these_jobs]), this_state), cred_req=these_jobs[0].backend.credential_requirements)  # TODO split jobs by cred_req
            else:
                bulk_result = bulk_time_lookup[this_state]
            for this_job in jobStateDict[this_state]:
                backend_id = this_job.backend.id
                if backend_id in bulk_result and bulk_result[backend_id]:
                    DiracBase._getStateTime(this_job, this_state, {this_state : bulk_result[backend_id]})
                else:
                    DiracBase._getStateTime(this_job, this_state)

    @staticmethod
    def _getStateTime(job, status, getStateTimeResult={}):
        """Returns the timestamps for 'running' or 'completed' by extracting
        their equivalent timestamps from the loggingInfo.
        Args:
            job (Job): This is the job object we want to update
            status (str): This is the Ganga status we're updating (running, completed... etc)
            getStateTimeResult (dict): This is the optional result of executing the approriate getStateTime
                                        against this job.backend.id, if not provided the command is called internally
        """
        # Now private to stop server cross-talk from user thread. Since updateStatus calles
        # this method whether called itself by the user thread or monitoring thread.
        # Now don't use hook but define our own private version
        # used in monitoring loop... messy but works.
        if job.status != status:
            b_list = ['running', 'completing', 'completed', 'failed']
            backend_final = ['failed', 'completed']
            # backend stamps
            if not job.subjobs and status in b_list:
                for childstatus in b_list:
                    if job.backend.id:
                        logger.debug("Accessing getStateTime() in diracAPI")
                        if childstatus in backend_final:
                            if childstatus in getStateTimeResult:
                                be_statetime = getStateTimeResult[childstatus]
                            else:
                                be_statetime = execute("getStateTime(%d,\'%s\')" % (job.backend.id, childstatus), cred_req=job.backend.credential_requirements)
                            job.time.timestamps["backend_final"] = be_statetime
                            logger.debug("Wrote 'backend_final' to timestamps.")
                            break
                        else:
                            time_str = "backend_" + childstatus
                            if time_str not in job.time.timestamps:
                                if childstatus in getStateTimeResult:
                                    be_statetime = getStateTimeResult[childstatus]
                                else:
                                    be_statetime = execute("getStateTime(%d,\'%s\')" % (job.backend.id, childstatus), cred_req=job.backend.credential_requirements)
                                job.time.timestamps["backend_" + childstatus] = be_statetime
                            logger.debug("Wrote 'backend_%s' to timestamps.", childstatus)
                    if childstatus == status:
                        break
            logger.debug("_getStateTime(job with id: %d, '%s') called.", job.id, job.status)
        else:
            logger.debug("Status changed from '%s' to '%s'. No new timestamp was written", job.status, status)

    def timedetails(self):
        """Prints contents of the loggingInfo from the Dirac API."""
        if not self.id:
            return None
        logger.debug("Accessing timedetails() in diracAPI")
        dirac_cmd = 'timedetails(%d)' % self.id
        return execute(dirac_cmd, cred_req=self.credential_requirements)

    @staticmethod
    def job_finalisation_cleanup(job, updated_dirac_status):
        """
        Method for reverting a job back to a clean state upon a failure in the job progression
        Args:
            job (Job) This is the job to change the status
            updated_dirac_status (str): Ganga status which is to be used somewhere
        """
        #   Revert job back to running state if we exit uncleanly
        if job.status == "completing":
            job.updateStatus('running')
            if job.master:
                job.master.updateMasterJobStatus()
        # FIXME should I add something here to cleanup on sandboxes pulled from
        # malformed job output?

    @staticmethod
    @require_disk_space
    def _internal_job_finalisation(job, updated_dirac_status):
        """
        This method performs the main job finalisation
        Args:
            job (Job): Thi is the job we want to finalise
            updated_dirac_status (str): String representing the Ganga finalisation state of the job failed/completed
        """
        if job.backend.finaliseOnMaster and job.master and updated_dirac_status == 'completed':
            job.updateStatus('completing')
            allComplete = True
            jobsToFinalise = []
            for sj in job.master.subjobs:
                if sj.status not in ['completing', 'failed', 'killed', 'removed', 'completed']:
                    allComplete = False
                    break
            if allComplete:
                DiracBase.finalise_jobs(job.master.subjobs, job.master.backend.downloadSandbox)
            return

        if updated_dirac_status == 'completed':
            start = time.time()
            # firstly update job to completing
            DiracBase._getStateTime(job, 'completing')
            if job.status in ['removed', 'killed']:
                return
            elif (job.master and job.master.status in ['removed', 'killed']):
                return  # user changed it under us

            job.updateStatus('completing')
            if job.master:
                job.master.updateMasterJobStatus()

            output_path = job.getOutputWorkspace().getPath()

            logger.debug('Contacting DIRAC for job: %s' % job.fqid)
            # Contact dirac which knows about the job
            job.backend.normCPUTime, getSandboxResult, file_info_dict, completeTimeResult = execute("finished_job(%d, '%s', %s, downloadSandbox=%s)" % (job.backend.id, output_path, job.backend.unpackOutputSandbox, job.backend.downloadSandbox), cred_req=job.backend.credential_requirements)

            now = time.time()
            logger.debug('%0.2fs taken to download output from DIRAC for Job %s' % ((now - start), job.fqid))

            #logger.info('Job ' + job.fqid + ' OutputDataInfo: ' + str(file_info_dict))
            #logger.info('Job ' + job.fqid + ' OutputSandbox: ' + str(getSandboxResult))
            #logger.info('Job ' + job.fqid + ' normCPUTime: ' + str(job.backend.normCPUTime))

            # Set DiracFile metadata
            if hasattr(job.outputfiles, 'get'):
                wildcards = [f.namePattern for f in job.outputfiles.get(DiracFile) if regex.search(f.namePattern) is not None]
            else:
                wildcards = []

            lfn_store = os.path.join(output_path, getConfig('Output')['PostProcessLocationsFileName'])

            # Make the file on disk with a nullop...
            if not os.path.isfile(lfn_store):
                with open(lfn_store, 'w'):
                    pass

            if hasattr(job.outputfiles, 'get') and job.outputfiles.get(DiracFile):

                # Now we can iterate over the contents of the file without touching it
                with open(lfn_store, 'ab') as postprocesslocationsfile:
                    if not hasattr(file_info_dict, 'keys'):
                        logger.error("Error understanding OutputDataInfo: %s" % str(file_info_dict))
                        raise GangaDiracError("Error understanding OutputDataInfo: %s" % str(file_info_dict))

                    ## Caution is not clear atm whether this 'Value' is an LHCbism or bug
                    list_of_files = file_info_dict.get('Value', list(file_info_dict.keys()))

                    for file_name in list_of_files:
                        file_name = os.path.basename(file_name)
                        info = file_info_dict.get(file_name)
                        #logger.debug("file_name: %s,\tinfo: %s" % (str(file_name), str(info)))

                        if not hasattr(info, 'get'):
                            logger.error("Error getting OutputDataInfo for: %s" % str(job.getFQID('.')))
                            logger.error("Please check the Dirac Job still exists or attempt a job.backend.reset() to try again!")
                            logger.error("Err: %s" % str(info))
                            logger.error("file_info_dict: %s" % str(file_info_dict))
                            raise GangaDiracError("Error getting OutputDataInfo")

                        valid_wildcards = [wc for wc in wildcards if fnmatch.fnmatch(file_name, wc)]
                        if not valid_wildcards:
                            valid_wildcards.append('')

                        for wc in valid_wildcards:
                            #logger.debug("wildcard: %s" % str(wc))

                            DiracFileData = 'DiracFile:::%s&&%s->%s:::%s:::%s\n' % (wc,
                                                                                    file_name,
                                                                                    info.get('LFN', 'Error Getting LFN!'),
                                                                                    str(info.get('LOCATIONS', ['NotAvailable'])),
                                                                                    info.get('GUID', 'NotAvailable')
                                                                                    )
                            #logger.debug("DiracFileData: %s" % str(DiracFileData))
                            postprocesslocationsfile.write(DiracFileData.encode())
                            postprocesslocationsfile.flush()

                logger.debug("Written: %s" % open(lfn_store, 'r').readlines())

            # check outputsandbox downloaded correctly
            if job.backend.downloadSandbox and not result_ok(getSandboxResult):
                logger.warning('Problem retrieving outputsandbox: %s' % str(getSandboxResult))
                DiracBase._getStateTime(job, 'failed')
                if job.status in ['removed', 'killed']:
                    return
                elif (job.master and job.master.status in ['removed', 'killed']):
                    return  # user changed it under us
                job.updateStatus('failed')
                if job.master:
                    job.master.updateMasterJobStatus()
                raise BackendError('Dirac', 'Problem retrieving outputsandbox: %s' % str(getSandboxResult))
            #If the sandbox dict includes a Succesful key then the sandbox has been download from grid storage, likely due to being oversized. Untar it and issue a warning.
            elif job.backend.downloadSandbox and isinstance(getSandboxResult['Value'], dict) and getSandboxResult['Value'].get('Successful', False):
                    try:
                        sandbox_name = list(getSandboxResult['Value']['Successful'].values())[0]
                        check_output(['tar', '-xvf', sandbox_name, '-C', output_path])
                        check_output(['rm', sandbox_name])
                        logger.warning('Output sandbox for job %s downloaded from grid storage due to being oversized.' % job.fqid)
                    except CalledProcessError:
                        logger.error('Failed to unpack output sandbox for job %s' % job.fqid)
            # finally update job to completed
            DiracBase._getStateTime(job, 'completed', completeTimeResult)
            if job.status in ['removed', 'killed']:
                return
            elif (job.master and job.master.status in ['removed', 'killed']):
                return  # user changed it under us
            job.updateStatus('completed')
            if job.master:
                job.master.updateMasterJobStatus()
            now = time.time()
            logger.debug('Job ' + job.fqid + ' Time for complete update : ' + str(now - start))

        elif updated_dirac_status == 'failed':
            # firstly update status to failed
            DiracBase._getStateTime(job, 'failed')
            if job.status in ['removed', 'killed']:
                return
            if (job.master and job.master.status in ['removed', 'killed']):
                return  # user changed it under us
            job.updateStatus('failed')
            if job.master:
                job.master.updateMasterJobStatus()

            # if requested try downloading outputsandbox anyway
            if configDirac['failed_sandbox_download']:
                execute("getOutputSandbox(%d,'%s', %s)" % (job.backend.id, job.getOutputWorkspace().getPath(), job.backend.unpackOutputSandbox), cred_req=job.backend.credential_requirements)
        else:
            logger.error("Job #%s Unexpected dirac status '%s' encountered" % (job.getFQID('.'), updated_dirac_status))

    @staticmethod
    def job_finalisation(job, updated_dirac_status):
        """
        Attempt to finalise the job given and auto-retry 5 times on error
        Args:
            job (Job): Job object to finalise
            updated_dirac_status (str): The Ganga status to update the job to, i.e. failed/completed
        """
        count = 1
        limit = 5
        sleep_length = 2.5

        while count != limit:

            try:
                count += 1
                # Check status is sane before we start
                if job.status != "running" and (not job.status in ['completed', 'killed', 'removed']):
                    job.updateStatus('submitted')
                    job.updateStatus('running')
                if job.status in ['completed', 'killed', 'removed']:
                    break
                DiracBase._internal_job_finalisation(job, updated_dirac_status)
                break

            except GangaDiskSpaceError as err:
                #If the user runs out of disk space for the job to completing so its not monitored any more. Print a helpful message.
                job.force_status('failed')
                raise GangaDiskSpaceError("Cannot finalise job %s. No disk space available! Clear some space and the do j.backend.reset() to try again." % job.getFQID('.'))

            except Exception as err:

                logger.warning("An error occured finalising job: %s" % job.getFQID('.'))
                logger.warning("Attemting again (%s of %s) after %s-sec delay" % (str(count), str(limit), str(sleep_length)))
                if count == limit:
                    logger.error("Unable to finalise job %s after %s retries due to error:\n%s" % (job.getFQID('.'), str(count), str(err)))
                    job.force_status('failed')
                    raise

            time.sleep(sleep_length)

        job.been_queued = False

    @staticmethod
    def finalise_jobs(allJobs, downloadSandbox = True):
        """
        Finalise the jobs given. This downloads the output sandboxes, gets the final Dirac stati, completion times etc.
        Everything is done in one DIRAC process for maximum speed. This is also done in parallel for maximum speed.
        """
        theseJobs = []

        for sj in allJobs:
            if stripProxy(sj).status in ['completing', 'failed', 'killed', 'removed']:
                theseJobs.append(sj)
            else:
                logger.warning("Job %s cannot be finalised" % stripProxy(sj).getFQID())
                
        if len(theseJobs) == 0:
            logger.warning("No jobs from the list are ready to be finalised yet. Be more patient.")
            return

        #First grab all the info from Dirac
        inputDict = {}
        #I have to reduce the no. of subjobs per process to prevent DIRAC timeouts
        nPerProcess = int(math.floor(configDirac['maxSubjobsFinalisationPerProcess']))
        nProcessToUse = math.ceil((len(theseJobs)*1.0)/nPerProcess)

        jobs = [stripProxy(j) for j in theseJobs]

        for i in range(0,int(nProcessToUse)):
            jobSlice = jobs[i*nPerProcess:(i+1)*nPerProcess]      
            getQueues()._monitoring_threadpool.add_function(DiracBase.finalise_jobs_thread_func, (jobSlice, downloadSandbox))

    @staticmethod
    @require_disk_space
    def finalise_jobs_thread_func(jobSlice, downloadSandbox = True):
        """
        Finalise the jobs given. This downloads the output sandboxes, gets the final Dirac statuses, completion times etc.
        Everything is done in one DIRAC process for maximum speed. This is also done in parallel for maximum speed.
        """
        inputDict = {}

        for sj in jobSlice:
            inputDict[sj.backend.id] = sj.getOutputWorkspace().getPath()
        statusmapping = configDirac['statusmapping']
        returnDict, statusList = execute("finaliseJobs(%s, %s, %s)" % (inputDict, repr(statusmapping), downloadSandbox), cred_req=jobSlice[0].backend.credential_requirements, new_subprocess = True)

        #Cycle over the jobs and store the info
        for sj in jobSlice:
            #Check we are able to get the job status - if not set to failed.
            if sj.backend.id not in statusList['Value'].keys():
                logger.error("Job %s with DIRAC ID %s has been removed from DIRAC. Unable to finalise it." % (sj.getFQID(), sj.backend.id))
                sj.force_status('failed')
                continue
            #If we wanted the sandbox make sure it downloaded OK.
            if downloadSandbox and not returnDict[sj.backend.id]['outSandbox']['OK']:
                logger.error("Output sandbox error for job %s: %s. Unable to finalise it." % (sj.getFQID(), returnDict[sj.backend.id]['outSandbox']['Message']))
                sj.force_status('failed')
                continue
            #Set the CPU time
            sj.backend.normCPUTime = returnDict[sj.backend.id]['cpuTime']

            # Set DiracFile metadata - this circuitous route is copied from the standard so as to be consistent with the rest of the job finalisation code.
            if hasattr(sj.outputfiles, 'get'):
                wildcards = [f.namePattern for f in sj.outputfiles.get(DiracFile) if regex.search(f.namePattern) is not None]
            else:
                wildcards = []

            lfn_store = os.path.join(sj.getOutputWorkspace().getPath(), getConfig('Output')['PostProcessLocationsFileName'])

            # Make the file on disk with a nullop...
            if not os.path.isfile(lfn_store):
                with open(lfn_store, 'w'):
                    pass

            if hasattr(sj.outputfiles, 'get') and sj.outputfiles.get(DiracFile):
                # Now we can iterate over the contents of the file without touching it
                with open(lfn_store, 'ab') as postprocesslocationsfile:
                    if not hasattr(returnDict[sj.backend.id]['outDataInfo'], 'keys'):
                        logger.error("Error understanding OutputDataInfo: %s" % str(returnDict[sj.backend.id]['outDataInfo']))
                        raise GangaDiracError("Error understanding OutputDataInfo: %s" % str(returnDict[sj.backend.id]['outDataInfo']))

                    ## Caution is not clear atm whether this 'Value' is an LHCbism or bug
                    list_of_files = returnDict[sj.backend.id]['outDataInfo'].get('Value', list(returnDict[sj.backend.id]['outDataInfo'].keys()))
                    for file_name in list_of_files:
                        file_name = os.path.basename(file_name)
                        info = returnDict[sj.backend.id]['outDataInfo'].get(file_name)
                        #logger.debug("file_name: %s,\tinfo: %s" % (str(file_name), str(info)))

                        if not hasattr(info, 'get'):
                            logger.error("Error getting OutputDataInfo for: %s" % str(sj.getFQID('.')))
                            logger.error("Please check the Dirac Job still exists or attempt a job.backend.reset() to try again!")
                            logger.error("Err: %s" % str(info))
                            logger.error("file_info_dict: %s" % str(returnDict[sj.backend.id]['outDataInfo']))
                            raise GangaDiracError("Error getting OutputDataInfo")

                        valid_wildcards = [wc for wc in wildcards if fnmatch.fnmatch(file_name, wc)]
                        if not valid_wildcards:
                            valid_wildcards.append('')

                        for wc in valid_wildcards:
                            #logger.debug("wildcard: %s" % str(wc))

                            DiracFileData = 'DiracFile:::%s&&%s->%s:::%s:::%s\n' % (wc,
                                                                                    file_name,
                                                                                    info.get('LFN', 'Error Getting LFN!'),
                                                                                    str(info.get('LOCATIONS', ['NotAvailable'])),
                                                                                    info.get('GUID', 'NotAvailable')
                                                                                    )
                            #logger.debug("DiracFileData: %s" % str(DiracFileData))
                            postprocesslocationsfile.write(DiracFileData)
                            postprocesslocationsfile.flush()

                logger.debug("Written: %s" % open(lfn_store, 'r').readlines())

            #Set the status of the subjob
            sj.updateStatus(statusmapping[statusList['Value'][sj.backend.id]['Status']])

    @staticmethod
    def requeue_dirac_finished_jobs(requeue_jobs, finalised_statuses):
        """
        Method used to requeue jobs whih are in the finalized state of some form, finished/failed/etc
        Args:
            requeue_jobs (list): This is a list of the jobs which are to be requeued to be finalised
            finalised_statuses (dict): Dict of the Dirac statuses vs the Ganga statuses after running
        """

        # requeue existing completed job
        for j in requeue_jobs:
            if j.been_queued:
                continue

            if monitoring_component:
                if monitoring_component.should_stop():
                    break
            # Job has changed underneath us don't attempt to finalize
            if j.backend.status not in finalised_statuses:
                j.been_queued = False
                continue
            if not configDirac['serializeBackend']:
                getQueues()._monitoring_threadpool.add_function(DiracBase.job_finalisation,
                                                           args=(j, finalised_statuses[j.backend.status]),
                                                           priority=5, name="Job %s Finalizing" % j.fqid)
                j.been_queued = True
            else:
                DiracBase.job_finalisation(j, finalised_statuses[j.backend.status])


    @staticmethod
    def monitor_dirac_running_jobs(monitor_jobs, finalised_statuses):
        """
        Method to update the configuration of jobs which are in a submitted/running state in Ganga&Dirac
        Args:
            monitor_jobs (list): Jobs which are to be monitored for their status change
            finalised_statuses (dict): Dict of the Dirac statuses vs the Ganga statuses after running
        """

        # now that can submit in non_blocking mode, can see jobs in submitting
        # that have yet to be assigned an id so ignore them
        # NOT SURE THIS IS VALID NOW BULK SUBMISSION IS GONE
        # EVEN THOUGH COULD ADD queues.add(j.submit) WILL KEEP AN EYE ON IT
        # dirac_job_ids    = [ j.backend.id for j in monitor_jobs if j.backend.id is not None ]
        # Correction this did become a problem for a crashed session during
        # submit, see #104454
        dead_jobs = (j for j in monitor_jobs if j.backend.id is None)
        for d in dead_jobs:
            d.updateStatus('failed')
            if d.master is not None:
                d.master.updateMasterJobStatus()

        ganga_job_status = [j.status for j in monitor_jobs if j.backend.id is not None]
        dirac_job_ids = [j.backend.id for j in monitor_jobs if j.backend.id is not None]

        logger.debug("GangaStatus: %s" % str(ganga_job_status))
        logger.debug("diracJobIDs: %s" % str(dirac_job_ids))

        if not dirac_job_ids:
            ## Nothing to do here stop bugging DIRAC about it!
            ## Everything else beyond here in the function depends on some ids present here, no ids means we can stop.
            return

        statusmapping = configDirac['statusmapping']

        result, bulk_state_result = execute('monitorJobs(%s, %s)' %( repr(dirac_job_ids), repr(statusmapping)), cred_req=monitor_jobs[0].backend.credential_requirements, new_subprocess = True)

        #result = results[0]
        #bulk_state_result = results[1]

        if len(result) != len(ganga_job_status):
            logger.warning('Dirac monitoring failed for %s, result = %s' % (str(dirac_job_ids), str(result)))
            logger.warning("Results: %s" % str(result))
            return

        requeue_job_list = []
        jobStateDict = {}

        jobs_to_update = {}
        master_jobs_to_update = []

        thread_handled_states = ['completed', 'failed']
        for job, state, old_state in zip(monitor_jobs, result, ganga_job_status):
            if monitoring_component and monitoring_component.should_stop():
                    break

            if job.been_queued:
                continue

            job.backend.statusInfo = state[0]
            job.backend.status = state[1]
            job.backend.actualCE = state[2]
            updated_dirac_status = state[3]
            try:
                job.backend.extraInfo = state[4]
            except Exception as err:
                logger.debug("gexception: %s" % str(err))
                pass
            logger.debug('Job status vector  : ' + job.fqid + ' : ' + repr(state))

            if updated_dirac_status not in jobStateDict:
                jobStateDict[updated_dirac_status] = []
            jobStateDict[updated_dirac_status].append(job)

            if job.backend.status in finalised_statuses:
                if job.status != 'running':
                    if job.status in ['completing', 'completed']:
                        continue
                    elif job.status in ['removed', 'killed']:
                        requeue_job_list.append(job)
                    elif (job.master and job.master.status in ['removed', 'killed']):
                        continue  # user changed it under us
                    else:
                        if 'running' not in jobs_to_update:
                            jobs_to_update['running'] = []
                        jobs_to_update['running'].append(job)
                        if job.master:
                            if job.master not in master_jobs_to_update:
                                master_jobs_to_update.append(job.master)
                        requeue_job_list.append(job)

            else:
                if job.status in ['removed', 'killed']:
                    continue
                elif (job.master and job.master.status in ['removed', 'killed']):
                    continue  # user changed it under us
                elif job.status != updated_dirac_status:
                    if job.status in ['completing', 'completed']:
                        # Another thread got there first
                        continue
                    if updated_dirac_status not in jobs_to_update:
                        jobs_to_update[updated_dirac_status] = []
                    jobs_to_update[updated_dirac_status].append(job)
                    if job.master:
                        if job.master not in master_jobs_to_update:
                            master_jobs_to_update.append(job.master)

        DiracBase._bulk_updateStateTime(jobStateDict, bulk_state_result)

        for status in jobs_to_update:
            for job in jobs_to_update[status]:
                job.updateStatus(status, update_master=False)

        for j in master_jobs_to_update:
            j.updateMasterJobStatus()

        DiracBase.requeue_dirac_finished_jobs(requeue_job_list, finalised_statuses)

    @staticmethod
    def updateMonitoringInformation(jobs_):
        """Check the status of jobs and retrieve output sandboxesi
        Args:
            jobs_ (list): List of the appropriate jobs to monitored
        """
        # Only those jobs in 'submitted','running' are passed in here for checking
        # if however they have already completed in Dirac they may have been put on queue
        # for processing from last time. These should be put back on queue without
        # querying dirac again. Their signature is status = running and job.backend.status
        # already set to Done or Failed etc.

        jobs = [stripProxy(j) for j in jobs_]

        # remove from consideration any jobs already in the queue. Checking this non persisted attribute
        # is better than querying the queue as cant tell if a job has just been taken off queue and is being processed
        # also by not being persistent, this attribute automatically allows queued jobs from last session to be considered
        # for requeing
        interesting_jobs = [j for j in jobs if not j.been_queued]
        # status that correspond to a ganga 'completed' or 'failed' (see DiracCommands.status(id))
        # if backend status is these then the job should be on the queue
        finalised_statuses = configDirac['finalised_statuses']

        monitor_jobs = [j for j in interesting_jobs if j.backend.status not in finalised_statuses]
        requeue_jobs = [j for j in interesting_jobs if j.backend.status in finalised_statuses]

        #logger.debug('Interesting jobs: ' + repr([j.fqid for j in interesting_jobs]))
        #logger.debug('Monitor jobs    : ' + repr([j.fqid for j in monitor_jobs]))
        #logger.debug('Requeue jobs    : ' + repr([j.fqid for j in requeue_jobs]))

        try:
            # Split all the monitorable jobs into groups based on the
            # credential used to communicate with DIRAC
            for requeue_jobs_group in group_jobs_by_backend_credential(requeue_jobs):
                DiracBase.requeue_dirac_finished_jobs(requeue_jobs_group, finalised_statuses)
            for monitor_jobs_group in group_jobs_by_backend_credential(monitor_jobs):
                DiracBase.monitor_dirac_running_jobs(monitor_jobs_group, finalised_statuses)
        except GangaDiracError as err:
            logger.warning("Error in Monitoring Loop, jobs on the DIRAC backend may not update")
            logger.debug(err)

def finalise_jobs_func(jobs, getSandbox = True):
    """Finalise the provided set of jobs."""
    DiracBase.finalise_jobs(jobs, getSandbox)

exportToGPI('finalise_jobs', finalise_jobs_func, 'Functions')


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

