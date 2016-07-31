#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""The Ganga backendhandler for the Dirac system."""

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import re
import fnmatch
import time
import datetime
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.Core import BackendError, GangaException
from GangaDirac.Lib.Backends.DiracUtils import result_ok, get_job_ident, get_parametric_datasets, outputfiles_iterator, outputfiles_foreach
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.Utilities.DiracUtilities import execute, _proxyValid
from Ganga.Utility.ColourText import getColour
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.GPIDev.Credentials import getCredential
from Ganga.GPIDev.Base.Proxy import stripProxy, isType, getName
from Ganga.Core.GangaThread.WorkerThreads import getQueues
configDirac = getConfig('DIRAC')
logger = getLogger()
regex = re.compile('[*?\[\]]')


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
        'settings': SimpleItem(defvalue={'CPUTime': 2 * 86400},
                               doc='Settings for DIRAC job (e.g. CPUTime, BannedSites, etc.)')
    })
    _exportmethods = ['getOutputData', 'getOutputSandbox', 'removeOutputData',
                      'getOutputDataLFNs', 'peek', 'reset', 'debug']
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

        from Ganga.GPIDev.Lib.Job.Job import Job
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
        result = execute(dirac_cmd)
        # Could use the below code instead to submit on a thread
        # If submitting many then user may terminate ganga before
        # all jobs submitted
#        def submit_checker(result, job, script):
#            err_msg = 'Error submitting job to Dirac: %s' % str(result)
#            if not result_ok(result) or 'Value' not in result:
#                logger.error(err_msg)
#                raise BackendError('Dirac',err_msg)
#
#            idlist = result['Value']
#            if type(idlist) is list:
#                return job._setup_bulk_subjobs(idlist, script)
#            job.id = idlist
#        server.execute_nonblocking(dirac_cmd, callback_func=submit_checker, args=(self, dirac_script))
#        return True

        err_msg = 'Error submitting job to Dirac: %s' % str(result)
        if not result_ok(result) or 'Value' not in result:
            logger.error(err_msg)
            logger.error("\n\n===\n%s\n===\n" % dirac_script)
            logger.error("\n\n====\n")
            with open(dirac_script, 'r') as file_in:
                logger.error("%s" % file_in.read())
            logger.error("\n====\n")
            raise BackendError('Dirac', err_msg)

        idlist = result['Value']
        if type(idlist) is list:
            return self._setup_bulk_subjobs(idlist, dirac_script)

        self.id = idlist
        return type(self.id) == int

    def _addition_sandbox_content(self, subjobconfig):
        '''any additional files that should be sent to dirac
        Args:
            subjobcofig (unknown): This is the config for this subjob (I think)'''
        return []

    def submit(self, subjobconfig, master_input_sandbox):
        """Submit a DIRAC job
        Args:
            subjobconfig (unknown):
            master_input_sandbox (list): file names which are in the master sandbox of the master sandbox (if any)
        """
        j = self.getJobObject()

        sboxname = j.createPackedInputSandbox(subjobconfig.getSandboxFiles())

        input_sandbox = master_input_sandbox[:]
        input_sandbox += sboxname

        input_sandbox += self._addition_sandbox_content(subjobconfig)

        ## Add LFN to the inputfiles section of the file
        input_sandbox_userFiles = []
        for this_file in j.inputfiles:
            if isType(this_file, DiracFile):
                input_sandbox_userFiles.append('LFN:'+str(this_file.lfn))
        if j.master:
            for this_file in j.master.inputfiles:
                if isType(this_file, DiracFile):
                    input_sandbox_userFiles.append('LFN:'+str(this_file.lfn))

        for this_file in input_sandbox_userFiles:
            input_sandbox.append(this_file)

        logger.debug("dirac_script: %s" % str(subjobconfig.getExeString()))
        logger.debug("sandbox_cont:\n%s" % str(input_sandbox))

        dirac_script = subjobconfig.getExeString().replace('##INPUT_SANDBOX##', str(input_sandbox))

        dirac_script_filename = os.path.join(j.getInputWorkspace().getPath(), 'dirac-script.py')
        f = open(dirac_script_filename, 'w')
        f.write(dirac_script)
        f.close()
        return self._common_submit(dirac_script_filename)

    def master_auto_resubmit(self, rjobs):
        '''Duplicate of the IBackend.master_resubmit but hooked into auto resubmission
        such that the monitoring server is used rather than the user server
        Args:
            rjobs (list): This is a list of jobs which are to be auto-resubmitted'''
        from Ganga.Core import IncompleteJobSubmissionError, GangaException
        from Ganga.Utility.logging import log_user_exception
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
                    result = b._resubmit()
                    if result:
                        sj.updateStatus('submitted')
                        # sj._commit() # PENDING: TEMPORARY DISABLED
                        incomplete = 1
                    else:
                        return handleError(IncompleteJobSubmissionError(fqid, 'resubmission failed'))
                except Exception as x:
                    log_user_exception(logger, debug=isType(x, GangaException))
                    return handleError(IncompleteJobSubmissionError(fqid, str(x)))
        finally:
            master = self.getJobObject().master
            if master:
                master.updateMasterJobStatus()
        return 1

    def resubmit(self):
        """Resubmit a DIRAC job"""
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
        for key, value in self.settings.iteritems():
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

    def kill(self):
        """ Kill a Dirac jobs"""
        if not self.id:
            return None
        dirac_cmd = 'kill(%d)' % self.id
        result = execute(dirac_cmd)
        if not result_ok(result):
            raise BackendError('Dirac', 'Could not kill job: %s' % str(result))
        return result['OK']

    def peek(self, filename=None, command=None):
        """Peek at the output of a job (Note: filename/command are ignored).
        Args:
            filename (str): Ignored but is filename of a file in the sandbox
            command (str): Ignored but is a command which could be executed"""
        dirac_cmd = 'peek(%d)' % self.id
        result = execute(dirac_cmd)
        if result_ok(result):
            logger.info(result['Value'])
        else:
            logger.error("No peeking available for Dirac job '%i'.", self.id)

    def getOutputSandbox(self, outputDir=None):
        """Get the outputsandbox for the job object controlling this backend
        Args:
            outputDir (str): This string represents the output dir where the sandbox is to be placed
        """
        j = self.getJobObject()
        if outputDir is None:
            outputDir = j.getOutputWorkspace().getPath()
        dirac_cmd = "getOutputSandbox(%d,'%s')"  % (self.id, outputDir)
        result = execute(dirac_cmd)
        if not result_ok(result):
            msg = 'Problem retrieving output: %s' % str(result)
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
        if j.subjobs:
            for sj in j.subjobs:
                outputfiles_foreach(sj, DiracFile, lambda x: x.remove())
        else:
            outputfiles_foreach(j, DiracFile, lambda x: x.remove())

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
            raise GangaException("Designated outupt path '%s' must exist and be a directory" % outputDir)

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
                if isType(dirac_file, DiracFile):
                    dirac_file.get(localPath=dirac_file.localDir)
                else:
                    dirac_file.get()
                return dirac_file.lfn
            # should really make the get method throw if doesn't suceed. todo
            except GangaException as e:
                logger.warning(e)

        suceeded = []
        if j.subjobs:
            for sj in j.subjobs:
                suceeded.extend([download(f, sj, True) for f in outputfiles_iterator(sj, DiracFile) if f.lfn != '' and (names is None or f.namePattern in names)])
        else:
            suceeded.extend([download(f, j, False) for f in outputfiles_iterator(j, DiracFile) if f.lfn != '' and (names is None or f.namePattern in names)])

        return filter(lambda x: x is not None, suceeded)

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

    def debug(self):
        '''Obtains some (possibly) useful DIRAC debug info. '''
        # check services
        cmd = 'getServicePorts()'
        result = execute(cmd)
        if type(result) == str:
            try:
                result = eval(result)
            except Exception as err:
                logger.debug("Exception, err: %s" % str(err))
                pass
        if not result_ok(result):
            logger.warning('Could not obtain services: %s' % str(result))
            return
        services = result.get('Value', {})
        for category in services:
            system, service = category.split('/')
            cmd = "ping('%s','%s')" % (system, service)
            result = execute(cmd)
            if type(result) == str:
                try:
                    result = eval(result)
                except Exception as err:
                    logger.debug("Exception: %s" % str(err))
                    pass
            msg = 'OK.'
            if not result_ok(result):
                msg = '%s' % result['Message']
            logger.info('%s: %s' % (category, msg))
        # get pilot info for this job
        if type(self.id) != int:
            return
        j = self.getJobObject()
        cwd = os.getcwd()
        debug_dir = j.getDebugWorkspace().getPath()
        cmd = "getJobPilotOutput(%d,'%s')" % \
              (self.id, debug_dir)
        result = execute(cmd)
        if result_ok(result):
            logger.info('Pilot Info: %s/pilot_%d/std.out.' %
                        (debug_dir, self.id))
        else:
            logger.error(result.get('Message', ''))

    @staticmethod
    def _bulk_updateStateTime(jobStateDict, bulk_time_lookup={} ):
        """ This performs the same as the _getStateTime method but loops over a list of job ids within the DIRAC namespace (much faster)
        Args:
            jobStateDict (dict): This is a dict of {job.backend.id : job_status, } elements
            bulk_time_lookup (dict): Dict of result of multiple calls to getBulkStateTime, performed in advance
        """
        for this_state, these_jobs in jobStateDict.iteritems():
            if bulk_time_lookup == {} or this_state not in bulk_time_lookup:
                bulk_result = execute("getBulkStateTime(%s,\'%s\')" %
                                        (repr([j.backend.id for j in these_jobs]), this_state))
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
                                be_statetime = execute("getStateTime(%d,\'%s\')" % (job.backend.id, childstatus))
                            job.time.timestamps["backend_final"] = be_statetime
                            logger.debug("Wrote 'backend_final' to timestamps.")
                            break
                        else:
                            time_str = "backend_" + childstatus
                            if time_str not in job.time.timestamps:
                                if childstatus in getStateTimeResult:
                                    be_statetime = getStateTimeResult[childstatus]
                                else:
                                    be_statetime = execute("getStateTime(%d,\'%s\')" % (job.backend.id, childstatus))
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
        return execute(dirac_cmd)

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
    def _internal_job_finalisation(job, updated_dirac_status):
        """
        This method performs the main job finalisation
        Args:
            job (Job): Thi is the job we want to finalise
            updated_dirac_status (str): String representing the Ganga finalisation state of the job failed/completed
        """

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

            logger.info('Contacting DIRAC for job: %s' % job.fqid)
            # Contact dirac which knows about the job
            job.backend.normCPUTime, getSandboxResult, file_info_dict, completeTimeResult = execute("finished_job(%d, '%s')" % (job.backend.id, output_path))

            now = time.time()
            logger.info('%0.2fs taken to download output from DIRAC for Job %s' % ((now - start), job.fqid))

            #logger.info('Job ' + job.fqid + ' OutputDataInfo: ' + str(file_info_dict))
            #logger.info('Job ' + job.fqid + ' OutputSandbox: ' + str(getSandboxResult))
            #logger.info('Job ' + job.fqid + ' normCPUTime: ' + str(job.backend.normCPUTime))

            # Set DiracFile metadata
            wildcards = [f.namePattern for f in job.outputfiles.get(DiracFile) if regex.search(f.namePattern) is not None]

            lfn_store = os.path.join(output_path, getConfig('Output')['PostProcessLocationsFileName'])

            # Make the file on disk with a nullop...
            if not os.path.isfile(lfn_store):
                with open(lfn_store, 'w'):
                    pass

            if job.outputfiles.get(DiracFile):

                # Now we can iterate over the contents of the file without touching it
                with open(lfn_store, 'ab') as postprocesslocationsfile:
                    if not hasattr(file_info_dict, 'keys'):
                        logger.error("Error understanding OutputDataInfo: %s" % str(file_info_dict))
                        from Ganga.Core.exceptions import GangaException
                        raise GangaException("Error understanding OutputDataInfo: %s" % str(file_info_dict))

                    ## Caution is not clear atm whether this 'Value' is an LHCbism or bug
                    list_of_files = file_info_dict.get('Value', file_info_dict.keys())

                    for file_name in list_of_files:
                        file_name = os.path.basename(file_name)
                        info = file_info_dict.get(file_name)
                        #logger.debug("file_name: %s,\tinfo: %s" % (str(file_name), str(info)))

                        if not hasattr(info, 'get'):
                            logger.error("Error getting OutputDataInfo for: %s" % str(job.getFQID('.')))
                            logger.error("Please check the Dirac Job still exists or attempt a job.backend.reset() to try again!")
                            logger.error("Err: %s" % str(info))
                            logger.error("file_info_dict: %s" % str(file_info_dict))
                            from Ganga.Core.exceptions import GangaException
                            raise GangaException("Error getting OutputDataInfo")

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

            # check outputsandbox downloaded correctly
            if not result_ok(getSandboxResult):
                logger.warning('Problem retrieving outputsandbox: %s' % str(getSandboxResult))
                DiracBase._getStateTime(job, 'failed')
                if job.status in ['removed', 'killed']:
                    return
                elif (job.master and job.master.status in ['removed', 'killed']):
                    return  # user changed it under us
                job.updateStatus('failed')
                if job.master:
                    job.master.updateMasterJobStatus()
                raise BackendError('Problem retrieving outputsandbox: %s' % str(getSandboxResult))

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
                execute("getOutputSandbox(%d,'%s')" % (job.backend.id, job.getOutputWorkspace().getPath()))
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
                # make sure proxy is valid
                if DiracBase.checkDiracProxy():
                    # perform finalisation
                    DiracBase._internal_job_finalisation(job, updated_dirac_status)
                else:
                    # exit gracefully
                    logger.warning("Cannot process job: %s. DIRAC monitoring has been disabled. To activate your grid proxy type: \'gridProxy.renew()\'" % job.fqid)
                break
            except Exception as err:

                logger.warning("An error occured finalising job: %s" % job.getFQID('.'))
                logger.warning("Attemting again (%s of %s) after %s-sec delay" % (str(count), str(limit), str(sleep_length)))
                if count == limit:
                    logger.error("Unable to finalise job after %s retries due to error:\n%s" % (job.getFQID('.'), str(err)))
                    job.force_status('failed')
                    raise

            time.sleep(sleep_length)

        job.been_queued = False

    @staticmethod
    def requeue_dirac_finished_jobs(requeue_jobs, finalised_statuses):
        """
        Method used to requeue jobs whih are in the finalized state of some form, finished/failed/etc
        Args:
            requeue_jobs (list): This is a list of the jobs which are to be requeued to be finalised
            finalised_statuses (dict): Dict of the Dirac statuses vs the Ganga statuses after running
        """

        from Ganga.Core import monitoring_component

        # requeue existing completed job
        for j in requeue_jobs:
            if j.been_queued:
                continue

            if monitoring_component:
                if monitoring_component.should_stop():
                    break
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

        result, bulk_state_result = execute('monitorJobs(%s, %s)' %( repr(dirac_job_ids), repr(statusmapping)))

        if not DiracBase.checkDiracProxy():
            return

        #result = results[0]
        #bulk_state_result = results[1]

        if len(result) != len(ganga_job_status):
            logger.warning('Dirac monitoring failed for %s, result = %s' % (str(dirac_job_ids), str(result)))
            logger.warning("Results: %s" % str(results))
            return

        from Ganga.Core import monitoring_component

        requeue_job_list = []
        jobStateDict = {}

        jobs_to_update = {}
        master_jobs_to_update = []

        thread_handled_states = ['completed', 'failed']
        for job, state, old_state in zip(monitor_jobs, result, ganga_job_status):
            if monitoring_component:
                if monitoring_component.should_stop():
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
                    if job.status in ['removed', 'killed']:
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
                if (job.master and job.master.status in ['removed', 'killed']):
                    continue  # user changed it under us
                if job.status != updated_dirac_status:
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
    def checkDiracProxy():
        # make sure proxy is valid
        if not _proxyValid(shouldRenew = False, shouldRaise = False):
            if DiracBase.dirac_monitoring_is_active is True:
                logger.warning('DIRAC monitoring inactive (no valid proxy found).')
                logger.warning('Type: \'gridProxy.renew()\' to (re-)activate')
            DiracBase.dirac_monitoring_is_active = False
        else:
            DiracBase.dirac_monitoring_is_active = True
        return DiracBase.dirac_monitoring_is_active

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

        # make sure proxy is valid
        if not DiracBase.checkDiracProxy():
            return

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

        DiracBase.requeue_dirac_finished_jobs(requeue_jobs, finalised_statuses)
        DiracBase.monitor_dirac_running_jobs(monitor_jobs, finalised_statuses)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

