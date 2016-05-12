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
        return None

    def _setup_bulk_subjobs(self, dirac_ids, dirac_script):
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
        master_job._commit()
        return True

    def _common_submit(self, dirac_script):
        '''Submit the job via the Dirac server.'''
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
        '''any additional files that should be sent to dirac'''
        return []

    def submit(self, subjobconfig, master_input_sandbox):
        """Submit a DIRAC job"""
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
        such that the monitoring server is used rather than the user server'''
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
        monitoring will run on it again."""
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
        """Peek at the output of a job (Note: filename/command are ignored)."""
        dirac_cmd = 'peek(%d)' % self.id
        result = execute(dirac_cmd)
        if result_ok(result):
            logger.info(result['Value'])
        else:
            logger.error("No peeking available for Dirac job '%i'.", self.id)

    def getOutputSandbox(self, outputDir=None):
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
    def _getStateTime(job, status):
        """Returns the timestamps for 'running' or 'completed' by extracting
        their equivalent timestamps from the loggingInfo."""
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
                            be_statetime = execute("getStateTime(%d,\'%s\')" % (job.backend.id, childstatus))
                            job.time.timestamps["backend_final"] = be_statetime
                            logger.debug("Wrote 'backend_final' to timestamps.")
                            break
                        else:
                            time_str = "backend_" + childstatus
                            if time_str not in job.time.timestamps:
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

    def job_finalisation_cleanup(job, updated_dirac_status):

        logger = getLogger()

        #   Revert job back to running state if we exit uncleanly
        if job.status == "completing":
            job.updateStatus('running')
            if job.master:
                job.master.updateMasterJobStatus()
        # FIXME should I add something here to cleanup on sandboxes pulled from
        # malformed job output?

    @staticmethod
    def _internal_job_finalisation(job, updated_dirac_status):

        logger = getLogger()

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

            # Contact dirac which knows about the job
            job.backend.normCPUTime, getSandboxResult, file_info_dict = execute("finished_job(%d, '%s')" % (job.backend.id, output_path))

            now = time.time()
            logger.debug('Job ' + job.fqid + ' Time for Dirac metadata : ' + str(now - start))

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

            # Now we can iterate over the contents of the file without touchin it
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
            DiracBase._getStateTime(job, 'completed')
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
            logger.error("Unexpected dirac status '%s' encountered" % updated_dirac_status)

    @staticmethod
    def job_finalisation(job, updated_dirac_status):

        count = 1
        limit = 5
        sleep_length = 2.5

        while count != limit:

            try:
                count += 1
                if job.status != "running" and (not job.status in ['completed', 'killed', 'removed']):
                    job.updateStatus('submitted')
                    job.updateStatus('running')
                if job.status in ['completed', 'killed', 'removed']:
                    break
                DiracBase._internal_job_finalisation(job, updated_dirac_status)
                break
            except Exception as err:

                logger.warning("An error occured finalising job: %s" % job.getFQID('.'))
                logger.warning("Attemting again (%s of %s) after %s-sec delay" % (str(count), str(limit), str(sleep_length)))
                if count == limit:
                    logger.error("Unable to finalise job after %s retries due to error:\n%s" % (job.getFQID('.'), str(err)))
                    job.force_status('failed')
                    raise err

            time.sleep(sleep_length)

        job.been_queued = False

    @staticmethod
    def updateMonitoringInformation(_jobs):
        """Check the status of jobs and retrieve output sandboxes"""
        # Only those jobs in 'submitted','running' are passed in here for checking
        # if however they have already completed in Dirac they may have been put on queue
        # for processing from last time. These should be put back on queue without
        # querying dirac again. Their signature is status = running and job.backend.status
        # already set to Done or Failed etc.

        jobs = [stripProxy(j) for j in _jobs]

        logger = getLogger()

        # make sure proxy is valid
        if not _proxyValid():
            if DiracBase.dirac_monitoring_is_active:
                logger.warning('DIRAC monitoring inactive (no valid proxy found).')
                DiracBase.dirac_monitoring_is_active = False
            return
        else:
            DiracBase.dirac_monitoring_is_active = True

        # remove from consideration any jobs already in the queue. Checking this non persisted attribute
        # is better than querying the queue as cant tell if a job has just been taken off queue and is being processed
        # also by not being persistent, this attribute automatically allows queued jobs from last session to be considered
        # for requeing
        interesting_jobs = [j for j in jobs if not j.been_queued]
        # status that correspond to a ganga 'completed' or 'failed' (see DiracCommands.status(id))
        # if backend status is these then the job should be on the queue
        queueable_dirac_statuses = configDirac['queueable_dirac_statuses']

        monitor_jobs = [j for j in interesting_jobs if j.backend.status not in queueable_dirac_statuses]
        requeue_jobs = [j for j in interesting_jobs if j.backend.status in queueable_dirac_statuses]

        logger.debug('Interesting jobs: ' + repr([j.fqid for j in interesting_jobs]))
        logger.debug('Monitor jobs    : ' + repr([j.fqid for j in monitor_jobs]))
        logger.debug('Requeue jobs    : ' + repr([j.fqid for j in requeue_jobs]))

        from Ganga.Core.GangaThread.WorkerThreads import getQueues

        from Ganga.Core import monitoring_component

        # requeue existing completed job
        for j in requeue_jobs:
            if j.been_queued:
                continue

            if monitoring_component:
                if monitoring_component.should_stop():
                    break
            getQueues()._monitoring_threadpool.add_function(DiracBase.job_finalisation,
                                                       args=(j, queueable_dirac_statuses[j.backend.status]),
                                                       priority=5, name="Job %s Finalizing" % j.fqid)
            j.been_queued = True

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

        result = execute('status(%s, %s)' %( str(dirac_job_ids), repr(statusmapping)))

        if len(result) != len(ganga_job_status):
            logger.warning('Dirac monitoring failed for %s, result = %s' % (
                str(dirac_job_ids), str(result)))
            return


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
                logger.debug("gxception: %s" % str(err))
                pass
            logger.debug('Job status vector  : ' + job.fqid + ' : ' + repr(state))

            # Is this really catching a real problem?
            if job.status != old_state:
                logger.warning('User changed Ganga job status from %s -> %s' % (str(old_state), job.status))
                continue
            ####################

            if updated_dirac_status == job.status:
                continue

            if updated_dirac_status in thread_handled_states:
                if job.status != 'running':
                    DiracBase._getStateTime(job, 'running')
                    if job.status in ['removed', 'killed']:
                        continue
                    if (job.master and job.master.status in ['removed', 'killed']):
                        continue  # user changed it under us
                    job.updateStatus('running')
                    if job.master:
                        job.master.updateMasterJobStatus()

                if job.been_queued:
                    continue

                getQueues()._monitoring_threadpool.add_function(DiracBase.job_finalisation,
                                                           args=(job, updated_dirac_status),
                                                           priority=5, name="Job %s Finalizing" % job.fqid)
                job.been_queued = True

            else:
                DiracBase._getStateTime(job, updated_dirac_status)
                if job.status in ['removed', 'killed']:
                    continue
                if (job.master and job.master.status in ['removed', 'killed']):
                    continue  # user changed it under us
                job.updateStatus(updated_dirac_status)
                if job.master:
                    job.master.updateMasterJobStatus()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

