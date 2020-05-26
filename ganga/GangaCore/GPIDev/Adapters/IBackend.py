##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IBackend.py,v 1.2 2008-10-02 10:31:05 moscicki Exp $
##########################################################################

from GangaCore.Core.exceptions import GangaKeyError, IncompleteJobSubmissionError
from GangaCore.Core.GangaRepository.SubJobXMLList import SubJobXMLList
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Base.Proxy import stripProxy, isType, getName
from GangaCore.GPIDev.Lib.Dataset import GangaDataset
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.GPIDev.Credentials import credential_store, needed_credentials

import GangaCore.Utility.logging

from GangaCore.Utility.logic import implies

from GangaCore.Core.exceptions import GangaException, IncompleteJobSubmissionError

import os
import itertools
import time
from collections import defaultdict

from GangaCore.Core.GangaThread.WorkerThreads import getQueues
from GangaCore.Utility.Config import getConfig

logger = GangaCore.Utility.logging.getLogger()

class IBackend(GangaObject):

    """
    Base class for all backend objects.

    Backend  classes in  Ganga 4.0.x  have only  the  submit() method.
    This is sufficient to allow emulated bulk submission to function.

    Note  that the master_submit()  method is  always called  by the
    framework unlike the submit()  method, which is only called as
    a  part of  default  implementation of  master_submit(). If  you
    provide a special support  for bulk submission than you should
    either  call submit()  method  explicitly in  the  case of  no
    splitting, or make  the implementation of master_submit() handle
    the non-split jobs as well.

    """

    _schema = Schema(Version(0, 0), {})
    _category = 'backends'
    _hidden = 1

    __slots__ = list()

    # specify how the default implementation of master_prepare() method
    # creates the input sandbox
    _packed_input_sandbox = True

    def __init__(self):
        super(IBackend, self).__init__()

    def setup(self):
        """ This is a hook called for each job when GangaCore.Core services are
        started up. The hook is called before the monitoring subsystem is
        enabled. This hook may be used by some backends to do specialized setup
        (e.g. to open ssh transport pipes of the Remote backend)
        """
        pass

    def _parallel_submit(self, b, sj, sc, master_input_sandbox, fqid, logger):

        try:
            sj.updateStatus('submitting')
            if b.submit(sc, master_input_sandbox):
                sj.info.increment()
                return 1
            else:
                raise IncompleteJobSubmissionError(fqid, 'submission failed')
        except Exception as err:
            logger.error("Parallel Job Submission Failed: %s" % err)
            return 0

    def _successfulSubmit(self, out, sj, incomplete_subjobs):
        if out == 0:
            incomplete_subjobs.append(sj.getFQID('.'))
            sj.updateStatus('new', update_master = False)
        else:
            sj.updateStatus('submitted', update_master = False)

    def master_submit(self, rjobs, subjobconfigs, masterjobconfig, keep_going=False, parallel_submit=False):
        """  Submit   the  master  job  and  all   its  subjobs.   The
        masterjobconfig  is  shared,  individual  subjob  configs  are
        defined  in  subjobconfigs.   Submission  of  individual  jobs
        (not-split) also  always goes via  this method.  In  that case
        the subjobconfigs contains just one element - the job itself.

        The default  implementation of  this method emulates  the bulk
        submission  calling  a submit()  method  on individual  subjob
        objects.  If submission  of any of the subjobs  fails then the
        whole   process  is  aborted   with  IncompleteSubmissionError
        exception. The subjobs which  have already been submitted stay
        submitted.

        The default implementation does not process the masterjobconfig.
        Therefore this method may be overriden in the derived class
        in the following way:

        def master_submit(self,masterjobconfig,subjobconfigs,keep_going):
           ... 
           do_some_processsing_of(masterjobconfig)
           ...
           return IBackend.master_submit(self,subjobconfigs,masterjobconfig,keep_joing)


        Implementation note: we set keep_going to be optional in the
        signature of IBackend.master_submit() to allow the existing
        backend implementations which do not support keep_going=True
        and which at some point may call IBackend.master_submit() to
        work without change. It may sometimes be non-trivial to enable
        support for keep_going=True in some backends, even if the
        finally call IBackend.master_submit(). Therefore it is left to
        the decision of backend developer to explicitly enable the
        support for keep_going flag.

        """
        from GangaCore.Utility.logging import log_user_exception

        logger.debug("SubJobConfigs: %s" % len(subjobconfigs))
        logger.debug("rjobs: %s" % len(rjobs))
        assert(implies(rjobs, len(subjobconfigs) == len(rjobs)))

        incomplete = 0
        incomplete_subjobs = []

        def handleError(x):
            if keep_going:
                incomplete_subjobs.append(fqid)
                return False
            else:
                if incomplete:
                    raise x
                else:
                    return True

        master_input_sandbox = self.master_prepare(masterjobconfig)
        # Shall we submit in parallel
        if parallel_submit:

            from GangaCore.Core.GangaThread.WorkerThreads import getQueues

            threads_before = getQueues().totalNumIntThreads()

            for sc, sj in zip(subjobconfigs, rjobs):

                b = sj.backend

                # Must check for credentials here as we cannot handle missing credentials on Queues by design!
                if hasattr(b, 'credential_requirements') and b.credential_requirements is not None:
                    from GangaCore.GPIDev.Credentials.CredentialStore import credential_store
                    try:
                        cred = credential_store[b.credential_requirements]
                    except GangaKeyError:
                        credential_store.create(b.credential_requirements)

                fqid = sj.getFQID('.')
                # FIXME would be nice to move this to the internal threads not user ones
                getQueues()._monitoring_threadpool.add_function(self._parallel_submit, (b, sj, sc, master_input_sandbox, fqid, logger), callback_func = self._successfulSubmit, callback_args = (sj, incomplete_subjobs))

            def subjob_status_check(rjobs):
                has_submitted = True
                for sj in rjobs:
                    if sj.status not in ["submitted","failed","completed","running","completing"] and sj.getFQID('.') not in incomplete_subjobs:
                        has_submitted = False
                        break
                return has_submitted

            while not subjob_status_check(rjobs):
                import time
                time.sleep(1.)

            if incomplete_subjobs:
                raise IncompleteJobSubmissionError(
                    incomplete_subjobs, 'submission failed for subjobs %s' % incomplete_subjobs)
            return 1

        # Alternatively submit sequentially
        for sc, sj in zip(subjobconfigs, rjobs):

            fqid = sj.getFQID('.')
            logger.info("submitting job %s to %s backend", fqid, getName(sj.backend))
            try:
                b = stripProxy(sj.backend)
                sj.updateStatus('submitting')
                if b.submit(sc, master_input_sandbox):
                    sj.updateStatus('submitted')
                    # sj._commit() # PENDING: TEMPORARY DISABLED
                    incomplete = 1
                    stripProxy(sj.info).increment()
                else:
                    if handleError(IncompleteJobSubmissionError(fqid, 'submission failed')):
                        raise IncompleteJobSubmissionError(fqid, 'submission failed')
            except Exception as x:
                sj.updateStatus('new')
                if isType(x, GangaException):
                    logger.error("%s" % x)
                    log_user_exception(logger, debug=True)
                else:
                    log_user_exception(logger, debug=False)
                raise IncompleteJobSubmissionError(fqid, 'submission failed')

        return 1

    def submit(self, subjobconfig, master_input_sandbox):
        """
        Submit an individual job. Return 1 in case of success.

        master_input_sandbox is a list of file-names which is shared by all subjobs.

        This method  is not  called directly by  the framework.  It is
        only  called by  the default  implementation  of master_submit()
        method.

        Therefore if  the implementation  of master_submit() is  able to
        cope  with submission  of  individual jobs,  then submit()  is
        redundant.

        Transition from Ganga 4.0.x:
         - subjobconfig is equvalent to jobconfig in the older interface.
         - jobid is an obsolte parameter which will be removed in the future

        """
        raise NotImplementedError

    def master_prepare(self, masterjobconfig):
        """ Prepare the master job (shared sandbox files). This method is/should be called by master_submit() exactly once.
        The input sandbox is created according to self._packed_input_sandbox flag (a class attribute)
        """
        from GangaCore.GPIDev.Lib.File.OutputFileManager import getInputFilesPatterns
        from GangaCore.GPIDev.Lib.File.File import File, ShareDir

        job = self.getJobObject()

        create_sandbox = job.createInputSandbox
        if self._packed_input_sandbox:
            create_sandbox = job.createPackedInputSandbox

        if masterjobconfig:
            if hasattr(job.application, 'is_prepared') and isType(job.application.is_prepared, ShareDir):
                sharedir_pred = lambda f: f.name.find(job.application.is_prepared.name) > -1
                sharedir_files = filter(sharedir_pred, masterjobconfig.getSandboxFiles())
                nonsharedir_files = itertools.filterfalse(sharedir_pred, masterjobconfig.getSandboxFiles())
            # ATLAS use bool to bypass the prepare mechanism and some ATLAS
            # apps have no is_prepared
            else:
                sharedir_files = []
                nonsharedir_files = masterjobconfig.getSandboxFiles()
            inputsandbox = create_sandbox(nonsharedir_files, master=True)
            inputsandbox.extend(
                map(lambda f: f.name, sharedir_files))
            return inputsandbox

        (fileNames, tmpDir) = getInputFilesPatterns(job)
        files = map(lambda f: File(f), fileNames)
        if not fileNames:
            # RTHandler is not required to produce masterjobconfig, in that
            # case just use the inputsandbox
            files = job.inputsandbox

        result = create_sandbox(files, master=True)
        if tmpDir is not None:
            import shutil
            # remove temp dir
            if os.path.exists(tmpDir):
                shutil.rmtree(tmpDir)
        return result

    def master_auto_resubmit(self, rjobs):
        """ A hook in case someone wanted to override the auto
        resubmission behaviour. Otherwise, it auto_resubmit is
        equivalent for all practical purposes to a normal resubmit (so
        it automatcially benefits from bulk resubmit if implemented).
        """
        return self.master_resubmit(rjobs)

    def check_auto_resubmit(self):
        """ A hook for the backend to check if this job can be
        automatically resubmitted.
        """
        return True

    def master_resubmit(self, rjobs, backend=None):
        """ Resubmit (previously submitted) job. Configuration phase is skipped.
        Default implementation works is an emulated-bulk operation.
        If you override this method for bulk optimization then make sure that you call updateMasterJobStatus() on the master job,
        so the master job will be monitored by the monitoring loop.
        """
        from GangaCore.Utility.logging import log_user_exception
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
                    if backend is None:
                        result = b.resubmit()
                    else:
                        result = b.resubmit(backend=backend)
                    if result:
                        sj.updateStatus('submitted')
                        # sj._commit() # PENDING: TEMPORARY DISABLED
                        incomplete = 1
                    else:
                        return handleError(IncompleteJobSubmissionError(fqid, 'resubmission failed'))
                except Exception as x:
                    log_user_exception(
                        logger, debug=isType(x, GangaException))
                    return handleError(IncompleteJobSubmissionError(fqid, str(x)))
        finally:
            master = self.getJobObject().master
            if master is not None:
                master.updateMasterJobStatus()
        return 1

    def resubmit(self):
        raise NotImplementedError

    def master_kill(self):
        """ Kill a job and all its subjobs. Return 1 in case of success.

        The default implementation uses the kill() method and emulates
        the bulk  operation on all subjobs.  It tries to  kill as many
        subjobs  as  possible even  if  there  are  failures.  If  the
        operation is incomplete then raise IncompleteKillError().
        """

        job = self.getJobObject()

        r = True

        if len(job.subjobs):
            problems = []
            for s in job.subjobs:
                if s.status in ['submitted', 'running']:
                    if not s.backend.kill():
                        r = False
                        problems.append(s.id)
            if not r:
                from GangaCore.Core.exceptions import IncompleteKillError
                raise IncompleteKillError(
                    'subjobs %s were not killed' % problems)
        else:
            r = job.backend.kill()
        return r

    def kill(self):
        """  Kill a job  (and also  all its  subjobs). This  method is
        never called by the framework directly.  It may only be called
        by the default implementation of master_kill().  """

        raise NotImplementedError

    def peek(self, filename="", command=""):
        """
        Allow viewing of job output files on the backend
        (i.e. while job is in 'running' state)

        Arguments other than self:
        filename : name of file to be viewed
                  => Path specified relative to work directory
        command  : command to be used for file viewing

        Return value: None
        """

        # This is a dummy implementation - it only provides access
        # to files in the job's output directory

        job = self.getJobObject()
        job.peek(filename=os.path.join("..", filename), command=command)
        return None

    def remove(self):
        """When the job is removed then this backend method is called.
        The primary use-case is the Remote (ssh) backend. """
        pass

    def getStateTime(self, status):
        """Get the timestamps for the job's transitions into the 'running' and 'completed' states.
        """
        return None

    def timedetails(self):
        """Returns all available backend specific timestamps.
        """
        pass

    @staticmethod
    def master_updateMonitoringInformation(jobs):
        """ Update monitoring information for  jobs: jobs is a list of
        jobs  in   this  backend  which   require  monitoring  (either
        'submitted' or 'running' state).  The jobs list never contains
        the subjobs.

        The default implementation  iterates  over subjobs and calls
        updateMonitoringInformation().
        """

        from GangaCore.Core import monitoring_component
        was_monitoring_running = monitoring_component and monitoring_component.isEnabled(False)

        logger.debug("Running Monitoring for Jobs: %s" % [j.getFQID('.') for j in jobs])

        ## Only process 10 files from the backend at once
        #blocks_of_size = 10
        poll_config = getConfig('PollThread')
        try:
            blocks_of_size = poll_config['numParallelJobs']
        except Exception as err:
            logger.debug("Problem with PollThread Config, defaulting to block size of 5 in master_updateMon...")
            logger.debug("Error: %s" % err)
            blocks_of_size = 5
        ## Separate different backends implicitly
        simple_jobs = {}

        multiThreadMon = poll_config['enable_multiThreadMon']

        # FIXME Add some check for (sub)jobs which are in a transient state but
        # are not locked by an active session of ganga

        queues = getQueues()

        for j in jobs:
            ## All subjobs should have same backend
            if len(j.subjobs) > 0:
                #logger.info("Looking for sj")
                monitorable_subjob_ids = []

                if isType(j.subjobs, SubJobXMLList):
                    cache = j.subjobs.getAllCachedData()
                    for sj_id in range(0,len(j.subjobs)):
                        if cache[sj_id]['status'] in ['submitted', 'running']:
                            if j.subjobs.isLoaded(sj_id):
                                ## SJ may have changed from cache in memory
                                this_sj = j.subjobs(sj_id)
                                if this_sj.status in ['submitted', 'running']:
                                    monitorable_subjob_ids.append(sj_id)
                            else:
                                monitorable_subjob_ids.append(sj_id)
                else:
                    for sj in j.subjobs:
                        if sj.status in ['submitted', 'running']:
                            monitorable_subjob_ids.append(sj.id)

                #logger.info('Monitoring subjobs: %s', monitorable_subjob_ids)

                if not monitorable_subjob_ids:
                    continue

                #logger.info("Dividing")

                monitorable_blocks = []
                temp_block = []

                for this_sj_id in monitorable_subjob_ids:
                    temp_block.append(this_sj_id)
                    if len(temp_block) == blocks_of_size:
                        monitorable_blocks.append(temp_block)
                        temp_block = []

                if temp_block:
                    monitorable_blocks.append(temp_block)
                    temp_block = []

                for this_block in monitorable_blocks:

                    # If the monitoring function was running at the start of the function but has since stopped, break.
                    if was_monitoring_running and monitoring_component and not monitoring_component.isEnabled(False) or not monitoring_component:
                        break

                    try:
                        subjobs_to_monitor = []
                        for sj_id in this_block:
                            subjobs_to_monitor.append(j.subjobs[sj_id])
                        if multiThreadMon:
                            if queues.totalNumIntThreads() < getConfig("Queues")['NumWorkerThreads']:
                                queues._addSystem(j.backend.updateMonitoringInformation, args=(subjobs_to_monitor,), name="Backend Monitor")
                        else:
                            j.backend.updateMonitoringInformation(subjobs_to_monitor)
                    except Exception as err:
                        logger.error("Monitoring Error: %s" % err)

                j.updateMasterJobStatus()

            else:
                backend_name = getName(j.backend)
                if backend_name not in simple_jobs:
                    simple_jobs[backend_name] = []
                simple_jobs[backend_name].append(j)

        if len(simple_jobs) > 0:
            for this_backend in simple_jobs.keys():
                logger.debug('Monitoring jobs: %s', repr([jj._repr() for jj in simple_jobs[this_backend]]))
                if multiThreadMon:
                    if queues.totalNumIntThreads() < getConfig("Queues")['NumWorkerThreads']:
                        queues._addSystem(stripProxy(simple_jobs[this_backend][0].backend).updateMonitoringInformation,
                                          args=(simple_jobs[this_backend],), name="Backend Monitor")
                else:
                    stripProxy(simple_jobs[this_backend][0].backend).updateMonitoringInformation(simple_jobs[this_backend])

        logger.debug("Finished Monitoring request")

        if not multiThreadMon:
            return

        loop = True
        while loop:
            for stat in queues._monitoring_threadpool.worker_status():
                loop = False;
                if stat[0] is not None and stat[0].startswith("Backend Monitor"):
                    loop = True;
                    break;
            time.sleep(1.)

    @staticmethod
    def updateMonitoringInformation(jobs):
        """ Update monitoring information for individual jobs: jobs is
        a  list which  may contain  subjobs as  well as  the non-split
        jobs.  This method is  never called by the framework directly.
        It  may  only  be  called  by the  default  implementation  of
        master_updateMonitoringInformation().
        """

        raise NotImplementedError


def group_jobs_by_backend_credential(jobs):
    # type: (List[Job]) -> List[List[Job]]
    """
    Split a list of jobs based on the credential required by their backends.

    Any missing or invalid credentials are added to ``needed_credentials``

    Args:
        jobs: a list of ``Job``s

    Returns:
        a list of list of ``Job``s with each sublist sharing a credential
    """
    jobs_by_credential = defaultdict(list)
    for j in jobs:
        try:
            cred_req = j.backend.credential_requirements
            cred = credential_store[cred_req]
            if not cred.is_valid():
                logger.debug('Required credential %s is not valid', cred)
                needed_credentials.add(cred_req)
                continue
            jobs_by_credential[cred].append(j)
        except KeyError:
            logger.debug('Required credential %s is missing', cred_req)
            needed_credentials.add(cred_req)
    return list(jobs_by_credential.values())

