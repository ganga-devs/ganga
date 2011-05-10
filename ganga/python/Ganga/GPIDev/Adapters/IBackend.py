################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IBackend.py,v 1.2 2008-10-02 10:31:05 moscicki Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import os

import datetime
import time

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

    _schema =  Schema(Version(0,0), {})
    _category = 'backends'
    _hidden = 1

    # specify how the default implementation of master_prepare() method creates the input sandbox
    _packed_input_sandbox = True

    def __init__(self):
        super(IBackend,self).__init__()
##         import sys
##         frame = sys._getframe(1)
##         logger = Ganga.Utility.logging.getLogger(frame=frame)
##         del frame
##         from Ganga.Utility.util import GenericWrapper, wrap_callable_filter
##         def before(args,kwargs):
##             args[0] = 'hello'+args[0]
##             return args,kwargs
##         def after():
##             pass
##         self.logger = GenericWrapper(logger,before,after, forced = ['info','error','warning','critical', 'debug'], wrapper_function=wrap_callable_filter)


    def setup(self):
        """ This is a hook called for each job when Ganga.Core services are
        started up. The hook is called before the monitoring subsystem is
        enabled. This hook may be used by some backends to do specialized setup
        (e.g. to open ssh transport pipes of the Remote backend)
        """
        pass

    def master_submit(self,rjobs,subjobconfigs,masterjobconfig,keep_going=False):

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
        from Ganga.Core import IncompleteJobSubmissionError, GangaException
        from Ganga.Utility.logging import log_user_exception
        
        job = self.getJobObject()
        assert(implies(rjobs,len(subjobconfigs)==len(rjobs)))

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

        master_input_sandbox=self.master_prepare(masterjobconfig)

        for sc,sj in zip(subjobconfigs,rjobs):
            fqid = sj.getFQID('.')
            logger.info("submitting job %s to %s backend",fqid,sj.backend._name)
            try:
                b = sj.backend
                sj.updateStatus('submitting')
                if b.submit(sc,master_input_sandbox):
                    sj.updateStatus('submitted')
                    #sj._commit() # PENDING: TEMPORARY DISABLED
                    incomplete = 1
                else:
                    if handleError(IncompleteJobSubmissionError(fqid,'submission failed')):
                        return 0
            except Exception,x:
                sj.updateStatus('new')
                if isinstance(x,GangaException):
                    logger.error(str(x))
                    log_user_exception(logger,debug = True)
                else:
                    log_user_exception(logger,debug = False)
                if handleError(IncompleteJobSubmissionError(fqid,str(x))):
                    return 0
                
        if incomplete_subjobs:
            raise IncompleteJobSubmissionError(incomplete_subjobs,'submission failed')

        return 1

    def submit(self,subjobconfig,master_input_sandbox):
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


    def master_prepare(self,masterjobconfig):
        """ Prepare the master job (shared sandbox files). This method is/should be called by master_submit() exactly once.
        The input sandbox is created according to self._packed_input_sandbox flag (a class attribute)
        """
        
        job = self.getJobObject()
        files = []
        if masterjobconfig:
            files = masterjobconfig.getSandboxFiles() # FIXME: assume that all jobconfig object have getSandboxFiles() method
        else:
            files = self.getJobObject().inputsandbox # RTHandler is not required to produce masterjobconfig, in that case just use the inputsandbox

        if self._packed_input_sandbox:
            return job.createPackedInputSandbox(files,master=True)
        else:
            return job.createInputSandbox(files,master=True)


    def master_auto_resubmit(self,rjobs):
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
        
    def master_resubmit(self,rjobs,backend=None):
        """ Resubmit (previously submitted) job. Configuration phase is skipped.
        Default implementation works is an emulated-bulk operation.
        If you override this method for bulk optimization then make sure that you call updateMasterJobStatus() on the master job,
        so the master job will be monitored by the monitoring loop.
        """
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
                logger.info("resubmitting job %s to %s backend",fqid,sj.backend._name)
                try:
                    b = sj.backend
                    sj.updateStatus('submitting')
                    if backend is None:
                        result = b.resubmit()
                    else:
                        result = b.resubmit(backend=backend)
                    if result:
                        sj.updateStatus('submitted')
                        #sj._commit() # PENDING: TEMPORARY DISABLED
                        incomplete = 1
                    else:
                        return handleError(IncompleteJobSubmissionError(fqid,'resubmission failed'))
                except Exception,x:
                    log_user_exception(logger,debug=isinstance(x,GangaException))
                    return handleError(IncompleteJobSubmissionError(fqid,str(x)))
        finally:
            master = self.getJobObject().master
            if master:
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
                if s.status in ['submitted','running']:
                    if not s.backend.kill():
                        r = False
                        problems.append(s.id)
            if not r:
                from Ganga.Core import IncompleteKillError
                raise IncompleteKillError('subjobs %s were not killed'%problems)
        else:
            r = job.backend.kill()
        return r
    
    def kill(self):

        """  Kill a job  (and also  all its  subjobs). This  method is
        never called by the framework directly.  It may only be called
        by the default implementation of master_kill().  """
        
        raise NotImplementedError

    def peek( self, filename = "", command = "" ):
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
        job.peek( filename = os.path.join( "..", filename ), command = command )
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
    

    def master_updateMonitoringInformation(jobs):
        
        """ Update monitoring information for  jobs: jobs is a list of
        jobs  in   this  backend  which   require  monitoring  (either
        'submitted' or 'running' state).  The jobs list never contains
        the subjobs.

        The default implementation  iterates  over subjobs and calls
        updateMonitoringInformation().
        """

        simple_jobs = []
        
        for j in jobs:
            if len(j.subjobs):
                monitorable_subjobs = [s for s in j.subjobs if s.status in ['submitted','running'] ]
                logger.debug('Monitoring subjobs: %s',repr([jj._repr() for jj in monitorable_subjobs]))
                j.backend.updateMonitoringInformation(monitorable_subjobs)
                j.updateMasterJobStatus()
            else:
                simple_jobs.append(j)

        if simple_jobs:
            logger.debug('Monitoring jobs: %s',repr([jj._repr() for jj in simple_jobs]))
            simple_jobs[0].backend.updateMonitoringInformation(simple_jobs)

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

        
    def updateMonitoringInformation(jobs):
        """ Update monitoring information for individual jobs: jobs is
        a  list which  may contain  subjobs as  well as  the non-split
        jobs.  This method is  never called by the framework directly.
        It  may  only  be  called  by the  default  implementation  of
        master_updateMonitoringInformation().
        """

        raise NotImplementedError

    updateMonitoringInformation = staticmethod(updateMonitoringInformation)
          
