################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Job.py,v 1.1 2008-07-17 16:40:54 moscicki Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Lib.Mergers.Merger import runAutoMerge

import Ganga.Utility.logging
from Ganga.GPIDev.Adapters.IMerger import MergerError
logger = Ganga.Utility.logging.getLogger()

from Ganga.Utility.logging import log_user_exception

from Ganga.Core import GangaException

class JobStatusError(GangaException):
    def __init__(self,*args):
        GangaException.__init__(self,*args)

class JobError(GangaException):
    def __init__(self,what):
        GangaException.__init__(self,what)
        self.what=what
    def __str__(self):
        return "JobError: %s"%str(self.what)
    
class JobInfo(GangaObject):
    ''' Additional job information.
        Partially implemented
    '''    
    _schema = Schema(Version(0,0),{ 'submit_counter' : SimpleItem(defvalue=0,protected=1,doc="job submition/resubmission counter"),
                                    })
    _category = 'jobinfos'
    _name = 'JobInfo'
    
    def __init__(self):
        super(JobInfo, self).__init__()
        #self.submit_counter = 0

    def increment(self):
        self.submit_counter += 1

class Job(GangaObject):
    '''Job is an interface for submision, killing and querying the jobs :-).

    Basic configuration:

    The    "application"    attribute    defines   what    should    be
    run. Applications  may be generic arbitrary  executable scripts or
    complex, predefined objects.

    The  "backend" attribute  defines  where and  how  to run.  Backend
    object  represents  a resource  or  a  batch  system with  various
    configuration parameters.

    Available applications, backends and other job components may be listed
    using the plugins() function. See help on plugins() function.

    The "status"  attribute represents the  state of Ganga  job object.
    It  is automatically  updated  by the  monitoring  loop. Note  that
    typically at the  backends the jobs have their  own, more detailed
    status.    This    information   is   typically    available   via
    "job.backend.status" attribute.

    Bookkeeping and persistency:
    
    Job objects contain basic book-keeping information: "id", "status"
    and  "name".  Job  objects   are  automatically  saved  in  a  job
    repository which may be a  special directory on a local filesystem
    or a remote database.

    Input/output and file workspace:
    
    There   is  an  input/output   directory  called   file  workspace
    associated with each  job ("inputdir" and "outputdir" properties).
    When a  job is submitted, all  input files are copied  to the file
    workspace  to keep  consistency  of  the input  while  the job  is
    running. Ganga then ships all  files in the input workspace to the
    backend systems in a sandbox.

    The   list  of  input   files  is   defined  by   the  application
    (implicitly).  Additional files may be explicitly specified in the
    "inputsandbox" attribute.

    Job splitting:

    The "splitter" attributes defines how a large job may be divided into
    smaller subjobs.  The subjobs are automatically created when the main
    (master) job is submitted. The "subjobs" attribute gives access to
    individual subjobs. The "master" attribute of a subjob points back to the
    master job.

    Merging:

    The "merger" attribute defines how the output of the subjobs may be merged.
    Merging is not perfromed automatically and it is triggered by the merge() method.
        
    Datasets: PENDING
    Datasets are highly application and virtual organisation specific.
    '''
    _schema = Schema(Version(1,6),{ 'inputsandbox' : FileItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File'],sequence=1,doc="list of File objects shipped to the worker node "),
                                    'outputsandbox' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of filenames or patterns shipped from the worker node"),
                                    'info':ComponentItem('jobinfos',defvalue=None,doc='JobInfo '),
                                    'application' : ComponentItem('applications',doc='specification of the application to be executed'),
                                    'backend': ComponentItem('backends',doc='specification of the resources to be used (e.g. batch system)'),
                                    'id' : SimpleItem('',protected=1,comparable=0,doc='unique Ganga job identifier generated automatically'),
                                    'status': SimpleItem('new',protected=1,checkset='_checkset_status',doc='current state of the job, one of "new", "submitted", "running", "completed", "killed", "unknown", "incomplete"'),
                                    'name':SimpleItem('',doc='optional label which may be any combination of ASCII characters',typelist=['str']),
                                    'inputdir':SimpleItem(getter="getStringInputDir",defvalue=None,transient=1,protected=1,comparable=0,load_default=0,optional=1,copyable=0,doc='location of input directory (file workspace)'),
                                    
                                    'outputdir':SimpleItem(getter="getStringOutputDir",defvalue=None,transient=1,protected=1,comparable=0,load_default=0,optional=1,copyable=0,doc='location of output directory (file workspace)'),                                    

                                    'inputdata':ComponentItem('datasets',defvalue=None,typelist=['Ganga.GPIDev.Lib.Dataset.Dataset'],load_default=0,optional=1,doc='dataset definition (typically this is specific either to an application, a site or the virtual organization'),
                                    'outputdata':ComponentItem('datasets',defvalue=None,load_default=0,optional=1,doc='dataset definition (typically this is specific either to an application, a site or the virtual organization'),
                                    'splitter':ComponentItem('splitters',defvalue=None,load_default=0,optional=1,doc='optional splitter'),
                                    'subjobs':ComponentItem('jobs',defvalue=[],sequence=1,protected=1,load_default=0,copyable=0,optional=1,proxy_get="_subjobs_proxy",doc='list of subjobs (if splitting)',summary_print = '_subjobs_summary_print'),
                                    'master':ComponentItem('jobs',getter="_getParent",transient=1,protected=1,load_default=0,defvalue=None,optional=1,copyable=0,comparable=0,doc='master job',visitable=0),
                                    'merger':ComponentItem('mergers',defvalue=None,load_default=0,optional=1,doc='optional output merger'),
                                    'fqid':SimpleItem(getter="getStringFQID",transient=1,protected=1,load_default=0,defvalue=None,optional=1,copyable=0,comparable=0,doc='fully qualified job identifier',visitable=0)
                                    })

    _category = 'jobs'
    _name = 'Job'
    _exportmethods = ['submit','remove','kill', 'resubmit', 'peek','fail' ]

    default_registry = 'native_jobs'

    # preferences for the GUI...
    _GUIPrefs = [ { 'attribute' : 'id' },
                  { 'attribute' : 'status' },
                  { 'attribute' : 'inputsandbox', 'displayLevel' : 1 },
                  { 'attribute' : 'inputdata' },
                  { 'attribute' : 'outputsandbox' } ]


    # TODO: usage of **kwds may be envisaged at this level to optimize the overriding of values, this must be reviewed
    def __init__(self):
        super(Job, self).__init__()
        self._setRegistry(None)

    def _readonly(self):
        return self.status != 'new'

    # status may only be set directly using updateStatus() method
    # only modules comming from Ganga.GPIDev package may change it directly
    def _checkset_status(self,value):
        try:
            id = self.id
        except KeyError:
            id = None
        try:
            oldstat = self.status
        except KeyError:
            oldstat = None
        logger.debug('job %s "%s" setting raw status to "%s"',str(id),str(oldstat),value)
        import inspect,os
        frame = inspect.stack()[2]
        #if not frame[0].f_code.co_name == 'updateStatus' and
        if frame[0].f_code.co_filename.find('/Ganga/GPIDev/')==-1 and frame[0].f_code.co_filename.find('/Ganga/Core/')==-1:
            raise AttributeError('cannot modify job.status directly, use job.updateStatus() method instead...')
        del frame

    class State:
        def __init__(self,state,transition_comment='',hook=None):
            self.state = state
            self.transition_comment = transition_comment
            self.hook = hook

    class Transitions(dict):
        def __init__(self,*states):
            self.states = {}
            for s in states:
                assert(not s.state in self)
                self[s.state] = s
    
    status_graph = {'new' : Transitions(State('submitting','j.submit()'),
                                        State('removed','j.remove()')),
                    'submitting' : Transitions(State('new','submission failed',hook='rollbackToNewState'),
                                               State('submitted',hook='monitorSubmitted_hook'),
                                               State('unknown','forced remove OR remote jobmgr error'),
                                               State('failed','manually forced')),
                    'submitted' : Transitions(State('running'),
                                              State('killed','j.kill()'),
                                              State('unknown','forced remove'),
                                              State('failed', 'j.fail(force=1)'),
                                              State('completing','job output already in outputdir',hook='postprocess_hook'),
                                              State('completed',hook='postprocess_hook'),
                                              State('submitting','j.resubmit(force=1)')),
                    'running' : Transitions(State('completing'),
                                            State('completed','job output already in outputdir',hook='postprocess_hook'),
                                            State('failed','backend reported failure OR j.fail(force=1)'),
                                            State('killed','j.kill()',),
                                            State('unknown','forced remove'),
                                            State('submitting','j.resubmit(force=1)'),
                                            State('submitted','j.resubmit(force=1)')),
                    'completing' : Transitions(State('completed',hook='postprocess_hook'),
                                               State('failed','postprocessing error OR j.fail(force=1)'),
                                               State('unknown','forced remove'),
                                               State('submitting','j.resubmit(force=1)'),
                                               State('submitted','j.resubmit(force=1)')),
                    'killed' : Transitions(State('removed','j.remove()'),
                                           State('failed','j.fail()'),
                                           State('submitting','j.resubmit()'),
                                           State('submitted','j.resubmit()')),
                    'failed' : Transitions(State('removed','j.remove()'),
                                           State('submitting','j.resubmit()'),
                                           State('submitted','j.resubmit()')),
                    'completed' : Transitions(State('removed','j.remove()'),
                                              State('failed','j.fail()'),
                                              State('submitting','j.resubmit()'),
                                              State('submitted','j.resubmit()')),
                    'incomplete' : Transitions(State('removed','j.remove()')),
                    'unknown' : Transitions(State('removed','forced remove')),
                    'template': Transitions(State('removed'))
                    }
    
    transient_states = ['incomplete','removed','unknown']
    initial_states = ['new','incomplete','template']

    def updateStatus(self,newstatus, transition_update = True):
        """ Move job to the new status. according to the state transition graph (Job.status_graph).
        If transition is allowed:
          - call the specific transition hook (if exists)
          - call the general transition update hook (triggers the auto merger and application hooks)
          - commit the job
        If job cannot be commited, then revert to old status and raise JobStatusError.
        If transition is not allowed then raise JobStatusError.

        Transitions from to the current state are allowed by default (so you can updateStatus('running') if job is 'running').
        Such default transitions do not have hooks.
        """
        fqid = self.getFQID('.')
        logger.debug('attempt to change job %s status from "%s" to "%s"',fqid, self.status,newstatus)

        try:
            state = self.status_graph[self.status][newstatus]
        except KeyError:
            # allow default transitions: s->s, no hook
            if newstatus == self.status:
                state = Job.State(newstatus)
            else:
                raise JobStatusError('forbidden status transition of job %s from "%s" to "%s"'%(fqid, self.status,newstatus)) 

        saved_status = self.status

        try:
            if state.hook:
                getattr(self,state.hook)()

            if transition_update:
                #we call this even if there was a hook
                newstatus = self.transition_update(newstatus)	    
    	    self.status = newstatus # move to the new state AFTER hooks are called
            self._commit()
        except Exception,x:
            self.status = saved_status
            log_user_exception()
            raise JobStatusError(x)

        logger.info('job %s status changed to "%s"',fqid,self.status)

    def transition_update(self,new_status):
        """Propagate status transitions""" 
        try:
            runAutoMerge(self, new_status)
        except MergerError:
            #stop recursion
	    new_status = 'failed'
	    self.updateStatus(new_status, transition_update = False)
        
        #Propagate transition updates to applications
        if self.application:
            self.application.transition_update(new_status)
	return new_status

    def updateMasterJobStatus(self):
        """
        Update master job status based on the status of subjobs.
        This is an auxiliary method for implementing bulk subjob monitoring.
        """

        j = self
        stats = [s.status for s in j.subjobs]

        # ignore non-split jobs
        if not stats:
            logger.warning('ignoring master job status updated for job %s (NOT MASTER)',self.getFQID('.'))
            return 

        new_stat = None
        
        for s in ['submitting','submitted','running','failed','killed','completing','completed']:
            if s in stats:
                new_stat = s
                break

        if new_stat == j.status:
            return

        if not new_stat:
            logger.critical('undefined state for job %d, stats=%s',j.id,str(stats))

        j.updateStatus(new_stat)

    def getMonitoringService(self):
        import Ganga.GPIDev.MonitoringServices
        return Ganga.GPIDev.MonitoringServices.getMonitoringObject(self)
        
    def monitorSubmitted_hook(self):
        """Send monitoring information (e.g. Dashboard) at the time of job submission """
        import Ganga.GPIDev.MonitoringServices
        self.getMonitoringService().submit()

        #logger.debug('monitorSubmitted_hook: %s',str(x))
    
    def postprocess_hook(self):
        self.application.postprocess()

    def _auto__init__(self,registry=None):
        if registry is None:
            from Ganga.GPIDev.Lib.JobRegistry import JobRegistryDev
            registry = JobRegistryDev.allJobRegistries[self.default_registry]

        # register the job (it will also commit it)
        # job gets its id now
        #self._setRegistry(registry)
        registry._add(self)
        self._init_workspace()
        self._setDirty(1)
        
    def _init_workspace(self):
        #self.inputdir = self.getInputWorkspace().getPath()
        #self.outputdir = self.getOutputWorkspace().getPath()
        pass

    def getWorkspace(self,what,create=True):
        import Ganga.Core.FileWorkspace
        Workspace = getattr(Ganga.Core.FileWorkspace,what)
        w = Workspace()
        import os
        w.jobid = self.getFQID(os.sep)
        if create:
            w.create(w.jobid)
        return w

    def createPackedInputSandbox(self, files, master=False):
        """ Create a packed input sandbox which contains files (a list of File or FileBuffer objects).
        'master' flag is used to make a difference between the master and shared input sandbox which is important
        if the job is not split (subjob and masterjob are the same object)
        """

        import Ganga.Core.Sandbox as Sandbox
        name = '_input_sandbox_'+self.getFQID('_')+'%s.tgz'
        if master:
            name = name % "_master"
        else:
            name = name % ""
            
        return Sandbox.createPackedInputSandbox(files,self.getInputWorkspace(),name)

    def createInputSandbox(self, files, master=False):
        """ Create an unpacked input sandbox which contains files (a list of File or FileBuffer objects).
        'master' flag is not used in this case, and it provided only for uniformity with createPackedInputSandbox() method
        """

        import Ganga.Core.Sandbox as Sandbox
        return Sandbox.createInputSandbox(files,self.getInputWorkspace())

    def getStringFQID(self):
        return self.getFQID('.')

    def getStringInputDir(self):
        return self.getInputWorkspace(create=self.status != 'removed').getPath()

    def getStringOutputDir(self):
        return self.getOutputWorkspace(create=self.status != 'removed').getPath()    
    
    def getFQID(self,sep=None):
        """Return a fully qualified job id (within registry): a list of ids [masterjob_id,subjob_id,...]
        If optional sep is specified FQID string is returned, ids are separated by sep.
        For example: getFQID('.') will return 'masterjob_id.subjob_id....'
        """

        fqid = [self.id]
        cur = self._getParent() # FIXME: or use master attribute?
        while cur:
            fqid.append(cur.id)
            cur = cur._getParent()
        fqid.reverse()

        if sep:
            return sep.join([str(id) for id in fqid])
        return fqid
               
    def getInputWorkspace(self,create=True):
        return self.getWorkspace('InputWorkspace',create=create)

    def getOutputWorkspace(self,create=True):
        return self.getWorkspace('OutputWorkspace',create=create)
    
    def _setRegistry(self, registry):
        #print "Setting registry for ",self.getFQID('.')
        self._registry = registry
        # set the registry in the subjobs as well (bugfix 23737)
        for s in self.subjobs:
            s._setRegistry(registry)

    def _getRegistry(self):
        return self._registry

    def __getstate__(self):
        dict = super(Job, self).__getstate__()
        #FIXME: dict['_data']['id'] = 0 # -> replaced by 'copyable' mechanism in base class
        dict['_registry'] = None
        return dict

    def peek( self, filename = "", command = "" ):
        '''
        Allow viewing of job output (and input) files

        Arguments other than self:
        filename : name of file to be viewed
                   => For backends where this is enabled, the filename
                      for a job in the "running" state is relative to
                      the job's work directory unless the filename begins
                      with "../".  In all other cases, the filename is
                      relative to the job's output directory
        command  : command to be used for viewing the file
                   => If no command is given, then the command defined in
                      the [File_Associations] section of the Ganga
                      configuration file(s) is used

        Example usage:

           # examine contents of output/work directory
           peek()

           # examine contents of output directory,
           # even in case of job in "running" state
           peek( "../output" )

           # examine contents of input directory
           peek( "../input" )

           # View ROOT histograms, running root.exe in a new terminal window
           peek( "histograms.root", "root.exe &&" )

           # View contents of file in output/work directory, using
           # command defined in configuration file
           peek( "output.txt" )

           # View ROOT histograms in ouput/work directory,
           # running root.exe in a new terminal window
           peek( "histograms.root", "root.exe &&" )
                      
        Return value: None
        '''

        import os
        pathStart = filename.split( os.sep )[ 0 ]
        if ( ( "running" == self.status ) and ( pathStart != ".." ) ):
            self.backend.peek( filename = filename, command = command )
        else:
           topdir = os.path.dirname( self.inputdir.rstrip( os.sep ) )
           path = os.path.join( topdir, "output", filename ).rstrip( os.sep )
           self.viewFile( path = path, command = command )
        return None

    def viewFile( self, path = "", command = "" ):
        '''
        Allow file viewing

        Arguments other than self:
        path    : path to file to be viewed
        command  : command to be used for viewing the file
                   => If no command is given, then the command defined in
                      the [File_Associations] section of the Ganga
                      configuration file(s) is used

        This is intended as a helper function for the peek() method.

        Return value: None
        '''

        import os
        from Ganga.Utility.Config import ConfigError, getConfig
        from exceptions import IndexError
        config = getConfig( "File_Associations" )
        if os.path.exists( path ):
           if os.path.islink( path ):
              path = os.readlink( path )
           if not command:
              if os.path.isdir( path ):
                 command = config[ "listing_command" ]
              else:
                 suffix = os.path.splitext( path )[ 1 ].lstrip( "." )
                 try:
                    command = config[ suffix ]
                 except ConfigError:
                    command = config[ "fallback_command" ]

           mode = os.P_WAIT

           try:
              tmpList = command.split( "&&" )
              termCommand = tmpList[ 1 ]
              if not termCommand:
                 termCommand = config[ "newterm_command" ]
              exeopt = config[ "newterm_exeopt" ]
              exeCommand = " ".join( [ tmpList[ 0 ], path ] )
              argList = [ termCommand, exeopt, exeCommand ]
              mode = os.P_NOWAIT
           except IndexError:
              tmpList = command.split( "&" )
              if ( len( tmpList ) > 1 ):
                 mode = os.P_NOWAIT
                 command = tmpList[ 0 ]
              argList = command.split()
              argList.append( path )

           cmd = argList[ 0 ]
           os.spawnvp( mode, cmd, argList )
        else:
           logger.warning( "File/directory '%s' not found" % path )

        return None

    def submit(self,keep_going=False):
        '''Submits a job. Return true on success.

        First  the  application   is  configured  which  may  generate
        additional  input files,  preprocess  executable scripts  etc.
        Then  backend handler is  used to  submit the  configured job.
        The job is automatically checkpointed to persistent storage.

        For split jobs: consult https://twiki.cern.ch/twiki/bin/view/ArdaGrid/GangaSplitters#Subjob_submission
        '''

        from Ganga.Core import ApplicationConfigurationError, JobManagerError, IncompleteJobSubmissionError, GangaException

        # make sure nobody writes to the cache during this operation
        #job._registry.cache_writers_mutex.lock()

        if keep_going:
            msg = 'job.submit(keep_going=True) is not implemented yet.'
            logger.warning(msg)
            raise JobError(msg)

        # can only submit jobs in a 'new' state
        if self.status != 'new':
            msg = "cannot submit job %s which is in '%s' state"%(self.getFQID('.'),self.status)
            logger.warning(msg)
            raise JobError(msg)

        assert(self.subjobs == [])
        
        if self.master is not None:
            msg = "Cannot submit subjobs directly."
            logger.warning(msg)
            raise JobError(msg)

        # select the runtime handler
        from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
        try:
            rtHandler = allHandlers.get(self.application._name,self.backend._name)()
        except KeyError,x:
            msg = 'runtime handler not found for application=%s and backend=%s'%(self.application._name,self.backend._name)
            logger.warning(msg)
            raise JobError(msg)

        try:
            logger.info("submitting job %d",self.id)
            # prevent other sessions from submitting this job concurrently
            self.status = 'submitting'
            try:
                self._commit()
            except Exception,x:
                msg = 'cannot commit the job %s, submission aborted'%self.id
                logger.warning(msg)
                self.status = 'new'
                raise JobError(msg)

            appmasterconfig = self.application.master_configure()[1] # FIXME: obsoleted "modified" flag
            # split into subjobs
#            try:
            if 1:
                if self.splitter:
                    subjobs = self.splitter.validatedSplit(self)
                    #print "*"*80
                    #import sys
                    #subjobs[0].printTree(sys.stdout)

                    self.subjobs = subjobs
                    registry = self._getRegistry()
                    registry.repository.registerJobs(self.subjobs, self)
                    for j in self.subjobs:
                        j._setRegistry(registry)
                        j.status='new'
                        j._init_workspace()                        

                    rjobs = self.subjobs
                    self._commit(self.subjobs)
                else:
                    rjobs = [self]

            # configure the application of each subjob
            appsubconfig = [ j.application.configure(appmasterconfig)[1] for j in rjobs ] #FIXME: obsoleted "modified" flag
            appconfig = (appmasterconfig,appsubconfig)
            
            # prepare the master job with the runtime handler
            jobmasterconfig = rtHandler.master_prepare(self.application,appmasterconfig)

            # prepare the subjobs with the runtime handler
            jobsubconfig = [ rtHandler.prepare(j.application,s,appmasterconfig,jobmasterconfig) for (j,s) in zip(rjobs,appsubconfig) ]

            # submit the job
            try:
                if not self.backend.master_submit(rjobs,jobsubconfig,jobmasterconfig):
                    raise JobManagerError('error during submit')
            except IncompleteJobSubmissionError,x:
                logger.warning('Not all subjobs have been sucessfully submitted: %s',x)
	    self.info.increment()
            self.status = 'submitted' # FIXME: if job is not split, then default implementation of backend.master_submit already have set status to "submitted"
            self._commit() # make sure that the status change goes to the repository

            return 1
        except Exception,x:
            # revert to the new status
            log_user_exception(logger,debug = isinstance(x,GangaException))
            logger.warning('%s ... reverting job %s to the new status', str(x), self.getFQID('.') )
            self.updateStatus('new')
            raise JobError(x)


    def rollbackToNewState(self):
	''' 
	Rollback the job to the "new" state if submitting of job failed:
	    - cleanup the input and output workspace preserving the top dir(bug ##19434)
	    - cleanup subjobs
	This method is used as a hook for submitting->new transition
	@see updateJobStatus() 
	'''
	self.getInputWorkspace().remove(preserve_top=True)
	self.getOutputWorkspace().remove(preserve_top=True)
        #delete subjobs
        try:
            rep = self._getRegistry().repository
            rep.deleteJobs(map(lambda sj: tuple(sj.getFQID()), self.subjobs))#FIXME: convert to tuple because ARDA JobRepository checks if is tuple
	    #FIXME: this should be fixed when the new function to update to new status in repository is available
        except Exception, x:
            logger.error('Cannot delete subjobs of the job %d while reverting to the previous state (%s)', self.id, str(x))
        else:                                                                         
            self.subjobs = []
    
    def remove(self,force=False):
        '''Remove the job.

        If job  has been submitted try  to kill it  first. Then remove
        the   file   workspace   associated   with   the   job.

        If force=True then remove job without killing it.
        '''
        
        logger.info('removing job %d',self.id)

        template = self.status=='template'
        
        if self.status == 'removed':
            msg = 'job %d already removed'%self.id
            logger.warning(msg)
            raise JobError(msg)

        if self.master:
            msg = 'cannot remove subjob %s'%self.getFQID('.')
            logger.info(msg)
            raise JobError(msg)
        
        if self.status in ['submitted','running']:
            try:
                if not force:
                    self._kill(transition_update=False)
            except GangaException,x:
                log_user_exception(logger,debug = True)
            except Exception,x:
                log_user_exception(logger)                
                logger.warning('unhandled exception in j.kill(), job id=%d',self.id)
            
        if self._registry:
            self._registry._remove_by_object(self,auto_removed=1)

        self._data['status'] = 'removed'

        if not template:
            # remove the corresponding workspace files

            # FIXME: this is a hack to remove the entire job directory, should be properly solved with the self.workspace()
            from Ganga.Core.FileWorkspace import InputWorkspace
            wsp = InputWorkspace()
            wsp.subpath=''
            wsp.jobid = self.id

            def doit(f):
                try:
                    f()
                except OSError,x:
                    logger.warning('cannot remove file workspace associated with the job %d : %s',self.id,str(x))

            doit(wsp.remove)


    def fail(self,force=False):
        ''' Force job to enter the failed state. This may be used for marking jobs "bad" jobs or jobs which are stuck in one of the internal ganga
        states (e.g. completing).
        '''
        if not force:
            if self.status in ['submitted','running']:
                try:
                    self._kill(transition_update=False)
                except JobError,x:
                    x.what += "Use fail(force=True) to ignore kill errors."
                    raise x
        try:
            self.updateStatus('failed')
        except JobStatusError,x:
            logger.warning(x)
            raise x

    def kill(self):
        '''Kill the job. Raise JobError exception on error.
        '''
        self._kill(transition_update=True)
        
    def _kill(self,transition_update):
        '''Private helper. Kill the job. Raise JobError exception on error.
        '''
        try:
            from Ganga.Core import GangaException
            
            # make sure nobody writes to the cache during this operation
            #job._registry.cache_writers_mutex.lock()

            fqid = self.getFQID('.')
            logger.info('killing job %s',fqid)
            if not self.status in ['submitted','running']:
                msg = "cannot kill job which is in '%s' state. "%self.status
                logger.warning(msg)
                raise JobError(msg)
            try:
                if self.backend.master_kill():
                    self.updateStatus('killed',transition_update=transition_update)
                    self._commit()
                    return True
                else:
                    msg = "backend.master_kill() returned False"
                    raise JobError(msg)
            except GangaException,x:
                msg = "failed to kill job %s: %s"%(fqid,str(x))
                logger.warning(msg)
                raise JobError(msg)
        finally:
            pass #job._registry.cache_writers_mutex.release()

    def resubmit(self):
        """Resubmit a failed or completed job. """

        # there are possible two race condition which must be handled somehow:
        #  - a simple job is monitorable and at the same time it is 'resubmitted' - potentially the monitoring may update the status back!
        #  - same for the master job...

        from Ganga.Core import GangaException, IncompleteJobSubmissionError, JobManagerError
             
        fqid = self.getFQID('.')
        logger.info('resubmitting job %s',fqid)

        if not self.status in ['completed','failed','killed']:
            logger.warning("cannot resubmit job %s which is in '%s' state",fqid,self.status)
            return 0

        oldstatus = self.status

        self.status = 'submitting'

        try:
            self._commit()
        except Exception,x:
            logger.warning('cannot commit the job %s, resubmission aborted',fqid)
            self.status = oldstatus
            return 0

        try:
            rjobs = self.subjobs
            if not rjobs:
                rjobs = [self]

            try:
                if not self.backend.master_resubmit(rjobs):
                    raise JobManagerError('error during submit')
            except IncompleteJobSubmissionError,x:
                logger.warning('Not all subjobs of job %s have been sucessfully re-submitted: %s',fqid,x)
                
	    self.info.increment()
            self.status = 'submitted' # FIXME: if job is not split, then default implementation of backend.master_submit already have set status to "submitted"
            self._commit() # make sure that the status change goes to the repository
            return True
        except GangaException,x:
            logger.warning("failed to resubmit job, %s" % (str(x),))
            logger.warning('reverting job %s to the %s status', fqid, oldstatus )
            self.status = oldstatus
            self._commit() #PENDING: what to do if this fails?
            return False

    def _commit(self,objects=None):
        """ Helper method to unconditionally commit to the repository. The 'objects' list specifies objects
        to be commited (for example the subjobs). If objects are not specified then just the self is commited """

        if objects is None:
            objects = [self]
        self._getRegistry()._flush(objects)
        

    def _attribute_filter__set__(self,n,v):
        # a workaround for bug #8111
##        if n == 'name':
##            if len(v)>0 and not v.isalnum():
##                raise ValueError('%s: the job name may contain only numbers and letters (a temporary workaround for bug #8111)'%v)
            
        return v

    def _repr(self):
        if self.id is None:
            id = "None"
        else:
            id = self.getFQID('.')
            #id = self.fully_qualified_id()
            #if len(id)==1: id = id[0]
            #id = str(id)
            #id = id.replace(' ','')
        return "%s#%s"%(str(self.__class__.__name__),id)

##     def fully_qualified_id(j):
##         index = []
##         while j:
##             index.append(j.id)
##             j = j.master
##         index.reverse()
##         return tuple(index)
        
    def merge(self, sum_outputdir = None , subjobs = None, **options):
        '''Merge the output of subjobs.

        By default merge all subjobs into the master outputdir.
        The output location and the list of subjobs may be overriden.
        The options (keyword arguments) are passed onto the specific merger implementation.
        Refer to the specific merger documentation for more information about available options.
        '''

        if sum_outputdir is None:
            sum_outputdir = self.outputdir

        if subjobs is None:
            subjobs = self.subjobs

        try:
            if self.merger:
                self.merger.merge(sum_outputdir, subjobs, **options)
            else:
                logger.warning('Cannot merge job %d: merger is not defined'%self.id)
        except Exception,x:
            log_user_exception()
            raise
            
    def _subjobs_proxy(self):
        from Ganga.GPIDev.Lib.JobRegistry.JobRegistryDev import JobRegistryInstanceInterface
        from Ganga.GPIDev.Lib.JobRegistry.JobRegistry import _wrap
        subjobs = JobRegistryInstanceInterface('jobs(%d).subjobs'%self.id)
        for j in self.subjobs:
            subjobs.jobs[j.id] = j
        #print 'return slice',subjobs
        return _wrap(subjobs)

    def _subjobs_summary_print(self,value,verbosity_level):
        rslice = self._subjobs_proxy()
        return rslice._display(1)
        
    
class JobTemplate(Job):
    """A placeholder for Job configuration parameters.

    JobTemplates are normal Job objects but they are never submitted. They have their own JobRegistry, so they do not get mixed up with
    normal jobs. They have always a "template" status.

    Create a job with an existing job template t:
    
         j = Job(t)

    Save a job j as a template t:

         t = JobTemplate(j)

    You may save commonly used job parameters in a template and create new jobs easier and faster.
    """
    _category = 'jobs'
    _name = 'JobTemplate'

    _schema = Job._schema.inherit_copy()
    
    default_registry = 'native_templates'
    
    def __init__(self):
        super(JobTemplate, self).__init__()

    def _readonly(self):
        return 0
    
    # FIXME: for the moment you have to explicitly define all methods if you want to export them...
    def remove(self):
        '''See Job for documentation.'''
        return super(JobTemplate,self).remove()

    def submit(self):
        """ Templates may not be submitted, return false."""
        return super(JobTemplate,self).submit()

    def kill(self):
        """ Templates may not be killed, return false."""
        return super(JobTemplate,self).kill()
    

#
#
# $Log: not supported by cvs2svn $
# Revision 1.62.4.17  2008/04/21 08:46:51  wreece
# Imports missing symbol. test Ganga/test/Bugs/Savannah28511 now passes
#
# Revision 1.62.4.16  2008/04/18 13:42:02  moscicki
# remove obsolete printout
#
# Revision 1.62.4.15  2008/04/18 07:06:24  moscicki
# bugfix 13404  (reintroduced in 5)
#
# Revision 1.62.4.14  2008/04/02 11:29:35  moscicki
# inputdir and outputdir are not stored persistently anymore but are calculated wrt to the current workspace configuration
#
# this makes it easier to relocate local repository and, in the future, to implement local workspace cache for remote repository/workspace
#
# Revision 1.62.4.13  2008/03/17 19:38:40  roma
# bug fix #28511
#
# Revision 1.62.4.12  2008/03/06 14:10:35  moscicki
# added warning if fail() fails
#
# Revision 1.62.4.11  2008/03/04 10:23:43  amuraru
# fixed rollbackToNewStatu
#
# Revision 1.62.4.10  2008/03/03 14:57:06  amuraru
# fixed the state transition when merger fails
#
# Revision 1.62.4.9  2008/02/29 10:27:21  moscicki
# fixes in Job._kill() method
#
# added fail() and remove() method in GPI slices (not all keywords implemented yet)
#
# Revision 1.62.4.8  2008/02/28 15:48:11  moscicki
# cannot submit subjobs directly
#
# Revision 1.62.4.7  2008/02/28 13:07:37  moscicki
# added fail() and improved behaviour of kill()
# improved logging messages
# removed race condition in status update hook
#
# Revision 1.62.4.6  2007/12/19 16:27:17  moscicki
# remove(): ignore kill failures
# fixed messaging on kill
#
# Revision 1.62.4.5  2007/12/18 09:06:42  moscicki
# integrated typesystem from Alvin
#
# Revision 1.62.4.4  2007/12/10 17:29:01  amuraru
# merged changes from Ganga 4.4.4
#
# Revision 1.62.4.3  2007/11/13 18:37:26  wreece
# Merges head change in Job with branch. Fixes warnings in Mergers. Merges MergerTests with head. Adds new test to GangaList. Fixes config problems in Root.
#
# Revision 1.62.4.2  2007/11/08 11:56:44  moscicki
# pretty print for subjobs added
#
# Revision 1.62.4.1  2007/11/07 17:02:12  moscicki
# merged against Ganga-4-4-0-dev-branch-kuba-slices with a lot of manual merging
#
# Revision 1.65  2007/12/04 14:11:02  moscicki
# added submitted->submitting transition to fix problems with master job status in case of subjob resubmission
#
# Revision 1.64  2007/10/19 15:24:40  amuraru
# allow *->submitting transition in order to support resubmission (hurng-chun)
#
# Revision 1.63  2007/10/09 13:05:17  moscicki
# JobInfo object from Vladimir
# Savannah #30045 bugfix from Will (merger recursive updateStatus() call)
#
# Revision 1.62  2007/09/12 16:40:46  amuraru
# use the internal logger when in log_user_exception
#
# Revision 1.61  2007/07/27 15:13:38  moscicki
# merged the monitoring services branch from kuba
#
# Revision 1.60  2007/07/27 13:52:00  moscicki
# merger updates from Will (from GangaMergers-1-0 branch)
#
# Revision 1.59  2007/07/10 13:08:30  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.58.2.1  2007/05/14 13:32:11  wreece
# Adds the merger related code on a seperate branch. All tests currently
# run successfully.
#
# Revision 1.58  2007/05/11 13:26:05  moscicki
# fix in the state transition to support:
#
# temporary functions to help getting jobs out of completing and submitting states:
# force_job_completed(j): may be applied to completing jobs
# force_job_failed(j): may be applied to submitting or completing jobs
#
# Revision 1.57.4.2  2007/06/18 14:35:20  moscicki
# mergefrom_HEAD_18Jun07
#
# Revision 1.57.4.1  2007/06/18 10:16:34  moscicki
# slices prototype
#
# Revision 1.58  2007/05/11 13:26:05  moscicki
# fix in the state transition to support:
#
# temporary functions to help getting jobs out of completing and submitting states:
# force_job_completed(j): may be applied to completing jobs
# force_job_failed(j): may be applied to submitting or completing jobs
#
# Revision 1.57  2007/04/20 17:29:34  moscicki
# re-enabled the removal of subjobs table in the repository on job submission failure (revert to new status)
#
# Revision 1.56  2007/03/29 16:59:20  moscicki
# exception handling fixed
#
# Revision 1.55  2007/03/26 16:14:36  moscicki
#  - changed formatting of exception messages
#  - fix removing the subjobs and reseting the counter when the split job submission fails (Ganga/test/GPI/CrashMultipleSubmitSubjobs.gpi) - TO BE CHECKED!
#
# Revision 1.54  2007/02/28 18:16:56  moscicki
# support for generic: self.application.postprocess()
#
# removed JobManager from resubmit() and kill()
#
# Revision 1.53  2007/02/22 13:46:10  moscicki
# simplification of the internal code: removal of JobManager and ApplicationManager
#
# bugfix 23737
#
# Revision 1.52  2007/01/25 16:18:21  moscicki
# mergefrom_Ganga-4-2-2-bugfix-branch_25Jan07 (GangaBase-4-14)
#
# Revision 1.51  2006/10/26 16:27:24  moscicki
# explicit subjob support (Alexander Soroko)
#
# Revision 1.50.2.3  2006/12/14 18:21:16  kuba
# monitoring hook for job.submit()
#
# Revision 1.50.2.2  2006/11/24 14:31:39  amuraru
# implementation of peek() function
#
# Revision 1.50.2.1  2006/11/02 09:27:10  amuraru
# Fixed [bug #21225]
#
# Revision 1.50  2006/10/22 22:31:54  adim
# allow manual subjob submmission (relaxed the checks in Job.submit() changing the assertion with a warning)
#
# Revision 1.49  2006/10/19 12:34:18  adim
# *** empty log message ***
#
# Revision 1.48  2006/10/19 12:33:35  adim
# *** empty log message ***
#
# Revision 1.47  2006/10/19 12:05:32  adim
# allow manual subjob submmission (relaxed the checks in Job.submit() changing the assertion with a warning)
#
# Revision 1.46  2006/10/03 10:41:27  moscicki
# log user exceptions in job state transitions
#
# Revision 1.45  2006/10/02 14:48:49  moscicki
# make a difference between master and sub packed input sandbox in case there is not splitting (they were overriding each other)
#
# Revision 1.44  2006/09/19 09:40:33  adim
# Bug fix #17080
#
# Revision 1.43  2006/09/08 13:03:08  adim
# Fixed Bug#19434
# Added a rollback hook for the submitting->new transition (activated when
# submission fails) which cleans up the Input and Output workspaces.
#
# Revision 1.42  2006/08/29 12:54:02  moscicki
# trivial fix
#
# Revision 1.41  2006/08/29 12:03:26  moscicki
# - createInputSandbox()
# - fixes in resubmit()
#
# Revision 1.40  2006/08/24 16:58:26  moscicki
# added createPackedInputSandbox()
#
# Revision 1.39  2006/08/11 13:35:10  moscicki
# preliminary resubmit implementation
#
# Revision 1.38  2006/08/02 08:28:29  moscicki
# id of subjobs starts with 0 not 1
#
# Revision 1.37  2006/08/01 09:28:57  moscicki
# updated state list
#
# Revision 1.36  2006/07/31 12:15:43  moscicki
# updateMasterJobStatus() helper method for bulk subjob monitoring
# more transitions from submitted added
#
# Revision 1.35  2006/07/28 15:01:32  moscicki
# small bugfix
#
# Revision 1.34  2006/07/28 12:52:37  moscicki
# default self transitions enabled (s->s)
# improved FQID (removed broken caching)
# use FQID for logging messages
# comments
#
# Revision 1.33  2006/07/27 20:13:46  moscicki
#  - JobStatusError
#  - added simple job state machine
#  - status has "checkset" metaproperty
#  - updateStatus()
#  - postprocessing_hook()
#  - getInputWorkspace(), getOutputWorkspace()
#  - getFQID()
#  - changed subjob ids (short numbers)
#  - fixed commit (suboptimal)
#  - changes to exception handling
#
# Revision 1.32  2006/07/10 13:24:41  moscicki
# changes from Johannes: outputdata handling
#
# exception fixes...
#
# Revision 1.31  2006/06/13 12:34:29  moscicki
#   _category = "mergers" # plural -> following other category names
#
#  Job.merge(self, sum_outputdir = None , subjobs = None, **options)
#   -> see the docstring for more details (logfile is passed in options)
#
# Revision 1.30  2006/06/13 08:50:04  moscicki
# Added merger
#
# Revision 1.29  2006/06/09 14:31:51  moscicki
# exception fix
#
# Revision 1.28  2006/02/13 15:19:14  moscicki
# support for two-phase confguration (...,master config, splitting, sub config,...)
#
# Revision 1.27  2006/02/10 14:23:13  moscicki
# fixed: bug #14524 overview: jobs[ id ].remove() doesn't delete top level job directory
#
# Revision 1.26  2005/12/08 12:01:03  moscicki
# _init_workspace() method (this is a temporary name)
# inputdir/outputdir of subjobs now point to the real directories (TODO: which are still not in a correct place in the filesystem)
#
# Revision 1.25  2005/12/02 15:30:35  moscicki
# schema changes: master, subjobs, splitter properties
# splitting support
# customizable _repr() method
#
# Revision 1.24  2005/11/14 10:34:16  moscicki
# added running state, GUI prefs
#
#
# Revision 1.23  2005/10/21 13:17:56  moscicki
# bufix #12475 (killing job in a running state)
#
# Revision 1.22.2.1  2005/11/04 11:40:12  ctan
# *** empty log message ***
#
# Revision 1.22  2005/09/23 09:30:17  moscicki
# minor
#
# Revision 1.21  2005/09/19 10:57:31  asaroka
# Restriction on job name format is removed as redundant.
#
# Revision 1.20  2005/08/29 10:01:36  moscicki
# added docs
#
# Revision 1.19  2005/08/26 09:55:49  moscicki
# outputsandbox property, many comments added
#
# Revision 1.18  2005/08/24 15:33:50  moscicki
# added docstrings
#
# Revision 1.17  2005/08/23 17:09:27  moscicki
# minor changes
#
#
#
