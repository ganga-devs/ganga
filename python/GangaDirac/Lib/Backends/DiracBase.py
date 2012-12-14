#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""The Ganga backendhandler for the Dirac system."""

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.Core import BackendError,GangaException
from DiracUtils import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.util import unique
import GangaDirac.Lib.Server.DiracServer as DiracServer
import GangaDirac.Lib.Files.DiracFile as DiracFile
from GangaDirac.Lib.Server.DiracClient import DiracClient
from Ganga.Core.GangaThread import GangaThread
import os
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Core.exceptions import GangaException
logger = getLogger()


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
    
    user_server_pid, user_server_port = \
                     DiracServer.start_server(env_file            = Ganga.Utility.Config.getConfig('DIRAC')['DiracEnvFile'],
                                              port_min            = Ganga.Utility.Config.getConfig('DIRAC')['ServerPortMin'],
                                              port_max            = Ganga.Utility.Config.getConfig('DIRAC')['ServerPortMax'],
                                              command_files       = Ganga.Utility.Config.getConfig('DIRAC')['DiracCommandFiles'],
                                              command_timeout     = Ganga.Utility.Config.getConfig('DIRAC')['Timeout'],
                                              num_worker_threads  = Ganga.Utility.Config.getConfig('DIRAC')['NumWorkerThreads'],
                                              end_data_str        = Ganga.Utility.Config.getConfig('DIRAC')['EndDataString'],
                                              server_shutdown_str = Ganga.Utility.Config.getConfig('DIRAC')['ServerShutdownString'],
                                              poll_delay          = Ganga.Utility.Config.getConfig('DIRAC')['StartUpWaitTime'],
                                              show_dirac_output   = Ganga.Utility.Config.getConfig('DIRAC')['ShowDIRACstdout'])
    monitoring_server_pid, monitoring_server_port = \
                           DiracServer.start_server(env_file            = Ganga.Utility.Config.getConfig('DIRAC')['DiracEnvFile'],
                                                    port_min            = Ganga.Utility.Config.getConfig('DIRAC')['ServerPortMin'],
                                                    port_max            = Ganga.Utility.Config.getConfig('DIRAC')['ServerPortMax'],
                                                    command_files       = Ganga.Utility.Config.getConfig('DIRAC')['DiracCommandFiles'],
                                                    command_timeout     = Ganga.Utility.Config.getConfig('DIRAC')['Timeout'],
                                                    num_worker_threads  = Ganga.Utility.Config.getConfig('DIRAC')['NumWorkerThreads'],
                                                    end_data_str        = Ganga.Utility.Config.getConfig('DIRAC')['EndDataString'],
                                                    server_shutdown_str = Ganga.Utility.Config.getConfig('DIRAC')['ServerShutdownString'],
                                                    poll_delay          = Ganga.Utility.Config.getConfig('DIRAC')['StartUpWaitTime'],
                                                    show_dirac_output   = Ganga.Utility.Config.getConfig('DIRAC')['ShowDIRACstdout'])
    dirac_ganga_server = DiracClient(port                = user_server_port,
                                     num_worker_threads  = Ganga.Utility.Config.getConfig('DIRAC')['NumWorkerThreads'],
                                     end_data_str        = Ganga.Utility.Config.getConfig('DIRAC')['EndDataString'],
                                     server_shutdown_str = Ganga.Utility.Config.getConfig('DIRAC')['ServerShutdownString'])
    dirac_monitoring_server = DiracClient(port                = monitoring_server_port,
                                          num_worker_threads  = Ganga.Utility.Config.getConfig('DIRAC')['NumWorkerThreads'],
                                          end_data_str        = Ganga.Utility.Config.getConfig('DIRAC')['EndDataString'],
                                          server_shutdown_str = Ganga.Utility.Config.getConfig('DIRAC')['ServerShutdownString'])

    if user_server_pid is None:
        raise GangaException('Failed to set up the user DIRAC server on any of the given port')
    if monitoring_server_pid is None:
        raise GangaException('Failed to set up the monitoring DIRAC server on any of the given port')
        
    dirac_monitoring_is_active = True
    
    _schema = Schema(Version(3, 2),{
        'id' : SimpleItem(defvalue=None, protected=1, copyable=0,
                          typelist=['int','type(None)'],
                          doc='The id number assigned to the job by the DIRAC WMS. If seeking help'\
                          ' on jobs with the Dirac backend, please always report this id ' \
                          'number in addition to a full description of your problem. The id '\
                          'can also be used to further inspect the job at ' \
                          'https://lhcbweb.pic.es/DIRAC/info/general/diracOverview'),
        'status' : SimpleItem(defvalue=None, protected=1, copyable=0,
                              typelist=['str','type(None)'],
                              doc='The detailed status as reported by the DIRAC WMS'),
        'actualCE' : SimpleItem(defvalue=None, protected=1, copyable=0,
                                typelist=['str','type(None)'],
                                doc='The location where the job ran'),
        'normCPUTime' : SimpleItem(defvalue=None, protected=1, copyable=0,
                                   typelist=['str','type(None)'],
                                   doc='The normalized CPU time reported by the DIRAC WMS'),
        'statusInfo' : SimpleItem(defvalue='', protected=1, copyable=0,
                                  typelist=['str','type(None)'],
                                  doc='Minor status information from Dirac'),
        'diracOpts' : SimpleItem(defvalue='',
                                 doc='DIRAC API commands to add the job definition script. Only edit ' \
                                 'if you *really* know what you are doing'),
        'settings' : SimpleItem(defvalue={'CPUTime':2*86400},
                                doc='Settings for DIRAC job (e.g. CPUTime, BannedSites, etc.)')
        })
    _exportmethods = ['getOutputData','getOutputSandbox',
                      'getOutputDataLFNs','peek','reset','debug']
    _packed_input_sandbox = True
    _category = "backends"
    _name = 'DiracBase'
    _hidden = True
    
    def master_prepare(self, masterjobconfig):
        def filt(sharedsandbox):
            if sharedsandbox:
                def shareboxfilter(item):
                    return item.name.find('_input_sandbox_'+self.getJobObject().application.is_prepared.name) != -1
                return shareboxfilter
            
            def nonshareboxfilter(item):
                return item.name.find('_input_sandbox_'+self.getJobObject().application.is_prepared.name) == -1
            return nonshareboxfilter
        
        
        if masterjobconfig:
            inputsandbox  = [f.name for f in filter(filt(True) , masterjobconfig.getSandboxFiles())]
            sjc = StandardJobConfig(inputbox=filter(filt(False), masterjobconfig.getSandboxFiles()))
            if sjc.getSandboxFiles():
                inputsandbox += super(DiracBase,self).master_prepare(sjc)
            return inputsandbox
        return []#super(DiracBase,self).master_prepare(masterjobconfig)


## OLD way might not have worked if package made API script 
##   used job.setPara... or parrot.setPara... instead of j.setPara...
##   now in dracutils.py
##     def __get_parametric_datasets(self, dirac_script_lines):
##         parametric_key = 'setParametricInputData('
##         def parametric_input_filter(API_line):
##             return API_line.find(parametric_key) >= 0
##             #return API_line.find('setParametricInputData(') >= 0

##         #f=open(dirac_script_filename,'r')
##         #script_lines = f.readlines()
##         #f.close()
##         parametric_line = filter(parametric_input_filter, dirac_script_lines)
##         parametric_data_start = parametric_line.find(parametric_key) + len(parametric_key)
##         if len(parametric_line) is 0:
##             raise BackendError('DiracBase','No "setParametricInputData()" lines in dirac API')
##         if len(parametric_line) is > 1:
##             raise BackendError('DiracBase','Multiple "setParametricInputData()" lines in dirac API')
##         return eval(parametric_line[0][parametric_data_start:-1])#-1 removes the trailing )
        

    def _setup_subjob_dataset(self, dataset):
        return None
    
    def _setup_bulk_subjobs(self, dirac_ids, dirac_script):
        f = open(dirac_script,'r')
        parametric_datasets = get_parametric_datasets(f.read().split('\n'))
        f.close()
        if len(parametric_datasets) != len(dirac_ids):
            raise BackendError('Dirac','Missmatch between number of datasets defines in dirac API script and those returned by DIRAC')
            
        from Ganga.GPIDev.Lib.Job.Job import Job
        master_job=self.getJobObject()
        for i in range(len(dirac_ids)):
            j=Job()
            j.copyFrom(master_job)
            j.splitter = None
#            j.merger = None
            j.backend.id = dirac_ids[i]
            j.id = i
            j.inputdata = self._setup_subjob_dataset(parametric_datasets[i])
            j.status = 'submitted'
            j.time.timenow('submitted')
            master_job.subjobs.append(j)
        master_job._commit()
        return True

    def _common_submit(self, dirac_script, server):
        '''Submit the job via the Dirac server.'''
        self.id = None
        self.actualCE = None
        self.status = None
        dirac_cmd = """execfile(\'%s\')""" % dirac_script
        result = server.execute(dirac_cmd)
  
        err_msg = 'Error submitting job to Dirac: %s' % str(result)
        if not result_ok(result) or not result.has_key('Value'):
            logger.error(err_msg)
            raise BackendError('Dirac',err_msg)
        
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
        
        input_sandbox   = master_input_sandbox[:]
        input_sandbox  += sboxname
        #why send this?
        #input_sandbox  += [dirac_script_filename]

        
        input_sandbox  += self._addition_sandbox_content(subjobconfig)
        
        dirac_script = subjobconfig.getExeString().replace('##INPUT_SANDBOX##',str(input_sandbox))

        dirac_script_filename = os.path.join(j.getInputWorkspace().getPath(),'dirac-script.py')
        f=open(dirac_script_filename,'w')
        f.write(dirac_script)
        f.close()
        return self._common_submit(dirac_script_filename, DiracBase.dirac_ganga_server)
 
    def master_auto_resubmit(self,rjobs):
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
                logger.info("resubmitting job %s to %s backend",fqid,sj.backend._name)
                try:
                    b = sj.backend
                    sj.updateStatus('submitting')
                    result = b._resubmit(DiracBase.dirac_monitoring_server)
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
        """Resubmit a DIRAC job"""
        return self._resubmit(DiracBase.dirac_ganga_server)

    def _resubmit(self, server):
        """Resubmit a DIRAC job"""
        j=self.getJobObject()
        parametric = False
        script_path = os.path.join(j.getInputWorkspace().getPath(),
                                       'dirac-script.py')
        ## Check old script
        if j.master is None and not os.path.exists(script_path):
             raise BackendError('Dirac','No "dirac-script.py" found in j.inputdir')
        if j.master is not None and not os.path.exists(script_path):
             script_path = os.path.join(j.master.getInputWorkspace().getPath(),
                                        'dirac-script.py')
             if not os.path.exists(script_path):
                  raise BackendError('Dirac','No "dirac-script.py" found in j.inputdir or j.master.inputdir')
             parametric = True

        ## Read old script
        f=open(script_path,'r')
        script = f.read()
        f.close()

        ## Create new script - ##note instead of using get_parametric_dataset could just use j.inputdata.
        if parametric is True:
            parametric_datasets = get_parametric_datasets(script.split('\n'))
            if len(parametric_datasets) != len(j.master.subjobs):
                raise BackendError('Dirac','number of parametric datasets defined in API script doesn\'t match number of master.subjobs')
            if set(parametric_datasets[j.id]).symmetric_difference(set([f.name for f in j.inputdata.files])):
                raise BackendError('Dirac','Mismatch between dirac-script and job attributes.')
            script = script.replace('.setParametricInputData(%s)' % str(parametric_datasets),
                                    '.setInputData(%s)' % str(parametric_datasets[j.id]))
            script = script.replace('%n',str(j.id)) #name

        start_user_settings = '# <-- user settings\n'
        new_script = script[:script.find(start_user_settings) + len(start_user_settings)]

        job_ident = get_job_ident(script.split('\n'))
        for key, value in self.settings.iteritems():
             if type(value)is type(''):
                  new_script += '%s.set%s("%s")\n' % (job_ident, key, value)
             else:
                  new_script += '%s.set%s(%s)\n' % (job_ident, key, str(value))
        new_script += script[script.find('# user settings -->'):]
             

        ## Save new script
        new_script_filename = os.path.join(j.getInputWorkspace().getPath(),
                              'dirac-script.py')
        f = open(new_script_filename, 'w')
        f.write(new_script)
        f.close()
        
        return self._common_submit(new_script_filename, server)
##     def resubmit(self):
##         """Resubmit a DIRAC job"""
##         j=self.getJobObject()
##         parametric = False
##         new_script_path = os.path.join(j.getInputWorkspace().getPath(),
##                                        'dirac-script.py')
##         ## Read old script
##         script_path = new_script_path
##         if not os.path.exists(script_path):
##             if j.master is None or \
##                    not isinstance(j.master.splitter,SplitByFiles) or \
##                    j.master.splitter.bulksubmit is False:
##                 raise BackendError('Dirac','No "dirac-script.py" found in j.inputdir')
##             script_path = os.path.join(j.master.getInputWorkspace().getPath(),
##                                        'dirac-script.py')
##             if not os.path.exist(script_path):
##                  raise BackendError('Dirac','No "dirac-script.py" found in j.inputdir')
##             parametric = True
            
##         f=open(script_path,'r')
##         script = f.read()
##         f.close()

##         ## Create new script
##         if parametric is True:
##             parametric_datasets = self.__get_parametric_datasets(script.split('\n'))
##             if len(parametric_datasets) is not len(j.master.subjobs):
##                   raise BackendError('Dirac','wrong number of subjobs!')
##             script = script.replace('j.setParametricInputData(%s)' % str(parametric_datasets),
##                                     'j.setInputData(%s)' % str(parametric_datasets[j.id]))
##             script = script.replace('%n',str(j.id)) #name

##         start_user_settings = '# <-- user settings\n'
##         new_script = script[:script.find(start_user_settings) + len(start_user_settings)]
##         for key, value in self.settings.iteritems():
##             if type(value)is type(''):
##                 new_script += 'j.set%s("%s")\n' % (key, value)
##             else:
##                 new_script += 'j.set%s(%s)\n' % (key, str(value))
##         new_script += script[script.find('# user settings -->'):]


##         f = open(new_script_path, 'w')
##         f.write(new_script)
##         f.close()
        
##         return self._submit(new_script)

    def reset(self, doSubjobs =False):
        """Resets the state of a job back to 'submitted' so that the
        monitoring will run on it again."""        
        j = self.getJobObject()

        disallowed = ['submitting','killed']
        if j.status in disallowed:
            logger.warning("Can not reset a job in status '%s'." % j.status)
        else:
            j.getOutputWorkspace().remove(preserve_top=True)
            j.updateStatus('submitted')
            if j.subjobs and not doSubjobs:
                logger.info('This job has subjobs, if you would like the backends '\
                            'of all the subjobs that are in status=\'completing\' or '\
                            'status=\'failed\' also reset then recall reset with the '\
                            'arg \'True\' i.e. job(3).backend.reset(True)')
            elif j.subjobs and doSubjobs:
                logger.info('resetting the backends of \'completing\' and \'failed\' subjobs.')
                for sj in j.subjobs:
                    if sj.status == 'completing' or sj.status == 'failed': sj.backend.reset()
            if j.master: j.master.updateMasterJobStatus()
    
    def kill(self):
        """ Kill a Dirac jobs"""         
        if not self.id: return None
        dirac_cmd = 'result = DiracCommands.kill(%d)' % self.id
        result = DiracBase.dirac_ganga_server.execute(dirac_cmd)
        if not result_ok(result):
            raise BackendError('Dirac','Could not kill job: %s' % str(result))
        return result['OK']

    def peek(self,filename=None,command=None):
        """Peek at the output of a job (Note: filename/command are ignored)."""
        dirac_cmd = 'result = DiracCommands.peek(%d)' % self.id
        result = DiracBase.dirac_ganga_server.execute(dirac_cmd)
        if result_ok(result): print result['Value']
        else: logger.error("No peeking available for Dirac job '%i'.", self.id)

    def getOutputSandbox(self,dir=None):
        j = self.getJobObject()
        if dir is None: dir = j.getOutputWorkspace().getPath()
        dirac_cmd = "result = DiracCommands.getOutputSandbox(%d,'%s')" \
                    % (self.id,dir)
        result = DiracBase.dirac_ganga_server.execute(dirac_cmd)
        if not result_ok(result):
            msg = 'Problem retrieving output: %s' % str(result)
            logger.warning(msg)
            return False

        return True

    def getOutputData(self,dir=None,names=None):
        """Retrieve data stored on SE to dir (default=job output workspace).
        If names=None, then all outputdata is downloaded otherwise names should
        be a list of files to download."""
        j = self.getJobObject()
        if not names: names = []
        if not dir: dir = j.getOutputWorkspace().getPath()


        if names:
            files_to_download = [f for f in j.outputfiles if isinstance(f, DiracFile) and f.namePattern in names]
        else:
            files_to_download = [f for f in j.outputfiles if isinstance(f, DiracFile)]

        suceeded = []
        for f in files_to_download:
            f.localDir = dir
            try:
                f.get()
            except GangaException, e:
                logger.warning(e)
                continue
            suceeded.append(f.lfn)
                            
        return suceeded
            
    def getOutputDataLFNs(self):
        """Retrieve the list of LFNs assigned to outputdata"""   

        j = self.getJobObject()
        return [f.lfn for f in j.outputfiles if isinstance(f, DiracFile) and f.lfn != ""]
        
    def debug(self):
        '''Obtains some (possibly) useful DIRAC debug info. '''
        # check services
        cmd = 'result = DiracCommands.getServicePorts()'
        result = DiracBase.dirac_ganga_server.execute(cmd)
        if not result_ok(result):
            logger.warning('Could not obtain services: %s' % str(result))
            return
        services = result.get('Value',{})
        for category in services:
            system,service = category.split('/')
            cmd = "result = DiracCommands.ping('%s','%s')" % (system,service)
            result = DiracBase.dirac_ganga_server.execute(cmd)
            msg = 'OK.'
            if not result_ok(result): msg = '%s' % result['Message']
            print '%s: %s' %  (category,msg)
        # get pilot info for this job
        if type(self.id) != int: return
        j = self.getJobObject()
        cwd = os.getcwd()
        debug_dir = j.getDebugWorkspace().getPath()
        cmd = "result = DiracCommands.getJobPilotOutput(%d,'%s')" % \
              (self.id, debug_dir)
        result = DiracBase.dirac_ganga_server.execute(cmd)
        #print 'result =', result
        if result_ok(result):
            print 'Pilot Info: %s/pilot_%d/std.out.'%(debug_dir,self.id)
        else:
            print result.get('Message','')

    def _getStateTime(job, status):
        """Returns the timestamps for 'running' or 'completed' by extracting
        their equivalent timestamps from the loggingInfo."""
        ## Now private to stop server cross-talk from user thread. Since updateStatus calles
        ## this method whether called itself by the user thread or monitoring thread.
        ## Now don't use hook but define our own private version
        ## used in monitoring loop... messy but works.
        if job.status != status:
            b_list = ['running', 'completing', 'completed', 'failed']
            backend_final = ['failed', 'completed']
            #backend stamps
            if not job.subjobs and status in b_list: 
                for childstatus in b_list:
                    if job.backend.id:
                        logger.debug("Accessing getStateTime() in diracAPI")
                        dirac_cmd = "result = DiracCommands.getStateTime(%d,\'%s\')" % (job.backend.id, childstatus)
                        be_statetime = DiracBase.dirac_monitoring_server.execute(dirac_cmd)
                        if childstatus in backend_final:
                            job.time.timestamps["backend_final"] = be_statetime 
                            logger.debug("Wrote 'backend_final' to timestamps.")
                        else:
                            job.time.timestamps["backend_"+childstatus] = be_statetime 
                            logger.debug("Wrote 'backend_%s' to timestamps.", childstatus)
                    if childstatus==status: break
            logger.debug("_getStateTime(job with id: %d, '%s') called.", job.id, job.status)
        else:
            logger.debug("Status changed from '%s' to '%s'. No new timestamp was written", job.status, status)
    _getStateTime = staticmethod(_getStateTime)

    def timedetails(self):
        """Prints contents of the loggingInfo from the Dirac API."""
        if not self.id: return None
        logger.debug("Accessing timedetails() in diracAPI")
        dirac_cmd = 'result = DiracCommands.timedetails(%d)' % self.id
        result = DiracBase.dirac_ganga_server.execute(dirac_cmd)
        return result


    def _finalisation_jobThread(job, updated_status):
        if updated_status == 'completed':
            cmd = 'result = DiracCommands.normCPUTime(%d)' % job.backend.id
            job.backend.normCPUTime = DiracBase.dirac_monitoring_server.execute(cmd)
            r = DiracBase.dirac_monitoring_server.execute("result = DiracCommands.getOutputSandbox(%d,'%s')" % (job.backend.id, job.getOutputWorkspace().getPath()),
                                                          priority=5)

            if not result_ok(r):
                logger.warning('Problem retrieving outputsandbox: %s' % str(r))
                DiracBase._getStateTime(job,'failed')
                job.updateStatus('failed')
                return
            if job.outputdata:
                r = DiracBase.dirac_monitoring_server.execute('DiracCommands.getOutputDataLFNs(%d)' % job.backend.id,
                                                              priority=5)
                if not result_ok(r):
                    logger.warning('LFN query failed: %s' % str(r))
                lfns = r.get('Value',[])
                file = open(job.getOutputWorkspace().getPath() + '/lfns.lst','w')
                for lfn in lfns: file.write(lfn.replace(' ','')+'\n')
                file.close()
            DiracBase._getStateTime(job,updated_status)
            job.updateStatus(updated_status)
            if job.master: job.master.updateMasterJobStatus()
        elif updated_status == 'failed':
            if getConfig('DIRAC')['failed_sandbox_download']:
                DiracBase.dirac_monitoring_server.execute("result = DiracCommands.getOutputSandbox(%d,'%s')" % (job.backend.id, job.getOutputWorkspace().getPath()),
                                                          priority=7)    
##             DiracBase._getStateTime(job,'failed')
##             job.updateStatus('failed')
##             if job.master: job.master.updateMasterJobStatus()
    _finalisation_jobThread = staticmethod(_finalisation_jobThread)
    
    def updateMonitoringInformation(jobs):
        """Check the status of jobs and retrieve output sandboxes"""
        ## Only those jobs in 'submitted','running' are passed in here for checking
        from Ganga.Core import monitoring_component
        ganga_job_status = [ j.status for j in jobs ]
      #  dirac_job_ids = [j.backend.id for j in jobs ]
##         for j in jobs: dirac_job_ids.append(j.backend.id)
        if not DiracBase.dirac_monitoring_server.proxyValid():
            if DiracBase.dirac_monitoring_is_active:
                logger.warning('DIRAC monitoring inactive (no valid proxy '\
                               'found).')
                DiracBase.dirac_monitoring_is_active = False
            return
        else:
            DiracBase.dirac_monitoring_is_active = True

        cmd = 'result = DiracCommands.status(%s)' % str([j.backend.id for j in jobs])
        result = DiracBase.dirac_monitoring_server.execute(cmd,priority=1)
        if type(result) != type([]):
            logger.warning('DIRAC monitoring failed: %s' % str(result))
            return
                

        thread_handled_states = {'completed':'completing', 'failed':'failed'}
        #thread_code           = {'completing':__completed_finalise, 'failed':__failed_finalise}
        for job, state, old_state in zip(jobs, result, ganga_job_status):
            if monitoring_component:
                if monitoring_component.should_stop(): break
            job.backend.statusInfo = state[0]
            job.backend.status     = state[1]
            job.backend.actualCE   = state[2]
            
            ## Is this really catching a real problem?
            if job.status != old_state:
                logger.warning('User changed Ganga job status from %s -> %s' % (str(old_state),j.status))
                continue
            ####################
            updated_status = state[3]
            if updated_status == job.status: continue

            ## maybe should have a thread Queue object here.
            if updated_status in thread_handled_states:
##                 t = GangaThread(name   = 'job_finalisation_thread',
##                                 target = DiracBase._finalisation_jobThread,
##                                 args   = (job, updated_status))
##                 t.daemon = True
##                 t.start()
##                updated_status = thread_handled_states[updated_status]
                DiracBase._getStateTime(job,thread_handled_states[updated_status])
                job.updateStatus(thread_handled_states[updated_status])
                DiracBase.dirac_monitoring_server.execute_nonblocking(command=None,
                                                                      finalise_code=DiracBase._finalisation_jobThread,
                                                                      args=(job,updated_status))
            else:
                #updated_status = thread_handled_states[updated_status]
                DiracBase._getStateTime(job,updated_status)
                job.updateStatus(updated_status)












                
##             if updated_status == 'completed':
##                 updated_status = 'completing'
##                 t = GangaThread(name   = 'job_completed_thread',
##                                 target = DiracBase.__completed_jobThread,
##                                 args   = (job,))
##                 t.daemon = True
##                 t.start()
##                 DiracBase._getStateTime(job,'completing')
##                 job.updateStatus('completing')
##                 cmd = 'DiracCommands.normCPUTime(%d)' % job.backend.id
##                 job.backend.normCPUTime = DiracBase.dirac_monitoring_server.execute(cmd)
##                 r = DiracBase.dirac_monitoring_server.execute("DiracCommands.getOutputSandbox(%d,'%s')" % (job.backend.id, job.getOutputWorkspace().getPath()) )
                
##                 if not result_ok(r):
##                     logger.warning('Problem retrieving output: %s' % str(r))
##                     DiracBase._getStateTime(job,'failed')
##                     job.updateStatus('failed')
##                     continue
##                 if job.outputdata:
##                     r = DiracBase.dirac_monitoring_server.execute('DiracCommands.getOutputDataLFNs(%d)' % job.backend.id)
##                     if not result_ok(r):
##                         logger.warning('LFN query failed: %s' % str(r))
##                     lfns = r.get('Value',[])
##                     file = open(job.getOutputWorkspace().getPath() + '/lfns.lst','w')
##                     for lfn in lfns: file.write(lfn.replace(' ','')+'\n')
##                     file.close()
##             elif updated_status == 'failed':
##                 if getConfig('DIRAC')['failed_sandbox_download']:
##                     t = GangaThread(name   = 'job_failed_thread',
##                                     target = DiracBase.__failed_jobThread,
##                                     args   = (job,))
##                     t.daemon = True
##                     t.start()

##             DiracBase._getStateTime(job,updated_status)
##             job.updateStatus(updated_status)
                   
    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

    def execAPI(cmd,priority = 1,timeout=Ganga.Utility.Config.getConfig('DIRAC')['Timeout']):
        """Executes DIRAC API commands.  If variable 'result' is set, then
        it is returned by this method. """
        return DiracBase.dirac_ganga_server.execute(cmd, priority,timeout)

    execAPI = staticmethod(execAPI)

    def killServer():
        '''Kills the DIRAC server child process.'''
        DiracBase.dirac_ganga_server.shutdown_server()
        DiracBase.dirac_monitoring_server.shutdown_server()
    killServer = staticmethod(killServer)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

## from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
## from GangaDirac.Lib.RTHandlers.ExeDiracRTHandler import ExeDiracRTHandler
## #from RootDiracRTHandler import *

## allHandlers.add('Executable','Dirac', ExeDiracRTHandler)
## #allHandlers.add('Root','Dirac', RootDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
