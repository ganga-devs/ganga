# Copyright information
__author__  = "Ole Weidner <oweidner@cct.lsu.edu>"
__date__    = "13 September 2009"
__version__ = "1.0"

# Ganga GPIDev Imports
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import FileBuffer

# Setup Utility Logger
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

# Setup Utility Config
import Ganga.Utility.Config
config = Ganga.Utility.Config.makeConfig('SAGA','parameters of the SAGA backend')
config.addOption('remove_workdir', True, 'remove automatically the local working directory when the job completed')

# Other Utility Imports
import Ganga.Utility.util
import Ganga.Utility.logic

# SAGA Job Package Imports
import saga.job

# The threaded download manager
from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm
from Ganga.Core import GangaException
from GangaSAGA.Lib.SAGA.SAGAFileTransferManager import SAGAFileTransferManager

# Python System Imports
import os,sys
import os.path,re,errno
import subprocess
import string

##############################################################################
## Helper functions for the multi-threaded file transfer manager
##
def start_saga_file_transfer_manager():
    global saga_file_transfer_manager
    saga_file_transfer_manager = None
    if not saga_file_transfer_manager:
        saga_file_transfer_manager = SAGAFileTransferManager(numThread=10)
        saga_file_transfer_manager.start()

def get_saga_file_transfer_manager():
    global saga_file_transfer_manager
    return saga_file_transfer_manager

# fire up the MTRunner
start_saga_file_transfer_manager()

##
##############################################################################


##############################################################################
##
class SAGA(IBackend):
    
    """Run jobs in the background using SAGA.
       The job is run localy or remotely - depending on which SAGA adaptor is selected. 
    """
    
    # Set category & unique backend name
    _category = 'backends'
    _name = 'SAGA'
    
    # Setup Job Attributes
    _schema = Schema(Version(1,2), {
        'status' : SimpleItem(defvalue=None,typelist=None,protected=1,copyable=0,hidden=1,doc='*NOT USED*'),
        'actualCE' : SimpleItem(defvalue='',protected=1,copyable=0,doc='Hostname where the job was submitted.'),
        'wrapper_pid' : SimpleItem(defvalue=-1,protected=1,copyable=0,hidden=1,doc='(internal) process id of the execution wrapper'),
                                    
        ## Public SAGA specific attributes - READ/WRITE - REQUIRED                        
        'jobservice_url' : SimpleItem(defvalue='fork://localhost',      doc='Resource manager URL that will be passed to SAGA'),
        'filesystem_url' : SimpleItem(defvalue='file://localhost/tmp/', doc='File System (working directory) URL that will be passed to SAGA'),
        
        ## Public SAGA specific attributes - READ/WRITE - OPTIONAL
        'workdir'             : SimpleItem(defvalue='',protected=0,copyable=0,doc='JSDL Attribute: Working Directory'),
        'queue'               : SimpleItem(defvalue='',protected=0,copyable=0,doc='JSDL Attribute: Queue.'),
        'number_of_processes' : SimpleItem(defvalue='1',protected=0,copyable=0,doc='JSDL Attribute: Number of Processes to Spawn'),
        'spmd_variation'      : SimpleItem(defvalue='single',protected=0,copyable=0,doc='JSDL Attribute: SPMD Variation.'),
        'allocation'         : SimpleItem(defvalue='',protected=0,copyable=0,doc='JSDL Attribute: Project / Allocation Name.'),

        ## Hidden SAGA specific attributes - READ ONLY
        'saga_job_out' : SimpleItem(defvalue='',protected=1,copyable=0,doc='SAGA-internal URL to stdout.'),
        'saga_job_err' : SimpleItem(defvalue='',protected=1,copyable=0,doc='SAGA-internal URL to stderr.'),
        'saga_job_id'  : SimpleItem(defvalue='',protected=1,copyable=0,doc='SAGA-internal Job ID.'),
        
    })
    
    _GUIPrefs = [ { 'attribute' : 'jobservice_url', 'widget' : 'String' },
                  { 'attribute' : 'filesystem_url', 'widget' : 'String' },
                  { 'attribute' : 'id', 'widget' : 'Int' },
                  { 'attribute' : 'status' , 'widget' : 'String' },
                  { 'attribute' : 'exitcode', 'widget' : 'String' } ]
                  
    _GUIAdvancedPrefs = [ { 'attribute' : 'nice', 'widget' : 'String' },
                          { 'attribute' : 'exitcode', 'widget' : 'String' } ]    


    ##########################################################################
    ##
    def __init__(self):
      super(SAGA,self).__init__()


    ##########################################################################
    ## Tries to create the local working directories properly
    ##
    def setupworkdir(self, path):
        import shutil, errno
        
        job = self.getJobObject()
        # As a side effect, these functions create the
        # workspace directories if they don't exist
        input_wd_path = job.getInputWorkspace().getPath()
        output_wd_path = job.getOutputWorkspace().getPath()
        
        logger.debug('local workspace - input dir: %s', input_wd_path)
        logger.debug('local workspace - output dir: %s', output_wd_path)
        
        return True
        
        
    ##########################################################################
    ## Tries to pres-stage the input sandbox
    ##
    def prestagesandbox(self, jobconfig):
        sandbox_files = jobconfig.getSandboxFiles()
        
        if (len(sandbox_files) > 0):
            logger.info("prestaging %s input sandbox files", len(sandbox_files))
            for f in sandbox_files:
                try:
                    if(f.subdir != '.'): # create subdirectory & copy
                        target = saga.url(self.filesystem_url+"/"+f.subdir+"/")
                        
                        sd = saga.filesystem.directory(target, saga.filesystem.Create)
                        sf = saga.filesystem.file(f.name) # copy the file
                        
                        logger.info("  * %s -> %s ", f.name, target)
                        sf.copy(target, saga.filesystem.Overwrite)  
                        
                    else: # copy to remote work dir
                        logger.info("  * %s -> %s ", f.name, self.filesystem_url)
                        sf = saga.filesystem.file(f.name) 
                        sf.copy(self.filesystem_url, saga.filesystem.Overwrite)
                        
                except saga.exception, e:
                    logger.error('exception caught while transfering file: %s', 
                    e.get_message())
                    return False
            
        return True
                
    ##########################################################################
    ## Tries to submit a ganga job through saga
    ##
    def submit(self, jobconfig, master_input_sandbox):
    
        # Make sure that all REQUIRED attributes are set
        if len(self.jobservice_url) == 0 :
            logger.error('jobservice_url attribute needs to be set')
            return 0
        if len(self.filesystem_url) == 0 :
            logger.error('filesystem_url attribute needs to be set')
            return 0
    
        self.setupworkdir(self.workdir)
                    
        try: 
            jd = saga.job.description()
            jd = self.makesagajobdesc(self.getJobObject(), jobconfig)
            
            if (True != self.prestagesandbox(jobconfig)):
                return False
                                    
            self.run(self.jobservice_url, jd)
                
        except saga.exception, e:
            logger.error('exception caught while submitting job: %s', 
                         e.get_message())
            return False
      
        return True # sets job to 'submitted'
        

    ##########################################################################
    ## Tries to resubmit an existing ganga job through saga
    ##
    def resubmit(self):                
        try: 
            jd = saga.job.description()
            jd = self.makesagajobdesc(self.getJobObject())
                  
            self.run(self.jobservice_url, jd)
                
        except saga.exception, e:
            logger.error('exception caught while submitting job: %s', 
                         e.get_message())
            return False
      
        return True # sets job to 'submitted'

           
      
    ##########################################################################
    ## Run a SAGA job
    ##
    def run(self, js_contact, jd):
        # actualCE is the same as the resource manager (for now)
        self.actualCE = self.jobservice_url;
        
        try:
            logger.info("submitting job to %s", self.jobservice_url);
            
            js_url = saga.url(self.jobservice_url)
            js =  saga.job.service(js_url)
            saga_job = js.create_job(jd)
      
            # execute the job
            saga_job.run()
            self.saga_job_id = saga_job.get_job_id()
            logger.info("job submitted with internal job id: %s", self.saga_job_id)
                  
        except saga.exception, e:
            logger.error('exception caught while submitting job: %s', 
                         e.get_message())
            return False
        
        return True

            
    ##########################################################################
    ## Tries to kill a running saga job
    ##        
    def kill(self):
        job = self.getJobObject()
        
        # create a new service object in order
        # to reconnect to the job we want to kill
        try :
            js_url = saga.url(job.backend.jobservice_url)
            js = saga.job.service(js_url)
            saga_job = js.get_job(job.backend.saga_job_id)
            
            # KILL KILL KILL
            saga_job.cancel()
                            
        except saga.exception, e:
            logger.error('exception caught while killing job: %s', 
                         e.get_message())
            return False
            
        return True # sets job to 'killed'
        

    ##########################################################################
    ## Creates and returns a saga.job.description from a given
    ## job object
    ##
    def makesagajobdesc(self, job, jobconfig):
        jd = saga.job.description()
        logger.debug("setting up new saga job with id: %s", job.id)
        
        # executable
        jd.executable = jobconfig.getExeString()
        logger.debug("  * application.exe -> saga.executable: %s", jd.executable)
        
        # arguments
        argList = jobconfig.getArgStrings()
        #for arg in job.application.args:
        #    argList.append( arg ) #"\\'%s\\'" % arg ) 
        if len(argList) != 0:
            jd.arguments = argList
            logger.debug("  * application.args -> saga.arguments: %s", jd.arguments)

        # environment
        envList = [] 
        for k, v in job.application.env.items():
            envList.append( k+"="+v ) #"\\'%s\\'" % arg ) 
        if len(envList) != 0:
            jd.environment = envList
            logger.debug("  * application.env -> saga.environment: %s", jd.environment)

        # workdir
        if len(job.backend.workdir) != 0:        
            jd.working_directory = job.backend.workdir
            logger.debug("  * backend.workdir -> saga.workdir: %s", jd.working_directory)
        else: #default to the remote filesystem path component
            jd.working_directory = saga.url(self.filesystem_url).path
            logger.debug("  * saga.workdir: %s (not given - extracted from 'filesystem_url')", jd.working_directory)

        # queue
        if len(job.backend.queue) != 0:
            jd.queue = job.backend.queue
            logger.debug("  * backend.queue -> saga.queue: %s", jd.queue)

        # allocation 
        if len(job.backend.allocation) != 0:
            jd.job_project = [job.backend.allocation]
            logger.debug("  * backend.allocation -> saga.job_project: %s", jd.job_project)

        # spmd_variation
        if len(job.backend.spmd_variation) != 0:
            jd.spmd_variation = job.backend.spmd_variation
            logger.debug("  * backend.spmd_variation -> saga.spmd_variation: %s", jd.spmd_variation)

        # number_of_processes
        if len(job.backend.number_of_processes) != 0:
            jd.number_of_processes = job.backend.number_of_processes
            logger.debug("  * backend.number_of_processes -> saga.number_of_processes: %s", jd.number_of_processes)

        ## We have to create special filenames for stdout/stderr redirection
        ## To avoid name clashes, we append a UUID to the filename.               
        path_component = saga.url(self.filesystem_url).path
        logger.debug("  * local output/working directory on the remote system is: %s", path_component)

        uuid_str = "0"
        if sys.version_info < (2, 4):
            uuid_str = os.popen("/usr/bin/uuidgen").read()
            uuid_str = uuid_str.rstrip("\n") 
        else:
            import uuid
            uuid_str = str(uuid.uuid4()) 

        ## STDOUT
        self.saga_job_out = 'ganga-saga-'+str(uuid_str)+'.out' 
        jd.output = saga.url(path_component + "/" + self.saga_job_out).url # does normalize
        
        logger.debug("  * stdout should become available at: %s",
            saga.url(job.backend.filesystem_url+"/"+job.backend.saga_job_out))

        ## STDERR
        self.saga_job_err = 'ganga-saga-'+str(uuid_str)+'.err'
        jd.error = saga.url(path_component + "/" + self.saga_job_err).url # does normalize

        logger.debug("  * stdout should become available at: %s",
            saga.url(job.backend.filesystem_url+"/"+job.backend.saga_job_err))
        
        return jd
                

    ##########################################################################
    ## Method gets triggered by a ganga monitoring thread periodically 
    ## in order to update job information, like state, etc...
    ##
    def updateMonitoringInformation(jobs):
        
        for j in jobs:
            # Skip job status query in case the job is already in
            # 'completed' status. That should avoid a great amount of
            # overhead if there are many completed jobs in the list
            if j.status == 'completed':
                continue
                        
            # create a new service object in order
            # to reconnect to the job
            try :
                js_url = saga.url(j.backend.jobservice_url)

                # dirty hack no. 1: the default job (fork) adaptor
                # doesn't support re-connect! So if the URL starts with
                # something like fork://localhost... don't even try.
                if False: #((j.status != 'new') and (js_url.scheme == 'fork')):
                    # that's the only thing we can do right now. set the
                    # job status to completing and hope that it has actually
                    # completed and the output files are available
                    downloader = get_saga_file_transfer_manager()
                    downloader.addTask(j)
                    # download will set the status either to complete of failed
                   
                else:
                    js = saga.job.service(js_url)
                    saga_job = js.get_job(j.backend.saga_job_id)
            
                    # query saga job state
                    job_state = saga_job.get_state()
      
                    if job_state == saga.job.Done:
                        downloader = get_saga_file_transfer_manager()
                        downloader.addTask(j)
                        # download will set the status either to complete of failed

                    elif job_state == saga.job.Running:
                        if(j.status != 'running'):
                            j.updateStatus('running')
                        
                    elif job_state == saga.job.Failed:
                        if(j.status != 'failed'): 
                            j.updateStatus('failed')
                
            except saga.exception, e:
                logger.error('exception caught while updating job: %s', 
                             e.get_message())
    
    ##########################################################################
    ## Make the monitoring function available to the update thread
    ##         
    updateMonitoringInformation = staticmethod(updateMonitoringInformation)




    ##################################################
    ##
    def peek( self, filename = "", command = "" ):
      """
      Allow viewing of output files in job's work directory
      (i.e. while job is in 'running' state)
                                                                                
      Arguments other than self:
      filename : name of file to be viewed
                => Path specified relative to work directory
      command  : command to be used for file viewing
                                                                                
      Return value: None
      """
      #job = self.getJobObject()
      #topdir = self.workdir.rstrip( os.sep )
      #path = os.path.join( topdir, filename ).rstrip( os.sep )
      #job.viewFile( path = path, command = command )
      return None
      
#    def remove_workdir(self):
#        if config['remove_workdir']:
#            import shutil
#            try:
#                shutil.rmtree(self.workdir)
#            except OSError,x:
#                logger.warning('problem removing the workdir %s: %s',str(self.id),str(x))                        

      
##      
##############################################################################


##############################################################################
##
## Register a default runtime handler
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.Lib.Executable import RTHandler
allHandlers.add('Executable','SAGA', RTHandler)
 
