# Copyright information
__author__  = "Ole Weidner <oweidner@cct.lsu.edu>"
__date__    = "13 September 2009"
__version__ = "1.0"

# Import Ganga Threading Stuff
from GangaCore.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm

# Setup Utility Logger
from GangaCore.Utility.logging import getLogger
logger = getLogger()

import os
import sys
import tarfile

# SAGA Job Package Imports
import saga.job

##############################################################################
##
class SAGAFileTransferTask:

    _attributes = ('jobObj')

    ##########################################################################
    ##
    def __init__(self, jobObj):
        self.jobObj  = jobObj

    ##########################################################################
    ##
    #def __eq__(self, other):
    #    """
    #    download task comparison based on job's FQID.
    #    """
    #    if self.jobObj.getFQID('.') == other.jobObj.getFQID('.'):
    #        return True
    #    else:
    #        return False

    ##########################################################################
    ##
    #def __str__(self):
    #    """
    #    represents the task by the job object
    #    """
    #    return 'downloading task for job %s' % self.jobObj.getFQID('.')


##############################################################################
##
class SAGAFileTransferAlgorithm(Algorithm):

    ##########################################################################
    ##
    def process(self, item):

        job = item.jobObj

        ## It is very likely that the job's downloading task has been created 
        ## and assigned in a previous monitoring loop ignore such kind of cases
        if job.status in ['completing', 'completed', 'failed']:
            return True

        job.updateStatus('completing')
        
        try:
            logger.info("poststaging output sandbox files")

            logger.info("  * %s", saga.url(job.backend.saga_job_out).url)
            stdout = saga.filesystem.file(saga.url(job.backend.saga_job_out));
            stdout.copy("file://localhost/"+job.getOutputWorkspace().getPath()+"stdout");
    
            logger.info("  * %s", saga.url(job.backend.saga_job_err).url)
            stderr = saga.filesystem.file(saga.url(job.backend.saga_job_err));
            stderr.copy("file://localhost/"+job.getOutputWorkspace().getPath()+"stderr");

            if len(job.outputsandbox) > 0 :
                output_sandbox = saga.url(job.backend.filesystem_url+"/"+job.backend.workdir_uuid+"/_output_sandbox.tgz")
                logger.info("  * %s", output_sandbox.url)
                osb = saga.filesystem.file(output_sandbox)
                osb.copy("file://localhost/"+job.getOutputWorkspace().getPath())
                
                ## Unpack the output sandbox and delete the archive
                osbpath = job.getOutputWorkspace().getPath()+"_output_sandbox.tgz"
                if os.path.exists(osbpath):
                    if os.system("tar -C %s -xzf %s"%(job.getOutputWorkspace().getPath(),job.getOutputWorkspace().getPath()+"/_output_sandbox.tgz")) != 0:
                        job.updateStatus('failed')
                        raise Exception('cannot upack output sandbox')
                    os.remove(osbpath)

            job.updateStatus('completed')
        
        except saga.exception as e:
            logger.error('exception caught while poststaging: %s', e.get_full_message())
            job.updateStatus('failed')
        
        return True

##############################################################################
##
class SAGAFileTransferManager(MTRunner):

    ##########################################################################
    ##
    def __init__(self, numThread=10):

        logger.debug('starting new MTRunner instance: %s.', "saga_file_transfer_manager")

        MTRunner.__init__(self, name='saga_file_transfer_manager', 
                          data=Data(collection=[]), 
                          algorithm=SAGAFileTransferAlgorithm())
        
        self.keepAlive = True
        self.numThread = numThread

    ##########################################################################
    ##
    def addTask(self, job):
        
        task = SAGAFileTransferTask(job)
        self.addDataItem(task)
        
        logger.debug( 'added new file transfer thread for job id %s', job.id )
        
        return True
        
        

