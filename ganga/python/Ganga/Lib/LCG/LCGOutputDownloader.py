from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.logging import getLogger
from Ganga.Lib.LCG.MTRunner import MTRunner, Data, Algorithm  
from Ganga.Lib.LCG.Utility import * 

logger = getLogger()

class LCGOutputDownloadTask:

    _attributes = ('gridObj', 'jobObj', 'use_wms_proxy')

    def __init__(self, gridObj, jobObj, use_wms_proxy):
        self.gridObj = gridObj
        self.jobObj  = jobObj
        self.use_wms_proxy = use_wms_proxy

class LCGOutputDownloadAlgorithm(Algorithm):

    def process(self, item):
        """
        downloads output of one LCG job 
        """

        pps_check = (True,None)

        grid = item.gridObj
        job  = item.jobObj
        wms_proxy = item.use_wms_proxy

        ## it is very likely that the job's downloading task has been
        ## created and assigned in a previous monitoring loop
        ## ignore such kind of cases
        if job.status in ['completing', 'completed', 'failed']:
            return True

        job.updateStatus('completing')
        outw = job.getOutputWorkspace()

        pps_check = grid.get_output(job.backend.id, outw.getPath(), wms_proxy=wms_proxy)

        if pps_check[0]:
            job.updateStatus('completed')
            job.backend.exitcode = 0
        else:
            job.updateStatus('failed')
            # update the backend's reason if the failure detected in the Ganga's pps 
            if pps_check[1] != 0:
                job.backend.reason = 'non-zero app. exit code: %s' % pps_check[1]
                job.backend.exitcode = pps_check[1]

        self.__appendResult__( job.getFQID('.'), True )

        return True

class LCGOutputDownloader:

    """
    Class for managing the LCG output downloading activities.
    """

    _attributes = ('data','algorithm','runner','keepAlive')

    def __init__(self, keepAlive=True):

        self.data      = Data(collection=[])
        self.algorithm = LCGOutputDownloadAlgorithm()
        self.keepAlive = keepAlive
        self.runner    = None
 
    def __create_new_runner__(self):
        self.runner = MTRunner(self.algorithm, self.data)
        ## the runner thread's name should have the prefix "GANGA_Updata_Thread" 
        ## so the the logging info can be cached until the next IPython prompt
        self.runner.setName('GANGA_Update_Thread_lcg_output_downloader')
        self.runner.debug = False
        self.runner.setDaemon(True)
        self.runner.keepAlive = self.keepAlive

    def addTask(self, grid, job, use_wms_proxy):

        task = LCGOutputDownloadTask(grid, job, use_wms_proxy)
        logger.debug( 'output download task: job %s' % job.getFQID('.') )
        self.data.addItem(task)
        return True

    def start(self):

        if self.runner and self.runner.isAlive():
            ## do nothing if there is already a alive runner
            pass
        else:
            self.__create_new_runner__()
            self.runner.start()
            logger.debug('LCGOutputDownloader started')

    def stop(self):
        self.runner.stop()
        logger.debug('LCGOutputDownloader stopped')
