from Ganga.Utility.logging import getLogger
from Ganga.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm
from . import Grid

logger = getLogger()


class LCGOutputDownloadTask(object):

    """
    Class for defining a data object for each output downloading task.
    """

    _attributes = ('jobObj', 'use_wms_proxy')

    def __init__(self, jobObj, use_wms_proxy):
        self.jobObj = jobObj
        self.use_wms_proxy = use_wms_proxy

    def __eq__(self, other):
        """
        download task comparison based on job's FQID.
        """
        if self.jobObj.getFQID('.') == other.jobObj.getFQID('.'):
            return True
        else:
            return False

    def __str__(self):
        """
        represents the task by the job object
        """
        return 'downloading task for job %s' % self.jobObj.getFQID('.')


class LCGOutputDownloadAlgorithm(Algorithm):

    """
    Class for implementing the logic of each downloading task.
    """

    def process(self, item):
        """
        downloads output of one LCG job 
        """

        pps_check = (True, None)

        job = item.jobObj
        wms_proxy = item.use_wms_proxy

        # it is very likely that the job's downloading task has been
        # created and assigned in a previous monitoring loop
        # ignore such kind of cases
        if job.status in ['completing', 'completed', 'failed']:
            return True

        # it can also happen that the job was killed/removed by user between
        # the downloading task was created in queue and being taken by one of
        # the downloading thread. Ignore suck kind of cases
        if job.status in ['removed', 'killed']:
            return True

        job.updateStatus('completing')
        outw = job.getOutputWorkspace()

        pps_check = Grid.get_output(job.backend.id, outw.getPath(), wms_proxy=wms_proxy)

        if pps_check[0]:
            job.updateStatus('completed')
            job.backend.exitcode = 0
        else:
            job.updateStatus('failed')
            # update the backend's reason if the failure detected in the
            # Ganga's pps
            if pps_check[1] != 0:
                job.backend.reason = 'non-zero app. exit code: %s' % pps_check[
                    1]
                job.backend.exitcode = pps_check[1]

        # needs to update the master job's status to give an up-to-date status
        # of the whole job
        if job.master:
            job.master.updateMasterJobStatus()

        self.__appendResult__(job.getFQID('.'), True)

        return True


class LCGOutputDownloader(MTRunner):

    """
    Class for managing the LCG output downloading activities based on MTRunner.
    """

    def __init__(self, numThread=10):

        MTRunner.__init__(self, name='lcg_output_downloader', data=Data(
            collection=[]), algorithm=LCGOutputDownloadAlgorithm())

        self.keepAlive = True
        self.numThread = numThread

    def countAliveAgent(self):

        return self.__cnt_alive_threads__()

    def addTask(self, job, use_wms_proxy):

        task = LCGOutputDownloadTask(job, use_wms_proxy)

        logger.debug('add output downloading task: job %s' % job.getFQID('.'))

        self.addDataItem(task)

        return True
