from Ganga.GPIDev.Adapters.ISplitter import ISplitter, SplittingError
from Ganga.GPIDev.Lib.Job import Job
from SplitterUtils import DatasetSplitter
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
#import copy
import Ganga.Utility.logging
# from GangaGaudi.Lib.Datasets import GaudiDataset

logger = Ganga.Utility.logging.getLogger()


class GaudiInputDataSplitter(ISplitter):

    """Splits a job into sub-jobs by partitioning the input data

    GaudiInputDataSplitter can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'GaudiInputDataSplitter'
    _schema = Schema(Version(1, 0), {
        'filesPerJob': SimpleItem(defvalue=10,
                                  doc='Number of files per subjob',
                                  typelist=['int']),
        'maxFiles': SimpleItem(defvalue=None,
                               doc='Maximum number of files to use in a masterjob (None = all files)',
                               typelist=['int', 'type(None)'])
    })

    # returns splitter generator can be overridden for modified behaviour
    # job is passed in only so that it has full information at the time of splitting
    # this may be useful if generator must change dependent on job properties like
    # backend for example. (see GangaLHCb.Lib.Splitters.SplitByFiles.py)
    def _splitter(self, job, inputdata):
        if (inputdata is None) or (len(inputdata.files) == 0):
            logger.error('Cannot split if no inputdata given!')
            raise SplittingError('inputdata is None')
        logger.debug("Found %s files inputdata" % str(len(inputdata.files)))
        return DatasetSplitter(inputdata, self.filesPerJob, self.maxFiles)

    # returns a subjob based on the master and the reduced dataset can be overridden
    # for modified behaviour
    def _create_subjob(self, job, dataset):
        logger.debug("_create_subjob")
        j = Job()
        j.copyFrom(job)
        j.splitter = None
        j.merger = None
        j.inputsandbox = []  # master added automatically
        j.inputdata = GaudiDataset(files=dataset)
##         if not j.inputdata: j.inputdata = GaudiDataset(files=dataset)
# else:               j.inputdata.files = dataset
        return j

    def split(self, job):
        logger.debug("split")
        if self.filesPerJob < 1:
            logger.error('filesPerJob must be greater than 0.')
            raise SplittingError('filesPerJob < 1 : %d' % self.filesPerJob)
        elif (self.maxFiles is not None) and self.maxFiles < 1:
            logger.error('maxFiles must be greater than 0.')
            raise SplittingError('maxFiles < 1 : %d' % self.maxFiles)

        subjobs = []

        logger.debug("Creating all_jobs")
        all_jobs = self._splitter(job, job.inputdata)

        logger.info("Constructing subjobs")
        logger.debug("Filling DataSet")
        for dataset in all_jobs:
            logger.debug("Creating Subjobs with dataset of size: %s" % str(len(dataset)))
            #logger.debug( "Creating Subjobs with dataset: %s" % str(dataset) )
            subjobs.append(self._create_subjob(job, dataset))

        logger.info("Finished Splitting")
        logger.debug("Returning all subjobs")
        return subjobs
