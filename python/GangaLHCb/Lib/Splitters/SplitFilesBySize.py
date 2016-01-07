from GangaGaudi.Lib.Splitters.GaudiInputDataSplitter import GaudiInputDataSplitter
from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
from Ganga.GPIDev.Base.Proxy import stripProxy
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.Job import Job
logger = Ganga.Utility.logging.getLogger()
import os
import copy
import pickle


class SplitFilesBySize(GaudiInputDataSplitter):

    """Splits a job into sub-jobs by partitioning the input data

    SplitFilesBySize can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitFilesBySize'
    _schema = GaudiInputDataSplitter._schema.inherit_copy()
    _schema.datadict['bulksubmit'] = SimpleItem(defvalue=False,
                                                doc='determines if subjobs are split '
                                                'server side in a "bulk" submission or '
                                                'split locally and submitted individually')
    _schema.datadict['ignoremissing'] = SimpleItem(defvalue=False,
                                                   doc='Skip LFNs if they are not found '
                                                   'in the LFC. This option is only used if'
                                                   'jobs backend is Dirac')

    def _attribute_filter__set__(self, name, value):
        if name is 'sizePerJob':
            if value > 100:
                logger.warning('filesPerJob exceeded DIRAC maximum')
                logger.warning('DIRAC has a maximum dataset limit of 100.')
                logger.warning(
                    'BE AWARE!... will set it to this maximum value at submit time if backend is Dirac')
        return value

    def _create_subjob(self, job, dataset):
        logger.debug("_create_subjob")

        datatmp = []
        if isinstance(dataset, LHCbDataset):
            for i in dataset:
                if isinstance(i, DiracFile):
                    datatmp.extend(i)
                else:
                    logger.error(
                        "Unkown file-type %s, cannot perform split with file %s" % (type(i), str(i)))
                    from Ganga.Core.exceptions import GangaException
                    raise GangaException(
                        "Unkown file-type %s, cannot perform split with file %s" % (type(i), str(i)))
        elif isinstance(dataset, list):
            from Ganga.GPIDev.Base.Proxy import isType
            for i in dataset:
                if type(i) is str:
                    datatmp.append(DiracFile(lfn=i))
                elif isType(i, DiracFile()):
                    datatmp.extend(i)
                else:
                    x = GangaException(
                        "Unknown(unexpected) file object: %s" % i)
                    raise x
        else:
            logger.error("Unkown dataset type, cannot perform split here")
            from Ganga.Core.exceptions import GangaException
            raise GangaException(
                "Unkown dataset type, cannot perform split here")

        logger.debug("Creating new Job in Splitter")
        j = Job()
        j.copyFrom(stripProxy(job))
        j.splitter = None
        j.merger = None
        j.inputsandbox = []  # master added automatically
        j.inputfiles = []
        j.inputdata = LHCbDataset(files=datatmp[:],
                                  persistency=self.persistency,
                                  depth=self.depth)
        j.inputdata.XMLCatalogueSlice = self.XMLCatalogueSlice

        return j

    # returns splitter generator
    def _splitter(self, job, inputdata):

        logger.debug("_splitter")

        indata = stripProxy(copy.deepcopy(job.inputdata))

        if not job.inputdata:
            share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                      'shared',
                                      getConfig('Configuration')['user'],
                                      job.application.is_prepared.name,
                                      'inputdata',
                                      'options_data.pkl')
            if os.path.exists(share_path):
                f = open(share_path, 'r+b')
                indata = pickle.load(f)
                f.close()
            else:
                logger.error('Cannot split if no inputdata given!')
                raise SplittingError(
                    'job.inputdata is None and no inputdata found in optsfile')

        self.depth = indata.depth
        self.persistency = indata.persistency
        self.XMLCatalogueSlice = indata.XMLCatalogueSlice

        if stripProxy(job.backend).__module__.find('Dirac') > 0:
            if self.filesPerJob > 100:
                self.filesPerJob = 100  # see above warning
            logger.debug("indata: %s " % str(indata))
            outdata = DiracSplitter(indata,
                                    self.filesPerJob,
                                    self.maxFiles,
                                    self.ignoremissing)
            logger.debug("outdata: %s " % str(outdata))
            return outdata
        else:
            logger.error(
                "This Splitter HAS NOT, yet been implemented for all IGangaFile objects")
            raise NotImplementedError
            # return super(SplitFilesBySize,self)._splitter(job, indata)

    def split(self, job):
        logger.debug("split")
        if self.maxFiles == -1:
            self.maxFiles = None
        if self.bulksubmit:
            if stripProxy(job.backend).__module__.find('Dirac') > 0:
                logger.debug("Returning []")
                return []
        split_return = super(SplitFilesBySize, self).split(job)
        logger.debug("split_return: %s" % split_return)
        return split_return
