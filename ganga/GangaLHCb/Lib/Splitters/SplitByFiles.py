from GangaCore.Core.exceptions import SplitterError
from GangaGaudi.Lib.Splitters.GaudiInputDataSplitter import GaudiInputDataSplitter
from GangaCore.Core.exceptions import SplitterError
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.files import expandfilename
from GangaCore.GPIDev.Base.Proxy import stripProxy, isType, getName
import GangaCore.Utility.logging
from GangaCore.GPIDev.Lib.Job import Job
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.Credentials.DiracProxy import DiracProxy
import os
import copy
import pickle

from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
from GangaCore.GPIDev.Base.Filters import allComponentFilters
from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
from GangaCore.GPIDev.Credentials import require_credential
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from GangaLHCb.Lib.LHCbDataset.LHCbCompressedDataset import LHCbCompressedDataset
logger = GangaCore.Utility.logging.getLogger()


def getBackend():
    """
    Returns the backend which is to be used by the splitter based upon the LHCb config
    """
    lhcbConfig = getConfig('LHCb')
    return lhcbConfig['SplitByFilesBackend']


class SplitByFiles(GaudiInputDataSplitter):

    """Splits a job into sub-jobs by partitioning the input data

    SplitByFiles can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitByFiles'
    _schema = Schema(Version(1, 0), {
        'filesPerJob': SimpleItem(defvalue=10,
                                  doc='Number of files per subjob',
                                  typelist=['int']),

        'maxFiles': SimpleItem(defvalue=None,
                               doc='Maximum number of files to use in a masterjob (None = all files)',
                               typelist=['int', 'type(None)']),

        'bulksubmit': SimpleItem(defvalue=False,
                                 doc='determines if subjobs are split '
                                 'server side in a "bulk" submission or '
                                 'split locally and submitted individually'),

        'ignoremissing': SimpleItem(defvalue=False,
                                    doc='Skip LFNs if they are not found '
                                    'in the LFC. This option is only used if'
                                    'jobs backend is Dirac'),

        'splitterBackend': SimpleItem(defvalue=getBackend(),
                                      doc='name of the backend algorithm to use for splitting',
                                      typelist=['str'], protected=1, visitable=0),
        'credential_requirements': ComponentItem('CredentialRequirement', defvalue=DiracProxy),
        })

    _exportmethods = ['split']

    def _attribute_filter__set__(self, name, value):
        if name is 'filesPerJob':
            if value > 100:
                logger.warning('filesPerJob exceeded DIRAC maximum')
                logger.warning('DIRAC has a maximum dataset limit of 100.')
                logger.warning(
                    'BE AWARE!... will set it to this maximum value at submit time if backend is Dirac')
        return value

    def _create_subjob(self, job, dataset):
        logger.debug("_create_subjob")
        datatmp = []

        logger.debug("dataset size: %s" % str(len(dataset)))
        #logger.debug( "dataset: %s" % str(dataset) )

        if isinstance(dataset, LHCbDataset):
            for i in dataset:
                if isType(i, DiracFile):
                    datatmp.append(i)
                else:
                    logger.error("Unkown file-type %s, cannot perform split with file %s" % (type(i), str(i)))
                    from GangaCore.Core.exceptions import GangaException
                    raise GangaException("Unkown file-type %s, cannot perform split with file %s" % (type(i), str(i)))
        elif isinstance(dataset, (list, GangaList)):
            for this_file in dataset:
                if type(this_file) is str:
                    datatmp.append(allComponentFilters['gangafiles'](this_file, None))
                elif isType(this_file, IGangaFile):
                    datatmp.append(this_file)
                else:
                    logger.error("Unexpected type: %s" % str(type(this_file)))
                    logger.error("Wanted object to inherit from type: %s: %s" % (str(type(IGangaFile()))))
                    from GangaCore.Core.exceptions import GangaException
                    x = GangaException("Unknown(unexpected) file object: %s" % this_file)
                    raise x
        elif type(dataset) is str:
            datatmp.append(DiracFile(lfn=dataset))
        else:
            logger.error("Unkown dataset type, cannot perform split here")
            from GangaCore.Core.exceptions import GangaException
            logger.error("Dataset found: " + str(dataset))
            raise GangaException("Unkown dataset type, cannot perform split here")

        logger.debug("Creating new Job in Splitter")
        j = Job()
        logger.debug("Copying From Job")
        j.splitterCopy(stripProxy(job), ['splitter', 'subjobs', 'inputdata', 'inputsandbox', 'inputfiles', 'fqid', 'outputdir', 'master', 'merger', 'metadata', 'been_queued', 'parallel_submit', 'inputdir', 'non_copyable_outputfiles', 'id','status'])
        logger.debug("Unsetting Splitter")
        j.splitter = None
        logger.debug("Setting InputData")
        if isType(job.inputdata, LHCbCompressedDataset):
            newDs = LHCbCompressedDataset(datatmp[:])
        else:
            newDs = LHCbDataset(files=datatmp[:],
                                  persistency=self.persistency,
                                  depth=self.depth)
        j.inputdata = newDs
        logger.debug("Returning new subjob")
        return j

    @require_credential
    def _splitter(self, job, inputdata):
        """
        This is the main method used in splitting by inputdata for Dirac backends

        Args:
            job (Job): This is the master-job object which contains everything we need to split by inputdata
            inputdata (list): List of (DiracFile) objects which we're to use in the splitting

        Returns:
            outdata (generator): a splitter Generator

        """

        logger.debug("_splitter")

        if isType(inputdata, LHCbCompressedDataset):
            indata = inputdata.getFullDataset()
        else:
            indata = inputdata

        if indata is not None:

            self.depth = indata.depth
            self.persistency = indata.persistency
            self.XMLCatalogueSlice = indata.XMLCatalogueSlice

        else:

            self.depth = 0
            self.persistency = None
            self.XMLCatalogueSlice = None

        if stripProxy(job.backend).__module__.find('Dirac') > 0:
            bannedSites = []
            if 'BannedSites' in job.backend.settings:
                bannedSites = job.backend.settings['BannedSites']
            logger.debug("found Dirac backend")

            if self.filesPerJob > 100:
                self.filesPerJob = 100  # see above warning
            logger.debug("indata: %s " % str(indata))

            if self.splitterBackend == "GangaDiracSplitter":
                from GangaDirac.Lib.Splitters.GangaSplitterUtils import GangaDiracSplitter
                outdata = GangaDiracSplitter(indata,
                                             self.filesPerJob,
                                             self.maxFiles,
                                             self.ignoremissing)
            elif self.splitterBackend == "OfflineGangaDiracSplitter":
                from GangaDirac.Lib.Splitters.OfflineGangaDiracSplitter import OfflineGangaDiracSplitter
                outdata = OfflineGangaDiracSplitter(indata,
                                                    self.filesPerJob,
                                                    self.maxFiles,
                                                    self.ignoremissing,
                                                    bannedSites)
            elif self.splitterBackend == "splitInputDataBySize":
                from GangaLHCb.Lib.Splitters.LHCbSplitterUtils import DiracSizeSplitter
                outdata = DiracSizeSplitter(indata,
                                            self.filesPerJob,
                                            self.maxFiles,
                                            self.ignoremissing)
            elif self.splitterBackend == "splitInputData":
                indata = stripProxy(copy.deepcopy(inputdata))
                from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
                outdata = DiracSplitter(indata,
                                        self.filesPerJob,
                                        self.maxFiles,
                                        self.ignoremissing)
            else:
                raise SplitterError("Backend algorithm not selected!")

            logger.debug("outdata: %s " % str(outdata))
            return outdata
        #If we are not running the jobs on Dirac but are using DiracFiles we want some of the same checks
        elif all(isinstance(this_file, DiracFile) for this_file in indata):
            from GangaDirac.Lib.Splitters.OfflineGangaDiracSplitter import OfflineGangaDiracSplitter
            outdata = OfflineGangaDiracSplitter(indata,
                                              self.filesPerJob,
                                              self.maxFiles,
                                              self.ignoremissing)
            return outdata
        else:
            logger.debug("Calling Parent Splitter as not on Dirac")
            return super(SplitByFiles, self)._splitter(job, indata)

    @require_credential
    def split(self, job):
        """
        This is the main method which will split the given job

        Args:
            job(Job): This is the master job which will be used to split based upon the inputdata

        Return:
            split_return(list): This is the list of subjobs created from the input job
        """
        logger.debug("split")
        if self.maxFiles == -1:
            self.maxFiles = None
        if self.bulksubmit == True:
            if stripProxy(job.backend).__module__.find('Dirac') > 0:
                logger.debug("Returning []")
                return []
        split_return = super(SplitByFiles, self).split(job)
        logger.debug("split_return: %s" % split_return)
        return split_return

