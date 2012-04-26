from GangaGaudi.Lib.Splitters.GaudiInputDataSplitter import GaudiInputDataSplitter
from Ganga.GPIDev.Adapters.ISplitter import ISplitter, SplittingError
from SplitterUtils import DiracSplitter
from GangaGaudi.Lib.Splitters.SplitterUtils import DatasetSplitter
from Ganga.GPIDev.Schema import *
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
import os
import copy

class SplitByFiles(GaudiInputDataSplitter):
    """Splits a job into sub-jobs by partitioning the input data

    SplitByFiles can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitByFiles'
    _schema = GaudiInputDataSplitter._schema.inherit_copy()
    _schema.datadict['ignoremissing'] = SimpleItem(defvalue=False,
                                                   doc='Skip LFNs if they are not found ' \
                                                   'in the LFC. This option is only used if' \
                                                   'jobs backend is Dirac')

        



    def _attribute_filter__set__(self, name, value):
        if name is 'filesPerJob':
            if value >100:
                logger.warning( 'filesPerJob exceeded DIRAC maximum' )
                logger.warning( 'DIRAC has a maximum dataset limit of 100.' )
                logger.warning( 'BE AWARE!... will set it to this maximum value ' \
                                'at submit time if backend is Dirac')
        return value

    def _create_subjob( self,
                        job,
                        dataset,
                        persistency,
                        depth,
                        metadata,
                        XMLCatalogueSlice ):
        
        j=copy.deepcopy(job)
        j.inputdata = LHCbDataset( files             = dataset,
                                   persistency       = persistency,
                                   depth             = depth )
        j.inputdata.metadata          = metadata
        j.inputdata.XMLCatalogueSlice = XMLCatalogueSlice

        return j


    def split(self, job):
        if self.maxFiles == -1: self.maxFiles = None

        indata = copy.deepcopy(job.inputdata)
        if not job.inputdata:
            share_path = os.path.join(job.application.is_prepared.name,'inputdata','options_data.pkl')
            if os.path.exists(share_path):
                f=open(share_path,'r+b')
                indata = pickle.load(f)
                f.close()
            else:
                logger.error('Cannot split if no inputdata given!')
                raise SplittingError( 'job.inputdata is None and no inputdata '\
                                      'found in optsfile' )         


        splitter = None
        if job.backend.__module__.find('Dirac') > 0:
            if self.filesPerJob > 100: self.filesPerJob = 100 # see above warning
            splitter =  DiracSplitter(indata,
                                      self.filesPerJob,
                                      self.maxFiles,
                                      self.ignoremissing)
        else:
            splitter =  DatasetSplitter(indata, self.filesPerJob, self.maxFiles)

        subjobs = []
        for dataset in splitter:
            subjobs.append(self._create_subjob( job,
                                                dataset,
                                                indata.persistency,
                                                indata.depth,
                                                indata.metadata,
                                                indata.XMLCatalogueSlice) )

        return subjobs
