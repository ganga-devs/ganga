from GangaGaudi.Lib.Splitters.GaudiInputDataSplitter import GaudiInputDataSplitter
#from GangaGaudi.Lib.Splitters.SplitterUtils import DatasetSplitter
from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
#from SplitterUtils import DiracSplitter
from GangaLHCb.Lib.LHCbDataset import LogicalFile
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from Ganga.GPIDev.Schema import *
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

class SplitByFiles(GaudiInputDataSplitter):
    """Splits a job into sub-jobs by partitioning the input data

    SplitByFiles can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitByFiles'
    _schema = GaudiInputDataSplitter._schema.inherit_copy()
    _schema.datadict['bulksubmit']    = SimpleItem(defvalue=True,
                                                   doc='determines if subjobs are split '\
                                                   'server side in a "bulk" submission or '\
                                                   'split locally and submitted individually')
    _schema.datadict['ignoremissing'] = SimpleItem(defvalue=False,
                                                   doc='Skip LFNs if they are not found ' \
                                                   'in the LFC. This option is only used if' \
                                                   'jobs backend is Dirac')

        



    def _attribute_filter__set__(self, name, value):
        if name is 'filesPerJob':
            if value >100:
                logger.warning('filesPerJob exceeded DIRAC maximum')
                logger.warning('DIRAC has a maximum dataset limit of 100.')
                logger.warning('BE AWARE!... will set it to this maximum value at submit time if backend is Dirac')
        return value

    def _create_subjob(self, job, dataset):
        if True in (isinstance(i,str) for i in dataset):
            dataset = [LogicalFile(file) for file in dataset]
        j=Job()
        j.copyFrom(stripProxy(job))
        j.splitter = None
        j.merger = None
        j.inputsandbox = [] ## master added automatically
        j.inputdata = LHCbDataset( files             = dataset[:],
                                   persistency       = self.persistency,
                                   depth             = self.depth )
        j.inputdata.XMLCatalogueSlice = self.XMLCatalogueSlice

        return j


    # returns splitter generator 
    def _splitter(self, job, inputdata):

        indata = stripProxy(copy.deepcopy(job.inputdata))
        if not job.inputdata:
            share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                      'shared',
                                      getConfig('Configuration')['user'],
                                      job.application.is_prepared.name,
                                      'inputdata',
                                      'options_data.pkl')
            if os.path.exists(share_path):
                f=open(share_path,'r+b')
                indata = pickle.load(f)
                f.close()
            else:
                logger.error('Cannot split if no inputdata given!')
                raise SplittingError('job.inputdata is None and no inputdata found in optsfile')         


        self.depth             = indata.depth
        self.persistency       = indata.persistency
        self.XMLCatalogueSlice = indata.XMLCatalogueSlice

        if stripProxy(job.backend).__module__.find('Dirac') > 0:
            if self.filesPerJob > 100: self.filesPerJob = 100 # see above warning
            return DiracSplitter(indata,
                                 self.filesPerJob,
                                 self.maxFiles,
                                 self.ignoremissing)
        else:
            return super(SplitByFiles,self)._splitter(job, indata)


    def split(self, job):
        if self.maxFiles == -1: self.maxFiles = None
        if self.bulksubmit and stripProxy(job.backend).__module__.find('Dirac') > 0:
            return []
        return super(SplitByFiles,self).split(job)
