from GangaGaudi.Lib.Splitters.GaudiInputDataSplitter import GaudiInputDataSplitter
#from GangaGaudi.Lib.Splitters.SplitterUtils import DatasetSplitter
from GangaDirac.Lib.Splitters.GangaSplitterUtils import GangaDiracSplitter
from GangaDirac.Lib.Splitters.SplitterUtils import DiracSplitter
#from SplitterUtils import DiracSplitter
from GangaDirac.Lib.Files.DiracFile import DiracFile
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
    _schema.datadict['bulksubmit']    = SimpleItem(defvalue=False,
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
        logger.debug( "_create_subjob" )
        datatmp = []
        #print dataset
        #print type(dataset)
        #print type([])
        #try:
        #    logger.debug( "dataset len: %s" % str(len(dataset)) )
        #except:
        #    pass
        #from Ganga.GPI import GangaList
        from Ganga.GPIDev.Lib.GangaList import GangaList

        if isinstance( dataset, LHCbDataset ):
            for i in dataset:
                if isinstance( i, DiracFile ):
                    datatmp.append( i )
                else:
                    logger.error( "Unkown file-type %s, cannot perform split with file %s" % ( type(i), str(i) ) )
                    from Ganga.Core.exceptions import GangaException
                    raise GangaException( "Unkown file-type %s, cannot perform split with file %s" % ( type(i), str(i) ) )
        elif type(dataset) == type( [] ) or type(dataset) == type(GangaList()):
            for i in dataset:
                if type(i) == type(''):
                    datatmp.append( DiracFile( lfn=i ) )
                elif type(i) == type( DiracFile() ):
                    datatmp.append( i )
                else:
                    x = GangaException( "Unknown(unexpected) file object: %s" % i )
                    raise x
        elif type(dataset) == type( '' ):
            datatmp.append( DiracFile( lfn=dataset ) )
        else:
            logger.error( "Unkown dataset type, cannot perform split here" )
            from Ganga.Core.exceptions import GangaException
            logger.error( "Dataset found: " + str(dataset) )
            raise GangaException( "Unkown dataset type, cannot perform split here" )

        logger.debug( "Creating new Job in Splitter" )
        j=Job()
        j.copyFrom(stripProxy(job))
        j.splitter = None
        j.merger = None
        j.inputsandbox = [] ## master added automatically
        j.inputfiles = []
        j.inputdata = LHCbDataset( files             = datatmp[:],
                                   persistency       = self.persistency,
                                   depth             = self.depth )
        j.inputdata.XMLCatalogueSlice = self.XMLCatalogueSlice

        return j


    # returns splitter generator 
    def _splitter(self, job, inputdata):

        logger.debug( "_splitter" )

        indata = stripProxy(copy.deepcopy(job.inputdata))

        #logger.debug( indata )
        logger.debug( "indata length: %s" % str( indata ) )

        if not job.inputdata:
            logger.debug( "no job.inputdata" )
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

            logger.debug( "found Dirac backend" )

            if self.filesPerJob > 100: self.filesPerJob = 100 # see above warning
            logger.debug( "indata: %s " % str( indata ) )

            from Ganga.Utility.Config import getConfig
            if not getConfig('LHCb')['useGangaDiracSplitter']:
                outdata = DiracSplitter(indata,
                                     self.filesPerJob,
                                     self.maxFiles,
                                     self.ignoremissing)
            else:
                outdata = GangaDiracSplitter(indata,
                                            self.filesPerJob,
                                            self.maxFiles,
                                            self.ignoremissing)

            #print outdata
            #exit(0)

            logger.debug( "outdata: %s " % str( outdata ) )
            return outdata
        else:
            logger.debug( "Calling Parent Splitter as not on Dirac" )
            return super(SplitByFiles,self)._splitter(job, indata)


    def split(self, job):
        logger.debug( "split" )
        if self.maxFiles == -1:
            self.maxFiles = None
        if self.bulksubmit == True:
            if stripProxy(job.backend).__module__.find('Dirac') > 0:
                logger.debug( "Returning []" )
                return []
        split_return = super(SplitByFiles,self).split(job)
        logger.debug( "split_return: %s" % split_return )
        return split_return

