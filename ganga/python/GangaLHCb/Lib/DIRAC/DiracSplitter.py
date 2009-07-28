#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
''' Splitter for DIRAC jobs. '''

from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
import Ganga.Utility.logging
from GangaLHCb.Lib.Gaudi.Splitters import SplitByFiles
from Dirac import Dirac
from DiracUtils import *

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracSplitter(SplitByFiles):
    """Query the LFC, via Dirac, to find optimal data file grouping.

    This Splitter will query the Logical File Catalog (LFC) to find
    at which sites a particular file is stored. Subjobs will be created
    so that all the data required for each subjob is stored in
    at least one common location. This prevents the submission of jobs that
    are unrunnable due to data availability.
    """

    _name = 'DiracSplitter'
    _schema = Schema(Version(1,0),{
        'filesPerJob' : SimpleItem(defvalue=10,
                                   doc='Number of files per subjob'),
        'maxFiles':SimpleItem(defvalue=-1,
                              doc='Maximum number of files to use in ' \
                              'a masterjob. A value of "-1" means all files'),
        'ignoremissing' : SimpleItem(defvalue=False,
                                     doc='Skip LFNs if they are not found ' \
                                     'in the LFC.')
        })
    
    def _splitFiles(self, inputs):
        files = []
        for f in inputs.files:
            if f.isLFN(): files.append(f.name[4:])
            if self.maxFiles > 0 and len(files) >= self.maxFiles: break
        cmd = 'result = DiracCommands.splitInputData(%s,%d)' \
              % (files,self.filesPerJob)
        result = Dirac.execAPI(cmd)
        if not result_ok(result):
            logger.error('Error splitting files: %s' % str(result))
            raise SplittingError('Error splitting files.')
        split_files = result.get('Value',[])
        if len(split_files) == 0:
            raise SplittingError('An unknown error occured.')
        datasets = []
        # check that all files were available on the grid
        for file in inputs.files:
            found = False
            for list in split_files:
                for f in list:
                    if file.name.find(f) >= 0:
                        found = True
                        break
            if not found:
                if self.ignoremissing:
                    logger.warning('Ignored file %s' % file.name)
                else: raise SplittingError('File not found: %s' % file.name)
        
        for list in split_files:
            dataset = LHCbDataset()
            dataset.datatype_string = inputs.datatype_string
            dataset.depth = inputs.depth
            dataset.cache_date = inputs.cache_date
            for file in list:
                for dfile in inputs.files:
                    if dfile.name.find(file) >= 0:
                        dataset.files.append(dfile)
                        break
            datasets.append(dataset)
        return datasets
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
