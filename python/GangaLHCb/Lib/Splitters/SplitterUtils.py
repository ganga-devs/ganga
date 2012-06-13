from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaLHCb.Lib.LHCbDataset import LHCbDataset, LogicalFile
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
from GangaLHCb.Lib.DIRAC.Dirac import Dirac
from GangaLHCb.Lib.DIRAC.DiracUtils import *
import pickle
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

def DiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    inputs.files = inputs.files[:maxFiles]
    files = []
    for f in inputs.files:
        if isLFN(f): files.append(f.name)
        if maxFiles > 0 and len(files) >= maxFiles: break
    cmd = 'result = DiracCommands.splitInputData(%s,%d)' \
          % (files,filesPerJob)
    result = Dirac.execAPI(cmd)
    if not result_ok(result):
        logger.error('Error splitting files: %s' % str(result))
        raise SplittingError('Error splitting files.')
    split_files = result.get('Value',[])
    if len(split_files) == 0:
        raise SplittingError('An unknown error occured.')
    datasets = []
    # check that all files were available on the grid
    big_list = []
    for l in split_files: big_list.extend(l)
    diff = set(inputs.getFileNames()).difference(big_list)
    if len(diff) > 0:            
        for f in diff: logger.warning('Ignored file: %s' % f)
        if not ignoremissing:
            raise SplittingError('Some files not found!')

    for l in split_files:
        dataset = []
        for file in l:
            dataset.append(LogicalFile(file))
        yield dataset
