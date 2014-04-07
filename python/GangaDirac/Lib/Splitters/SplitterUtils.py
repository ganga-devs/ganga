from Ganga.GPIDev.Adapters.ISplitter    import SplittingError
#from GangaDirac.BOOT                    import dirac_ganga_server
from GangaDirac.Lib.Utilities.DiracUtilities import execute
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from Ganga.Utility.Config                    import getConfig
from Ganga.Utility.logging              import getLogger
logger = getLogger()


def igroup(iterable, num, leftovers=False):
    '''
    Generator producing sequential groups of size num from input iterable
    '''
    nsplits = len(iterable)/num
    for i in xrange(nsplits):
        yield iterable[i*nsplits:(i+1)*nsplits]
    if leftovers and len(iterable)%num:
        yield iterable[(i+1)*nsplits:]

def DiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """
    split_files = []
    i=inputs.__class__()
    for files in igroup(inputs.files[:maxFiles],
                        getConfig('DIRAC')['splitFilesChunks'],
                        leftovers=True):
        i.files = files
        result = execute('splitInputData(%s,%d)'\
                         % (i.getLFNs(),filesPerJob)
                         )

        if not result_ok(result):
            logger.error('Error splitting files: %s' % str(result))
            raise SplittingError('Error splitting files.')
        split_files += result.get('Value',[])
    if len(split_files) == 0:
        raise SplittingError('An unknown error occured.')

    # check that all files were available on the grid
    big_list = []
    for l in split_files: big_list.extend(l)
    diff = set(inputs.getFileNames()).difference(big_list)
    if len(diff) > 0:            
        for f in diff: logger.warning('Ignored file: %s' % f)
        if not ignoremissing:
            raise SplittingError('Some files not found!')
    ###
   
    for dataset in split_files:
        yield dataset
        
