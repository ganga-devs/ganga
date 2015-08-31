from Ganga.GPIDev.Adapters.ISplitter import SplittingError
#from GangaDirac.BOOT                    import dirac_ganga_server
from GangaDirac.Lib.Utilities.DiracUtilities import execute
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from Ganga.Utility.Config import getConfig
from GangaDirac.Lib.Files import DiracFile
from Ganga.Utility.logging import getLogger
logger = getLogger()


def igroup(iterable, num, leftovers=False):
    '''
    Generator producing sequential groups of size num from input iterable
    '''
    size = len(iterable)
    for i in xrange(0, size, num):
        if i + num > size and not leftovers:
            return
        yield iterable[i: i + num]


def DiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """
    #logger.debug( "DiracSplitter" )
    #logger.debug( "inputs: %s" % str( inputs ) )
    split_files = []
    i = inputs.__class__()

    if len(inputs.getLFNs()) != len(inputs.files):
        raise SplittingError("Error trying to split dataset using DIRAC backend with non-DiracFile in the inputdata")

    all_files = igroup(inputs.files[:maxFiles], getConfig('DIRAC')['splitFilesChunks'], leftovers=True)

    #logger.debug( "Looping over all_files" )
    #logger.debug( "%s" % str( all_files ) )

    for files in all_files:

        i.files = files

        LFNsToSplit = i.getLFNs()

        if(len(LFNsToSplit)) > 1:

            result = execute('splitInputData(%s, %d)' % (i.getLFNs(), filesPerJob))

            if not result_ok(result):
                logger.error('DIRAC:: Error splitting files: %s' % str(result))
                raise SplittingError('Error splitting files.')

            split_files += result.get('Value', [])

        else:

            split_files = [LFNsToSplit]

    if len(split_files) == 0:
        raise SplittingError('An unknown error occured.')

    # FIXME
    # check that all files were available on the grid
    big_list = []
    for l in split_files:
        big_list.extend(l)
    diff = set(inputs.getFileNames()[:maxFiles]).difference(big_list)
    if len(diff) > 0:
        for f in diff:
            logger.warning('Ignored file: %s' % f)
        if not ignoremissing:
            raise SplittingError('Some files not found!')
    ###

    logger.debug("Split Files: %s" % str(split_files))

    for _dataset in split_files:
        dataset = []
        for _lfn in _dataset:
            dataset.append(DiracFile(lfn=_lfn))
        yield dataset
