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
    size = len(iterable)
    for i in xrange(0, size, num):
        if i+num > size and not leftovers:
            return
        yield iterable[i : i+num]

def DiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """
    split_files = []
    i=inputs.__class__()

    all_files = igroup( inputs.files[:maxFiles], getConfig('DIRAC')['splitFilesChunks'],
                        leftovers=True )

    #import traceback
    #traceback.print_stack()

    for files in all_files:

        i.files = files

        LFNsToSplit = i.getLFNs()

        #print "LFNsToSplit: " + str(LFNsToSplit)

        if( len( LFNsToSplit ) ) > 1:

            logger.debug( "Splitting inputData" )

            result = execute('splitInputData(%s,%d)'\
                             % ( i.getLFNs(), filesPerJob ) )

            if not result_ok(result):
                logger.error('DIRAC:: Error splitting files: %s' % str(result))
                raise SplittingError('Error splitting files.')

            split_files += result.get( 'Value', [] )

        else:

            logger.debug( "Don't try to split a single file" )

            split_files = [ LFNsToSplit ]

    if len(split_files) == 0:
        raise SplittingError('An unknown error occured.')

    # FIXME
    # check that all files were available on the grid
    big_list = []
    for l in split_files: big_list.extend(l)
    diff = set(inputs.getFileNames()[:maxFiles]).difference(big_list)
    if len(diff) > 0:            
        for f in diff: logger.warning('Ignored file: %s' % f)
        if not ignoremissing:
            raise SplittingError('Some files not found!')
    ###

    logger.debug( "Split Files: %s" % str(split_files) )
    #print split_files

    for dataset in split_files:
        yield dataset

