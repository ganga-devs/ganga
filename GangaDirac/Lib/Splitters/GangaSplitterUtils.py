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

def GangaDiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """

    split_files = []
    i=inputs.__class__()

    if len(inputs.getLFNs()) != len( inputs.files ):
        raise SplittingError( "Error trying to split dataset using DIRAC backend with non-DiracFile in the inputdata" )

    for i in inputs:
        i.getMetadata()
        file_replicas[i.lfn] = i.replicas
    #file_replicas = result.get( 'Value', {} )
    #print file_replicas

    from sets import Set
    super_dict = dict()
    for lfn, repz in file_replicas.items():
        sitez=Set([])
        for i in repz:
            sitez.add( i )
            super_dict[ lfn ] = sitez

    allSubSets = []
    allChosenSets = {}

    import random
    for i in super_dict.keys():

        if len(super_dict[i]) > 2:
            req_sitez = Set( random.sample( super_dict[i], 2 ) )
        else:
            req_sitez = Set( super_dict[i] )

        allChosenSets[ i ] = req_sitez


    for i in super_dict.keys():

        req_sitez = allChosenSets[i]
        j = 0
        this_subset = []
        for k in super_dict.keys():
            if req_sitez.issubset( super_dict[k] ):
                j = j+1
                if j>=maxFiles:
                    break
                this_subset.append( k )
                del super_dict[ k ]

        if len( this_subset ) > 0:
            allSubSets.append( this_subset )

    split_files = allSubSets

    logger.debug( "Split Files: %s" % str(split_files) )

    for dataset in split_files:
        yield dataset

