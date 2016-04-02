from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaDirac.Lib.Utilities.DiracUtilities import execute
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from Ganga.Utility.Config import getConfig
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


def GangaDiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """

    split_files = []
    i = inputs.__class__()

    if len(inputs.getLFNs()) != len(inputs.files):
        raise SplittingError(
            "Error trying to split dataset using DIRAC backend with non-DiracFile in the inputdata")

    file_replicas = {}

    from Ganga.Core.GangaThread.WorkerThreads import getQueues

    for i in inputs:
        #logging.debug( "getting metadata: %s" % str(i.lfn) )
        getQueues().add(i.getReplicas)

    logger.info("Requesting LFN replica info")

    # This finds all replicas for all LFNs...
    # This will probably struggle for LFNs which don't exist
    all_lfns = [i.locations for i in inputs]
    while [] in all_lfns:
        import time
        time.sleep(0.5)
        all_lfns = [i.locations for i in inputs]

    logger.info("Got replicas")

    for i in inputs:
        file_replicas[i.lfn] = i.locations
        #logger.info( "%s" % str( i.accessURL() ) )

    logger.debug("found all replicas")

    super_dict = dict()
    for lfn, repz in file_replicas.iteritems():
        sitez = set([])
        for i in repz:
            # print i
            sitez.add(i)
        super_dict[lfn] = sitez

    allSubSets = []
    allChosenSets = {}

    logger.info("Determining overlap")

    import random
    for i in super_dict.keys():

        # Randomly Select 2 SE as the starting point for spliting jobs
        if len(super_dict[i]) > 2:
            req_sitez = set([])
            chosen = random.sample(super_dict[i], 2)
            for s in chosen:
                req_sitez.add(s)
        # Keep the 2 or less SE as the SE of choice
        else:
            req_sitez = set([])
            for s in super_dict[i]:
                req_sitez.add(s)

        allChosenSets[i] = req_sitez

    logger.debug("Found all SE in use")

    Tier1Sites = set([])

    for i in super_dict.keys():

        req_sitez = allChosenSets[i]
        _this_subset = []

        # Starting with i, populate subset with LFNs which have an
        # overlap of at least 2 SE

        for k in super_dict.keys():
            if req_sitez.issubset(super_dict[k]):
                if len(_this_subset) >= filesPerJob:
                    break
                _this_subset.append(str(k))
                super_dict.pop(k)

        if len(_this_subset) > 0:
            allSubSets.append(_this_subset)

    split_files = allSubSets

    logger.info("Created %s subsets" % str(len(split_files)))

    #logger.info( "Split Files: %s" % str(split_files) )

    for dataset in split_files:
        yield dataset
