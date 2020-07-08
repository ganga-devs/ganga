from GangaCore.Core.exceptions import SplitterError
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger
from GangaDirac.Lib.Utilities.DiracUtilities import execute, GangaDiracError
from GangaCore.Core.GangaThread.WorkerThreads import getQueues
from GangaDirac.Lib.Files.DiracFile import DiracFile
from copy import deepcopy
import random
import time
import math

"""
    This is the Ganga DIRAC splitter
    (questions to rcurrie@cern.ch)

    This splitter follows the logical progression described below to try to optimally split
    a list of files such that the mininum number of total jobs are created.

    Pre-Splitting:

    1. For a given set of files determine the valid SE and CE we can use
    2. Construct a mapping between these.
        This allows us to answer:
            For a given SE which CE can access it?
            For a given CE which SE can we access?

    3. Get a full list of all of the replicas for all files against all of the valid SE
        This is attempted to be done in large chunks goverened by LFN_parallel_limit
        Requesting all replicas for >3,000 files can cause timeouts and other problems
        I opted to reduce this and run mulitple queries in parallel to speed this up.

    4. Keep track of all LFN with no valid replicas.
        Inform the user of them and remove them from the splitting if requested.


    Splitting the files into subsets:

    For the next LFN in the list:

        How many common SE do we want to be able to submit this job to?
            If there are multiple replicas of the data on the grid we can submit jobs
            such that there is maximum failover if a site goes into downtime.
            If the user requests uniqueSE the possible CE where the job can run will
            try to share the same SE.

        Select randomly the given maximum number of SE up to the common-SE amount
            (if available)

        How many files per subjob do we want as a maximum?
            (More files == Less jobs == Longer job CPUtime)

        Loop through all of the LFN and attempt to assemble LFN subsets which are >= 0.75
        the amount of LFN the user requested per subjob.

        Once we have a subset with the required number of LFN which are at the required
        number of SE add this subset to the list of used LFN and continue to the next
        unallocated LFN.

        
        So on the first iteration of the splitter we identify how many files we can put
        into a larger subset of files.
            i.e. how many jobs >= 0.75 * 100LFN and <= 100LFN with 2 site redundancy
            can we construct?

    After the list has been run through once we lower the size of the subset to construct.
        Run through the previous logic again and attempt to construct smaller LFN subset.
        (This is not currently configurable and is set to 0.75 but in principle this can be
        configured)

        i.e. for the next iteration of the LFN accept subsets
        >= 0.75* 0.75 * 100  in size

        next iteration over all of the LFN this looks for subsets
        >= 0.75 * 0.75 * 0.75 * 100 in size

    Keep going with multiple iterations until the acceptable size of the subset has dropped
    to 50% of the originally required number of jobs per site. (accepted < 0.5 * 100)

        Now, reduce the number of sites required for this job to be able to run at
        (if possible) and restart requiring subjobs be >= 0.75 * requested subsetsize again

    Now keep going until we eventually construct subsets of 1 LFN in size and then
    return all subsets




    This favours generating larger subsets with multiple sites where the jobs can run 
    but when there are LFN which can't be allocated to sites with multiple SE the algorithm
    will attempt to find larger subsets with reduced redundancy.
"""



configDirac = getConfig('DIRAC')
logger = getLogger()

global_random = random

LFN_parallel_limit = 2500.
def wrapped_execute(command, expected_type, new_subprocess = False):
    """
    A wrapper around execute to protect us from commands which had errors
    Args:
        command (str): This is the command to be exectuted against DIRAC
        expected_type (type): This is the type of the object which is returned from DIRAC
    """
    try:
        result = execute(command, new_subprocess)
        assert isinstance(result, expected_type)
    except AssertionError:
        raise SplitterError("Output from DIRAC expected to be of type: '%s', we got the following: '%s'" % (expected_type, result))
    except GangaDiracError as err:
        raise SplitterError("Error from Dirac: %s" % err)
    return result


def find_random_site(original_SE_list, banned_SE):
    """
    Find a random element from a python list that isn't in a given banned list
    This is used for selecting a site element but it doesn't matter which is used

    Args:
        original_SE_list (list): This is a list of given 'SE'. The same SE may appear more than once!
        banned_SE (list): This is a list of SE which are 'banned' from being selected
    """
    input_list = deepcopy(original_SE_list)
    chosen_element = ""

    while chosen_element == "" and len(input_list) > 0:
        global global_random
        this_element = global_random.sample(input_list, 1)[0]
        if not this_element in banned_SE:
            chosen_element = this_element
            break
        else:
            input_list.remove(this_element)

    return chosen_element


def getLFNReplicas(allLFNs, index, allLFNData):
    """
    This method gets the location of all replicas for 'allLFNs' and stores the infomation in 'allLFNData'

    This is a 'static' method which is called multiple times with the same 'allLFNs' and 'allLFNData' and different index.
    e.g. This allows Dirac to determine the replicas for ~250LFN all at once rather than for ~40,000 all at once which risks timeouts and other errors

    Args:
        allLFNs (list): This is a list of all LFN which have replicas on the grid
        index (int): This is used to determine which slice of LFNs we want to look at
        allLFNData (dict): This is a dict where the replica information is to be stored temporarily
    """
    output = None

    global LFN_parallel_limit

    this_min = int(index * LFN_parallel_limit)

    if (index + 1) * LFN_parallel_limit > len(allLFNs):
        this_max = len(allLFNs)
    else:
        this_max = int((index + 1) * LFN_parallel_limit)

    try:
        output = wrapped_execute('getReplicasForJobs(%s)' % str(allLFNs[this_min:this_max]), dict, new_subprocess = True)
    except SplitterError:
        logger.error("Failed to Get Replica Info: [%s:%s] of %s" % (str(this_min), str(this_max), len(allLFNs)))
        raise

    import GangaCore.Runtime.Repository_runtime
    GangaCore.Runtime.Repository_runtime.updateLocksNow()

    allLFNData[index] = output

    logger.info("Got Replica Info: [%s:%s] of %s" % (str(this_min), str(this_max), len(allLFNs)))


def generate_site_selection(input_site, wanted_common_SE, uniqueSE, CE_to_SE_mapping, SE_to_CE_mapping):
    """
    Return a set of sites which are a subset of the given input_site.
    The size of the returned set is determined by 'wanted_common_SE'.
    'uniqueSE' governs if all elements in the returned set should be accessing uniqueSE or not.

    Args:
        input_site (list): list of input sites
        wanted_common_SE (int): the size of the subset we want
        uniqueSE (bool): Should returned sites be able to share the same SE
        CE_to_SE_mapping (dict): Dict which has sites as keys and SE as values
        SE_to_CE_mapping (dict): Dict which has sites as values and SE as keys
    """

    req_sitez = set([])
    if len(input_site) > wanted_common_SE:
        used_SE = set([])

        # Loop through the possible CE which we want to 
        for _ in range(wanted_common_SE):
            this_SE = find_random_site(input_site, used_SE)
            req_sitez.add(this_SE)

            # If we're demanding each CE definately not share an SE
            if uniqueSE:
                # Which CE can map to the SE
                these_CE = SE_to_CE_mapping[this_SE]
                for this_CE in these_CE:
                    # For each CE make sure we add the SE it can see to the used list
                    for SE in CE_to_SE_mapping[this_CE]:
                        used_SE.add(SE)

            # Just use any SE
            else:
                used_SE.add(this_SE)
    else:
        for s in input_site:
            req_sitez.add(s)
    return req_sitez


def calculateSiteSEMapping(file_replicas, uniqueSE, CE_to_SE_mapping, SE_to_CE_mapping, bannedSites, ignoremissing, bad_lfns):
    """
    If uniqueSE:
        This constructs 2 dicts which allow for going between SE and sites based upon a key/value lookup.
        This generates this knowledge from looping through the LFN replicas at given sites and given SE.
    else:
        Don't construct a site<->SE mapping as it's not needed

    This returns a dict of which sites each LFN are accessible at

    Args:
        file_replicas (dict): This is the dictionary of LFN replicas with LFN as the key
        CE_to_SE_mapping (dict): Dict which has sites as keys and SE as values
        SE_to_CE_mapping (dict): Dict which has sites as values and SE as keys
        bannedSites (list) : List which has the sites banned by the job
        ignoremissing (bool) : Bool for whether to continue if an LFN has no available SEs
        bad_lfns (list): List of LFN which are known to not have valid replicas on the grid

    Returns:
        site_dict (dict): Dict of {'LFN':set([sites]), ...}
    """

    SE_dict = dict()
    maps_size = 0
    found = []

    logger.info("Calculating site<->SE Mapping")

    # First find the SE for each site - there is a handy DIRAC function that gives us everything quickly
    CE_to_SE_mapping = wrapped_execute('getSESiteMapping()', dict)

    for lfn, repz in file_replicas.items():
        sitez = set([])
        for replica in repz:
            sitez.add(replica)

        SE_dict[lfn] = sitez

    # Remove the banned sites (CE) from the mappings
    for iSE in list(CE_to_SE_mapping.keys()):
        for site in CE_to_SE_mapping[iSE]:
            if any(site == item for item in bannedSites):
                CE_to_SE_mapping[iSE].remove(site)
            if not CE_to_SE_mapping[iSE]:
                del CE_to_SE_mapping[iSE]

    # Now calculate the 'inverse' dictionary of site for each SE
    for SE, sites in CE_to_SE_mapping.items():
        for site_i in sites:
            if site_i not in SE_to_CE_mapping:
                SE_to_CE_mapping[site_i] = set([])
            if SE not in SE_to_CE_mapping[site_i]:
                SE_to_CE_mapping[site_i].add(SE)

    # These can be used to select the site which know of a given SE
    # Or vice versa

    # Now lets generate a dictionary of some chosen site vs LFN to use in
    # constructing subsets
    site_dict = {}
    for lfn, sites in SE_dict.items():
        site_dict[lfn] = set([])
        for site in sites:
            if site in CE_to_SE_mapping.keys():
                for SE in CE_to_SE_mapping[site]:
                    site_dict[lfn].add(SE)
        if site_dict[lfn] == set([]) and not ignoremissing:
            raise SplitterError('LFN %s has no site available and ignoremissing = false! Perhaps you have banned too many sites.' % str(lfn))
        elif site_dict[lfn] == set([]) and ignoremissing:
            logger.warning('LFN %s has no site available and ignoremissing = true! Removing this LFN from the dataset!' % str(lfn))
            del site_dict[lfn]
            bad_lfns.append(lfn)

    if site_dict == {}:
        raise SplitterError('There are no LFNs in the dataset - perhaps you have banned too many sites.')
    else:
        return site_dict


def lookUpLFNReplicas(inputs, ignoremissing):
    """
    This method launches several worker threads to collect the replica information for all LFNs which are given as inputs and stores this in allLFNData
    Args:
        inputs (list): This is a list of input DiracFile which are 
    Returns:
        bad_lfns (list): A list of LFN which have no replica information when querying `getReplicasForJobs` from DIRAC
    """
    allLFNData = {}
    # Build a useful dictionary and list
    allLFNs = [_lfn.lfn for _lfn in inputs]
    LFNdict = dict.fromkeys(allLFNs)
    for _lfn in inputs:
        LFNdict[_lfn.lfn] = _lfn

    # Request the replicas for all LFN 'LFN_parallel_limit' at a time to not overload the
    # server and give some feedback as this is going on
    global LFN_parallel_limit
    for i in range(int(math.ceil(float(len(allLFNs)) / LFN_parallel_limit))):

        getQueues()._monitoring_threadpool.add_function(getLFNReplicas, (allLFNs, i, allLFNData))

    while len(allLFNData) != int(math.ceil(float(len(allLFNs)) / LFN_parallel_limit)):
        time.sleep(1.)
        # This can take a while so lets protect any repo locks
        import GangaCore.Runtime.Repository_runtime
        GangaCore.Runtime.Repository_runtime.updateLocksNow()

    bad_lfns = []

    # Sort this information and store is in the relevant Ganga objects
    updateLFNData(bad_lfns, allLFNs, LFNdict, ignoremissing, allLFNData)

    file_replicas = {}
    for _lfn in LFNdict:
        if not _lfn in bad_lfns:
            file_replicas[_lfn] = LFNdict[_lfn].locations

    # Check if we have any bad lfns
    if bad_lfns and ignoremissing is False:
        logger.error("Errors found getting LFNs:\n%s" % str(bad_lfns))
        raise SplitterError("Error trying to split dataset with invalid LFN and ignoremissing = False")

    return bad_lfns, file_replicas


def updateLFNData(bad_lfns, allLFNs, LFNdict, ignoremissing, allLFNData):
    """
    Method to re-sort the LFN replica data and check for bad LFNs

    Args:
        bad_lfns (list): This is the list which will contain LFNs which have no replicas
        allLFNs (list): List of all of the LFNs in the inputs which have accessible replicas
        LFNdict (dict): dict of LFN to DiracFiles
        ignoremissing (bool): Check if we have any bad lfns
        allLFNData (dict): All LFN replica data
    """

    global LFN_parallel_limit

    for i in range(int(math.ceil(float(len(allLFNs)) / LFN_parallel_limit))):
        output = allLFNData.get(i)

        if output is None:
            msg = "Error getting Replica information from Dirac: [%s,%s]" % ( str(i * LFN_parallel_limit), str((i + 1) * LFN_parallel_limit))
            raise SplitterError(msg)

        # Identify files which have Failed to be found by DIRAC
        results = output
        values = results.get('Successful')

        upper_limit = (i+1)*LFN_parallel_limit
        if upper_limit > len(allLFNs):
            upper_limit = len(allLFNs)

        #logger.debug("Updating LFN Physical Locations: [%s:%s] of %s" % (str(i * LFN_parallel_limit), str(upper_limit), str(len(allLFNs))))

        for this_lfn in values.keys():
            #logger.debug("LFN: %s" % str(this_lfn))
            this_dict = {}
            this_dict[this_lfn] = values.get(this_lfn)

            if this_lfn in LFNdict:
                #logger.debug("Updating RemoteURLs")
                LFNdict[this_lfn]._updateRemoteURLs(this_dict)
                #logger.debug("This_dict: %s" % str(this_dict))
            else:
                logger.error("Error updating remoteURLs for: %s" % str(this_lfn))

            # If we find NO replicas then also mark this as bad!
            if list(this_dict[this_lfn].keys()) == []:
                bad_lfns.append(this_lfn)

        for this_lfn in bad_lfns:
            logger.warning("LFN: %s was either unknown to DIRAC or unavailable, Ganga is ignoring it!" % str(this_lfn))
            if this_lfn in LFNdict:
                del LFNdict[this_lfn]
            if this_lfn in allLFNs:
                allLFNs.remove(this_lfn)


# Actually Do the work of the splitting

def OfflineGangaDiracSplitter(_inputs, filesPerJob, maxFiles, ignoremissing, bannedSites=[]):
    """
    Generator that yields a datasets for dirac split jobs

    Args:
        _inputs (list): This is a list of input DiracFile objects
        filesPerJob (int): Max files per jobs as defined by splitter
        maxFiles (int): This is the max number of files per subset(subjob)
        ignoremissing (bool): Should we ignore missing LFN
        bannedSites (list): List of banned sites of which the SEs will not be used

    Yields:
        dataset (list): A list of LFNs for each subset(subjob)
    """

    if maxFiles is not None and maxFiles > 0:
        inputs = _inputs[:maxFiles]
    else:
        inputs = _inputs

    # First FIND ALL LFN REPLICAS AND SE<->SITE MAPPINGS AND STORE THIS IN MEMORY
    # THIS IS DONE IN PARALLEL TO AVOID OVERLOADING DIRAC WITH THOUSANDS OF
    # REQUESTS AT ONCE ON ONE CONNECTION

    wanted_common_site = configDirac['OfflineSplitterMaxCommonSites']
    uniqueSE = configDirac['OfflineSplitterUniqueSE']

    if inputs is None:
        raise SplitterError("Cannot Split Job as the inputdata appears to be None!")

    if len(inputs.getLFNs()) != len(inputs.files):
        raise SplitterError("Error trying to split dataset using DIRAC backend with non-DiracFile in the inputdata")

    file_replicas = {}

    logger.info("Requesting LFN replica info")

    # Perform a lookup of where LFNs are all stored
    bad_lfns, file_replicas = lookUpLFNReplicas(inputs, ignoremissing)

    logger.info("Got all good replicas")

    # This contains information on the mapping between CE and SE(site) in DIRAC as multiple CE may access an SE(site)
    CE_to_SE_mapping = {}
    # This contains the mapping between the SE and the CE
    SE_to_CE_mapping = {}

    allSubSets = []

    # Now lets generate a dictionary of some chosen site vs LFN to use in
    # constructing subsets
    site_dict = calculateSiteSEMapping(file_replicas, uniqueSE, CE_to_SE_mapping, SE_to_CE_mapping, bannedSites, ignoremissing, bad_lfns)

    allChosenSets = {}
    # Now select a set of site to use as a seed for constructing a subset of
    # LFN
    for lfn in site_dict.keys():
        allChosenSets[lfn] = generate_site_selection(site_dict[lfn], wanted_common_site, uniqueSE, CE_to_SE_mapping, SE_to_CE_mapping)

    logger.debug("Found all SE in use")


    #logger.info("%s" % str(CE_to_SE_mapping))
    #logger.info("%s" % str(SE_to_CE_mapping))

    # BELOW IS WHERE THE ACTUAL SPLITTING IS DONE

    logger.info("Calculating best data subsets")

    allSubSets = performSplitting(site_dict, filesPerJob, allChosenSets, wanted_common_site, uniqueSE, CE_to_SE_mapping, SE_to_CE_mapping)

    avg = 0.
    for this_set in allSubSets:
        avg += float(len(this_set))
    avg /= float(len(allSubSets))

    logger.info("Average Subset size is: %s" % (str(avg)))

    # FINISHED SPLITTING CHECK!!!
    check_count = 0
    for i in allSubSets:
        check_count = check_count + len(i)

    if check_count != len(inputs) - len(bad_lfns):
        logger.error("SERIOUS SPLITTING ERROR!!!!!")
        logger.warning("%s != %s - %s" % (check_count, len(inputs), len(bad_lfns)))
        logger.warning("inputs:\n%s" % str(inputs))
        logger.warning("bad_lfns:\n%s" % str(bad_lfns))
        logger.warning("check_count:\n%s" % str(check_count))
        raise SplitterError("Files Missing after Splitting!")
    else:
        logger.info("File count checked! Ready to Submit")

    # RETURN THE RESULT

    logger.info("Created %s subsets" % len(allSubSets))

    for dataset in allSubSets:
        yield dataset

def performSplitting(site_dict, filesPerJob, allChosenSets, wanted_common_site, uniqueSE, CE_to_SE_mapping, SE_to_CE_mapping):
    """
    This is the main method which loops through the LFNs and creates subsets which are returned a list of list of LFNs

    Args:
        site_dict (dict): This is a dict with LFNs as keys and sites for each LFN as value
        filesPerJob (int): Max files per jobs as defined by splitter
        allChosenSets (dict): A dict with LFNs as keys and a sub-set of sites where each LFN is replicated
        wanted_common_site (int): Number of sites which we want to have in common for each LFN

        uniqueSE (bool): Should we check to make sure CE don't share an SE
        CE_to_SE_mapping (dict): Dict which has CE as keys and SE as values
        SE_to_CE_mapping (dict): Dict which has CE as values and SE as keys

    Returns:
        allSubSets (list): Return a list of subsets each subset being a list of LFNs
    """

    good_fraction = configDirac['OfflineSplitterFraction']
    bad_fraction = 1.0
    iterative_limit = configDirac['OfflineSplitterLimit']

    allSubSets = []

    iterations = 0
    # Loop over all LFNs
    while len(site_dict) > 0:

        # LFN left to be used
        # NB: Can't modify this list and iterate over it directly in python
        LFN_instances = list(site_dict.keys())
        # Already used LFN
        chosen_lfns = set()

        for iterating_LFN in LFN_instances:

            # If this has previously been selected lets ignore it and move on
            if iterating_LFN in chosen_lfns:
                continue

            # Use this seed to try and construct a subset
            req_sitez = allChosenSets[iterating_LFN]
            _this_subset = []

            if len(req_sitez) < wanted_common_site:
                continue

            limit = int(math.floor(float(filesPerJob) * good_fraction))
                     
            max_limit = int(math.ceil(float(filesPerJob) * bad_fraction))

            # Construct subset
            # Starting with i, populate subset with LFNs which have an
            # overlap of at least 2 SE
            for this_LFN in LFN_instances:
                if this_LFN in chosen_lfns:
                    continue
                if req_sitez.issubset(site_dict[this_LFN]):
                    if len(_this_subset) >= min(filesPerJob, max_limit):
                        break
                    _this_subset.append(this_LFN)

            # If subset is too small throw it away
            if len(_this_subset) < limit and len(_this_subset) < max_limit:
                #logger.debug("%s < %s" % (str(len(_this_subset)), str(limit)))
                allChosenSets[iterating_LFN] = generate_site_selection(site_dict[iterating_LFN], wanted_common_site, uniqueSE, CE_to_SE_mapping, SE_to_CE_mapping)
                continue
            else:
                #logger.info("found common LFN for: " + str(allChosenSets[iterating_LFN]))
                #logger.info("%s > %s" % (str(len(_this_subset)), str(limit)))
                # else Dataset was large enough to be considered useful
                logger.debug("Generating Dataset of size: %s" % str(len(_this_subset)))
                ## Construct DiracFile here as we want to keep the above combination
                allSubSets.append([DiracFile(lfn=str(this_LFN)) for this_LFN in _this_subset])

                for lfn in _this_subset:
                    site_dict.pop(lfn)
                    allChosenSets.pop(lfn)
                    chosen_lfns.add(lfn)

        # Lets keep track of how many times we've tried this
        iterations = iterations + 1

        #logger.info("Iteration: %s" % iterations)
        #logger.info("%s %s" % (good_fraction, bad_fraction))

        # Can take a while so lets not let threads become un-locked
        import GangaCore.Runtime.Repository_runtime
        GangaCore.Runtime.Repository_runtime.updateLocksNow()

        # If on final run, will exit loop after this so lets try and cleanup
        if iterations >= iterative_limit:

            if good_fraction < 0.2:
                good_fraction = good_fraction * 0.75
                bad_fraction = bad_fraction * 0.75
                iterations = 0
            elif wanted_common_site > 1:
                logger.debug("Reducing Common Site Size")
                wanted_common_site = wanted_common_site - 1
                iterations = 0
                good_fraction = 0.5
                bad_fraction = 0.75
            else:
                good_fraction = good_fraction * 0.75
                #bad_fraction = bad_fraction * 0.75

            logger.debug("good_fraction: %s" % str(good_fraction))

    return allSubSets
