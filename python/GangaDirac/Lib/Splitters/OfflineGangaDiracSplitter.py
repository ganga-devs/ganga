from Ganga.Core.exceptions import SplitterError
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from copy import deepcopy
import random

configDirac = getConfig('DIRAC')
logger = getLogger()

global_random = random

LFN_parallel_limit = 250
limit_divide_one = 1. / float(LFN_parallel_limit)

# Find a random element from a python list that isn't in a given banned list
# This is used for selecting a site element but it doesn't matter which is used


def find_random_site(original_SE_list, banned_SE):
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

# This function is used for adding all of the known site for a given SE
# The output is stored in the dictionary site_to_SE_mapping


def addToMapping(SE, site_to_SE_mapping):

    from GangaDirac.Lib.Utilities.DiracUtilities import execute
    result = execute('getSitesForSE("%s")' % str(SE))
    if result.get('OK') != True:
        logger.error("Error getting SE's for site: %s" % str(SE))
        site_to_SE_mapping[SE] = []
    else:
        usable_site = result.get('Value')
        site_to_SE_mapping[SE] = usable_site


def getLFNReplicas(allLFNs, index, allLFNData):

    output = None

    toy_num = 0

    global LFN_parallel_limit

    this_min = index * LFN_parallel_limit

    if (index + 1) * LFN_parallel_limit > len(allLFNs):
        this_max = len(allLFNs)
    else:
        this_max = (index + 1) * LFN_parallel_limit

    for toy_num in range(5):

        try:
            from GangaDirac.Lib.Utilities.DiracUtilities import execute
            output = execute('getReplicas(%s)' % str(allLFNs[this_min:this_max]))
            these_values = output.get('Value').get('Successful')
            break
        except Exception, err:
            logger.error("Dirac Error: %s" % str(err))
            # catch 'Successful' not found and others
            pass

    import Ganga.Runtime.Repository_runtime
    Ganga.Runtime.Repository_runtime.updateLocksNow()

    allLFNData[index] = output

    if toy_num == 4:
        logger.error("Failed to Get Replica Info: [%s:%s] of %s" (
            str(this_min), str(this_max), len(allLFNs)))
    else:
        logger.info("Got Replica Info: [%s:%s] of %s" % (
            str(this_min), str(this_max), len(allLFNs)))

# For a given site select 'wanted_common_site' many site from a given Set
# If uniqueSE is requestd the site selected will NOT share a common SE


def generate_site_selection(input_site, wanted_common_site, uniqueSE, site_to_SE_mapping, SE_to_site_mapping):
    req_sitez = set([])
    if len(input_site) > wanted_common_site:
        used_site = set([])
        for se in range(wanted_common_site):
            this_site = find_random_site(input_site, used_site)
            req_sitez.add(this_site)
            if uniqueSE:

                these_SE = SE_to_site_mapping[this_site]
                for this_SE in these_SE:

                    for site in site_to_SE_mapping[this_SE]:
                        used_site.add(site)
            else:
                used_site.add(this_site)
    else:
        for s in input_site:
            req_sitez.add(s)
    return req_sitez


def calculateSiteSEMapping(file_replicas, wanted_common_site, uniqueSE, site_to_SE_mapping, SE_to_site_mapping):

    SE_dict = dict()
    maps_size = 0
    found = []

    # First find the SE for each site
    for lfn, repz in file_replicas.iteritems():
        sitez = set([])
        for replica in repz:
            sitez.add(replica)
            if not replica in found:

                from Ganga.Core.GangaThread.WorkerThreads import getQueues
                getQueues()._monitoring_threadpool.add_function(addToMapping, (str(replica), site_to_SE_mapping))

                maps_size = maps_size + 1
                found.append(replica)

        SE_dict[lfn] = sitez

    # Doing this in parallel so wait for it to finish
    while len(site_to_SE_mapping) != maps_size:
        import time
        time.sleep(0.1)

    # Now calculate the 'inverse' dictionary of site for each SE
    for _SE, _sites in site_to_SE_mapping.iteritems():
        for site_i in _sites:
            if site_i not in SE_to_site_mapping:
                SE_to_site_mapping[site_i] = set([])
            if _SE not in SE_to_site_mapping[site_i]:
                SE_to_site_mapping[site_i].add(_SE)

    # These can be used to select the site which know of a given SE
    # Or vice versa

    # Now lets generate a dictionary of some chosen site vs LFN to use in
    # constructing subsets
    allSubSets = []
    allChosenSets = {}

    site_dict = {}
    for _lfn, sites in SE_dict.iteritems():
        site_dict[_lfn] = set([])
        for _site in sites:
            for _SE in site_to_SE_mapping[_site]:
                site_dict[_lfn].add(_SE)

    # Now select a set of site to use as a seed for constructing a subset of
    # LFN
    for lfn in site_dict.keys():
        allChosenSets[lfn] = generate_site_selection(
            site_dict[lfn], wanted_common_site, uniqueSE, site_to_SE_mapping, SE_to_site_mapping)

    return site_dict, allSubSets, allChosenSets


def lookUpLFNReplicas(inputs, allLFNData):
    # Build a useful dictionary and list
    allLFNs = []
    LFNdict = {}
    for _lfn in inputs:
        allLFNs.append(_lfn.lfn)
        LFNdict[_lfn.lfn] = _lfn

    # Request the replicas for all LFN 'LFN_parallel_limit' at a time to not overload the
    # server and give some feedback as this is going on
    from GangaDirac.Lib.Utilities.DiracUtilities import execute
    import math
    global limit_divide_one
    for i in range(int(math.ceil(float(len(allLFNs)) * limit_divide_one))):

        from Ganga.Core.GangaThread.WorkerThreads import getQueues
        getQueues()._monitoring_threadpool.add_function(getLFNReplicas, (allLFNs, i, allLFNData))

    while len(allLFNData) != int(math.ceil(float(len(allLFNs)) * limit_divide_one)):
        import time
        time.sleep(1.)
        # This can take a while so lets protect any repo locks
        import Ganga.Runtime.Repository_runtime
        Ganga.Runtime.Repository_runtime.updateLocksNow()

    return allLFNs, LFNdict


def sortLFNreplicas(bad_lfns, allLFNs, LFNdict, ignoremissing, allLFNData, inputs):
    from Ganga.GPIDev.Base.Proxy import stripProxy, isType
    from Ganga.GPIDev.Base.Objects import GangaObject

    myRegistry = {}
    for this_LFN in inputs:
        if isType(this_LFN, GangaObject):
            myRegistry[this_LFN.lfn] = stripProxy(this_LFN)._getRegistry()
            if myRegistry[this_LFN.lfn] is not None:
                myRegistry[this_LFN.lfn].turnOffAutoFlushing()

    try:
        return _sortLFNreplicas(bad_lfns, allLFNs, LFNdict, ignoremissing, allLFNData)
    except Exception as err:
        logger.debug("Sorting Exception: %s" % str(err))
        raise err
    finally:
        for this_LFN in myRegistry.keys():
            if myRegistry[this_LFN] is not None:
                myRegistry[this_LFN].turnOnAutoFlushing()

def _sortLFNreplicas(bad_lfns, allLFNs, LFNdict, ignoremissing, allLFNData):

    import math
    from Ganga.GPIDev.Base.Proxy import stripProxy

    errors = []

    # FIXME here to keep the repo settings as they were before we changed the
    # flush count
    original_write_perm = {}

    global LFN_parallel_limit
    global limit_divide_one

    for i in range(int(math.ceil(float(len(allLFNs)) * limit_divide_one))):
        #logger.info( "%s of %s" % (str(i), str(int(math.ceil( float(len(allLFNs))*limit_divide_one )))) )
        output = allLFNData.get(i)

        if output is None:
            logger.error("Error getting Replica information from Dirac: [%s,%s]" % ( str(i * LFN_parallel_limit), str((i + 1) * LFN_parallel_limit)))
            logger.error("%s" % str(allLFNData))
            raise Exception('Error from DIRAC')

        # Identify files which have Failed to be found by DIRAC
        try:
            results = output.get('Value')
            if len(results.get('Failed').keys()) > 0:
                values = results.get('Failed')
                errors.append(str(values))
                #raise SplittingError( "Error getting LFN Replica information:\n%s" % str(values) )
                for this_lfn in results.get('Failed').keys():
                    bad_lfns.append(this_lfn)
        except SplittingError as split_Err:
            raise split_Err
        except Exception as err:
            try:
                error = output
                logger.error("%s" % str(output))
            except Exception as err:
                logger.debug("Error: %s" % str(err))
                pass
            logger.error("Unknown error ion Dirac LFN Failed output")
            raise

        try:
            results = output.get('Value')
            values = results.get('Successful')
        except Exception as err:
            logger.error("Unknown error in parsing Dirac LFN Successful output")
            raise

        upper_limit = (i+1)*LFN_parallel_limit
        if upper_limit > len(allLFNs):
            upper_limit = len(allLFNs)

        logger.debug("Updating LFN Physical Locations: [%s:%s] of %s" % (str(i * LFN_parallel_limit), str(upper_limit), str(len(allLFNs))))

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
            if this_dict[this_lfn].keys() == []:
                bad_lfns.append(this_lfn)

        for this_lfn in bad_lfns:
            logger.warning("LFN: %s was either unknown to DIRAC or unavailable, Ganga is ignoring it!" % str(this_lfn))
            if this_lfn in LFNdict:
                del LFNdict[this_lfn]
            if this_lfn in allLFNs:
                allLFNs.remove(this_lfn)

    return errors

# Actually Do the work of the splitting


def OfflineGangaDiracSplitter(_inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """

    if maxFiles is not None and maxFiles > 0:
        inputs = _inputs[:maxFiles]
    else:
        inputs = _inputs

    from GangaDirac.Lib.Files.DiracFile import DiracFile
    from Ganga.GPIDev.Adapters.ISplitter import SplittingError
    # First FIND ALL LFN REPLICAS AND SE<->SITE MAPPINGS AND STORE THIS IN MEMORY
    # THIS IS DONE IN PARALLEL TO AVOID OVERLOADING DIRAC WITH THOUSANDS OF
    # REQUESTS AT ONCE ON ONE CONNECTION

    wanted_common_site = configDirac['OfflineSplitterMaxCommonSites']
    iterative_limit = configDirac['OfflineSplitterLimit']
    good_fraction = configDirac['OfflineSplitterFraction']
    uniqueSE = configDirac['OfflineSplitterUniqueSE']

    split_files = []

    if inputs is None:
        raise SplittingError("Cannot Split Job as the inputdata appears to be None!")

    if len(inputs.getLFNs()) != len(inputs.files):
        raise SplittingError("Error trying to split dataset using DIRAC backend with non-DiracFile in the inputdata")

    file_replicas = {}

    logger.info("Requesting LFN replica info")

    allLFNData = {}

    # Perform a lookup of where LFNs are all stored
    allLFNs, LFNdict = lookUpLFNReplicas(inputs, allLFNData)

    for _lfn in allLFNData:
        if allLFNData[_lfn] is None:
            logger.error("Error in Getting LFN Replica information, aborting split")
            raise SplittingError("Error in Getting LFN Replica information, aborting split")

    bad_lfns = []

    # Sort this information and store is in the relevant Ganga objects
    errors = sortLFNreplicas(bad_lfns, allLFNs, LFNdict, ignoremissing, allLFNData, inputs)

    if len(bad_lfns) != 0:
        if ignoremissing is False:
            logger.error("Errors found getting LFNs:\n%s" % str(errors))
            raise SplittingError("Error trying to split dataset with invalid LFN and ignoreMissing = False")

    # This finds all replicas for all LFNs...
    # This will probably struggle for LFNs which don't exist
    # Bad LFN should have been removed by this point however
    all_lfns = [LFNdict[this_lfn].locations for this_lfn in LFNdict if this_lfn not in bad_lfns]

    logger.info("Got replicas")

    for this_input in inputs:
        if this_input.lfn not in bad_lfns:
            file_replicas[this_input.lfn] = this_input.locations

    logger.info("found all replicas")

    logger.info("Calculating site<->SE Mapping")

    site_to_SE_mapping = {}
    SE_to_site_mapping = {}

    # Now lets generate a dictionary of some chosen site vs LFN to use in
    # constructing subsets
    site_dict, allSubSets, allChosenSets = calculateSiteSEMapping(
        file_replicas, wanted_common_site, uniqueSE, site_to_SE_mapping, SE_to_site_mapping)

    logger.debug("Found all SE in use")

    # BELOW IS WHERE THE ACTUAL SPLITTING IS DONE

    logger.info("Calculating best data subsets")

    import math
    iterations = 0
    # Loop over all LFNs
    while len(site_dict.keys()) > 0:

        # LFN left to be used
        # NB: Can't modify this list and iterate over it directly in python
        LFN_instances = site_dict.keys()
        # Already used LFN
        chosen_lfns = []

        for iterating_LFN in LFN_instances:

            # If this has previously been selected lets ignore it and move on
            if iterating_LFN in chosen_lfns:
                continue

            # Use this seed to try and construct a subset
            req_sitez = allChosenSets[iterating_LFN]
            _this_subset = []

            #logger.debug("find common LFN for: " + str(allChosenSets[iterating_LFN]))

            # Construct subset
            # Starting with i, populate subset with LFNs which have an
            # overlap of at least 2 SE
            for this_LFN in LFN_instances:
                if this_LFN in chosen_lfns:
                    continue
                if req_sitez.issubset(site_dict[this_LFN]):
                    if len(_this_subset) >= filesPerJob:
                        break
                    _this_subset.append(this_LFN)

            limit = int(math.floor(float(filesPerJob) * good_fraction))

            #logger.debug("Size limit: %s" % str(limit))

            # If subset is too small throw it away
            if len(_this_subset) < limit:
                #logger.debug("%s < %s" % (str(len(_this_subset)), str(limit)))
                allChosenSets[iterating_LFN] = generate_site_selection(site_dict[iterating_LFN], wanted_common_site, uniqueSE, site_to_SE_mapping, SE_to_site_mapping)
                continue
            else:
                logger.debug("found common LFN for: " + str(allChosenSets[iterating_LFN]))
                logger.debug("%s > %s" % (str(len(_this_subset)), str(limit)))
                # else Dataset was large enough to be considered useful
                logger.debug("Generating Dataset of size: %s" % str(len(_this_subset)))
                ## Construct DiracFile here as we want to keep the above combination
                allSubSets.append([DiracFile(lfn=str(this_LFN)) for this_LFN in _this_subset])

                for lfn in _this_subset:
                    site_dict.pop(lfn)
                    allChosenSets.pop(lfn)
                    chosen_lfns.append(lfn)

        # Lets keep track of how many times we've tried this
        iterations = iterations + 1

        # Can take a while so lets not let threads become un-locked
        import Ganga.Runtime.Repository_runtime
        Ganga.Runtime.Repository_runtime.updateLocksNow()

        # If on final run, will exit loop after this so lets try and cleanup
        if iterations >= iterative_limit:

            if good_fraction < 0.5:
                good_fraction = good_fraction * 0.75
                iterations = 0
            elif wanted_common_site > 1:
                logger.debug("Reducing Common Site Size")
                wanted_common_site = wanted_common_site - 1
                iterations = 0
                good_fraction = 0.75
            else:
                good_fraction = good_fraction * 0.75

            logger.debug("good_fraction: %s" % str(good_fraction))

    split_files = allSubSets

    avg = float()
    for this_set in allSubSets:
        avg += float(len(this_set))
    avg /= float(len(allSubSets))

    logger.info("Average Subset size is: %s" % (str(avg)))

    # FINISHED SPLITTING CHECK!!!

    check_count = 0
    for i in split_files:
        check_count = check_count + len(i)

    if check_count != len(inputs) - len(bad_lfns):
        logger.error("SERIOUS SPLITTING ERROR!!!!!")
        raise SplitterError("Files Missing after Splitting!")
    else:
        logger.info("File count checked! Ready to Submit")

    # RETURN THE RESULT

    logger.info("Created %s subsets" % str(len(split_files)))

    #logger.info( "Split Files: %s" % str(split_files) )

    for dataset in split_files:
        yield dataset

