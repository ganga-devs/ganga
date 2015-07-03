from Ganga.GPIDev.Adapters.ISplitter    import SplittingError
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from Ganga.Utility.Config                    import getConfig
from Ganga.Utility.logging              import getLogger
logger = getLogger()

import random
global_random = random

##  Find a random element from a python list that isn't in a given banned list
##  This is used for selecting a site element but it doesn't matter which is used
def find_random_site( original_SE_list, banned_SE ):
    import copy
    input_list = copy.deepcopy( original_SE_list )
    chosen_element = ""

    while chosen_element == "" and len(input_list) > 0:
        global global_random
        this_element = global_random.sample( input_list, 1 )[0]
        #print this_element
        if not this_element in banned_SE:
            chosen_element = this_element
            break
        else:
            input_list.remove( this_element )

    return chosen_element

##  These are global and used to Store the site which use a given SE or vice versa
site_to_SE_mapping = {}
SE_to_site_mapping = {}

##  This function is used for adding all of the known site for a given SE
##  The output is stored in th global dictionary site_to_SE_mapping
def addToMapping( SE ):

    from GangaDirac.Lib.Utilities.DiracUtilities import execute
    result = execute('getSitesForSE( "%s" )' % str( SE ) )
    if result.get( 'OK' ) != True:
        logger.error( "Error getting SE's for site: %s" % str( SE ) )
        site_to_SE_mapping[ SE ] = []
    else:
        usable_site = result.get('Value')
        site_to_SE_mapping[ SE ] = usable_site

allLFNData = {}

def getLFNReplicas( allLFNs, index ):

    i = index

    from GangaDirac.Lib.Utilities.DiracUtilities import execute

    output = execute( 'getReplicas(%s)' % str( allLFNs[(i*500):((i+1)*500)] ) )

    min = i*500

    if (i+1)*500 > len(allLFNs):
        max = len(allLFNs)
    else:
        max = (i+1)*500

    import Ganga.Runtime.Repository_runtime
    Ganga.Runtime.Repository_runtime.updateLocksNow()

    allLFNData[i] = output

    logger.info( "Got Replica Info: [%s:%s] of %s" % ( str(min), str(max), len(allLFNs) ) )

##  For a given site select 'wanted_common_site' many site from a given Set
##  If uniqueSE is requestd the site selected will NOT share a common SE
def generate_site_selection( input_site, wanted_common_site, uniqueSE=False ):
    from sets import Set
    req_sitez = Set([])
    if len(input_site) > wanted_common_site:
        used_site = Set([])
        for se in range( wanted_common_site ):
            this_site = find_random_site( input_site, used_site )
            req_sitez.add( this_site )
            if uniqueSE:

                global SE_to_site_mapping
                these_SE = SE_to_site_mapping[this_site]
                for this_SE in these_SE:

                    global site_to_SE_mapping
                    for site in site_to_SE_mapping[this_SE]:
                        used_site.add( site )
            else:
                used_site.add( this_site )
    else:
        for s in input_site:
            req_sitez.add( s )
    return req_sitez

##  Actually Do the work of the splitting
def OfflineGangaDiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """

    ## First FIND ALL LFN REPLICAS AND SE<->SITE MAPPINGS AND STORE THIS IN MEMORY
    ## THIS IS DONE IN PARALLEL TO AVOID OVERLOADING DIRAC WITH THOUSANDS OF REQUESTS AT ONCE ON ONE CONNECTION

    wanted_common_site = 3
    iterative_limit = 50
    good_fraction = 0.75
    uniqueSE = True

    split_files = []

    if len(inputs.getLFNs()) != len( inputs.files ):
        raise SplittingError( "Error trying to split dataset using DIRAC backend with non-DiracFile in the inputdata" )

    file_replicas = {}

    logger.info( "Requesting LFN replica info" )

    ##  Build a useful dictionary and list
    allLFNs = []
    LFNdict = {}
    for i in inputs:
        allLFNs.append( i.lfn )
        LFNdict[i.lfn] = i

    ##  Request the replicas for all LFN 500 at a time to not overload the
    ##  server and give some feedback as this is going on
    from GangaDirac.Lib.Utilities.DiracUtilities import execute
    import math
    for i in range( int(math.ceil( float(len(allLFNs))*0.002 )) ):

        from Ganga.GPI import queues

        queues.add( getLFNReplicas, ( allLFNs, i ) )

    global allLFNData

    while len(allLFNData) != int(math.ceil( float(len(allLFNs))*0.002 )):
        import time
        time.sleep(1.)
        ## This can take a while so lets protect any repo locks
        import Ganga.Runtime.Repository_runtime
        Ganga.Runtime.Repository_runtime.updateLocksNow()

    ## FIXME here to keep the repo settings as they were before we changed the flush count
    original_write_perm = {}

    bad_lfns = []

    for i in range( int(math.ceil( float(len(allLFNs))*0.002 )) ):

        output = allLFNData[i]

        try:
            if len(output.get('Value')['Failed'].keys()) > 0:
                if ignoremissing is False:
                    values = output.get('Value')['Failed']
                    from Ganga.GPIDev.Adapters.ISplitter import SplittingError
                    raise SplittingError( "Error getting LFN Replica information:\n%s" % str(values) )
                else:
                    for this_lfn in output.get('Value')['Failed'].keys():
                        bad_lfns.append( this_lfn )
        except SplittingError, split_Err:
            raise split_Err
        except Exception, err:
            pass # FIXME

        try:
            values = output.get('Value')['Successful']
        except Exception, err:
            pass # FIXME

        logger.info( "Updating URLs: %s of %s" % (str(i*500), str(len(allLFNs)) ) )

        for this_lfn in values.keys():
            this_dict = {}
            this_dict[this_lfn] = values.get(this_lfn)

            ## FIXME HORRIBLE HACK BUT THERE ARE LYTERALLY THOUSANDS OF I/O
            ## OPERATIONS HAPPENING DUE TO THIS, LETS MINIMISE IT
            original_write_perm[this_lfn] = LFNdict[this_lfn]._getRegistry().dirty_flush_counter
            LFNdict[this_lfn]._getRegistry().dirty_flush_counter = 1000


            LFNdict[this_lfn]._updateRemoteURLs( this_dict )
            ##  If we find NO replicas
            if LFNdict[this_lfn].locations == []:
                bad_lfns.append( this_lfn )


    for this_lfn in bad_lfns:
        logger.warning( "LFN: %s was either unknown to DIRAC or unavailable, Ganga is ignoring it!" % str(this_lfn) )
        del LFNdict[ this_lfn ]
        allLFNs.remove( this_lfn )

    ## FIXME AS ABOVE THIS IS HERE TO RESTORE NORMALITY
    for k, v in original_write_perm.iteritems():
        if k in bad_lfns:
            continue
        LFNdict[k]._getRegistry().dirty_flush_counter  = v

    ## This finds all replicas for all LFNs...
    ## This will probably struggle for LFNs which don't exist
    all_lfns = [ LFNdict[this_lfn].locations for this_lfn in LFNdict if this_lfn not in bad_lfns ]

    logger.info( "Got replicas" )

    for this_input in inputs:
        if this_input.lfn not in bad_lfns:
            file_replicas[this_input.lfn] = this_input.locations

    logger.info( "found all replicas" )


    logger.info( "Calculating site<->SE Mapping" )
    global site_to_SE_mapping
    global SE_to_site_mapping

    from sets import Set
    SE_dict = dict()
    maps_size = 0
    found = []
    ## First find the SE for each site
    for lfn, repz in file_replicas.iteritems():
        sitez=Set([])
        for i in repz:
            sitez.add( i )
            if not i in found:

                from Ganga.GPI import queues

                queues.add( addToMapping, ( str(i), ) ) 

                maps_size = maps_size + 1
                found.append( i )

        SE_dict[ lfn ] = sitez

    ## Doing this in parallel so wait for it to finish
    while len( site_to_SE_mapping ) != maps_size:
        import time
        time.sleep( 0.5 )

    ## Now calculate the inverse dictionary of site for each SE
    for k, v in site_to_SE_mapping.iteritems():
        for i in v:
            if i not in SE_to_site_mapping:
                SE_to_site_mapping[i] = Set([])
            SE_to_site_mapping[i].add(k)

    ## These can be used to select the site which know of a given SE
    ## Or vice versa


    ## Now lets generate a dictionary of some chosen site vs LFN to use in constructing subsets
    allSubSets = []
    allChosenSets = {}

    site_dict = {}
    for k, v in SE_dict.iteritems():
        site_dict[k] = Set([])
        for i in v:
            for j in site_to_SE_mapping[i]:
                site_dict[k].add( j )

    ##  Now select a set of site to use as a seed for constructing a subset of LFN
    for lfn in site_dict.keys():
        allChosenSets[ lfn ] = generate_site_selection( site_dict[lfn], wanted_common_site, uniqueSE )

    logger.debug( "Found all SE in use" )




    ## BELOW IS WHERE THE ACTUAL SPLITTING IS DONE


    logger.info( "Calculating best data subsets" )

    iterations = 0
    while len( site_dict.keys() ) > 0:

        ## LFN left to be used
        ## NB: Can't modify this list and iterate over it directly in python
        LFN_instances = site_dict.keys()
        ## Already used LFN
        chosen_lfns = []

        for iterating_LFN in LFN_instances:

            ## If this has previously been selected lets ignore it and move on
            if iterating_LFN in chosen_lfns:
                continue

            ##  Use this seed to try and construct a subset
            req_sitez = allChosenSets[iterating_LFN]
            _this_subset = []

            logger.debug( "find common LFN for: " + str( allChosenSets[ iterating_LFN ]) )

            ## Construct subset
            ## Starting with i, populate subset with LFNs which have an
            ## overlap of at least 2 SE
            for this_LFN in LFN_instances:
                if this_LFN in chosen_lfns:
                    continue
                if req_sitez.issubset( site_dict[this_LFN] ):
                    if len(_this_subset) >= filesPerJob:
                        break
                    _this_subset.append( str(this_LFN) )

            limit = int(math.floor( float(filesPerJob) * good_fraction ))

            ##  If subset is too small throw it away
            if len( _this_subset ) < limit:
                allChosenSets[ iterating_LFN ] = generate_site_selection( site_dict[iterating_LFN], wanted_common_site )
                continue
            else:
                ##  else Dataset was large enough to be considered useful
                logger.info( "Generating Dataset of size: %s" % str(len(_this_subset)) )
                allSubSets.append( _this_subset )

                for lfn in _this_subset:
                    site_dict.pop( lfn )
                    allChosenSets.pop( lfn )
                    chosen_lfns.append( lfn )

        ##  Lets keep track of how many times we've tried this
        iterations = iterations + 1

        ##  Can take a while so lets not let threads become un-locked
        import Ganga.Runtime.Repository_runtime
        Ganga.Runtime.Repository_runtime.updateLocksNow()

        ##  If on final run, will exit loop after this so lets try and cleanup
        if iterations >= iterative_limit:

            if wanted_common_site > 1:
                wanted_common_site = wanted_common_site - 1
                iterations = 0
                good_fraction = 0.75
            else:
                good_fraction = good_fraction * 0.75

    split_files = allSubSets


    ## FINISHED SPLITTING CHECK!!!

    check_count = 0
    for i in split_files:
        check_count = check_count + len(i)

    if check_count != len(inputs)-len(bad_lfns):
        logger.error( "SERIOUS SPLITTING ERROR!!!!!" )
        raise SplitterError( "Files Missing after Splitting!" )
    else:
        logger.info( "File count checked! Ready to Submit" )


    ## RETURN THE RESULT

    logger.info( "Created %s subsets" % str( len(split_files) ) )

    #logger.info( "Split Files: %s" % str(split_files) )

    for dataset in split_files:
        yield dataset

