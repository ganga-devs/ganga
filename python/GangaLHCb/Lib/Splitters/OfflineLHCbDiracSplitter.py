from Ganga.GPIDev.Adapters.ISplitter    import SplittingError
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from Ganga.Utility.Config                    import getConfig
from Ganga.Utility.logging              import getLogger
logger = getLogger()

import random
global_random = random

def find_random_CE( original_SE_list, banned_SE ):

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

CE_to_SE_mapping = {}
SE_to_CE_mapping = {}

def addToMapping( SE ):

    from GangaDirac.Lib.Utilities.DiracUtilities import execute
    result = execute('getSitesForSE( "%s" )' % str( SE ) )
    if result.get( 'OK' ) != True:
        logger.error( "Error getting SE's for CE: %s" % str( SE ) )
        CE_to_SE_mapping[ SE ] = []
    else:
        usable_CE = result.get('Value')
        CE_to_SE_mapping[ SE ] = usable_CE

def generate_CE_selection( input_CE, wanted_common_CE, uniqueSE=False ):
    from sets import Set
    req_sitez = Set([])
    if len(input_CE) > wanted_common_CE:
        used_CE = Set([])
        for se in range( wanted_common_CE ):
            this_CE = find_random_CE( input_CE, used_CE )
            req_sitez.add( this_CE )
            if uniqueSE:
                #print "this_CE: " + str( this_CE )
                global SE_to_CE_mapping
                these_SE = SE_to_CE_mapping[this_CE]
                #print "these_SE: " + str( these_SE )
                for this_SE in these_SE:
                    #print "  this_SE: " + str( this_SE )
                    global CE_to_SE_mapping
                    #print "  CE_to_SE_mapping[this_SE]: " + str( CE_to_SE_mapping[this_SE] )
                    for CE in CE_to_SE_mapping[this_SE]:
                        used_CE.add( CE )
                    #print "used_CE: " + str( used_CE )
            else:
                used_CE.add( this_CE )
    else:
        for s in input_CE:
            req_sitez.add( s )
    return req_sitez

def OfflineLHCbDiracSplitter(inputs, filesPerJob, maxFiles, ignoremissing):
    """
    Generator that yields a datasets for dirac split jobs
    """

    wanted_common_CE = 2
    iterative_limit = 25
    good_fraction = 0.75
    good_fraction_cleanup = 0.25
    uniqueSE = True

    split_files = []

    if len(inputs.getLFNs()) != len( inputs.files ):
        raise SplittingError( "Error trying to split dataset using DIRAC backend with non-DiracFile in the inputdata" )

    file_replicas = {}

    logger.info( "Requesting LFN replica info" )

    allLFNs = []
    LFNdict = {}
    for i in inputs:
        allLFNs.append( i.lfn ) 
        LFNdict[i.lfn] = i

    from GangaDirac.Lib.Utilities.DiracUtilities import execute

    import math
    for i in range( int(math.ceil( float(len(allLFNs))*0.002 )) ):

        output = execute( 'getReplicas(%s)' % str( allLFNs[(i*500):((i+1)*500)] ) )

        try:
            values = output.get('Value')['Successful']
        except Exception, x:
            raise SplitterError( "Error getting LFN Replica information:\n%s" % str(x) )

        for k in values.keys():
            this_dict = {}
            this_dict[k] = values.get(k)
            LFNdict[k]._updateRemoteURLs( this_dict )

        if (i+1)*500 > len(allLFNs):
            max = len(allLFNs)
        else:
            max = (i+1)*500
        logger.info( "Got Replica Info: %s of %s" % ( str(max), len(allLFNs) ) )

    ## This finds all replicas for all LFNs...
    ## This will probably struggle for LFNs which don't exist
    all_lfns = [ i.locations for i in inputs ]
    while [] in all_lfns:
        import time
        time.sleep( 0.5 )
        all_lfns = [ i.locations for i in inputs ]
        count=0.
        for i in all_lfns:
            if i == []:
                count=count+1.

    logger.info( "Got replicas" )

    for i in inputs:
        file_replicas[i.lfn] = i.locations

    logger.debug( "found all replicas" )


    logger.info( "Calculating CE<->SE Mapping" )
    global CE_to_SE_mapping
    global SE_to_CE_mapping

    from sets import Set
    SE_dict = dict()
    maps_size = 0
    found = []
    ## First find the SE for each CE
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
    while len( CE_to_SE_mapping ) != maps_size:
        import time
        time.sleep( 0.5 )

    ## Not calculate the inverse dictionary of CE for each SE
    for k, v in CE_to_SE_mapping.iteritems():
        for i in v:
            if i not in SE_to_CE_mapping:
                SE_to_CE_mapping[i] = Set([])
            SE_to_CE_mapping[i].add(k)


    ## Now lets generate a dictionary of some chosen CE vs LFN to use in constructing subsets
    allSubSets = []
    allChosenSets = {}

    CE_dict = {}
    for k, v in SE_dict.iteritems():
        CE_dict[k] = Set([])
        for i in v:
            for j in CE_to_SE_mapping[i]:
                CE_dict[k].add( j )

    for lfn in CE_dict.keys():
        #print "Calculating CE selection: " + str(lfn)
        allChosenSets[ lfn ] = generate_CE_selection( CE_dict[lfn], wanted_common_CE, uniqueSE )
        #print allChosenSets[ lfn ]

    logger.debug( "Found all SE in use" )

    logger.info( "Calculating best data subsets" )

    iterations = 0
    while len( CE_dict.keys() ) > 0 and iterations < iterative_limit:

        #print "Iteration: " + str(iterations)
        ce_instances = CE_dict.keys()

        chosen_lfns = []

        for i in ce_instances:

            if i in chosen_lfns:
                continue

            req_sitez = allChosenSets[i]
            _this_subset = []

            ## Starting with i, populate subset with LFNs which have an
            ## overlap of at least 2 SE

            for k in ce_instances:
                if k in chosen_lfns:
                    continue
                if req_sitez.issubset( CE_dict[k] ):
                    if len(_this_subset) >= filesPerJob:
                        break
                    #print str(k)
                    _this_subset.append( str(k) )


            if len( _this_subset ) < math.ceil( float(filesPerJob) * good_fraction ):
                #print i
                a = allChosenSets[ i ]
                d = CE_dict[i]
                allChosenSets[ i ] = generate_CE_selection( CE_dict[i], wanted_common_CE )
                continue
            else:
                logger.info( "Generating Dataset of size: %s" % str(len(_this_subset)) )
                allSubSets.append( _this_subset )
                for lfn in _this_subset:
                    #print "remove: " + str(lfn)
                    CE_dict.pop( lfn )
                    allChosenSets.pop( lfn )
                    chosen_lfns.append( lfn )

        #print "subsets: " + str(split_files)
        #print "left: " + str( CE_dict.keys() )
        iterations = iterations + 1

        left_size = len( CE_dict.keys() )

        if iterations == iterative_limit:

            cleanup_limit = 0
            chosen_lfns = []
            while cleanup_limit < 5:
                cleanup_limit = cleanup_limit + 1

                for i in CE_dict.keys():
                    a = allChosenSets[ i ]
                    d = CE_dict[i]
                    allChosenSets[ i ] = generate_CE_selection( CE_dict[i], 1 )

                ce_instances = CE_dict.keys()

                for i in ce_instances:

                    if i in chosen_lfns:
                        continue

                    req_sitez = allChosenSets[i]
                    _this_subset = []

                    ## Starting with i, populate subset with LFNs which have an
                    ## overlap of at least 2 SE
                    for k in ce_instances:
                        if k in chosen_lfns:
                            continue
                        if req_sitez.issubset( CE_dict[k] ):
                            if len(_this_subset) >= filesPerJob:
                                break
                            _this_subset.append( str(k) )

                    ##  We're desperate now!
                    if len( _this_subset ) < math.ceil(good_fraction_cleanup * left_size):
                        #print i
                        a = allChosenSets[ i ]
                        d = CE_dict[i]
                        allChosenSets[ i ] = generate_CE_selection( CE_dict[i], 1 )
                        continue
                    else:
                        logger.info( "Generating Dataset of size: %s" % str(len(_this_subset)) )
                        allSubSets.append( _this_subset )
                        for lfn in _this_subset:
                            #print "remove: " + str(lfn)
                            CE_dict.pop( lfn )
                            allChosenSets.pop( lfn )
                            chosen_lfns.append( lfn )

    split_files = allSubSets

    left_after = CE_dict.keys()

    if len(left_after) > 0:
        for f in left_after:
            logger.debug( "Had diffifculty with: %s Possibly just random effect of matching" % str( f ) )

    #print "left: " + str( left_after )
    for i in left_after:
        logger.info( "Generating Dataset of size: 1" )
        split_files.append( [i] )

    check_count = 0
    for i in split_files:
        check_count = check_count + len(i)

    if check_count != len(inputs):
        logger.error( "SERIOUS SPLITTING ERROR!!!!!" )
        raise SplitterError( "Files Missing after Splitting!" )
    else:
        logger.info( "File count checked! Ready to Submit" )

    logger.info( "Created %s subsets" % str( len(split_files) ) )

    #logger.info( "Split Files: %s" % str(split_files) )

    for dataset in split_files:
        yield dataset

