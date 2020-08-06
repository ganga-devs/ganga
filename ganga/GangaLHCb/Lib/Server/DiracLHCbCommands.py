
# DiracLHCb commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

#def getRootVersions(): output( dirac.getRootVersions() )

#def getSoftwareVersions(): output( dirac.getSoftwareVersions() )


@diracCommand
def bkQueryDict(dict):
    return dirac.bkQuery(dict)


@diracCommand
def checkSites():
    return dirac.checkSites()


@diracCommand
def bkMetaData(files):
    return dirac.bkMetadata(files)


@diracCommand
def getLHCbInputDataCatalog(lfns, depth, site, xml_file):
    if depth > 0:
        result = dirac.getBKAncestors(lfns, depth)
        if not result or not result.get('OK', False):
            output(result)
            return
        lfns = result['Value']
    return dirac.getInputDataCatalog(lfns, site, xml_file)


def bookkeepingGUI(file):
    print(os.system('dirac-bookkeeping-gui %s' % file))


@diracCommand
def getDataset(path, dqflag, this_type, start, end, sel):
    if this_type is 'Path':
        result = dirac.bkQueryPath(path, dqflag)  # dirac
    elif this_type is 'RunsByDate':
        result = dirac.bkQueryRunsByDate(path, start, end,
                                             dqflag, sel)  # dirac
    elif this_type is 'Run':
        result = dirac.bkQueryRun(path, dqflag)  # dirac
    elif this_type is 'Production':
        result = dirac.bkQueryProduction(path, dqflag)  # dirac
    else:
        result = {'OK': False, 'Message': 'Unsupported type!'}

    return result

@diracCommand
def getAccessURL(lfn, SE, protocol=''):
    ''' Return the access URL for the given LFN, storage element and protocol. If 'root' or 'xroot' specified then request both as per LHCbDirac from which this is taken. '''
    if protocol == '':
        protocol=['xroot', 'root']
    elif 'root' in protocol and 'xroot' not in protocol:
        protocol.insert( protocol.index( 'root' ), 'xroot' )
    elif 'xroot' in protocol and 'root' not in protocol:
         protocol.insert( protocol.index( 'xroot' ) + 1, 'root' )
    elif 'xroot' in protocol and 'root' in protocol:
         indexOfRoot = protocol.index( 'root' )
         indexOfXRoot = protocol.index( 'xroot' )
         if indexOfXRoot > indexOfRoot:
             protocol[indexOfRoot], protocol[indexOfXRoot] = protocol[indexOfXRoot], protocol[indexOfRoot]
    result = dirac.getAccessURL(lfn, SE, protocol)
    if result.get('OK', True):
        result['Value']['Successful'] = result['Value']['Successful'][SE]
    return result

@diracCommand
def checkTier1s():
    result = dirac.gridWeather()
    if result.get('OK', False):
        result['Value'] = result['Value']['Tier-1s']
    return result

@diracCommand
def getDBtagsFromLFN( lfn ):
    ''' returns the DDDB and CONDDB tags for a given LFN. Uses the latest production step unless it is a merge, in which case the parent is used '''
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    import types
    bk = BookkeepingClient()
    tr = TransformationClient()
    prod = int(lfn.split('/')[5]) # not sure if this works in all cases 
    res = bk.getProductionInformation( prod )
    #What type of step is this production ID
    step_type = tr.getTransformation(prod).get('Value', {}).get('Type', 'Unknown')
    #Is there a parent production step
    parent_id = tr.getBookkeepingQuery(prod).get('Value', {}).get('ProductionID', '')
    res = {}
    #If the production ID of the given file is Merge, look at the parent ID for the tags
    if step_type == 'Merge':
        if parent_id:
            res = bk.getProductionInformation( parent_id )
    else:
        res = bk.getProductionInformation( prod )
    dddb = ''
    conddb = ''
    if res['OK']: # there should probably also be an 'else' for cases where no information could be retrieved 
        val = res['Value']
	steps = val['Steps']
	last_step = steps[-1] # the tags are taken from the last step of production
	dddb = last_step[4]
	conddb = last_step[5]
	return dddb, conddb
    else:
        res = {'OK': False, 'Message': 'Error getting DB tags!'}

@diracCommand
def getFileMetadata( lfns ):
    ''' returns all the information for a given LFN or list of LFNs '''
    from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
    bk = BookkeepingClient()
    res = bk.getFileMetadata( lfns )
    return res


