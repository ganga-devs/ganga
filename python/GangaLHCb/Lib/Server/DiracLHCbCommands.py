
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

