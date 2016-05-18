
# DiracLHCb commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

#def getRootVersions(): output( dirac.getRootVersions() )

#def getSoftwareVersions(): output( dirac.getSoftwareVersions() )


def bkQueryDict(dict): output(dirac.bkQuery(dict))


def checkSites(): output(dirac.checkSites())


def bkMetaData(files): output(dirac.bkMetadata(files))


def getLHCbInputDataCatalog(lfns, depth, site, xml_file):
    if depth > 0:
        result = dirac.getBKAncestors(lfns, depth)
        if not result or not result.get('OK', False):
            output(result)
            return
        lfns = result['Value']
    output(dirac.getInputDataCatalog(lfns, site, xml_file))


def bookkeepingGUI(file):
    print(os.system('dirac-bookkeeping-gui %s' % file))


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

    output(result)

def checkTier1s():
    result = dirac.gridWeather()
    if result.get('OK', False):
        result['Value'] = result['Value']['Tier-1s']
    output(result)

