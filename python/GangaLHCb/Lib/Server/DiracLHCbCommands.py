import os, sys, inspect
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
#import GangaDirac.Lib.Server.DiracCommands

## sys.path.append(inspect.getsourcefile(getRootVersions)[:inspect.getsourcefile(getRootVersions).find('GangaLHCb')]+'GangaDirac/Lib/Server')
## import DiracCommands


dirac=DiracLHCb()

# DiracLHCb commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

def getRootVersions(): return dirac.getRootVersions()

def getSoftwareVersions(): return dirac.getSoftwareVersions()

def bkQueryDict(dict): return dirac.bkQuery(dict)
   
def checkSites(): return dirac.checkSites()

def bkMetaData(files): return dirac.bkMetadata(files)

def getInputDataCatalog(lfns,depth,site,xml_file):
    if depth > 0:
        result = dirac.getBKAncestors(lfns,depth)
        if not result or not result.get('OK',False): return result
        lfns = result['Value']
    return dirac.getInputDataCatalog(lfns,site,xml_file)

def bookkeepingGUI(file):
    return os.system('dirac-bookkeeping-gui %s' % file)

def getDataset(path,dqflag,type,start,end,sel):
    if type is 'Path':
        result = dirac.bkQueryPath(path,dqflag)##diraclhcb
    elif type is 'RunsByDate':
        result = dirac.bkQueryRunsByDate(path,start,end,
                                         dqflag,sel)##diraclhcb
    elif type is 'Run':
        result = dirac.bkQueryRun(path,dqflag)##diraclhcb
    elif type is 'Production':
        result = dirac.bkQueryProduction(path,dqflag)##diraclhcb
    else:
        result = {'OK':False,'Message':'Unsupported type!'}
    return result

def checkTier1s():
    result =  dirac.gridWeather()
    if result.get('OK',False):
        result['Value'] = result['Value']['Tier-1s']
    return result


