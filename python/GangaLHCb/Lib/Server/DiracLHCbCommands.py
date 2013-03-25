import os, sys, inspect
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

diraclhcb=DiracLHCb()

# DiracLHCb commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

def getRootVersions(): print diraclhcb.getRootVersions()

def getSoftwareVersions(): print diraclhcb.getSoftwareVersions()

def bkQueryDict(dict): print diraclhcb.bkQuery(dict)
   
def checkSites(): print diraclhcb.checkSites()

def bkMetaData(files): print diraclhcb.bkMetadata(files)

def getLHCbInputDataCatalog(lfns,depth,site,xml_file):
    if depth > 0:
        result = diraclhcb.getBKAncestors(lfns,depth)
        if not result or not result.get('OK',False): 
            print result
            return
        lfns = result['Value']
    print diraclhcb.getInputDataCatalog(lfns,site,xml_file)

def bookkeepingGUI(file):
    print os.system('dirac-bookkeeping-gui %s' % file)

def getDataset(path,dqflag,type,start,end,sel):
    if type is 'Path':
        result = diraclhcb.bkQueryPath(path,dqflag)##diraclhcb
    elif type is 'RunsByDate':
        result = diraclhcb.bkQueryRunsByDate(path,start,end,
                                         dqflag,sel)##diraclhcb
    elif type is 'Run':
        result = diraclhcb.bkQueryRun(path,dqflag)##diraclhcb
    elif type is 'Production':
        result = diraclhcb.bkQueryProduction(path,dqflag)##diraclhcb
    else:
        result = {'OK':False,'Message':'Unsupported type!'}
    print result

def checkTier1s():
    result =  diraclhcb.gridWeather()
    if result.get('OK',False):
        result['Value'] = result['Value']['Tier-1s']
    print result


