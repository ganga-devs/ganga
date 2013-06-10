import os, sys, inspect, pickle
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

diraclhcb=DiracLHCb()
# Write to output pipe
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

#def output(object):
#    print >> sys.stdout, pickle.dumps(object)

# DiracLHCb commands
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/

def getRootVersions(): output( diraclhcb.getRootVersions() )

def getSoftwareVersions(): output( diraclhcb.getSoftwareVersions() )

def bkQueryDict(dict): output( diraclhcb.bkQuery(dict) )
   
def checkSites(): output( diraclhcb.checkSites() )

def bkMetaData(files): output( diraclhcb.bkMetadata(files) )

def getLHCbInputDataCatalog(lfns,depth,site,xml_file):
    if depth > 0:
        result = diraclhcb.getBKAncestors(lfns,depth)
        if not result or not result.get('OK',False): 
            output( result )
            return
        lfns = result['Value']
    output( diraclhcb.getInputDataCatalog(lfns,site,xml_file) )

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
    output( result )

def checkTier1s():
    result =  diraclhcb.gridWeather()
    if result.get('OK',False):
        result['Value'] = result['Value']['Tier-1s']
    output( result )


