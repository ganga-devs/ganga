'''
client methods

'''

import os
import re
import sys
import time
import types
import urllib
import urllib2
import commands
import cPickle as pickle
import xml.dom.minidom
import socket
import random
import tempfile

# configuration
try:
    baseURL = os.environ['PANDA_URL']
except:
    baseURL = 'http://pandasrv.usatlas.bnl.gov:25080/server/panda'
    #baseURL = 'http://gridui02.usatlas.bnl.gov:26080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://pandasrv.usatlas.bnl.gov:25443/server/panda'
    #baseURLSSL = 'https://gridui02.usatlas.bnl.gov:26443/server/panda'    

baseURLDQ2     = 'http://atlddmcat.cern.ch/dq2'
baseURLDQ2SSL  = 'https://atlddmcat.cern.ch:443/dq2'
baseURLSUBHome = "http://www.usatlas.bnl.gov/svn/panda/pathena"
baseURLSUB     = baseURLSUBHome+'/trf'
baseURLMON     = "http://gridui02.usatlas.bnl.gov:25880/server/pandamon/query"

# exit code
EC_Failed = 255

# python 2.3 compatibility

if sys.version_info[1] == 3:
    sys.path.insert(0,os.path.join(os.path.dirname(__file__),'python2.3'))
    import decimal
    del sys.path[0]

# retrieve pathena config
try:
    # get default timeout
    defTimeOut = socket.getdefaulttimeout()
    # set timeout
    socket.setdefaulttimeout(60)
except:
    pass
# get panda server's name
try:
    getServerURL = baseURL + '/getServer'
    req = urllib2.Request(getServerURL)
    res = urllib2.urlopen(req)
    # overwrite URL
    baseURLSSL = "https://%s/server/panda" % res.read()
except:
    type, value, traceBack = sys.exc_info()
    print type,value
    print "ERROR : could not getServer from %s" % getServerURL
    sys.exit(EC_Failed)
try:
    # reset timeout
    socket.setdefaulttimeout(defTimeOut)
except:
    pass


# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    print "No valid grid proxy certificate found"
    return ''


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl"'
        # verification of the host certificate
        self.verifyHost = False
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False

    # GET method
    def get(self,url,data):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # POST method
    def post(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT 
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        if self.verbose:
            print com
        # execute
        ret = commands.getstatusoutput(com)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # convert return
    def convRet(self,ret):
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        # add messages to silent errors
        if ret[0] == 35:
            ret = (ret[0],'SSL connect error. The SSL handshaking failed. Check grid certificate/proxy.')
        elif ret[0] == 7:
            ret = (ret[0],'Failed to connect to host.')            
        return ret
    

'''
public methods

'''

# get site specs
def getSiteSpecs():
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getSiteSpecs'
    status,output = curl.get(url,{})
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getSiteSpecs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr

# get Panda Sites
tmpStat,PandaSites = getSiteSpecs()
if tmpStat != 0:
    print "ERROR : cannot get Panda Sites" 
    sys.exit(EC_Failed)


# get LRC
def getLRC(site):
    ret = None
    # look for DQ2ID
    for id,val in PandaSites.iteritems():
        if id == site or val['ddm'] == site:
            if not val['dq2url'] in [None,"","None"]:
                ret = val['dq2url']
                break
    return ret


# get LFC
def getLFC(site):
    ret = None
    # look for DQ2ID
    for id,val in PandaSites.iteritems():
        if id == site or val['ddm'] == site:
            if not val['lfchost'] in [None,"","None"]:
                ret = val['lfchost']
                break
    return ret


# get SEs
def getSE(site):
    ret = []
    # look for DQ2ID
    for id,val in PandaSites.iteritems():
        if id == site or val['ddm'] == site:
            if not val['se'] in [None,"","None"]:
                for tmpSE in val['se'].split(','):
                    match = re.search('.+://([^:/]+):*\d*/*',tmpSE)
                    if match != None:
                        ret.append(match.group(1))
                break
    # return
    return ret


# submit jobs
def submitJobs(jobs,verbose=False):
    # set hostname
    hostname = commands.getoutput('hostname')
    for job in jobs:
        job.creationHost = hostname
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/submitJobs'
    data = {'jobs':strJobs}
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR submitJobs : %s %s" % (type,value)
        return EC_Failed,None


# get job status
def getJobStatus(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getJobStatus : %s %s" % (type,value)
        return EC_Failed,None


# kill jobs
def killJobs(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/killJobs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR killJobs : %s %s" % (type,value)
        return EC_Failed,None


# reassign jobs
def reassignJobs(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reassignJobs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR reassignJobs : %s %s" % (type,value)
        return EC_Failed,None


# query PandaIDs
def queryPandaIDs(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryPandaIDs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR queryPandaIDs : %s %s" % (type,value)
        return EC_Failed,None


# query last files in datasets
def queryLastFilesInDataset(datasets,verbose=False):
    # serialize
    strDSs = pickle.dumps(datasets)
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose    
    # execute
    url = baseURL + '/queryLastFilesInDataset'
    data = {'datasets':strDSs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR queryLastFilesInDataset : %s %s" % (type,value)
        return EC_Failed,None


# put file
def putFile(file,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/putFile'
    data = {'file':file}
    return curl.put(url,data)


# delete file
def deleteFile(file):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/deleteFile'
    data = {'file':file}
    return curl.post(url,data)


# query files in dataset
def queryFilesInDataset(name,verbose=False,v_vuids=None):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    try:
        errStr = ''
        # get VUID
        if v_vuids == None:
            url = baseURLDQ2 + '/ws_repository/rpc'
            # container
            if name.endswith('/'):
                names = getElementsFromContainer(name,verbose)
            else:
                names = [name]
            # loop over all names
            vuids = []
            for tmpName in names:    
                data = {'operation':'queryDatasetByName','dsn':tmpName,
                        'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
                status,out = curl.get(url,data)
                if status != 0 or out == '\x00' or (not out.has_key(tmpName)):
                    errStr = "ERROR : could not find %s in DQ2 DB. Check if the dataset name is correct" \
                             % tmpName
                    sys.exit(EC_Failed)
                # parse
                vuids += out[tmpName]['vuids']
        else:
            vuids = v_vuids
        # get files
        url = baseURLDQ2 + '/ws_content/rpc'
        data = {'operation': 'queryFilesInDataset','vuids':vuids,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
        status,out =  curl.post(url,data)
        if status != 0:
            errStr = "ERROR : could not get files in %s" % name
            sys.exit(EC_Failed)
        # parse
        ret = {}
        if out == '\x00' or len(out) < 2 or out==():
            # empty
            return ret
        generalLFNmap = {}
        for guid,vals in out[0].iteritems():
            # remove attemptNr
            generalLFN = re.sub('\.\d+$','',vals['lfn'])
            # choose greater attempt to avoid duplication
            if generalLFNmap.has_key(generalLFN):
                if vals['lfn'] > generalLFNmap[generalLFN]:
                    # remove lesser attempt
                    del ret[generalLFNmap[generalLFN]]
                else:
                    continue
            # append to map
            generalLFNmap[generalLFN] = vals['lfn']
            ret[vals['lfn']] = {'guid'   : guid,
                                'fsize'  : vals['filesize'],
                                'md5sum' : vals['checksum']}
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)
    return ret            


# get datasets
def getDatasets(name,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    try:
        errStr = ''
        # get VUID
        url = baseURLDQ2 + '/ws_repository/rpc'
        data = {'operation':'queryDatasetByName','dsn':name,'version':0,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
        status,out = curl.get(url,data)
        if status != 0:
            errStr = "ERROR : could not access DQ2 server"
            sys.exit(EC_Failed)
        # parse
        datasets = {}
        if out == '\x00' or (not out.has_key(name)):
            # no datasets
            return datasets
        # get VUIDs
        for dsname,idMap in out.iteritems():
            # check format
            if idMap.has_key('vuids') and len(idMap['vuids'])>0:
                datasets[dsname] = idMap['vuids'][0]
            else:
                # wrong format
                errStr = "ERROR : could not parse HTTP response for %s" % name
                sys.exit(EC_Failed)
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)
    return datasets


# register dataset
def addDataset(name,verbose=False):
    # generate DUID/VUID
    duid = commands.getoutput("uuidgen")
    vuid = commands.getoutput("uuidgen")
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    try:
        errStr = ''
        # add
        url = baseURLDQ2SSL + '/ws_repository/rpc'
        data = {'operation':'addDataset','dsn': name,'duid': duid,'vuid':vuid,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen'),'update':'yes'}
        status,out = curl.post(url,data)
        if status != 0 or (out != None and re.search('Exception',out) != None):
            errStr = "ERROR : could not add dataset to repository"
            sys.exit(EC_Failed)
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)

# get container elements
def getElementsFromContainer(name,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    try:
        errStr = ''
        # get elements
        url = baseURLDQ2 + '/ws_dq2/rpc'
        data = {'operation':'container_retrieve','name': name,
                'API':'030','tuid':commands.getoutput('uuidgen')}
        status,out = curl.get(url,data)
        if status != 0 or (isinstance(out,types.StringType) and re.search('Exception',out) != None):
            errStr = "ERROR : could not get container"
            sys.exit(EC_Failed)
        return out
    except:
        print status,out
        type, value, traceBack = sys.exc_info()
        print "%s %s" % (type,value)
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)


# convert srmv2 site to srmv1 site ID
def convSrmV2ID(tmpSite):
    # patch for SRM v2
    tmpSite = re.sub('-[^_]+_DATADISK$','DISK',tmpSite)
    tmpSite = re.sub('-[^_]+_MCDISK$','DISK',tmpSite)                    
    # patch for BNL
    if tmpSite in ['BNLDISK','BNLTAPE']:
        tmpSite = 'BNLPANDA'
    # return
    return tmpSite


# get locations
def getLocations(name,fileList,cloud,woFileCheck,verbose=False,useAMGA=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    try:
        errStr = ''
        # container
        if name.endswith('/'):
            names = getElementsFromContainer(name,verbose)
        else:
            names = [name]
        # loop over all names
        retSites   = []
        retSiteMap = {}
        countSite  = {}
        for tmpName in names:
            # get VUID
            url = baseURLDQ2 + '/ws_repository/rpc'
            data = {'operation':'queryDatasetByName','dsn':tmpName,'version':0,
                    'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
            status,out = curl.get(url,data)
            if status != 0 or out == '\x00' or (not out.has_key(tmpName)):
                if verbose:
                    print "ERROR : could not find %s in DQ2 DB. Check if the dataset name is correct" \
                          % tmpName
                    return retSites
            # parse
            duid  = out[tmpName]['duid']
            # get replica location
            url = baseURLDQ2 + '/ws_location/rpc'
            data = {'operation': 'listDatasetReplicas','duid': duid,
                    'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
            status,out = curl.post(url,data)
            if status != 0:
                errStr = "ERROR : could not query location for %s" % tmpName
                sys.exit(EC_Failed)
            for origTmpSite,origTmpInfo in out.iteritems():
                # count number of available files
                if not countSite.has_key(origTmpSite):
                    countSite[origTmpSite] = 0
                countSite[origTmpSite] += origTmpInfo[0]['found']
                # patch for SRM v2
                tmpSite = convSrmV2ID(origTmpSite)
                # check cloud, DQ2 ID and status
                for tmpID,tmpSpec in PandaSites.iteritems():
                    if (tmpSpec['cloud']==cloud and convSrmV2ID(tmpSpec['ddm'])==tmpSite and \
                        tmpSpec['status']=='online') or woFileCheck:
                        # exclude long,xrootd,local queues
                        if isExcudedSite(tmpID):
                            continue
                        if not tmpSite in retSites:
                            retSites.append(tmpSite)
                        # just collect locations when file check is disabled
                        if woFileCheck:    
                            break
                        # mapping between location and Panda siteID
                        if not retSiteMap.has_key(tmpSite):
                            retSiteMap[tmpSite] = []
                        if not tmpID in retSiteMap[tmpSite]:
                            retSiteMap[tmpSite].append(tmpID)
        # return list when file check is not required
        if woFileCheck:
            return retSites
        # chose site using AMGA
        if useAMGA:
            try:
                if verbose:
                    print "connecting to AMGA"
                # connect
                import mdclient
                mdC = mdclient.MDClient('ganga-o.cern.ch', 8822, 'root')
                iLFN  = 0
                guids = []
                countSite = {}
                # loop over all LFNs
                for lfn,val in fileList.iteritems():
                    iLFN += 1
                    guids.append(val['guid']) 
                    if iLFN % 100 == 0 or iLFN == len(fileList):
                        if verbose:
                            sys.stdout.write('.')
                            sys.stdout.flush()
                        # get replica list
                        res = mdC.replicaList(guids)
                        for tmpGUID,tmpSiteList in res.iteritems():
                            # count # of files
                            for tmpSite in tmpSiteList:
                                if not countSite.has_key(tmpSite):
                                    countSite[tmpSite] = 0
                                countSite[tmpSite] += 1    
                        # reset
                        iLFN  = 0
                        guids = []
            except:
                type, value, traceBack = sys.exc_info()
                print "ERROR : AMGA - %s %s" % (type,value)
                sys.exit(EC_Failed)                
            # choose max site
            if verbose:
                print "\nResult:%s" % countSite
            maxCount = -1
            maxSites = []
            for tmpSite,tmpCount in countSite.iteritems():
                # patch for SRM v2
                tmpSite = convSrmV2ID(tmpSite)
                # check if the site is in the cloud
                if tmpSite in retSites:
                    if maxCount < tmpCount:
                        maxCount = tmpCount
                        maxSites = [tmpSite]
                    elif maxCount == tmpCount:
                        maxSites.append(tmpSite)
            # remove sites except max
            for tmpSite in retSiteMap.keys():
                if not tmpSite in maxSites:
                    del retSiteMap[tmpSite]
        # return
        if verbose:
            print "getLocations -> %s" % retSiteMap
        return retSiteMap
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            type, value, traceBack = sys.exc_info()
            print "ERROR : invalid DQ2 response - %s %s" % (type,value)
        sys.exit(EC_Failed)
                

# erase dataset
def eraseDataset(name,gridSrc,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    try:
        errStr = ''
        # get VUID
        url = baseURLDQ2 + '/ws_repository/rpc'
        data = {'operation':'queryDatasetByName','dsn':name,'version':0,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
        status,out = curl.get(url,data)
        if status != 0 or out == '\x00' or (not out.has_key(name)):
            if verbose:
                print "ERROR : could not find %s in DQ2 DB. Check if the dataset name is correct" \
                      % name
            return    
        # parse
        vuids = out[name]['vuids']
        # get file list
        fileMap = queryFilesInDataset(name,verbose,vuids)
        # get replica location
        url = baseURLDQ2 + '/ws_location/rpc'
        data = {'operation': 'queryDatasetLocations','vuids':[vuids[0]],
                'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
        status,out = curl.post(url,data)
        if status != 0:
            errStr = "ERROR : could not query location for %s" % name
            sys.exit(EC_Failed)
        sites = []    
        if out.has_key(vuids[0]):
            sites = out[vuids[0]][0]+out[vuids[0]][1]
        # LRC/LFC list    
        lrcList = []
        lfcList = []
        for site in sites:
            tmpLRC = getLRC(site)
            tmpLFC = getLFC(site)
            if tmpLRC != None and (not tmpLRC in lrcList):
                # LRC 
                lrcList.append(tmpLRC)
            elif tmpLFC != None and (not tmpLFC in lfcList):
                # LFC
                lfcList.append(site)
        # delete command
        delCom = 'srm-advisory-delete -retry_num=3'
        # check X509_CERT_DIR
        out = commands.getoutput('%s echo $X509_CERT_DIR' % gridSrc)
        if out != '':
            delCom += ' -x509_user_trusted_certificates=%s' % out
        # delete local files for LRC
        for lrcURL in lrcList:
            pfns = _getPFNsLRC(fileMap,lrcURL,verbose)
            for pfn in pfns.values():
                # append port number
                pfn = re.sub('(?P<srm>^srm://[^:/]+)/','\g<srm>:8443/',pfn)
                # delete file
                com = '%s %s %s' % (gridSrc,delCom,pfn)
                if verbose:
                    print com
                status,out = commands.getstatusoutput(com)
                if verbose:
                    print status,out
                if status != 0 and re.search('Error file does not exist',out) == None:
                    errStr = "ERROR : could not delete %s" % pfn
                    sys.exit(EC_Failed)                        
                # delete LRC entry
                ##### FIXME
        # delete local files for LFC
        for lfcHost in lfcList:
            # get PFNs
            pfnMap = _getPFNsLFC(fileMap,lfcHost,False,verbose)
            for guid,pfns in pfnMap.iteritems():
                # delete LFC entry
                com = '%s env LFC_HOST=%s lcg-del --vo atlas -a guid:%s' % \
                      (gridSrc,getLFC(lfcHost),guid)
                if verbose:
                    print com
                status,out = commands.getstatusoutput(com)
                if verbose:
                    print status,out
                if status != 0:
                    errStr = "ERROR : could not delete %s from %s LFC" % (pfn.split('/')[-1],lfcHost)
                    sys.exit(EC_Failed)                        
        # delete then contents of each vuid
        url = baseURLDQ2SSL + '/ws_content/rpc'
        data = {'operation': 'deleteDataset', 'vuids': vuids,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
        status,out = curl.post(url,data)
        if status != 0:
            errStr = "ERROR : failed to delete dataset contents"
            sys.exit(EC_Failed)
        # delete all replicas from location catalog
        url = baseURLDQ2SSL + '/ws_location/rpc'
        data = {'operation': 'deleteDataset', 'vuids': vuids,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen'),
                'delete':'yes'}
        status,out = curl.get(url,data)
        if status != 0:
            errStr = "ERROR : failed to delete replicas"
            sys.exit(EC_Failed)
        # delete dataset state
        url = baseURLDQ2SSL + '/ws_repository/rpc'
        data = {'operation':'setState','dsn':name,'state':3,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen'),'update':'yes'}
        status,out = curl.post(url,data)
        if status != 0:
            errStr = "ERROR : failed to delete replicas"
            sys.exit(EC_Failed)
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            type, value, traceBack = sys.exc_info()
            print "ERROR : invalid DQ2 response - %s %s" % (type,value)
        sys.exit(EC_Failed)


#@ Returns number of events per file in a given dataset
#SP 2006
#
def nEvents(name, verbose=False, askServer=True, fileList = {}, scanDir = '.'):
    
    # @  These declarations can be moved to the configuration section at the very beginning
    # Here just for code clarity
    #
    # Parts of the query
    str1="/?dset="
    str2="&get=evperfile"
    # Form full query string
    m_query = baseURLMON+str1+name+str2
    manualEnter = True
    # Send query get number of events per file
    if askServer:
        nEvents=urllib.urlopen(m_query).read()
        if verbose:
            print m_query
            print nEvents
        if re.search('HTML',nEvents) == None and nEvents != '-1':
            manualEnter = False            
    else:
        # use ROOT to get # of events
        try:
            import ROOT
            rootFile = ROOT.TFile("%s/%s" % (scanDir,fileList[0]))
            tree = ROOT.gDirectory.Get( 'CollectionTree' )
            nEvents = tree.GetEntriesFast()
            # disable
            if nEvents > 0:
                manualEnter = False
        except:
            if verbose:
                type, value, traceBack = sys.exc_info()
                print "ERROR : could not get nEvents with ROOT - %s %s" % (type,value)
    # In case of error PANDAMON server returns full HTML page
    # Normally return an integer
    if manualEnter: 
        if askServer:
            print "Could not get the # of events from MetaDB for %s " % name
        while True:
            str = raw_input("Enter the number of events per file : ")
            try:
                nEvents = int(str)
                break
            except:
                pass
    if verbose:
       print "Dataset ", name, "has ", nEvents, " per file"
    return int(nEvents)


# get PFN from LRC
def _getPFNsLRC(lfns,dq2url,verbose):
    pfnMap   = {}
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # get PoolFileCatalog
    iLFN = 0
    strLFNs = ''
    url = dq2url + 'lrc/PoolFileCatalog'
    firstError = True
    # check if GUID lookup is supported
    useGUID = True
    status,out = curl.get(url,{'guids':'test'})
    if status ==0 and out == 'Must GET or POST a list of LFNs!':
        useGUID = False
    for lfn,vals in lfns.iteritems():
        iLFN += 1
        # make argument
        if useGUID:
            strLFNs += '%s ' % vals['guid']
        else:
            strLFNs += '%s ' % lfn
        if iLFN % 40 == 0 or iLFN == len(lfns):
            # get PoolFileCatalog
            strLFNs = strLFNs.rstrip()
            if useGUID:
                data = {'guids':strLFNs}
            else:
                data = {'lfns':strLFNs}
            # avoid too long argument
            strLFNs = ''
            # execute
            status,out = curl.get(url,data)
            time.sleep(2)
            if out.startswith('Error'):
                # LFN not found
                continue
            if status != 0 or (not out.startswith('<?xml')):
                if firstError:
                    print status,out
                    print "ERROR : LRC %s returned invalid response" % dq2url
                    firstError = False
                continue
            # parse
            try:
                root  = xml.dom.minidom.parseString(out)
                files = root.getElementsByTagName('File')
                for file in files:
                    # get PFN and LFN nodes
                    physical = file.getElementsByTagName('physical')[0]
                    pfnNode  = physical.getElementsByTagName('pfn')[0]
                    logical  = file.getElementsByTagName('logical')[0]
                    lfnNode  = logical.getElementsByTagName('lfn')[0]
                    # convert UTF8 to Raw
                    pfn = str(pfnNode.getAttribute('name'))
                    lfn = str(lfnNode.getAttribute('name'))
                    # remove /srm/managerv1?SFN=
                    pfn = re.sub('/srm/managerv1\?SFN=','',pfn)
                    # append
                    pfnMap[lfn] = pfn
            except:
                print status,out
                type, value, traceBack = sys.exc_info()
                print "ERROR : could not parse XML - %s %s" % (type, value)
                sys.exit(EC_Failed)
    # return        
    return pfnMap


# get list of missing LFNs from LRC
def getMissLFNsFromLRC(files,url,verbose=False):
    # get PFNs
    pfnMap = _getPFNsLRC(files,url,verbose)
    # check Files
    missFiles = []
    for file in files:
        if not file in pfnMap.keys():
            missFiles.append(file)
    return missFiles
                

# get PFN list from LFC
def _getPFNsLFC(fileMap,site,explicitSE,verbose=False):
    pfnMap = {}
    for path in sys.path:
        # look for PandaTools package
        if os.path.exists(path) and 'PandaTools' in os.listdir(path):
            lfcClient = '%s/PandaTools/LFCclient.py' % path
            if explicitSE:
                stList = getSE(site)
            else:
                stList = []
            lfcHost   = getLFC(site)
            inFile    = '%s_in'  % commands.getoutput('uuidgen')
            outFile   = '%s_out' % commands.getoutput('uuidgen')
            # write GUID/LFN
            ifile = open(inFile,'w')
            for lfn,vals in fileMap.iteritems():
                ifile.write('%s %s\n' % (vals['guid'],lfn))
            ifile.close()
            # construct command
            gridSrc = _getGridSrc()
            com = '%s python -Wignore %s -l %s -i %s -o %s' % (gridSrc,lfcClient,lfcHost,inFile,outFile)
            for index,stItem in enumerate(stList):
                if index != 0:
                    com += ',%s' % stItem
                else:
                    com += ' -s %s' % stItem
            if verbose:
                com += ' -v'
                print com
            # exeute
            status = os.system(com)
            if status == 0:
                ofile = open(outFile)
                line = ofile.readline()
                line = re.sub('\n','',line)
                exec 'pfnMap = %s' %line
                ofile.close()
            # remove tmp files    
            try:    
                os.remove(inFile)
                os.remove(outFile)
            except:
                pass
            # failed
            if status != 0:
                print "ERROR : failed to access LFC"
                sys.exit(EC_Failed)
            break
    # return
    return pfnMap


# get list of missing LFNs from LFC
def getMissLFNsFromLFC(fileMap,site,explicitSE,verbose=False):
    missList = []
    # get PFNS
    pfnMap = _getPFNsLFC(fileMap,site,explicitSE,verbose)
    for lfn,vals in fileMap.iteritems():
        if not vals['guid'] in pfnMap.keys():
            missList.append(lfn)
    # return
    return missList
    

# get grid source file
def _getGridSrc():
    # set Grid setup.sh if needed
    status,out = commands.getstatusoutput('which grid-proxy-info')
    if status == 0:
        gridSrc = ''
        status,athenaPath = commands.getstatusoutput('which athena.py')
        if status == 0 and athenaPath.startswith('/afs/in2p3.fr'):
            # for LYON, to avoid messing LD_LIBRARY_PATH
            gridSrc = '/afs/in2p3.fr/grid/profiles/lcg_env.sh'
        elif status == 0 and athenaPath.startswith('/afs/cern.ch'):
            # for CERN, VDT is already installed
            gridSrc = '/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh'
    else:
        # set Grid setup.sh
        if os.environ.has_key('PATHENA_GRID_SETUP_SH'):
            gridSrc = os.environ['PATHENA_GRID_SETUP_SH']
        else:
            if not os.environ.has_key('CMTSITE'):
                print "ERROR : CMTSITE is no defined in envvars"
                return False
            if os.environ['CMTSITE'] == 'CERN':
		gridSrc = '/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh'
            elif os.environ['CMTSITE'] == 'BNL':
                gridSrc = '/afs/usatlas.bnl.gov/lcg/current/etc/profile.d/grid_env.sh'
            else:
                # try to determin site using path to athena
                status,athenaPath = commands.getstatusoutput('which athena.py')
                if status == 0 and athenaPath.startswith('/afs/in2p3.fr'):
                    # LYON
                    gridSrc = '/afs/in2p3.fr/grid/profiles/lcg_env.sh'
                else:
                    print "ERROR : PATHENA_GRID_SETUP_SH is no defined in envvars"
                    print "  for CERN : export PATHENA_GRID_SETUP_SH=/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh"                
                    print "  for LYON : export PATHENA_GRID_SETUP_SH=/afs/in2p3.fr/grid/profiles/lcg_env.sh"
                    print "  for BNL  : export PATHENA_GRID_SETUP_SH=/afs/usatlas.bnl.gov/lcg/current/etc/profile.d/grid_env.sh"                
                    return False
    # check grid-proxy
    if gridSrc != '':
        gridSrc = 'source %s;' % gridSrc
        # some grid_env.sh doen't correct PATH/LD_LIBRARY_PATH
        gridSrc = "unset LD_LIBRARY_PATH; unset PYTHONPATH; export PATH=/usr/local/bin:/bin:/usr/bin; %s" % gridSrc
    # return
    return gridSrc


# get DN
def getDN(origString):
    shortName = ''
    distinguishedName = ''
    for line in origString.split('/'):
        if line.startswith('CN='):
            distinguishedName = re.sub('^CN=','',line)
            distinguishedName = re.sub('\d+$','',distinguishedName)
            distinguishedName = re.sub('\.','',distinguishedName)
            distinguishedName = distinguishedName.strip()
            if re.search(' ',distinguishedName) != None:
                # look for full name
                distinguishedName = distinguishedName.replace(' ','')
                break
            elif shortName == '':
                # keep short name
                shortName = distinguishedName
            distinguishedName = ''
    # use short name
    if distinguishedName == '':
        distinguishedName = shortName
    # return
    return distinguishedName



from HTMLParser import HTMLParser

class _monHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.map = {}
        self.switch = False
        self.td = False

    def getMap(self):
        retMap = {}
        if len(self.map) > 1:
            names = self.map[0]
            vals  = self.map[1]
            # values
            try:
                retMap['total']    = int(vals[names.index('Jobs')])
            except:
                retMap['total']    = 0
            try:    
                retMap['finished'] = int(vals[names.index('Finished')])
            except:
                retMap['finished'] = 0
            try:    
                retMap['failed']   = int(vals[names.index('Failed')])
            except:
                retMap['failed']   = 0
            retMap['running']  = retMap['total'] - retMap['finished'] - \
                                 retMap['failed']
        return retMap

    def handle_data(self, data):
        if self.switch:
            if self.td:
                self.td = False
                self.map[len(self.map)-1].append(data)
            else:
                self.map[len(self.map)-1][-1] += data
        else:
            if data == "Job Sets:":
                self.switch = True
        
    def handle_starttag(self, tag, attrs):
        if self.switch and tag == 'tr':
            self.map[len(self.map)] = []
        if self.switch and tag == 'td':
            self.td = True

    def handle_endtag(self, tag):
        if self.switch and self.td:
            self.map[len(self.map)-1].append("")
            self.td = False

# get jobInfo from Mon
def getJobStatusFromMon(id,verbose=False):
    # get name
    shortName = ''
    distinguishedName = ''
    for line in commands.getoutput('%s grid-proxy-info -identity' % _getGridSrc()).split('/'):
        if line.startswith('CN='):
            distinguishedName = re.sub('^CN=','',line)
            distinguishedName = re.sub('\d+$','',distinguishedName)
            distinguishedName = distinguishedName.strip()
            if re.search(' ',distinguishedName) != None:
                # look for full name
                break
            elif shortName == '':
                # keep short name
                shortName = distinguishedName
            distinguishedName = ''
    # use short name
    if distinguishedName == '':
        distinguishedName = shortName
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    data = {'job':'*',
            'jobDefinitionID' : id,
            'user' : distinguishedName,
            'days' : 100}
    # execute
    status,out = curl.get(baseURLMON,data)
    if status != 0 or re.search('Panda monitor and browser',out)==None:
        return {}
    # parse
    parser = _monHTMLParser()
    for line in out.split('\n'):
        if re.search('Job Sets:',line) != None:
            parser.feed( line )
            break
    return parser.getMap()


# run brokerage
def runBrokerage(sites,atlasRelease,cmtConfig=None,verbose=False):
    # serialize
    strSites = pickle.dumps(sites)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/runBrokerage'
    data = {'sites':strSites,
            'atlasRelease':atlasRelease}
    if cmtConfig != None:
        data['cmtConfig'] = cmtConfig
    return curl.get(url,data)
   

# exclude long,xrootd,local queues
def isExcudedSite(tmpID):
    excludedSite = False
    for exWord in ['ANALY_LONG_','_LOCAL','_test','_XROOTD']:
        if re.search(exWord,tmpID) != None:
            excludedSite = True
            break
    return excludedSite

