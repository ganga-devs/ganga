#!/bin/bash

"exec" "python" "-u" "-Wignore" "$0" "$@"

"""
Run Athena

Usage:

$ source $SITEROOT/setup.sh
$ source $T_DISTREL/AtlasRelease/*/cmt/setup.sh -tag_add=???
$ runAthena -l [libraries] -r [rundir] -j [jobOs] -i [inputs] -o [optputs] -c \
            -p [pool_refs] -u [lrc_url]

-l [libraries] : an archive which contains libraries
-r [rundir]    : relative path to the directory where Athena runs
-j [jobOs]     : job options passed to athena. format: 'options'
-i [inputs]    : list of input files. format: ['in1',...'inN']
-o [outputs]   : map of output files. format: {'type':'name',..}
                  type:'hist','ntuple','ESD','AOD','TAG','AANT','Stream1','THIST'
-b             : bytestream
-c             : event collection
-p [pool_refs] : list of POOL refs
-u [lrc_url]   : URL of LRC
-f [fragment]  : jobO fragment
-a [jobO files]: archive name of jobOs
-m [minbias]   : list of minimum bias files
-n [cavern]    : list of cavern files
--debug        : debug
--directIn     : read input files from SE
--oldPrefix    : old prefix to be replaced when converting TURL
--newPrefix    : new prefix to be used when converting TURL

Example:

runAthena \
  -l libraries.tgz \
  -r PhysicsAnalysis/AnalysisCommon/UserAnalysis/UserAnalysis-00-03-03/run \
  -j "-c 'EvtMax=10' opt.py RecExCommon.py" \
  -i ['input1.AOD.pool.root','input2.AOD.pool.root','input3.AOD.pool.root'] \
  -o ['hist':'hist.root','ntuple':'ntuple.root','log':'athena.log']

Procedure:

* expand libraries
* make PoolFileCatalog.xml
* create post-jobO which overwrites some parameters
* get PDGTABLE.MeV
* run athena

"""

# import optparse
import re
import os
import sys
import time
import types
import getopt
import commands
import urllib
import pickle
import xml.dom.minidom
import threading

# error code
EC_PoolCatalog  = 20
EC_MissingArg   = 30
EC_AthenaFail   = 40
EC_NoInput      = 141
EC_MissingInput = 142
EC_ARA          = 143
EC_DBRelease    = 144
EC_Coll         = 145
EC_WGET         = 146
EC_LFC          = 147

# command-line parameters
eventColl  = False
byteStream = False
backNavi   = False
debugFlag  = False
poolRefs = []
urlLRC = ''
libraries    = ''
fragmentJobO = ''
archiveJobO  = ''
minbiasFiles = []
cavernFiles  = []
beamHaloFiles= []
beamGasFiles = []
oldPrefix    = ''
newPrefix    = ''
directIn     = False
lfcHost      = ''
inputGUIDs   = []
minbiasGUIDs = []
cavernGUIDs  = []
shipInput    = False
addPoolFC    = []
corCheck     = False
sourceURL    = 'https://gridui07.usatlas.bnl.gov:25443'
mcData       = ''
notSkipMissing = False
givenPFN     = False
runTrf       = False
envvarFile   = ''
runAra       = False
dbrFile      = ''
generalInput = False
guidBoundary = []
collRefName  = ''
useNextEvent = False
liveLog      = ''
dbrRun       = -1
useLocalIO   = False
codeTrace    = False
useFileStager= False
usePFCTurl   = False
copyTool     = ''
eventPickTxt = ''
eventPickSt  = 0
eventPickNum = -1
skipInputByRetry = []

opts, args = getopt.getopt(sys.argv[1:], "l:r:j:i:o:bcp:u:f:a:m:n:e",
                           ["pilotpars","debug","oldPrefix=","newPrefix=",
                            "directIn","lfcHost=","inputGUIDs=","minbiasGUIDs=",
                            "cavernGUIDs=","shipInput","addPoolFC=","corCheck",
                            "sourceURL=","mcData=","notSkipMissing","givenPFN",
                            "beamHalo=","beamGas=","trf","envvarFile=","ara",
                            "dbrFile=","generalInput","guidBoundary=",
                            "collRefName=","useNextEvent","liveLog=",
                            "dbrRun=","useLocalIO","codeTrace","useFileStager",
                            "usePFCTurl",
                            "accessmode=","copytool=",
                            "eventPickTxt=","eventPickSt=","eventPickNum=",
                            "skipInputByRetry=",
                            ])
for o, a in opts:
    if o == "-l":
        libraries=a
    if o == "-r":
        runDir=a
    if o == "-j":
        jobO=urllib.unquote(a)
    if o == "-i":
        exec("inputFiles="+a)
    if o == "-o":
        exec("outputFiles="+a)
    if o == "-m":
        exec("minbiasFiles="+a)
    if o == "-n":
        exec("cavernFiles="+a)
    if o == "--beamHalo":
        exec("beamHaloFiles="+a)
    if o == "--beamGas":
        exec("beamGasFiles="+a)
    if o == "-b":
        byteStream = True
    if o == "-c":
        eventColl = True
    if o == "-p":
        exec("poolRefs="+a)
    if o == "-u":
        urlLRC=a
    if o == "-f":
        fragmentJobO=a
    if o == "-a":
        archiveJobO=a
    if o == "-e":
        backNavi = True
    if o == "--debug":
        debugFlag = True
    if o == "--oldPrefix":
        oldPrefix = a
    if o == "--newPrefix":
        newPrefix = a
    if o == "--directIn":
        directIn = True
    if o == "--lfcHost":
        lfcHost = a
    if o == "--inputGUIDs":
        exec("inputGUIDs="+a)
    if o == "--minbiasGUIDs":
        exec("minbiasGUIDs="+a)
    if o == "--cavernGUIDs":
        exec("cavernGUIDs="+a)
    if o == "--shipInput":
        shipInput = True
    if o == "--addPoolFC":
        addPoolFC = a.split(',')
    if o == "--corCheck":
        corCheck = True
    if o == "--sourceURL":
        sourceURL = a
    if o == "--mcData":
        mcData = a
    if o == "--notSkipMissing":
        notSkipMissing = True
    if o == "--givenPFN":
        givenPFN = True
    if o == "--trf":
        runTrf = True
    if o == "--envvarFile":
        envvarFile = a
    if o == "--ara":
        runAra = True
    if o == "--dbrFile":
        dbrFile = a
    if o == "--generalInput":
        generalInput = True
    if o == "--guidBoundary":
        exec("guidBoundary="+a)
    if o == "--collRefName":
        collRefName = a
    if o == "--useNextEvent":
        useNextEvent = True
    if o == "--liveLog":
        liveLog = a
    if o == "--dbrRun":
        dbrRun = a
    if o == "--useLocalIO":
        useLocalIO = True
    if o == "--codeTrace":
        codeTrace = True
    if o == "--useFileStager":
        useFileStager = True
    if o == "--usePFCTurl":
        usePFCTurl = True
    if o == "--copytool":
        copyTool = a
    if o == "--eventPickTxt":
        eventPickTxt = a
    if o == "--eventPickSt":
        eventPickSt = int(a)
    if o == "--eventPickNum":
        eventPickNum = int(a)
    if o == "--skipInputByRetry":
        skipInputByRetry = a.split(',')
        

# save current dir
currentDir = os.getcwd()

# change full path
if envvarFile != '':
    envvarFile = '%s/%s' % (currentDir,envvarFile)

# dump parameter
try:
    print "=== parameters ==="
    print "libraries",libraries
    print "runDir",runDir
    print "jobO",jobO
    print "inputFiles",inputFiles
    print "outputFiles",outputFiles
    print "byteStream",byteStream
    print "eventColl",eventColl
    print "backNavi",backNavi
    print "debugFlag",debugFlag
    print "poolRefs",poolRefs
    print "urlLRC",urlLRC
    print "fragmentJobO",fragmentJobO
    print "minbiasFiles",minbiasFiles
    print "cavernFiles",cavernFiles
    print "beamHaloFiles",beamHaloFiles
    print "beamGasFiles",beamGasFiles
    print "oldPrefix",oldPrefix
    print "newPrefix",newPrefix
    print "directIn",directIn
    print "lfcHost",lfcHost
    print "inputGUIDs",inputGUIDs
    print "minbiasGUIDs",minbiasGUIDs
    print "cavernGUIDs",cavernGUIDs
    print "addPoolFC",addPoolFC
    print "corCheck",corCheck
    print "sourceURL",sourceURL
    print "mcData",mcData
    print "notSkipMissing",notSkipMissing
    print "givenPFN",givenPFN
    print "runTrf",runTrf
    print "envvarFile",envvarFile
    print "runAra",runAra
    print "dbrFile",dbrFile
    print "generalInput",generalInput
    print "liveLog",liveLog
    print "dbrRun",dbrRun
    print "useLocalIO",useLocalIO
    print "codeTrace",codeTrace
    print "useFileStager",useFileStager
    print "usePFCTurl",usePFCTurl
    print "copyTool",copyTool
    print "eventPickTxt",eventPickTxt
    print "eventPickSt",eventPickSt
    print "eventPickNum",eventPickNum
    print "skipInputByRetry",skipInputByRetry
    print "==================="    
except:
    sys.exit(EC_MissingArg)

# disable direct input for unsupported cases
if directIn:
    if useLocalIO:
        # use local IO
        directIn = False
        print "disabled directIn due to useLocalIO"        
    elif byteStream and newPrefix.startswith('root://'):
        # BS on xrootd
        directIn = False
        print "disabled directIn for xrootd/RAW"

# remove skipped files
if skipInputByRetry != []: 
    tmpInputList = []
    for tmpLFN in inputFiles:
        if not tmpLFN in skipInputByRetry:
            tmpInputList.append(tmpLFN)
    inputFiles = tmpInputList
    print "removed skipped files -> %s" % str(inputFiles)
        
# log watcher
class LogWatcher (threading.Thread):
    # onstructor
    def __init__(self,fileName,logName):
        threading.Thread.__init__(self)
        self.fileName = fileName
        self.logName  = logName
        self.offset   = 0
        self.lock     = threading.Lock()

    # terminate thread
    def terminate(self):
        self.lock.acquire()

    # update
    def update(self):
        try:
            import zlib
            import socket
            import httplib
            import mimetools
            # read log
            logFH = open(self.fileName)
            logFH.seek(self.offset)
            logStr = logFH.read()
            logFH.close()
            # upload
            if len(logStr) != 0:
                # compress
                zStr = zlib.compress(logStr)
                # construct HTTP request
                boundary = mimetools.choose_boundary()
                body  = '--%s\r\n' % boundary
                body += 'Content-Disposition: form-data; name="file"; filename="%s"\r\n' % self.logName
                body += 'Content-Type: application/octet-stream\r\n'
                body += '\r\n' + zStr + '\r\n'
                body += '--%s--\r\n\r\n' % boundary
                headers = {'Content-Type': 'multipart/form-data; boundary=%s' % boundary,
                           'Content-Length': str(len(body))}
                url = '%s/server/panda/updateLog' % sourceURL
                match = re.search('[^:/]+://([^/]+)(/.+)',url)
                host = match.group(1)
                path = match.group(2)
                # set timeout
                socket.setdefaulttimeout(60)
                # HTTPS connection
                conn = httplib.HTTPSConnection(host)
                conn.request('POST',path,body,headers)
                resp = conn.getresponse()
                data = resp.read()
                conn.close()
                print "updated LogWatcher at %s" % time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                # increment offset
                self.offset += len(logStr)                
        except:
            type, value, traceBack = sys.exc_info()
            print 'failed to update LogWatcher %s - %s' % (type,value)
        
    # main
    def run(self):
        print "start LogWatcher"
        while True:
            # update
            self.update()
            # check lock
            if self.lock.acquire(0):
                self.lock.release()
                time.sleep(60)
            else:
                # last update just in case
                time.sleep(10)
                self.update()
                # terminate
                print "terminate LogWatcher"
                return
            

# get PFNs from LRC
def _getPFNsFromLRC (urlLRC ,items,isGUID=True,old_prefix='',new_prefix=''):
    import urllib2
    # old prefix for regex
    old_prefix_re = old_prefix.replace('?','\?')
    pfnMap = {}
    if len(items)>0:
        # get PoolFileCatalog
        iITEM = 0
        strITEMs = ''
        for item in items:
            iITEM += 1
            # make argument
            strITEMs += '%s ' % item
            if iITEM % 35 == 0 or iITEM == len(items):
                # get PoolFileCatalog
                strITEMs = strITEMs.rstrip()
                if isGUID:
                    data = {'guids':strITEMs}
                else:
                    data = {'lfns':strITEMs}                    
                # avoid too long argument
                strITEMs = ''
                # GET
                url = '%s/lrc/PoolFileCatalog?%s' % (urlLRC,urllib.urlencode(data))
                req = urllib2.Request(url)
                fd = urllib2.urlopen(req)
                out = fd.read()
                if out.startswith('Error'):
                    continue
                if not out.startswith('<?xml'):
                    continue
                # get SURLs
                try:
                    root  = xml.dom.minidom.parseString(out)
                    files = root.getElementsByTagName('File')
                    for file in files:
                        # get ID
                        id = str(file.getAttribute('ID'))
                        # get PFN node
                        physical = file.getElementsByTagName('physical')[0]
                        pfnNode  = physical.getElementsByTagName('pfn')[0]
                        # convert UTF8 to Raw
                        pfn = str(pfnNode.getAttribute('name'))
                        # remove :8443/srm/managerv1?SFN=
                        pfn = re.sub(':8443/srm/managerv1\?SFN=','',pfn)
                        if old_prefix=='':
                            # remove protocol and host
                            pfn = re.sub('^[^:]+://[^/]+','',pfn)
                            # remove redundant /
                            pfn = re.sub('^//','/',pfn)
                            # put dcache if /pnfs
                            if pfn.startswith('/pnfs'):
                                pfn = 'dcache:%s' % pfn
                        else:
                            # check matching
                            if re.search(old_prefix_re,pfn) == None:
                                continue
                            # replace prefix
                            pfn = re.sub(old_prefix_re,new_prefix,pfn)
                        # append
                        pfnMap[id] = pfn
                except:
                    pass
    return pfnMap


# run LFC stuff in grid runtime
def _getPFNsFromLFC (lfcHost,guids, oldPrefix='', newPrefix='', qryMetadata=False, envvarFile=''):

    lfcCommand = """
import os
import re
import sys
import time
import pickle

# get PFNs from LFC
def _getPFNsFromLFC (lfc_host,items,old_prefix='',new_prefix='',getMeta=False):
    retVal = 0
    pfnMap = {}
    metaMap = {}
    # old prefix for regex
    old_prefix_re = old_prefix.replace('?','\?')
    # import lfc
    try:
        import lfc
    except:
        print "ERROR : cound not import lfc"
        retVal = 1
        return retVal,pfnMap
    # set LFC HOST
    os.environ['LFC_HOST'] = lfc_host
    # check bulk-operation
    if not hasattr(lfc,'lfc_getreplicas'):
        print "ERROR : bulk-ops is unsupported"
        retVal = 2
        return retVal,pfnMap
    frList = []
    # set nGUID for bulk-ops
    nGUID = 100
    iGUID = 0
    mapLFN = {}
    listGUID = []
    # loop over all items
    for item in items:
        iGUID += 1
        listGUID.append(item)
        if iGUID % nGUID == 0 or iGUID == len(items):
            # get replica
            nTry = 5
            for iTry in range(nTry):
                ret,resList = lfc.lfc_getreplicas(listGUID,'')
                if ret == 0 or iTry+1 == nTry:
                    break
                print "sleep due to LFC error"
                time.sleep(60)
            if ret != 0:
                err_num = lfc.cvar.serrno
                err_string = lfc.sstrerror(err_num)
                print "ERROR : LFC access failure - %s" % err_string
            else:
                for fr in resList:
                    if fr != None and ((not hasattr(fr,'errcode')) or \
                                       (hasattr(fr,'errcode') and fr.errcode == 0)):
                        print "replica found for %s" % fr.guid
                        # skip empty or corrupted SFN
                        if fr.sfn == '' or re.search('[^\w\./\-\+\?:&=]',fr.sfn) != None:
                            print "WARNING : wrong SFN '%s'" % fr.sfn
                            continue
                        print fr.sfn
                        # check host
                        if old_prefix != '' and (not fr.sfn.startswith(old_prefix)) and re.search(old_prefix_re,fr.sfn) == None:
                            continue
                        guid = fr.guid
                        # use first one
                        if pfnMap.has_key(guid):
                            onDiskFlag = False
                            for diskPath in ['/MCDISK/','/BNLT0D1/','/atlasmcdisk/','/atlasdatadisk/']:
                                if re.search(diskPath,fr.sfn) != None:
                                    onDiskFlag = True
                                    break
                            if not onDiskFlag:
                                continue
                            print "use disk replica"
                        if (old_prefix != '' and re.search(old_prefix_re,fr.sfn) == None) or old_prefix == '':
                            # conver to compact format
                            pfn = fr.sfn
                            pfn = re.sub('(:\d+)*/srm/v\d+/server\?SFN=','',pfn)
                            pfn = re.sub('(:\d+)*/srm/managerv\d+\?SFN=','',pfn)
                            # remove protocol and host
                            pfn = re.sub('[^:]+://[^/]+','',pfn)
                            pfn = new_prefix + pfn
                        else:
                            pfn = re.sub(old_prefix_re,new_prefix,fr.sfn)
                        # assign
                        pfnMap[guid] = pfn
                        # get metadata
                        if getMeta:
                            tmpMetaItem = {}
                            tmpMetaItem['size'] = fr.filesize
                            if fr.csumtype == 'AD':
                                tmpMetaItem['checksum'] = fr.csumvalue
                            # assign
                            metaMap[guid] = tmpMetaItem
            # reset
            listGUID = []
    # return PFN and metadata
    if getMeta:
        return retVal,{'pfn':pfnMap,'meta':metaMap}
    # return PFN only
    return retVal,pfnMap
"""

    directMetaMap = {}
    directTmp     = {}

    lfcPy = '%s/%s.py' % (os.getcwd(),commands.getoutput('uuidgen 2>/dev/null'))
    lfcOutPi = '%s/lfc.%s' % (os.getcwd(),commands.getoutput('uuidgen 2>/dev/null'))
    lfcPyFile = open(lfcPy,'w')
    lfcPyFile.write(lfcCommand)

    if qryMetadata:
        tmpGetPfnLfcStr = "st,out= _getPFNsFromLFC ('%s',%s,old_prefix='%s',new_prefix='%s',getMeta=True)\n"
    else:
        tmpGetPfnLfcStr = "st,out= _getPFNsFromLFC ('%s',%s,old_prefix='%s',new_prefix='%s')\n"

    lfcPyFile.write(tmpGetPfnLfcStr % (lfcHost,guids,oldPrefix,newPrefix))

    lfcPyFile.write("""
outPickFile = open('%s','w')
pickle.dump(out,outPickFile)
outPickFile.close()
sys.exit(st)
""" % lfcOutPi)

    lfcPyFile.close()

    # run LFC access in grid runtime
    lfcSh = '%s.sh' % commands.getoutput('uuidgen 2>/dev/null')

    if envvarFile != '':
        commands.getoutput('cat %s > %s' % (envvarFile,lfcSh))

    # check LFC module
    print "->check LFC.py"
    lfcS,lfcO = commands.getstatusoutput('python -c "import lfc"')
    print lfcS
    print lfcO
    if lfcS == 0:
        commands.getoutput('echo "python %s" >> %s' % (lfcPy,lfcSh))
    else:
        # use system python
        print "->use /usr/bin/python"
        commands.getoutput('echo "/usr/bin/python %s" >> %s' % (lfcPy,lfcSh))
    commands.getoutput('chmod +x %s' % lfcSh)
    tmpSt,tmpOut = commands.getstatusoutput('./%s' % lfcSh)
    print tmpSt
    print tmpOut
    # error check
    if re.search('ERROR : LFC access failure',tmpOut) != None:
        sys.exit(EC_LFC)

    if tmpSt == 0:
        lfcOutPiFile = open(lfcOutPi)
        directTmp = pickle.load(lfcOutPiFile)
        lfcOutPiFile.close()
        # decompose if pfn+meta is returned
        if 'meta' in directTmp:
            directMetaMap = directTmp['meta']
            directTmp = directTmp['pfn']
    else:
        directTmp     = {}
        directMetaMap = {}

    return directTmp, directMetaMap


pCopyToolName = 'pCopyToolWrapper'

pCopyToolStr = """#!/bin/bash

"exec" "python" "-u" "-Wignore" "$0" "$@"

import os
import sys
import time
import zlib
import commands

# change stdout
stdOutFD = os.open('pCopyToolWrapper.log',os.O_WRONLY|os.O_APPEND|os.O_CREAT)
os.dup2(stdOutFD, sys.stdout.fileno())

execfile('pCopyToolWrapperConf.py')

for arg in sys.argv[1:]:
    com += ' %s' % arg
lfilename = sys.argv[-1]
rfilename = sys.argv[-2]
nTry = 3    
for iTry in range(nTry):
    # copy
    print 'INFO: Try %s/%s : %s' % (iTry+1,nTry,com)
    fork_child_pid = 0
    fork_child_pid = os.fork()
    if fork_child_pid == -1:
        print "ERROR: failed to fork"
        sys.exit(1)
    if fork_child_pid == 0:
        # copy process
        status = os.system(com)
        status %= 255
        if status != 0:
            print "ERROR: failed to copy with" % status
            os._exit(status)
        # get adler32
        if not metaMap.has_key(rfilename):
            print "INFO: skip checksum verification"
            print "INFO: succeeded"
            print
            os._exit(0)
        radler32 = metaMap[rfilename]
        # calculate adler32
        sum1 = 1L
        try:
            f = open(lfilename, 'rb')
        except:
            print "ERROR: failed to open %s" % lfilename
            os._exit(1)
        for line in f:
            sum1 = zlib.adler32(line, sum1)
        f.close()
        # correction for bug 32 bit zlib
        if sum1 < 0:
            sum1 = sum1 + 2**32
        # convert to hex
        sum1 = "%08x" % sum1
        # compare
        if radler32 == sum1:
            print "INFO: checksum verified"
            print "INFO: succeeded"
            print                
            os._exit(0)
        # checksum failure
        print "ERROR: invalid checksum remote:%s local:%s" % (radler32,sum1)
        os._exit(1)
    else:
        # master process
        childExit    = False
        childStatus  = None
        timeoutValue = 900
        for iTimeOutTry in range(timeoutValue):
            # check child status
            pStat = os.waitpid(fork_child_pid,os.WNOHANG)
            if pStat[0] == 0:
                time.sleep(1)
            else:
                childExit = True
                childStatus = (pStat[1] & 0xFFFF) >> 8
                if childStatus == 0:
                    # return SUCCEEDED
                    sys.exit(0)
                break
        # kill child processes since it timed out
        if not childExit:
            print "ERROR: timeout"
            masterPID  = os.getpid()
            masterPGID = os.getpgrp()
            ppIDList = [fork_child_pid]
            psOutput = commands.getoutput('ps axjfww')
            for psLine in psOutput.split('\\n'):
                psItems = psLine.split()
                # get PPID
                try:
                    ppID = int(psItems[0])
                except:
                    ppID = None
                # get PID
                try:
                    pID = int(psItems[1])
                except:
                    pID = None
                # get PGID
                try:
                    pgID = int(psItems[2])
                except:
                    pgID = None
                # kill if it belongs to the child process
                if pgID == masterPGID and ppID in ppIDList:
                    # collect PPID
                    if not pID in ppIDList:
                        ppIDList.append(pID)
                    try:
                        # kill with SIGTERM
                        os.kill(pID,signal.SIGTERM)
                    except:
                        pass
                    try:
                        # kill with SIGKILL just in case
                        os.kill(pID,signal.SIGKILL)
                    except:
                        pass
            # wait for termination of child process
            try:
                os.wait()
            except:
                pass
        # cleanup and sleep
        commands.getoutput('rm -f %s' % lfilename)
        if iTry+1 < nTry:
            time.sleep(60)
# eventually failed
print "ERROR: failed to stage %s" % rfilename
print
sys.exit(1)
"""

#### H.C.LEE - wrapping up environment variables with double quote
def wrapEnvironVars( envvarFile='' ):
    # add "" in envvar
    try:
        newString = '#!/bin/bash\n'
        if envvarFile != '':
            tmpEnvFile = open(envvarFile)
            for line in tmpEnvFile:
                # remove \n
                line = line[:-1]
                match = re.search('([^=]+)=(.*)',line)
                if match != None:
                    # add ""
                    newString += '%s="%s"\n' % (match.group(1),match.group(2))
                else:
                    newString += '%s\n' % line
            tmpEnvFile.close()
            # overwrite
            tmpEnvFile = open(envvarFile,'w')
            tmpEnvFile.write(newString)
            tmpEnvFile.close()
    except:
        type, value, traceBack = sys.exc_info()
        print 'WARNING: changing envvar %s - %s' % (type,value)

wrapEnvironVars( envvarFile=envvarFile)


##### H.C. LEE - get files from PoolFileCatalog
def getFilesFromPFC():
    """
    parsing PFC.xml and put file guid, lfn, pfn into separate dictionaries:

    - guidMapFromPFC: using "lfn" as key, "guid" as value
    - directTmpTurl : using "guid" as key, "pfn" as value
    """
    guidMapFromPFC = {}
    directTmpTurl = {}
    try:
        print "===== PFC from pilot ====="
        tmpPcFile = open("PoolFileCatalog.xml")
        print tmpPcFile.read()
        tmpPcFile.close()
        # parse XML
        root  = xml.dom.minidom.parse("PoolFileCatalog.xml")
        files = root.getElementsByTagName('File')
        for file in files:
            # get ID
            id = str(file.getAttribute('ID'))
            # get PFN node
            physical = file.getElementsByTagName('physical')[0]
            pfnNode  = physical.getElementsByTagName('pfn')[0]
            # convert UTF8 to Raw
            pfn = str(pfnNode.getAttribute('name'))
            lfn = pfn.split('/')[-1]
            # append
            guidMapFromPFC[lfn] = id
            directTmpTurl[id] = pfn
    except:
        type, value, traceBack = sys.exc_info()
        print 'ERROR : Failed to collect GUIDs : %s - %s' % (type,value)

    return guidMapFromPFC, directTmpTurl

guidMapFromPFC, directTmpTurl = getFilesFromPFC()
print "===== GUIDs in PFC ====="
print guidMapFromPFC

### H.C.LEE - resolve inputfile information
def resolveInputFiles(directTmpTurl=directTmpTurl):
    """
     - 3 cases: "directIn", "givenPFN", or others

     - for "directIn":
       * usePFCTurl (LFN:PFN given directly in PFC.xml)
       * getting PFN from LFC if lfcHost is given (in FS case, file metadata also queried from LFC)
       * getting PFN from LRC otherwise

     - for "givenPFN":
       * aggregate file LFN given by user

     - for "others":
       * get file LFN by listing current directory
    """
    directPfnMap = {}
    directMetaMap = {}
    if directIn:
        if usePFCTurl:
            # Use the TURLs from PoolFileCatalog.xml created by pilot
            print "===== GUIDs and TURLs in PFC ====="
            print directTmpTurl
            directTmp = directTmpTurl
        else:
            if lfcHost != '':

                directTmp, directMetaMap = _getPFNsFromLFC(lfcHost=lfcHost,
                                                           guids=inputGUIDs+minbiasGUIDs+cavernGUIDs,
                                                           oldPrefix=oldPrefix,
                                                           newPrefix=newPrefix,
                                                           qryMetadata=useFileStager,
                                                           envvarFile=envvarFile)
            else:
                # get PFNs from LRC
                directTmp = _getPFNsFromLRC (urlLRC,inputFiles+minbiasFiles+cavernFiles,
                                             isGUID=False,old_prefix=oldPrefix,
                                             new_prefix=newPrefix)
        # collect LFNs
        curFiles   = []
        directPFNs = {}
        for id in directTmp.keys():
            lfn = directTmp[id].split('/')[-1]
            curFiles.append(lfn)
            directPFNs[lfn] = directTmp[id]
        directPfnMap = directTmp
    elif givenPFN:
        # collect LFNs
        curFiles   = []
        for lfn in inputFiles+minbiasFiles+cavernFiles+beamHaloFiles+beamGasFiles:
            curFiles.append(lfn)
    else:
        curFiles = os.listdir('.')
        
    return directPfnMap,directMetaMap,directPFNs,curFiles

directPfnMap,directMetaMap,directPFNs,curFiles = resolveInputFiles(directTmpTurl=directTmpTurl)

#### H.C.LEE - update the input files with exactly the PFNs
def updateInputFiles( inputFiles, curFiles, directPFNs ):
    """
    update the given inputFiles with PFN given the previously resolved input file information
    """
    tmpFiles = tuple(inputFiles)

    for tmpF in tmpFiles:
        findF = False
        findName = ''
        for curF in curFiles:
            if re.search('^'+tmpF,curF) != None:
                findF = True
                findName = curF
                break
        # remove if not exist
        if not findF:
            print "%s not exist" % tmpF
            inputFiles.remove(tmpF)
        # use URL
        if directIn and findF:
            inputFiles.remove(tmpF)
            inputFiles.append(directPFNs[findName])
    if len(inputFiles) == 0:
        print "No input file is available"
        sys.exit(EC_NoInput)
    if notSkipMissing and len(inputFiles) != len(tmpFiles):
        print "Some input files are missing"
        sys.exit(EC_MissingInput)


flagMinBias = False
flagCavern  = False
flagBeamGas = False
flagBeamHalo= False

if len(inputFiles) > 0 and (not shipInput):
    updateInputFiles( inputFiles=inputFiles, curFiles=curFiles, directPFNs=directPFNs )
                         
if len(minbiasFiles) > 0:
    flagMinBias = True
    updateInputFiles( inputFiles=minbiasFiles, curFiles=curFiles, directPFNs=directPFNs )
        
if len(cavernFiles) > 0:
    flagCavern = True
    updateInputFiles( inputFiles=cavernFiles, curFiles=curFiles, directPFNs=directPFNs )

if len(beamHaloFiles) > 0:
    flagBeamHalo = True
    updateInputFiles( inputFiles=beamHaloFiles, curFiles=curFiles, directPFNs=directPFNs )

if len(beamGasFiles) > 0:
    flagBeamGas = True
    updateInputFiles( inputFiles=beamGasFiles, curFiles=curFiles, directPFNs=directPFNs )

print "=== New inputFiles ==="
print inputFiles
if flagMinBias:
    print "=== New minbiasFiles ==="
    print minbiasFiles
if flagCavern:    
    print "=== New cavernFiles ==="
    print cavernFiles
if flagBeamHalo:
    print "=== New beamHaloFiles ==="
    print beamHaloFiles
if flagBeamGas:
    print "=== New beamGasFiles ==="
    print beamGasFiles

#### H.C.LEE - preparing the workspace for running athena user code
def prepareWorkspace():
    """
    - create workdir
    - expand libraries
    - expend joboption archive
    - create cmtdir
    - resolve Athena version

    returns workDir,cmtDir,athenaVer
    """

    workDir = currentDir+"/workDir"
    commands.getoutput('rm -rf %s' % workDir)
    os.makedirs(workDir)
    os.chdir(workDir)

    # expand libraries
    if libraries == '':
        pass
    elif libraries.startswith('/'):
        print commands.getoutput('tar xvfzm %s' % libraries)
    else:
        print commands.getoutput('tar xvfzm %s/%s' % (currentDir,libraries))

    # expand jobOs if needed
    if archiveJobO != "":
        print "--- wget for jobO ---"
        output = commands.getoutput('wget -h')
        wgetCommand = 'wget'
        for line in output.split('\n'):
            if re.search('--no-check-certificate',line) != None:
                wgetCommand = 'wget --no-check-certificate'
                break
        com = '%s %s/cache/%s' % (wgetCommand,sourceURL,archiveJobO)
        nTry = 3
        for iTry in range(nTry):
            print 'Try : %s' % iTry
            status,output = commands.getstatusoutput(com)
            print output
            if status == 0:
                break
            if iTry+1 == nTry:
                print "ERROR : cound not get jobO files from panda server"
                sys.exit(EC_WGET)
            time.sleep(30)
        print commands.getoutput('tar xvfzm %s' % archiveJobO)

    # make rundir just in case
    commands.getoutput('mkdir %s' % runDir)
    # go to run dir
    os.chdir(runDir)

    # make cmt dir
    cmtDir = '%s/%s/cmt' % (workDir,commands.getoutput('uuidgen 2>/dev/null'))
    commands.getoutput('mkdir -p %s' % cmtDir)

    # get athena version
    com  = 'cd %s;' % cmtDir
    com += 'echo "use AtlasPolicy AtlasPolicy-*" > requirements;'
    com += 'cmt config;'
    com += 'source ./setup.sh;'
    com += 'cd -;'
    com += 'cmt show projects'
    out = commands.getoutput(com)
    athenaVer = None
    try:
        for line in out.split('\n'):
            if not line.startswith('#'):
                items = line.split()
                if items[0] in ('dist','AtlasRelease','AtlasOffline'):
                    athenaVer = items[1]
                    break
    except:
        print out
        print "ERROR : cannot get Athena Version"

    print "Athena Ver:%s" % athenaVer

    return workDir,cmtDir,athenaVer

workDir,cmtDir,athenaVer = prepareWorkspace()

# create PoolFC
def _createPoolFC(pfnMap):
    outFile = open('PoolFileCatalog.xml','w')
    # write header
    header = \
    """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
    <!-- Edited By POOL -->
    <!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
    <POOLFILECATALOG>
    """
    outFile.write(header)
    # write files
    item = \
    """
      <File ID="%s">
        <physical>
          <pfn filetype="ROOT_All" name="%s"/>
        </physical>
        <logical/>
      </File>
    """
    for guid,pfn in pfnMap.iteritems():
        outFile.write(item % (guid.upper(),pfn))
    # write trailer
    trailer = \
    """
    </POOLFILECATALOG>
    """
    outFile.write(trailer)
    outFile.close()
    

# build pool catalog

print "build pool catalog"
commands.getoutput('rm -f PoolFileCatalog.xml')
# for input files
if eventColl and mcData == '':
    # ROOT ver collection or AANT
    if len(inputFiles)>0:
        # use Collection Tools
        if guidBoundary != [] and collRefName != '':
            newInputFiles = []
            for fileName in inputFiles:
                # split
                com = 'CollSplitByGUID.exe -splitref %s -src PFN:%s RootCollection' % \
                      (collRefName,fileName)
                print com
                status,out = commands.getstatusoutput(com)
                if status != 0:
                    print out
                    print status
                    print "Failed to run %s" % com
                    sys.exit(EC_Coll)
                # get sub_collection_*
                subCollectionMap = {}
                filesInCurDir = os.listdir('.') 
                for tmpName in filesInCurDir:
                    match = re.search('sub_collection_(\d+)\.root',tmpName)
                    if  match != None:
                        subCollectionMap[int(match.group(1))] = tmpName
                # sort
                subCollection = subCollectionMap.keys()
                subCollection.sort()
                for idxSubColl in subCollection:
                    tmpName = subCollectionMap[idxSubColl]
                    # check if corresponding GUID is there
                    com = 'CollListFileGUID.exe -queryopt %s -src PFN:%s RootCollection' % \
                          (collRefName,tmpName)
                    print com
                    status,out = commands.getstatusoutput(com)
                    print out
                    if status != 0:
                        print status
                        print "Failed to run %s" % com
                        sys.exit(EC_Coll)
                    # look for GUID
                    foundGUID = False
                    for tmpGUID in guidBoundary:
                        if out.find(tmpGUID) != -1:
                            foundGUID = True
                            break
                    if foundGUID:
                        # rename
                        newName = '%s.%s' % (fileName,tmpName)
                        os.rename(tmpName,newName)
                        # append
                        newInputFiles.append(newName)
                    else:
                        # remove
                        os.remove(tmpName)
            # use new list
            if newInputFiles == []:
                print "Empty input list after CollXYZFileGUID.exe"
                sys.exit(EC_Coll)
            inputFiles = newInputFiles
            # add references
            poolRefs += guidBoundary
        else:
            # get extPoolRefs.C
            macroPoolRefs = 'extPoolRefs.C'
            # append workdir to CMTPATH
            env = 'CMTPATH=%s:$CMTPATH' % workDir
            com  = 'export %s;' % env
            com += 'cd %s;' % cmtDir
            com += 'echo "use AtlasPolicy AtlasPolicy-*" > requirements;'
            com += 'cmt config;'
            com += 'source ./setup.sh;'
            com += 'cd -;'
            com += 'get_files -jo %s' % macroPoolRefs
            print commands.getoutput(com)
            for fileName in inputFiles:
                # build ROOT command
                com = 'echo '
                if not directIn:
                    # form symlink to input file without attemptNr
                    newFileName = re.sub('\.\d+$','',fileName)
                    try:
                        os.symlink('%s/%s' % (currentDir,fileName),newFileName)
                    except:
                        pass
                    # create symlink with attemptNr for new Athena version
                    try:
                        os.symlink('%s/%s' % (currentDir,fileName),fileName)
                    except:
                        pass
                    # just in case for shipInput
                    if shipInput:
                        try:
                            os.rename(fileName,newFileName)
                        except:
                            pass
                        try:
                            os.symlink(newFileName,fileName)
                        except:
                            pass
                else:
                    # direct reading from SE
                    newFileName = fileName
                    
                if athenaVer == None or athenaVer < "14.4.0":
                    # use old macro
                    com += '%s ' % newFileName
                    com += ' --- | root.exe -b %s' % macroPoolRefs
                    print com
                    status,output = commands.getstatusoutput(com)
                    print output
                    # get POOL refs
                    for line in output.split('\n'):
                        if line.startswith('PoolRef:') or line.startswith('ESD Ref:') or \
                               line.startswith('RDO Ref:') or line.startswith('St1 Ref:') or \
                               line.startswith('RAW Ref:'):
                            match = re.search('\[DB=([^\]]+)\]',line)
                            if match != None:
                                if not match.group(1) in poolRefs:
                                    poolRefs.append(match.group(1))
                else:
                    # use CollListFileGUID for 14.4.0 onward
                    com = 'CollListFileGUID.exe -counts -src PFN:%s RootCollection' % newFileName
                    print com
                    status,output = commands.getstatusoutput(com)
                    print output
                    # get POOL refs
                    for line in output.split('\n'):
                        tmpItems = line.split()
                        if len(tmpItems) != 3:
                            continue
                        if re.search('^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$',tmpItems[1]) != None:
                            if not tmpItems[1] in poolRefs:
                                poolRefs.append(tmpItems[1])


    # new poolRefs
    print "=== New poolRefs ==="
    print poolRefs
    if len(poolRefs)>0:
        if lfcHost != '':
            # get PFNs from LFC
            pfnMap,dummy = _getPFNsFromLFC(lfcHost=lfcHost,
                                           guids=poolRefs,
                                           oldPrefix=oldPrefix,
                                           newPrefix=newPrefix,
                                           qryMetadata=False,
                                           envvarFile=envvarFile)
        else:
            # get PFNs from LRC 
            pfnMap = _getPFNsFromLRC (urlLRC,poolRefs,isGUID=True,
                                      old_prefix=oldPrefix,
                                      new_prefix=newPrefix)

        print "=== Create PoolFC ==="
        for ref in poolRefs:
            if ref not in pfnMap:
                print " %s not found" % ref
                
        # create PoolFC
        _createPoolFC(pfnMap)

elif len(inputFiles+minbiasFiles+cavernFiles+beamHaloFiles+beamGasFiles) > 0:
    # POOL or BS files
    filesToPfcMap = {}
    for fileName in inputFiles+minbiasFiles+cavernFiles+beamHaloFiles+beamGasFiles:
        if (not directIn) and (not givenPFN):
            targetName = fileName
            # for rome data
            if re.search(fileName,'\.\d+$')==None and (not fileName in curFiles):
                for cFile in curFiles:
                    if re.search('^'+fileName,cFile) != None:
                        targetName = cFile
                        break
            # form symlink to input file
            try:
                os.symlink('%s/%s' % (currentDir,targetName),fileName)
            except:
                pass
        if (not byteStream) and mcData == '' and (not generalInput) and not (runTrf and not runAra):
            # corruption check by scanning all TTrees
            if corCheck:
                # construct command
                print "=== check corruption for %s ===" % fileName
                optPy = '%s.py' % commands.getoutput('uuidgen 2>/dev/null')
                outFile = open(optPy,'w')
                outFile.write("""
import sys
import ROOT
try:
    import AthenaROOTAccess.transientTree
except:
    pass
t = ROOT.TFile('%s')
""" % fileName)
                outFile.write("""
keyList = t.GetListOfKeys()
scannedKeys = []
for keyItem in keyList:
    tree = keyItem.ReadObj()
    if tree.GetName() in scannedKeys:
        continue
    print '===%s===' % tree.GetName()
    if tree.GetName() == 'CollectionTree':
        try:
            import AthenaROOTAccess.transientTree
            tree = ROOT.AthenaROOTAccess.TChainROOTAccess('CollectionTree')
        except:
            pass
    nEvent = tree.GetEntriesFast()
    print nEvent
    detectFlag = False
    try:
        for iEvent in range(nEvent):
            ret = tree.GetEntry(iEvent)
            if ret < 0:
                print ret
                print iEvent
                detectFlag = True
                sys.exit(1)
    except:
        if detectFlag:
            sys.exit(1)
        type, value, traceBack = sys.exc_info()
        print 'EXCEPT: %s - %s' % (type,value)
        if 'St9bad_alloc' in str(value):
            sys.exit(2)
    scannedKeys.append(tree.GetName())
sys.exit(0)
""")
                outFile.close()
                # run checker
                status,out = commands.getstatusoutput('python %s' % optPy)
                print status
                print out
                commands.getoutput('rm -f %s' % optPy)
                if status != 0 \
                       or out.find("read too few bytes") != -1 \
                       or out.find("read too many bytes") != -1 \
                       or out.find("segmentation violation") != -1:
                    print "->skip %s" % fileName
                    continue
            # insert it to pool catalog
            tmpLFNforPFC = fileName.split('/')[-1]
            tmpLFNforPFC = re.sub('__DQ2-\d+$','',tmpLFNforPFC)
            if tmpLFNforPFC in guidMapFromPFC:
                filesToPfcMap[guidMapFromPFC[tmpLFNforPFC]] = fileName
            elif not givenPFN:
                print "ERROR : %s not found in the pilot PFC" % fileName
        # create PFC for directIn + trf
        if directIn and (runTrf and not runAra):
            _createPoolFC(directPfnMap)
            # form symlink to input file mainly for DBRelease
            for tmpID in directPfnMap.keys():
                lfn = directPfnMap[tmpID].split('/')[-1] 
                try:
                    targetName = '%s/%s' % (currentDir,lfn)
                    if os.path.exists(targetName):
                        os.symlink(targetName,lfn)
                except:
                    pass
    # create PFC for local files
    if filesToPfcMap != {}: 
        _createPoolFC(filesToPfcMap)
    elif givenPFN:
        # insert using pool_insertFTC since GUIDs are unavailabe from the pilot 
        for fileName in inputFiles+minbiasFiles+cavernFiles+beamHaloFiles+beamGasFiles:
            com = 'pool_insertFileToCatalog %s' % fileName
            print com
            os.system(com)
        
    # read PoolFileCatalog.xml
    pLines = ''
    try:
        pFile = open('PoolFileCatalog.xml')
        for line in pFile:
            pLines += line
        pFile.close()
    except:
        if mcData == '' and not (runTrf and not runAra):
            print "ERROR : cannot open PoolFileCatalog.xml"
    # remove corrupted files
    print "=== corruption check ==="
    # doesn't check BS/nonRoot files since they don't invoke insert_PFC
    if (not byteStream) and mcData == '' and (not generalInput) and not (runTrf and not runAra):
        tmpFiles = tuple(inputFiles)
        for tmpF in tmpFiles:
            if re.search(tmpF,pLines) == None:
                inputFiles.remove(tmpF)
                print "%s is corrupted or non-ROOT file" % tmpF
        if notSkipMissing and len(inputFiles) != len(tmpFiles):
            print "Some input files are missing"
            sys.exit(EC_MissingInput)
    if len(inputFiles)==0:        
        print "No input file is available after corruption check"
        sys.exit(EC_NoInput)        
    # extract POOL refs
    if backNavi:
        # construct command
        evtPy = 'EventCount.py'
        optPy = '%s.py' % commands.getoutput('uuidgen 2>/dev/null')
        print "=== run %s to extract POOL refs ===" % evtPy
        com  = 'get_files -jo %s;' % evtPy
        com += 'echo "theApp.EvtMax=-1" > %s;' % optPy
        com += 'athena.py -c "In=%s" %s %s;' % (inputFiles,evtPy,optPy)
        com += 'rm -f %s' % optPy
        # run athena
        status,out = commands.getstatusoutput(com)
        print out
        # extract
        flagStream = False
        for line in out.split('\n'):
            if re.search('^EventCount',line) == None:
                continue
            if re.search("Input contained references to the following File GUID",line) != None:
                flagStream = True
                continue
            if re.search("Input contained the following CLIDs and Keys",line) != None:
                flagStream = False
                continue
            if flagStream:
                match = re.search('([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})',
                                  line)
                if match != None:
                    poolRefs.append(match.group(1))


        # new poolRefs
        print "=== New poolRefs ==="
        print poolRefs
        if len(poolRefs)>0:
            if lfcHost != '':
                # get PFNs from LFC
                pfnMap,dummy = _getPFNsFromLFC(lfcHost=lfcHost,
                                               guids=poolRefs,
                                               oldPrefix=oldPrefix,
                                               newPrefix=newPrefix,
                                               qryMetadata=False,
                                               envvarFile=envvarFile)
            else:
                # get PFNs from LRC 
                pfnMap = _getPFNsFromLRC (urlLRC,poolRefs,isGUID=True,
                                          old_prefix=oldPrefix,
                                          new_prefix=newPrefix)

            print "=== add POOL refs to PoolFC ==="
            for ref in poolRefs:
                if ref not in pfnMap:
                    print " %s not found" % ref

            # extract FIDs from PoolFC
            try:
                root  = xml.dom.minidom.parse("PoolFileCatalog.xml")
                files = root.getElementsByTagName('File')
                for file in files:
                    # get ID
                    id = str(file.getAttribute('ID'))
                    # get PFN node
                    physical = file.getElementsByTagName('physical')[0]
                    pfnNode  = physical.getElementsByTagName('pfn')[0]
                    # convert UTF8 to Raw
                    pfn = str(pfnNode.getAttribute('name'))
                    # append
                    pfnMap[id] = pfn
            except:
                pass
            
            # create PoolFC
            _createPoolFC(pfnMap)

# for user specified files
for fileName in addPoolFC:
    # insert it to pool catalog    
    com = 'pool_insertFileToCatalog %s' % fileName
    print com
    status,output = commands.getstatusoutput(com)
    print output

# print PoolFC
print "=== PoolFileCatalog.xml ==="
print commands.getoutput('cat PoolFileCatalog.xml')
print

# create symlink for MC
if mcData != '' and len(inputFiles) != 0:
    print "=== make symlink for %s ===" % mcData
    # expand mcdata.tgz
    commands.getoutput('rm -f %s' % mcData)
    status,output = commands.getstatusoutput('tar xvfzm %s' % inputFiles[0])
    print output
    if status != 0:
        print "ERROR : MC data corrupted"
        sys.exit(EC_NoInput)
    # look for .dat
    foundMcData = False
    for line in output.split('\n'):
        if line.endswith('.dat'):
            status,output = commands.getstatusoutput('ln -fs %s %s' % \
                                                     (line.split()[-1],mcData))
            if status != 0:
                print output                
                print "ERROR : failed to create symlink for MC data"
                sys.exit(EC_NoInput)
            foundMcData = True
            break
    if not foundMcData:
        print "ERROR : cannot find *.dat in %s" % inputFiles[0]
        sys.exit(EC_NoInput)

# setup DB/CDRelease
if dbrFile != '':
    if dbrRun == -1:
        print "=== setup DB/CDRelease (old style) ==="
        # expand 
        status,out = commands.getstatusoutput('tar xvfzm %s/%s' % (currentDir,dbrFile))
        print out
        # remove
        print commands.getstatusoutput('rm %s/%s' % (currentDir,dbrFile))
    else:
        print "=== setup DB/CDRelease (new style) ==="
        # make symlink
        print commands.getstatusoutput('ln -fs %s/%s %s' % (currentDir,dbrFile,dbrFile))
        # run Reco_trf and set env vars
        dbCom = 'Reco_trf.py RunNumber=%s DBRelease=%s' % (dbrRun,dbrFile)
        print dbCom
        status,out = commands.getstatusoutput(dbCom)
        print out
        # remove
        print commands.getstatusoutput('rm %s/%s' % (currentDir,dbrFile))
    # look for setup.py
    tmpSetupDir = None
    for line in out.split('\n'):
        if line.endswith('setup.py'):
            tmpSetupDir = re.sub('setup.py$','',line)
            break
    # check
    if tmpSetupDir == None:
        print "ERROR : cound not find setup.py in %s" % dbrFile
        sys.exit(EC_DBRelease)
    # run setup.py
    dbrSetupStr  = "import os\nos.chdir('%s')\nexecfile('setup.py',{})\nos.chdir('%s')\n" % \
                   (tmpSetupDir,os.getcwd())
    dbrSetupStr += "import sys\nsys.stdout.flush()\nsys.stderr.flush()\n"
        
               
# create post-jobO file which overwrites some parameters
postOpt = 'post_' + commands.getoutput('uuidgen 2>/dev/null') + '.py'
oFile = open(postOpt,'w')
oFile.write("""
try:
    EventSelectorAthenaPool.__getattribute__ = orig_ESAP__getattribute
except:
    pass

def _Service(str):
    try:
        svcMgr = theApp.serviceMgr()
        return getattr(svcMgr,str)
    except:
        return Service(str)
""")
if len(inputFiles) != 0 and mcData == '' and not runAra:
    if (re.search('theApp.EvtMax',fragmentJobO) == None) and \
       (re.search('EvtMax',jobO) == None):
        oFile.write('theApp.EvtMax = -1\n')
    if byteStream:
        # BS
        oFile.write('ByteStreamInputSvc = _Service( "ByteStreamInputSvc" )\n')
        oFile.write('ByteStreamInputSvc.FullFileName = %s\n' % inputFiles)        
    else:
        oFile.write('EventSelector = _Service( "EventSelector" )\n')
        if eventColl:
            # TAG
            newInputs = []
            for infile in inputFiles:
                # remove suffix for event collection
                if athenaVer == None or athenaVer < "13.0.30":
                    newInputs.append(re.sub('\.root\.*\d*$','',infile))
                elif (not directIn):
                    # symlinks don't have attemptNr
                    newInputs.append(re.sub('\.\d+$','',infile))                
                else:
                    # directIn doen't create symlinks
                    newInputs.append(infile)
            oFile.write('EventSelector.InputCollections = %s\n' % newInputs)
            oFile.write('EventSelector.CollectionType = "ExplicitROOT"\n')
        else:
            # normal POOL
            oFile.write('EventSelector.InputCollections = %s\n' % inputFiles)
elif len(inputFiles) != 0 and runAra:
    for tmpInput in inputFiles:
        oFile.write("CollectionTree.Add('%s')\n" % tmpInput)
if flagMinBias:
    oFile.write('minBiasEventSelector = _Service( "minBiasEventSelector" )\n')
    oFile.write('minBiasEventSelector.InputCollections = %s\n' % minbiasFiles)
if flagCavern:
    oFile.write('cavernEventSelector = _Service( "cavernEventSelector" )\n')
    oFile.write('cavernEventSelector.InputCollections = %s\n' % cavernFiles)
if flagBeamHalo:
    oFile.write('BeamHaloEventSelector = _Service( "BeamHaloEventSelector" )\n')
    oFile.write('BeamHaloEventSelector.InputCollections = %s\n' % beamHaloFiles)
if flagBeamGas:
    oFile.write('BeamGasEventSelector = _Service( "BeamGasEventSelector" )\n')
    oFile.write('BeamGasEventSelector.InputCollections = %s\n' % beamGasFiles)
if 'hist' in outputFiles:    
    oFile.write('HistogramPersistencySvc=_Service("HistogramPersistencySvc")\n')
    oFile.write('HistogramPersistencySvc.OutputFile = "%s"\n' % outputFiles['hist'])
if 'ntuple' in outputFiles:
    oFile.write('NTupleSvc = _Service( "NTupleSvc" )\n')
    firstFlag = True
    for sName,fName in outputFiles['ntuple']:
        if firstFlag:
            firstFlag = False
            oFile.write('NTupleSvc.Output=["%s DATAFILE=\'%s\' OPT=\'NEW\'"]\n' % (sName,fName))            
        else:
            oFile.write('NTupleSvc.Output+=["%s DATAFILE=\'%s\' OPT=\'NEW\'"]\n' % (sName,fName))
oFile.write("""
_configs = []
seqList = []
pTmpStreamList = []
try:
    from AthenaCommon.AlgSequence import AlgSequence
    tmpKeys = AlgSequence().allConfigurables.keys()
    # get AlgSequences
    seqList = [AlgSequence()]
    try:
        for key in tmpKeys:
            # check if it is available via AlgSequence
            if not hasattr(AlgSequence(),key.split('/')[-1]):
                continue
            # get full name
            tmpConf = getattr(AlgSequence(),key.split('/')[-1])
            if hasattr(tmpConf,'getFullName'):
                tmpFullName = tmpConf.getFullName()
                # append AthSequencer
                if tmpFullName.startswith('AthSequencer/'):
                    seqList.append(tmpConf)
    except:
        pass
    # loop over all sequences
    for tmpAlgSequence in seqList:
        for key in tmpKeys:
            if key.find('/') != -1:
                key = key.split('/')[-1]
            if hasattr(tmpAlgSequence,key):    
                _configs.append(key)
except:
    pass

def _getConfig(key):
    if seqList == []:
        from AthenaCommon.AlgSequence import AlgSequence
        return getattr(AlgSequence(),key)
    else:
        for tmpAlgSequence in seqList:
            if hasattr(tmpAlgSequence,key):
                return getattr(tmpAlgSequence,key)

""")
if 'RDO' in outputFiles:
    oFile.write("""
key = "StreamRDO"    
if key in _configs:
    StreamRDO = _getConfig( key )
else:
    StreamRDO = Algorithm( key )
""")
    oFile.write('StreamRDO.OutputFile = "%s"\n' % outputFiles['RDO'])
    oFile.write('pTmpStreamList.append(StreamRDO)\n')
if 'ESD' in outputFiles:
    oFile.write("""
key = "StreamESD"    
if key in _configs:
    StreamESD = _getConfig( key )
else:
    StreamESD = Algorithm( key )
""")
    oFile.write('StreamESD.OutputFile = "%s"\n' % outputFiles['ESD'])
    oFile.write('pTmpStreamList.append(StreamESD)\n')
if 'AOD' in outputFiles:
    oFile.write("""
key = "StreamAOD"    
if key in _configs:
    StreamAOD = _getConfig( key )
else:
    StreamAOD = Algorithm( key )
""")
    oFile.write('StreamAOD.OutputFile = "%s"\n' % outputFiles['AOD'])
    oFile.write('pTmpStreamList.append(StreamAOD)\n')
if 'TAG' in outputFiles:
    oFile.write("""
key = "StreamTAG"    
if key in _configs:
    StreamTAG = _getConfig( key )
else:
    StreamTAG = Algorithm( key )
""")
    oFile.write('StreamTAG.OutputCollection = "%s"\n' % re.sub('\.root\.*\d*$','',outputFiles['TAG']))
if 'AANT' in outputFiles:
    firstFlag = True
    oFile.write('THistSvc = _Service ( "THistSvc" )\n')
    sNameList = []
    for aName,sName,fName in outputFiles['AANT']:
        if not sName in sNameList:
            sNameList.append(sName)    
            if firstFlag:
                firstFlag = False
                oFile.write('THistSvc.Output = ["%s DATAFILE=\'%s\' OPT=\'UPDATE\'"]\n' % (sName,fName))
            else:
                oFile.write('THistSvc.Output += ["%s DATAFILE=\'%s\' OPT=\'UPDATE\'"]\n' % (sName,fName))            
        oFile.write("""
key = "%s"
if key in _configs:
    AANTupleStream = _getConfig( key )
else:
    AANTupleStream = Algorithm( key )
""" % aName)
        oFile.write('AANTupleStream.StreamName = "%s"\n' % sName)
        oFile.write('AANTupleStream.OutputName = "%s"\n' % fName)        
    if 'THIST' in outputFiles:
        for sName,fName in outputFiles['THIST']:
            oFile.write('THistSvc.Output += ["%s DATAFILE=\'%s\' OPT=\'UPDATE\'"]\n' % (sName,fName))
else:
    if 'THIST' in outputFiles:
        oFile.write('THistSvc = _Service ( "THistSvc" )\n')
        firstFlag = True
        for sName,fName in outputFiles['THIST']:
            if firstFlag:
                firstFlag = False
                oFile.write('THistSvc.Output = ["%s DATAFILE=\'%s\' OPT=\'UPDATE\'"]\n' % (sName,fName))
            else:
                oFile.write('THistSvc.Output+= ["%s DATAFILE=\'%s\' OPT=\'UPDATE\'"]\n' % (sName,fName))
if 'Stream1' in outputFiles:
    oFile.write("""
key = "Stream1"    
if key in _configs:
    Stream1 = _getConfig( key )
else:
    try:
        Stream1 = getattr(theApp._streams,key)
    except:
        Stream1 = Algorithm( key )
""")
    oFile.write('Stream1.OutputFile = "%s"\n' % outputFiles['Stream1'])
    oFile.write('pTmpStreamList.append(Stream1)\n')
if 'Stream2' in outputFiles:
    oFile.write("""
key = "Stream2"    
if key in _configs:
    Stream2 = _getConfig( key )
else:
    try:
        Stream2 = getattr(theApp._streams,key)
    except:
        Stream2 = Algorithm( key )
""")
    oFile.write('Stream2.OutputFile = "%s"\n' % outputFiles['Stream2'])
    oFile.write('pTmpStreamList.append(Stream2)\n')
    oFile.write("""
key = "%s_FH" % key
Stream2_FH = None
if key in _configs:
    Stream2_FH = _getConfig( key )
else:
    try:
        Stream2_FH = getattr(theApp._streams,key)
    except:
        pass
""")
    oFile.write("""
if Stream2_FH != None:
    Stream2_FH.OutputFile = "%s"
""" % outputFiles['Stream2'])

if 'StreamG' in outputFiles:
    for stName,stFileName in outputFiles['StreamG']:
        oFile.write("""
key = "%s"    
if key in _configs:
    StreamX = _getConfig( key )
else:
    try:
        StreamX = getattr(theApp._streams,key)
    except:
        StreamX = Algorithm( key )
""" % stName)
        oFile.write('StreamX.OutputFile = "%s"\n' % stFileName)
        oFile.write('pTmpStreamList.append(StreamX)\n')    
if 'Meta' in outputFiles:
    for stName,stFileName in outputFiles['Meta']:
        oFile.write("""
key = "%s"    
if key in _configs:
    StreamX = _getConfig( key )
else:
    try:
        StreamX = getattr(theApp._streams,key)
    except:
        StreamX = Algorithm( key )
""" % stName)
        oFile.write('StreamX.OutputFile = "ROOTTREE:%s"\n' % stFileName)

if 'UserData' in outputFiles:
    for stFileName in outputFiles['UserData']:
        oFile.write("""
try:
    # try new style
    userDataSvc = None
    try:
        for typeNameExtSvc in theApp.ExtSvc:
            if typeNameExtSvc.startswith('UserDataSvc/'):
                nameExtSvc = typeNameExtSvc.split('/')[-1]
                userDataSvc = getattr(theApp.serviceMgr(),nameExtSvc)
    except:
        pass
    # use old style
    if userDataSvc == None: 
        userDataSvc = _Service('UserDataSvc')
    # delete existing stream
    try:
       THistSvc = _Service ('THistSvc')
       tmpStreams = tuple(THistSvc.Output)
       newStreams = []
       for tmpStream in tmpStreams:
            # skip userstream since it is set by userDataSvc.OutputStream later
            if not userDataSvc.name() in tmpStream.split()[0]: 
                newStreams.append(tmpStream)
       THistSvc.Output = newStreams
    except:
        pass
    for tmpStream in pTmpStreamList:
        try:
            if tmpStream.OutputFile == '%s':
                userDataSvc.OutputStream = tmpStream
                break
        except:
            pass
except:
    pass
""" % stFileName)
    
uniqueTag = commands.getoutput('uuidgen 2>/dev/null')
if 'BS' in outputFiles:
    oFile.write('ByteStreamEventStorageOutputSvc = _Service("ByteStreamEventStorageOutputSvc")\n')
    oFile.write('ByteStreamEventStorageOutputSvc.FileTag = "%s"\n' % uniqueTag)
    oFile.write("""
try:
    ByteStreamEventStorageOutputSvc.AppName = "%s"
except:
    pass
""" % uniqueTag)    
    oFile.write('ByteStreamEventStorageOutputSvc.OutputDirectory = "./"\n')
if fragmentJobO != "":
    oFile.write('%s\n' % fragmentJobO)

# event picking
if eventPickTxt != '':
    epRunEvtList = []
    epFH = open(eventPickTxt)
    iepNum = 0
    for epLine in epFH:
        items = epLine.split()
        if len(items) != 2:
            continue
        # check range    
        epSkipFlag = False    
        if eventPickNum > 0:
            if iepNum < eventPickSt:
                epSkipFlag = True
            if iepNum >= eventPickSt+eventPickNum:
                epSkipFlag = True
        iepNum += 1
        if epSkipFlag:
            continue
        # append        
        epRunEvtList.append((long(items[0]),long(items[1])))
    oFile.write("""
from AthenaCommon.AlgSequence import AthSequencer
seq = AthSequencer('AthFilterSeq')
from GaudiSequencer.PyComps import PyEvtFilter
seq += PyEvtFilter(
    'alg',
    evt_info='',
    )
seq.alg.evt_list = %s
seq.alg.filter_policy = 'accept'
for tmpStream in theApp._streams.getAllChildren():
    fullName = tmpStream.getFullName()
    if fullName.split('/')[0] == 'AthenaOutputStream':
        tmpStream.AcceptAlgs = [seq.alg.name()]
""" % str(epRunEvtList))

# FileStager
if useFileStager and directIn:
    try:
        print "=== preparation for FileStager ==="
        # remove log just in case
        commands.getoutput('rm -f %s.log' % pCopyToolName)
        # get FileStager jobO    
        fileStagerDir = 'FileStager'
        if newPrefix.startswith('srm:'):
            fileStagerJobO = 'input_FileStager.py'
        else:    
            fileStagerJobO = 'input_FileStagerRFCP.py'
        print commands.getoutput('rm -f %s;get_files %s/%s' % (fileStagerJobO,fileStagerDir,fileStagerJobO))
        # create dummy jobOs to disable FileStager which is already included in user's jobO
        commands.getoutput('mkdir -p %s' % fileStagerDir)
        for tmpFsJobO in ['input_FileStager.py','input_FileStagerRFCP.py']:
            commands.getoutput('rm -f %s/%s' % (fileStagerDir,tmpFsJobO))
            commands.getoutput('touch %s/%s' % (fileStagerDir,tmpFsJobO))
        # tweak jobO for dcap/xrootd
        fsStrs = ''
        fsFH = open(fileStagerJobO)
        copyToolCpCommand = ''
        for tmpLine in fsFH:
            tmpMatch = re.search('(.*)stagetool.CpCommand\s*=\s*".+"',tmpLine)
            if tmpMatch != None:
                # replace copy command
                if copyTool == 'lsm':
                    # local site mover
                    fsStrs += '%sstagetool.CpCommand="lsm-get"\n' % tmpMatch.group(1)
                elif newPrefix.startswith('root:'):
                    # xrootd
                    copyToolCpCommand = "xrdcp"
                    fsStrs += '%sstagetool.CpCommand="./%s"\n' % (tmpMatch.group(1),pCopyToolName)
                elif newPrefix.startswith('dcap:') or newPrefix.startswith('dcache:') or newPrefix.startswith('gsidcap:'):
                    # dCache
                    copyToolCpCommand = "dccp"
                    fsStrs += '%sstagetool.CpCommand="./%s"\n' % (tmpMatch.group(1),pCopyToolName)
                elif newPrefix.startswith('rfio:') or newPrefix.startswith('castor:'):
                    # rfio/castor
                    copyToolCpCommand = "rfcp"
                    fsStrs += '%sstagetool.CpCommand="./%s"\n' % (tmpMatch.group(1),pCopyToolName)
                elif newPrefix.startswith('/'):
                    # NAS
                    copyToolCpCommand = "cp"
                    fsStrs += '%sstagetool.CpCommand="./%s"\n' % (tmpMatch.group(1),pCopyToolName)
                else:
                    # keep original
                    fsStrs += tmpLine                
                fsStrs += '%sstagetool.tmpDir = "%s/"\n' % (tmpMatch.group(1),currentDir)
            else:
                fsStrs += tmpLine
        fsStrs += """
try:
    thejob.FileStager.KeepLogfiles = True
except:
    pass
try:
    thejob.FileStager.VerboseStager = True
except:
    pass
try:
    thejob.FileStager.CpCommand = stagetool.CpCommand
except:
    pass
try:
    thejob.FileStager.CpArguments = stagetool.CpArguments
except:
    pass
"""
        fsFH.close()
        # create new jobO
        fsFH = open(fileStagerJobO,'w')
        fsFH.write(fsStrs)
        fsFH.close()
        # create copy wrapper
        copyToolFH = open(pCopyToolName,'w')
        copyToolFH.write(pCopyToolStr)
        copyToolFH.close()
        commands.getoutput('chmod +x %s' % pCopyToolName)
        # create sample file
        sampleFileName = 'sample_%s.def' % commands.getoutput('uuidgen 2>/dev/null')
        sampleFH = open(sampleFileName,'w')
        sampleFH.write('TITLE: title\nFLAGS: GridCopy=1\n')
        metaDataForFH = {}
        for tmpInputName in inputFiles:
            tmpCheckSum = None
            # look for GUID
            for tmpGUIDforMeta,tmpPFNforMeta in directPfnMap.iteritems():
                if tmpPFNforMeta == tmpInputName:
                    # get checksum
                    if tmpGUIDforMeta in directMetaMap and 'checksum' in directMetaMap[tmpGUIDforMeta]:
                        tmpCheckSum = directMetaMap[tmpGUIDforMeta]['checksum']
                    break    
            if tmpInputName.startswith('dcache:'):
                # remove prefix 
                tmpInputName = re.sub('dcache:','',tmpInputName)
            # append checksum
            if tmpCheckSum != None:
                metaDataForFH[tmpInputName] = tmpCheckSum
            # add prefix
            tmpInputName = 'gridcopy://%s\n' % tmpInputName
            # write
            sampleFH.write(tmpInputName)
        sampleFH.close()
        # create config for copyTool
        copytoolConfFH = open('%sConf.py' % pCopyToolName,'w')
        copytoolConfFH.write('com = "%s"\n' % copyToolCpCommand)
        copytoolConfFH.write('metaMap = %s\n' % str(metaDataForFH))
        copytoolConfFH.close()
        print "----> jobO"
        print commands.getoutput('cat %s' % fileStagerJobO)
        print "----> sample"        
        print commands.getoutput('cat %s' % sampleFileName)
        print "----> config"
        print commands.getoutput('cat %sConf.py' % pCopyToolName)
    except:
        type,value,traceBack = sys.exc_info()
        print 'ERROR : failed to setup FileStager %s - %s' % (type,value)
    # modify PoolFC.xml
    pfcName = 'PoolFileCatalog.xml'
    try:
        pFile = open(pfcName)
        pFileStr = ''
        for tmpLine in pFile:
            pFileStr += tmpLine 
        pFile.close()
        for tmpInputName in inputFiles:
            # replace file name
            baseName = '%s/tcf_%s' % (currentDir,tmpInputName.split('/')[-1])
            pFileStr = re.sub(tmpInputName,baseName,pFileStr)
        # overwrite
        pFile = open(pfcName,'w')
        pFile.write(pFileStr)
        pFile.close()
    except:
        pass
    # add jobO
    oFile.write("""
try:
    del sampleList
except:
    pass
sampleFile = '%s'
doStaging = True
include ('%s')
""" % (sampleFileName,fileStagerJobO))
        
oFile.close()

# overwrite EventSelectorAthenaPool.InputCollections and AthenaCommon.AthenaCommonFlags.FilesInput for jobO level metadata extraction
preOpt = 'pre_' + commands.getoutput('uuidgen 2>/dev/null') + '.py'
oFile = open(preOpt,'w')
if len(inputFiles) != 0 and mcData == '':
    if not byteStream:
        oFile.write("""      
try:
    from EventSelectorAthenaPool.EventSelectorAthenaPoolConf import EventSelectorAthenaPool
    orig_ESAP__getattribute =  EventSelectorAthenaPool.__getattribute__

    def _dummy(self,attr):
        if attr == 'InputCollections':
            return %s
        else:
            return orig_ESAP__getattribute(self,attr)

    EventSelectorAthenaPool.__getattribute__ = _dummy
    print 'Overwrite InputCollections'
    print EventSelectorAthenaPool.InputCollections
except:
    try:
        EventSelectorAthenaPool.__getattribute__ = orig_ESAP__getattribute
    except:
        pass
""" % inputFiles)
    oFile.write("""      
try:
    import AthenaCommon.AthenaCommonFlags

    def _dummyFilesInput(*argv):
        return %s

    AthenaCommon.AthenaCommonFlags.FilesInput.__call__ = _dummyFilesInput
except:
    pass

try:
    import AthenaCommon.AthenaCommonFlags

    def _dummyGet_Value(*argv):
        return %s

    for tmpAttr in dir (AthenaCommon.AthenaCommonFlags):
        import re
        if re.search('^(Pool|BS).*Input$',tmpAttr) != None:
            try:
                getattr(AthenaCommon.AthenaCommonFlags,tmpAttr).get_Value = _dummyGet_Value
            except:
                pass
except:
    pass
""" % (inputFiles,inputFiles))
# filter for verbose expansion
try:
    if athenaVer >= "15.0.0":
        oFile.write("""
try:
    from AthenaCommon.Include import excludeTracePattern
    excludeTracePattern.append('*/CLIDComps/clidGenerator.py')
    excludeTracePattern.append('*/PyUtils/decorator.py')
    excludeTracePattern.append('*/PyUtils/Decorators.py')
    excludeTracePattern.append('*/PyUtils/Helper*.py')
except:
    pass
""")
except:
    pass
# for SummarySvc
oFile.write("""      
try:
    from AthenaServices.SummarySvc import *
    useAthenaSummarySvc()
except:
    pass
""")
                
oFile.close()

# dump

print "=== pre jobO ==="
oFile = open(preOpt)
lines = ''
for line in oFile:
    lines += line
print lines
oFile.close()
print

print "=== post jobO ==="
oFile = open(postOpt)
lines = ''
for line in oFile:
    lines += line
print lines
oFile.close()

# replace theApp.initialize when using theApp.nextEvent
if useNextEvent:
    initOpt = 'init_' + commands.getoutput('uuidgen 2>/dev/null') + '.py'
    initFile = open(initOpt,'w')
    initFile.write("""
origTheAppinitialize = theApp.initialize                   
def fakeTheAppinitialize():
    include('%s')
    origTheAppinitialize()
theApp.initialize = fakeTheAppinitialize    
""" % postOpt)
    initFile.close()
    
    print "=== init jobO ==="
    iFile = open(initOpt)
    lines = ''
    for line in iFile:
        lines += line
    print lines
    iFile.close()

    # modify jobO
    print "=== change jobO ==="
    newJobO = ''
    startPy = False
    for item in jobO.split():
        if (not startPy) and item.endswith('.py'):
            newJobO += (" " + initOpt)
            startPy = True
        newJobO += (" " + item)    
    print "  Old : " + jobO
    print "  New : " + newJobO
    jobO = newJobO
    
# modify macro for ARA
if runAra and len(inputFiles) != 0:
   try:
       # look for .C or .py
       for tmpIdx in range(len(jobO.split())):
           tmpRevIdx = -(tmpIdx+1)
           tmpMacroName = jobO.split()[tmpRevIdx]
           if (not tmpMacroName.startswith('-')) and \
               (tmpMacroName.endswith('.py') or tmpMacroName.endswith('.C')):
               break
       if tmpMacroName.endswith('.C'):
           # look for TPython::Exec
           tmpMacro = open(tmpMacroName)           
           for line in tmpMacro:
               match = re.search('TPython::Exec\s*\(.*execfile[^\'\"]+[\'\"]([^\'\"]+)',line)
               if match != None:
                   tmpMacroName = match.group(1)
                   break
           # close
           tmpMacro.close()
       # check startup file
       if not tmpMacroName.endswith('.py'):
           raise RuntimeError('startup file (%s) needs to be .py for ARA' % tmpMacroName)
       newMacroName = tmpMacroName+'.new'
       tmpMacro = open(tmpMacroName)
       newMacro = open(newMacroName,'w')       
       # look for chain building sector
       inputSector = False
       for line in tmpMacro:
           # remove comments
           if re.search('^\s*#',line) != None:
               continue
           # look for TChainROOTAccess       
           match = re.search('(\s*).*ROOT.AthenaROOTAccess.TChainROOTAccess',line)
           if match != None:
               inputSector = True
               newMacro.write(line)
               # execfile to set Chain
               newMacro.write('%sexecfile("%s")\n' % (match.group(1),postOpt))
               continue
           # look for makeTree    
           match = re.search('AthenaROOTAccess.transientTree.makeTree',line)
           if match != None:
               inputSector = False
               newMacro.write(line)
               continue
           # write out normal sectors
           if not inputSector:
               newMacro.write(line)
       tmpMacro.close()
       newMacro.close()
       # dump
       print "=== old %s ===" % tmpMacroName
       tmpFile = open(tmpMacroName)
       lines = ''
       for line in tmpFile:
           lines += line
       print lines
       tmpFile.close()
       print "=== new %s ===" % newMacroName
       tmpFile = open(newMacroName)
       lines = ''
       for line in tmpFile:
           lines += line
       print lines
       tmpFile.close()
       # rename
       commands.getoutput('mv %s %s' % (newMacroName,tmpMacroName))
   except:
       type, value, traceBack = sys.exc_info()
       print 'ERROR: failed to modify %s : %s - %s' % (tmpMacroName,type,value)
       sys.exit(EC_ARA)

# get PDGTABLE.MeV
commands.getoutput('get_files PDGTABLE.MeV')

# temporary output to avoid MemeoryError
tmpOutput = 'tmp.stdout.%s' % commands.getoutput('uuidgen 2>/dev/null')
tmpStderr = 'tmp.stderr.%s' % commands.getoutput('uuidgen 2>/dev/null')

# append workdir to CMTPATH
env = 'CMTPATH=%s:$CMTPATH' % workDir
# construct command
com  = 'export %s;' % env
# local RAC
if ('ATLAS_CONDDB' not in os.environ) or os.environ['ATLAS_CONDDB']=='to.be.set':
    if 'OSG_HOSTNAME' in os.environ:
        com += 'export ATLAS_CONDDB=%s;' % os.environ['OSG_HOSTNAME']
    elif 'GLOBUS_CE' in os.environ:
        tmpCE = os.environ['GLOBUS_CE'].split('/')[0]
        # remove port number
        tmpCE = re.sub(':\d+$','',tmpCE)
        com += 'export ATLAS_CONDDB=%s;' % tmpCE
    elif 'PBS_O_HOST' in os.environ:
        com += 'export ATLAS_CONDDB=%s;' % os.environ['PBS_O_HOST']    
com += 'cd %s;' % cmtDir
com += 'echo "use AtlasPolicy AtlasPolicy-*" > requirements;'
com += 'cmt config;'
com += 'source ./setup.sh;'
com += 'export TestArea=%s;' % workDir
com += 'cd -;env;'
if (not runTrf) and dbrFile == '':
    # run Athena
    com += 'athena.py '
    if codeTrace:
        com += '-s '
    com += '%s %s %s' % (preOpt,jobO,postOpt)
elif dbrFile != '':
    # run setup.py and athena.py in a python
    tmpTrfName = 'trf.%s.py' % commands.getoutput('uuidgen 2>/dev/null')
    tmpTrfFile = open(tmpTrfName,'w')
    tmpTrfFile.write(dbrSetupStr)
    tmpTrfFile.write('import sys\nstatus=os.system("""athena.py ')
    if codeTrace:
        tmpTrfFile.write('-s ')
    tmpTrfFile.write('%s %s %s""")\n' % (preOpt,jobO,postOpt))
    tmpTrfFile.write('status %= 255\nsys.exit(status)\n\n')
    tmpTrfFile.close()
    com += 'cat %s;python -u %s' % (tmpTrfName,tmpTrfName)
else:
    # run transformation
    com += '%s' % jobO

print    
print "=== execute ==="
print com
# run athena
if not debugFlag:
    if liveLog != '':
        # create empty log
        commands.getstatusoutput('cat /dev/null > %s' % tmpOutput)
        # start watcher
        logWatcher = LogWatcher(tmpOutput,liveLog)
        logWatcher.start()
    # write stdout to tmp file
    com += ' > %s 2> %s' % (tmpOutput,tmpStderr)
    status,out = commands.getstatusoutput(com)
    print out
    statusChanged = False
    try:
        tmpOutFile = open(tmpOutput)
        for line in tmpOutFile:
            print line[:-1]
            # set status=0 for AcerMC
            if re.search('ACERMC TERMINATES NORMALY: NO MORE EVENTS IN FILE',line) != None:
                status = 0
                statusChanged = True
        tmpOutFile.close()
    except:
        pass
    if statusChanged:
        print "\n\nStatusCode was overwritten for AcerMC\n"
    try:
        tmpErrFile = open(tmpStderr)
        for line in tmpErrFile:
            print line[:-1]
        tmpErrFile.close()
    except:
        pass
    # print 'sh: line 1:  8278 Aborted'
    try:
        if status != 0:
            print out.split('\n')[-1]
    except:
        pass
    if liveLog != '':        
        # terminate watcher
        logWatcher.terminate()
        logWatcher.join()
else:
    status = os.system(com)

if useFileStager and directIn:
    print
    print "=== FileStager log ==="
    print commands.getoutput('cat %s.log' % pCopyToolName)
    print commands.getoutput('cat %s/tcf_*stage.err 2>/dev/null' % currentDir)

print
print "=== list in run dir ==="    
print commands.getoutput('ls -l')

# rename or archive iROOT files
if 'IROOT' in outputFiles:
    for iROOT in outputFiles['IROOT']:
        if iROOT[0].find('*') != -1:
            # archive *
            commands.getoutput('tar cvfz %s %s' % (iROOT[-1],iROOT[0]))
        else:
            # rename 
            commands.getoutput('mv %s %s' % iROOT)
        # modify PoolFC.xml
        pfcName = 'PoolFileCatalog.xml'
        try:
            pLines = ''
            pFile = open(pfcName)
            for line in pFile:
                # replace file name
                line = re.sub('"%s"' % iROOT[0],'"%s"' % iROOT[-1],line)
                pLines += line
            pFile.close()
            # overwrite
            pFile = open(pfcName,'w')
            pFile.write(pLines)
            pFile.close()
        except:
            pass

# rename TAG files
if 'TAG' in outputFiles:
    woAttrNr = re.sub('\.\d+$','',outputFiles['TAG'])
    if woAttrNr != outputFiles['TAG']:
        print commands.getoutput('mv %s %s' % (woAttrNr,outputFiles['TAG']))
    # since 13.0.30 StreamTAG doesn't append .root automatically
    woRootAttrNr = re.sub('\.root\.*\d*$','',outputFiles['TAG'])
    if woRootAttrNr != outputFiles['TAG']:
        print commands.getoutput('mv %s %s' % (woRootAttrNr,outputFiles['TAG']))

# rename BS file
if 'BS' in outputFiles:
    bsS,bsO = commands.getstatusoutput('mv daq.%s* %s' % (uniqueTag,outputFiles['BS']))
    print bsS,bsO
    if bsS != 0:
        print commands.getstatusoutput('mv data_test.*%s* %s' % (uniqueTag,outputFiles['BS']))    
    
# copy results
for file in outputFiles.values():
    if not isinstance(file,types.StringType):
        # for AANT
        for aaT in file:
            commands.getoutput('mv %s %s' % (aaT[-1],currentDir))
    else:
        commands.getoutput('mv %s %s' % (file,currentDir))

# copy PoolFC.xml
commands.getoutput('mv -f PoolFileCatalog.xml %s' % currentDir)

# copy AthSummary.txt
commands.getoutput('mv -f AthSummary.txt %s' % currentDir)

# go back to current dir
os.chdir(currentDir)

print
print "=== list in top dir ==="    
print commands.getoutput('pwd')
print commands.getoutput('ls -l')

# remove work dir
if not debugFlag:
    commands.getoutput('rm -rf %s' % workDir)

# return
print
print "=== result ==="    
if status:
    print "execute script: Running athena failed : %d" % status
    sys.exit(EC_AthenaFail)
else:
    print "execute script: Running athena was successful"
    sys.exit(0)
