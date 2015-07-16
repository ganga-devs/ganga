#! /usr/bin/env python
###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ganga-stage-in-out-dq2.py,v 1.50 2009-07-17 08:36:59 elmsheus Exp $
###############################################################################
# DQ2 dataset download and PoolFileCatalog.xml generation

import os, sys, imp, re, time, commands, signal, popen2, socket, urllib
import string
import xml.dom.minidom
from dq2.info import TiersOfATLAS 
from dq2.info.TiersOfATLAS import _refreshToACache, ToACache
from getopt import getopt,GetoptError
from commands import getstatusoutput
import lfc
from dq2.common.DQException import DQInvalidRequestException
from dq2.content.DQContentException import DQInvalidFileMetadataException
from dq2.location.DQLocationException import DQLocationExistsException
from dq2.location.DQLocationException import DQLocationExistsException
from dq2.content.DQContentException import DQFileExistsInDatasetException
from dq2.repository.DQRepositoryException import DQDatasetExistsException
from dq2.repository.DQRepositoryException import DQUnknownDatasetException

try:
    import hashlib
    md = hashlib.md5()
except ImportError:
    # for Python << 2.5
    import md5
    md = md5.new()

_refreshToACache()

from threading import Lock
dq2_lock = Lock()

from dq2.clientapi.DQ2 import DQ2
dq2=DQ2()

#try:
#    # ignore Python C API version mismatch
#    sys.stderr = open("/dev/null", "w")
#    # import
#    import lfc
#except:
#    pass
# repair stderr
#sys.stderr = sys.__stderr__

# error codes
# WRAPLCG_UNSPEC
EC_UNSPEC        = 410000 
# WRAPLCG_WNCHECK_UNSPEC
EC_Configuration = 410100
# WRAPLCG_STAGEIN_UNSPEC
EC_QueryFiles    = 410300
# WRAPLCG_STAGEIN_LCGCP
EC_DQ2GET        = 410302
# WRAPLCG_WNCHECK_PROXY
EC_PROXY         = 410101
# WRAPLCG_STAGEOUT_UNSPEC
EC_STAGEOUT      = 410400

# configuration
try:
    # DQ2 server
    baseURLDQ2 = os.environ['DQ2_URL_SERVER']
except:
    print "ERROR : DQ2_URL_SERVER is not defined"
    #sys.exit(EC_Configuration)
try:
    # local site ID
    DQ2LOCALSITEID = os.environ['DQ2_LOCAL_SITE_ID']
except:
    print "ERROR : DQ2_LOCAL_SITE_ID is not defined"
#    sys.exit(EC_Configuration)
try:
    # local access protocol
    configLOCALPROTOCOL = os.environ['DQ2_LOCAL_PROTOCOL']
except:
    configLOCALPROTOCOL = 'rfio'
try:
    # prefix for local access
    configLOCALPREFIX = os.environ['DQ2_LOCAL_PREFIX']
except:
    configLOCALPREFIX = ''

configSETYPE = 'NULL'

## setting DQ2_COPY_COMMAND can cause dq2-get 0.1.17 failed 
## with an unknown attribute in EndpointTool instance.
#try:
    # remote copy command
#    configCOPYCOMMAND = os.environ['DQ2_COPY_COMMAND']
#except:
#    configCOPYCOMMAND = 'lcg-cp -v --vo atlas'
#    os.environ['DQ2_COPY_COMMAND'] = configCOPYCOMMAND

# Set default values for output LFC
config_lfc_host = ''
config_lfc_home = '/grid/atlas/'

# global flags
globalVerbose = False

########################################################################
def usage():

    print 'Name:'
    print '    ganga-stage-in-out-dq2.py'
    print
    print 'Arguments:'
    print '    logical names'
    print 
    print 'Options:'
    print '    -h, --help            this prinout'
    print '    -i, --input file      list of logical names'
    print '    -o, --output files    list of output files'
    print '    -g, --guids guids     list of guid names'
    print '    -d, --directory path  to stage the input files (default $PWD)'
    print '    -t, --timeout seconds for the staging in (default 900)'
    print '    -r, --retry number    for the staging command (default 3)'
    print '    -v, --verbose         verbosity'

########################################################################
def fhandler(signum, frame):
    print "GFAL alarm - timeout!"

########################################################################
def ghandler(signum, frame):
    print "lcg-gt alarm - timeout!"

########################################################################
class PoolFileCatalog:

    def __init__(self,name='PoolFileCatalog.xml'):

        self.pfc = open(name,'w')
        print >>self.pfc,'<?xml version="1.0" ?>'
        print >>self.pfc,'<POOLFILECATALOG>'

    def addFile(self,guid,lfn,pfn):

        print >>self.pfc,'    <File ID="%s">' % guid
        print >>self.pfc,'        <logical>'
        print >>self.pfc,'            <lfn name="%s"/>' % lfn
        print >>self.pfc,'        </logical>'
        print >>self.pfc,'        <physical>'
        print >>self.pfc,'            <pfn filetype="ROOT_All" name="%s"/>' % pfn
        print >>self.pfc,'        </physical>'
        print >>self.pfc,'    </File>'

    def close(self):

        print >>self.pfc,'</POOLFILECATALOG>'

########################################################################
# get default storage
def _getDefaultStorage(id):
    # parse
    match = re.findall('^[^:]+://([^:/]+)',id)
    if len(match) != 1:
        print "ERROR : could not parse default storage"
        sys.exit(EC_Configuration)
        
    return [match[0]]

########################################################################
# append protocol prefix
def _appendProtocol(pfnMap,protocol):
    # define prefix
    #    if configLOCALPREFIX != '':
    #        prefix = configLOCALPREFIX
    if protocol=='gfal':
        prefix = 'gfal:'
    elif protocol=='dcap':
        prefix = 'dcap:'
    elif protocol=='rfio':
        prefix = 'rfio:'        
    elif configLOCALPREFIX != '':
        prefix = configLOCALPREFIX
    else:
        return
    
    # loop over all LFNs
    for lfn in pfnMap.keys():
        pfn = "%s%s" % (prefix,pfnMap[lfn])
        pfnMap[lfn] = pfn 

########################################################################
def getAggName(site):
    # aggregated T1
    if site in ToACache.topology['TIER1S']:
        return site
    # look for cloud
    for id,idSites in ToACache.topology.iteritems():
        # ignore high level names
        if id in ('ALL','TIER1S'):
            continue
        if site in idSites:
            # CERN
            if id == 'CERN':
                return id
            # EGEE T2 or normal T2
            if id.endswith('TIER2S') or id.endswith('TIER3S') or id in ToACache.topology['ALL']:
                return site
            return id
    # return
    return site
  
########################################################################
# get PFN from LFC
def _getPFNsLFC(guidMap, defaultSE, localsitesrm):

    guidReplicas = {}
    mapLFN = {}
    stUrlMap = {}
    fsizeMap  = {}
    md5sumMap = {}
    usedProtocol = ''
    
    print 'defaultSE: %s' %defaultSE

    protocols = ''
    for p in configLOCALPROTOCOL:
        protocols = protocols + ' ' + p 

    # lfc_list structure
    stat  = lfc.lfc_filestatg()
    # support of bulk-operation
    enableBulkOps = hasattr(lfc,'lfc_getreplicas')
    #enableBulkOps = False
 
    if enableBulkOps:
        print 'LFC bulk reading...'
        guids = guidMap.values()
        lfcattempts = 0
        while lfcattempts<5:
            (res, rep_entries) = lfc.lfc_getreplicas(guids, '')
            if res == 0 or rep_entries != None:
                break
            lfcattempts = lfcattempts + 1
            
        print 'End of LFC bulk reading.'
        
        for lfn,guid in guidMap.iteritems():
            mapLFN[guid] = lfn

        for rep in rep_entries:
            if rep != None and ((not hasattr(rep,'errcode')) or \
                                (hasattr(rep,'errcode') and rep.errcode == 0)):
                #pat = re.compile(r'[^srm://][^/]+')
                pat = re.compile(r'^[^:]+://([^:/]+)')
                name = re.findall(pat, rep.sfn)
                if name:
                    host = name[0]
                else:
                    host = ''

                if (defaultSE and host in defaultSE) or \
                       rep.sfn.startswith(localsitesrm):

                    surl = rep.sfn

                    if (surl.find('atlasmctape')>0) or (surl.find('atlasdatatape')>0):
                        if globalVerbose:
                            print 'Skip atlasmctape or atlasdatatape replica'
                        continue

                    lfn = mapLFN[rep.guid]
                    guidReplicas[lfn] = surl
                    
                    fsizeMap[lfn] = long(rep.filesize)
                    md5sumMap[lfn] = rep.csumvalue
                    
                    # TURL
                    match = re.search('^[^:]+://([^:/]+):*\d*/', surl)
                    try:
                        sURLHost = match.group(1)
                    except:
                        sURLHost = defaultSE[0]
                    turl = []    
                    
                    if not 'gfal' in configLOCALPROTOCOL \
                       and sURLHost not in stUrlMap \
                       and usedProtocol!='file' \
                       and not 'ccsrm.in2p3.fr' in defaultSE:

                        print 'Using lcg-gt for turl retrieval ...'
                        # check which version of lcg-utils we're on
                        if 'lcgutil_num' in os.environ and os.environ['lcgutil_num']!='' and eval(os.environ['lcgutil_num']) >= 1007002:
                            cmd = "lcg-gt --connect-timeout 60 --sendreceive-timeout 60 --srm-timeout 60 --bdii-timeout 60 " + surl + " " + protocols
                        else:
                            cmd = "lcg-gt -t 60 " + surl + " " + protocols
                        print cmd

                        count = 0
                        retry = 5
                        while count<=retry:
                            try:
                                signal.signal(signal.SIGALRM, ghandler)
                                signal.alarm(240)
                                child = popen2.Popen3(cmd,1)
                                child.tochild.close()
                                out=child.fromchild
                                err=child.childerr
                                line=out.readline()
                                if line:
                                    match = re.search('^[^:]+://([^:/]+:*\d*)/', line)
                                    if match:
                                        turl = line.split()
                                    elif line.startswith('file:'):
                                        usedProtocol = 'file'
                                signal.alarm(0)
                            except IOError:
                                print 'lcg-gt time out !'
                                pass
                            signal.alarm(0)
                            
                            if turl:
                                count = retry
                                break
                            else:
                                if count == retry:
                                    print '!!! lcg-gt error after %s retries - giving up !!!' %count
                                    count = count + 1          
                                else:
                                    count = count + 1
                                    print 'lcg-gt error - will start retry no. %s' %count
                                    time.sleep(120)

                        print turl
                        if turl and turl[0]:
                            match = re.search('^[^:]+://([^:/]+:*\d*)/', turl[0])
                            tURLHost = match.group(1)
                            stUrlMap[sURLHost] = tURLHost
                            match = re.search('^(\S*)://.*', turl[0])
                            usedProtocol = match.group(1)
                            print usedProtocol
                                        
    else:
        print 'LFC single reading...'
        # start LFC session
        try:
            lfc.lfc_startsess('','')
        except NameError:
            pass

        for lfn,guid in guidMap.iteritems():
            mapLFN[guid] = lfn
            if globalVerbose:
                sys.stdout.write('.')
                sys.stdout.flush()
            # get replica
            listp = lfc.lfc_list()
            fr = lfc.lfc_listreplica('',guid,lfc.CNS_LIST_BEGIN,listp)
            while fr:
                if defaultSE and fr.host in defaultSE:
                    break
                fr = lfc.lfc_listreplica('',guid,lfc.CNS_LIST_CONTINUE,listp)
            lfc.lfc_listreplica('',guid,lfc.CNS_LIST_END,listp)
            # found
            if fr:
                if fr.sfn.startswith(localsitesrm):
                    surl = fr.sfn
                    if (surl.find('atlasmctape')>0) or (surl.find('atlasdatatape')>0):
                        if globalVerbose:
                            print 'Skip atlasmctape or atlasdatatape replica'
                        continue
                    guidReplicas[lfn] = surl

                    res = lfc.lfc_statg("",guid,stat)
                    fsizeMap[lfn] = long(stat.filesize)
                    md5sumMap[lfn] = stat.csumvalue

                    # TURL
                    match = re.search('^[^:]+://([^:/]+):*\d*/', surl)
                    try:
                        sURLHost = match.group(1)
                    except:
                        sURLHost = defaultSE[0]
                    turl = []
                    if not 'gfal' in configLOCALPROTOCOL \
                           and sURLHost not in stUrlMap \
                           and not 'ccsrm.in2p3.fr' in defaultSE:
                        print 'Using lcg-gt for turl retrieval ...'
                        # check which version of lcg-utils we're on
                        if 'lcgutil_num' in os.environ and os.environ['lcgutil_num']!='' and eval(os.environ['lcgutil_num']) >= 1007002:
                            cmd = "lcg-gt --connect-timeout 60 --sendreceive-timeout 60 --srm-timeout 60 --bdii-timeout 60 " + surl + " " + protocols
                        else:
                            cmd = "lcg-gt -t 60 " + surl + " " + protocols
                        try:
                            signal.signal(signal.SIGALRM, ghandler)
                            signal.alarm(240)
                            child = popen2.Popen3(cmd,1)
                            child.tochild.close()
                            out=child.fromchild
                            err=child.childerr
                            line=out.readline()
                            if line:
                                match = re.search('^[^:]+://([^:/]+:*\d*)/', line)
                                if match:
                                    turl = line.split()
                                elif line.startswith('file:'):
                                    usedProtocol = 'file'
                            else:
                                print line, err.readline()
                            signal.alarm(0)
                        except IOError:
                            print 'lcg-gt time-out !'
                            pass
                        signal.alarm(0)

                        if turl and turl[0]:
                            match = re.search('^[^:]+://([^:/]+:*\d*)/', turl[0])
                            tURLHost = match.group(1)
                            stUrlMap[sURLHost] = tURLHost
                            match = re.search('^(\S*)://.', turl[0])
                            usedProtocol = match.group(1)

        try:
            lfc.lfc_endsess()
        except NameError:
            pass

    print 'usedProtocol: %s' %usedProtocol
    if usedProtocol=='':
        try:
            usedProtocol = configLOCALPROTOCOL[0]
        except:
            pass
        print 'usedProtocol: %s' %usedProtocol

    # Create TURL map
    tUrlMap = {}

    # First try RFIO/DPM and FILE 
    if ( usedProtocol == "rfio" and ( configSETYPE == 'dpm' )) \
           or ( usedProtocol == "file" ):

        turl = []
        # Determine of bulk TURL retrieval can be used
        rc, out = commands.getstatusoutput('which lcg-getturls')
        if not rc:
            useBulkTurl = True
        else:
            useBulkTurl = False

        # Create surl list and split it up in chunks of 50 surls    
        surls = ''
        surlList = []
        isurl = 0
        
        for s in guidReplicas.values():
            isurl = isurl + 1
            surls = surls + " " + s
            if (isurl % 50) == 0 :
                surlList.append(surls)
                surls = ''

        if not surls == '':
            surlList.append(surls)

        bulkprotocols = re.sub(' ',',',protocols.strip())

        if useBulkTurl:
            lines = []
            for surls in surlList:             
                attempt = 0
                # Try 3 times
                while attempt < 3:
                    # Calc timeout
                    timeout = int(60 * 2**attempt)
                    if timeout<60:
                        timeout = 60
                    if 'lcgutil_num' in os.environ and os.environ['lcgutil_num']!='' and eval(os.environ['lcgutil_num']) >= 1007002:
                        cmd = "lcg-getturls --connect-timeout %s --sendreceive-timeout %s --srm-timeout %s --bdii-timeout %s -p %s %s" %(timeout, timeout, timeout, timeout, bulkprotocols, surls)
                    else:
                        cmd = "lcg-getturls -t %s -p %s %s" %(timeout, bulkprotocols, surls)
                    print 'Using lcg-getturls for turl retrieval ...'
                    print cmd
                    rc, out = commands.getstatusoutput(cmd)
                    if not rc:
                        lines = lines + out.split()
                        break
                    else:
                        print out
                        attempt = attempt + 1
            i = 0
            for lfn, surl in guidReplicas.iteritems():
                tUrlMap[lfn] = lines[i]
                i = i + 1 
        else:
            # Single file lcg-gt   
            print 'Using lcg-gt for turl retrieval ...'
            for lfn, surl in guidReplicas.iteritems():                
                # check which version of lcg-utils we're on
                if 'lcgutil_num' in os.environ and os.environ['lcgutil_num']!='' and eval(os.environ['lcgutil_num']) >= 1007002:
                    cmd = "lcg-gt --connect-timeout %s --sendreceive-timeout %s --srm-timeout %s --bdii-timeout %s " %(timeout, timeout, timeout, timeout) + surl + " " + protocols
                else:
                    cmd = "lcg-gt -t %s " %(timeout) + surl + " " + protocols
                print cmd
                try:
                    signal.signal(signal.SIGALRM, ghandler)
                    signal.alarm(240)
                    child = popen2.Popen3(cmd,1)
                    child.tochild.close()
                    out=child.fromchild
                    err=child.childerr
                    line=out.readline()
                    if line and line.find('rfio://')>=0:
                        turl = [line.strip()]
                    elif line and line.find('file://')>=0:
                        turl = [line.strip()]
                    else:
                        print line, err.readline()
                    signal.alarm(0)
                except IOError:
                    print 'lcg-gt time-out !'
                    pass
                signal.alarm(0)

                if turl and turl[0]:
                    pfn = turl[0]
                    break
                else:
                    # remove protocol and host
                    pfn = re.sub('^[^:]+://[^/]+','',surl)
                    # remove redundant /
                    pfn = re.sub('^//','/',pfn)
                    # prepend protocol
                    pfn = configLOCALPROTOCOL + ":" + pfn
                attempt = attempt + 1

                tUrlMap[lfn] = pfn

                
    else:
        # The other protocols use search and replace
        if 'ccsrm.in2p3.fr' in defaultSE:
            usedProtocol = 'dcap'
        for lfn, surl in guidReplicas.iteritems():
            if usedProtocol in [ "dcap", 'gsidcap', 'Xrootd', 'root' ]:
                match = re.search('^[^:]+://([^:/]+):*\d*/', surl)
                try:
                    sURLHost = match.group(1)
                except:
                    sURLHost = defaultSE[0]
                if sURLHost in stUrlMap:
                    pfn = re.sub(sURLHost,stUrlMap[sURLHost],surl)
                else:
                    if not 'ccsrm.in2p3.fr' in defaultSE:
                        pfn = 'gfal:'+surl
                    else:
                        pfn = surl

                if usedProtocol == "dcap" and (sURLHost in stUrlMap or 'ccsrm.in2p3.fr' in defaultSE):
                    pfn = re.sub('srm://','dcap://',pfn)
                    # Hack for ccin2p3
                    pfn = re.sub('ccsrm','ccdcapatlas',pfn)

                    # Hack for TRIUMF
                    if 'srm.triumf.ca' in defaultSE:
                        pfn = re.sub('/atlas/dq2/','//pnfs/triumf.ca/data/atlas/dq2/',pfn)
                        pfn = re.sub('/atlas/users/','//pnfs/triumf.ca/data/atlas/users/',pfn)
                        pfn = re.sub('22125/atlas/','22125//pnfs/triumf.ca/data/atlas/',pfn)
                    # Hack for SFU
                    if 'wormhole.westgrid.ca' in defaultSE:
                        pfn = re.sub('/atlas/dq2/','//pnfs/sfu.ca/data/atlas/dq2/',pfn)
                        pfn = re.sub('/atlas/users/','//pnfs/sfu.ca/data/atlas/users/',pfn)
                        pfn = re.sub('22125/atlas/','22125//pnfs/sfu.ca/data/atlas/',pfn)

                elif usedProtocol in [ "root", "Xrootd" ] and (sURLHost in stUrlMap or 'ccsrm.in2p3.fr' in defaultSE):
                    pfn = re.sub('srm://','root://',pfn)
                    # Hack for ccin2p3
                    pfn = re.sub('ccsrm','ccxroot',pfn)
                    pfn = re.sub('ccdcamli01','ccxroot',pfn)
                    pfn = re.sub(':1094',':1094/',pfn)
                    # Hack for LSF CERN
                    if 'GANGA_ATHENA_WRAPPER_MODE' in os.environ and os.environ['GANGA_ATHENA_WRAPPER_MODE']!='' and os.environ['GANGA_ATHENA_WRAPPER_MODE']=='local':
                        pfn = re.sub('root://castoratlas.cern.ch/castor','root://castoratlas//castor',pfn)

                elif usedProtocol == "gsidcap" and sURLHost in stUrlMap:
                    #pfn = re.sub('srm://','gfal:gsidcap://',pfn)
                    pfn = re.sub('srm://','gsidcap://',pfn)
                    pfn = re.sub('22128/pnfs','22128//pnfs',pfn)
                    pfn = re.sub('gfal:gfal:','gfal:',pfn)

            elif (usedProtocol == "rfio" and configSETYPE == 'castor'):
                #\
                #     or localsitesrm.find('gla.scotgrid.ac.uk')>-1:
                # remove protocol and host
                pfn = re.sub('^[^:]+://[^/]+','',surl)
                # remove redundant /
                pfn = re.sub('^//','/',pfn)
                pfn = "rfio:" + pfn
            elif ( usedProtocol == "rfio" and ( configSETYPE == 'dpm' )) \
                     or ( usedProtocol == "file" ): 
                pass

            else:
                pfn = "gfal:"+surl

            tUrlMap[lfn] = pfn
            
    return guidReplicas, tUrlMap, fsizeMap, md5sumMap 

########################################################################
# make PoolFileCatalog
def _makePoolFileCatalog(files):
    # header
    header = \
"""<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- Edited By POOL -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>
"""
    # item
    item = \
"""
  <File ID="%s">
    <physical>
      <pfn filetype="ROOT_All" name="%s"/>
    </physical>
    <logical>
      <lfn name="%s"/>
    </logical>
  </File>
"""
    # trailer
    trailer = \
"""
</POOLFILECATALOG>
"""
    # check if PoolFileCatalog exists
    oldXML = []
    oldGUIDs = []
    pfcName = 'PoolFileCatalog.xml'
    if os.path.exists(pfcName):
        # read lines
        inFile = open(pfcName)
        oldXML = inFile.readlines()
        inFile.close()
        # extract GUIDs
        # rename
        os.rename(pfcName,pfcName+'.BAK')
    # open 
    outFile = open(pfcName,'w')
    # write header
    outFile.write(header)
    # write files
    newGUIDs = []
    for lfn,file in files.iteritems():
        outFile.write(item % (file['guid'].upper(),file['pfn'],lfn))
        newGUIDs.append(file['guid'].upper())
    # write old files
    fileFlag = False
    for line in oldXML:
        # look for file item
        match = re.search('<File ID="([^"]+)"',line)
        if match != None:
            # avoid duplication
            guid = match.group(1)
            if not guid in newGUIDs:
                fileFlag = True
                outFile.write('\n')
            else:
                print "WARNING: duplicated GUID %s in %s. Replaced" % (guid,pfcName)
        # write
        if fileFlag:
            outFile.write(line)
        # look for item end
        if re.search('</File>',line) != None:
            fileFlag = False
    # write trailer
    outFile.write(trailer)
    outFile.close()

########################################################################
# prepending jobOptions
def _preJobO(inputFileList = [], inputFileListPeeker = [] ):

    return """
try:
    from EventSelectorAthenaPool.EventSelectorAthenaPoolConf import EventSelectorAthenaPool
    orig_ESAP__getattribute =  EventSelectorAthenaPool.__getattribute__

    def _dummy(self,attr):
        if attr == 'InputCollections':
            return %(inputFileList)s 
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
      
try:
    import AthenaCommon.AthenaCommonFlags

    def _dummyFilesInput(*argv):
        return %(inputFileListPeeker)s 

    AthenaCommon.AthenaCommonFlags.FilesInput.__call__ = _dummyFilesInput
except:
    pass

try:
    import AthenaCommon.AthenaCommonFlags

    def _dummyGet_Value(*argv):
        return %(inputFileListPeeker)s 

    for tmpAttr in dir (AthenaCommon.AthenaCommonFlags):
        import re
        if re.search('^(Pool|BS).*Input$',tmpAttr) != None:
            try:
                getattr(AthenaCommon.AthenaCommonFlags,tmpAttr).get_Value = _dummyGet_Value
            except:
                pass
except:
    pass

try:
    from AthenaServices.SummarySvc import *
    useAthenaSummarySvc()
except:
    pass

""" % { 'inputFileList' : inputFileList, 'inputFileListPeeker' : inputFileListPeeker }

########################################################################
# make job option file
def _makeJobO(files, tag=False, type='TAG', version=12, dtype='MC', usePrependJobO=False):
    if version >= 13:
        versionString='ServiceMgr.'
    else:
        versionString = ''
        
    # sort
    lfns = files.keys()
    lfns.sort()
    # open jobO
    joName = 'input.py'
    outFile = open(joName,'w')

    if usePrependJobO:
        joName = 'preJobO.py'
        outFilePre = open(joName,'w')
        inputFileList = []
        inputFileListPeeker = []
        for lfn in lfns:
            if (configSETYPE == 'dpm') and 'surl' in files[lfn]:
                surl = files[lfn]['surl']
                # remove protocol and host
                pfn = re.sub('^gfal:','',surl)
                pfn = re.sub('^[^:]+://[^/]+','',pfn)
                # remove redundant /
                pfn = re.sub('^//','/',pfn)
                pfn = "rfio:" + pfn
                inputFileListPeeker.append(pfn)
                inputFileList.append(files[lfn]['pfn'])
            else:
                inputFileList.append(files[lfn]['pfn'])
                inputFileListPeeker.append(files[lfn]['pfn'])
        #preJobO = _preJobO(inputFileList, inputFileListPeeker)
        preJobO = _preJobO(inputFileListPeeker, inputFileListPeeker)
        outFilePre.write(preJobO)
        outFilePre.close()

    if 'RECEXTYPE' not in os.environ or os.environ['RECEXTYPE'] == '':

        try:
            if 'ATHENA_MAX_EVENTS' in os.environ:
                evtmax = int(os.environ['ATHENA_MAX_EVENTS'])
            else:
                evtmax = -1
        except:
            evtmax = -1
        outFile.write('theApp.EvtMax = %d\n' %evtmax)

        skipevt = 0
        try:
            if 'ATHENA_SKIP_EVENTS' in os.environ:
                skipevt = int(os.environ['ATHENA_SKIP_EVENTS'])
            else:
                skipevt = 0
        except:
            skipevt = 0
        if skipevt != 0:
            outFile.write('ServiceMgr.EventSelector.SkipEvents = %d\n' %skipevt)

        outFile.write('try:\n')

        if dtype == 'DATA':
            outFile.write('    %sByteStreamInputSvc.FullFileName = ['%versionString)
        elif dtype == 'MC':
            outFile.write('    %sEventSelector.InputCollections = ['%versionString)
        elif dtype == 'MuonCalibStream':
            outFile.write('    svcMgr.MuonCalibStreamFileInputSvc.InputFiles = [')
        else:
            outFile.write('    %sEventSelector.InputCollections = ['%versionString)

            if tag:
##                 if type == 'TAG_REC':
##                     if version >= 13:
##                         outFile.write('PoolTAGInput = [')
##                     else:
##                         outFile.write('CollInput = [')
                outFile.write('    %sEventSelector.CollectionType="ExplicitROOT"\n'%versionString)
            #outFile.write('%sEventSelector.InputCollections = ['%versionString)

    else:
        # Write input for RecExCommon jobs
        outFile.write('from AthenaCommon.AppMgr import ServiceMgr\n')
        outFile.write('from AthenaCommon.AppMgr import ServiceMgr as svcMgr\n')
        outFile.write('from AthenaCommon.AthenaCommonFlags import athenaCommonFlags\n')
        outFile.write('ganga_input_files = [')

        try:
            if 'ATHENA_MAX_EVENTS' in os.environ:
                evtmax = int(os.environ['ATHENA_MAX_EVENTS'])
            else:
                evtmax = -1
        except:
            evtmax = -1

        skipevt = 0
        try:
            if 'ATHENA_SKIP_EVENTS' in os.environ:
                skipevt = int(os.environ['ATHENA_SKIP_EVENTS'])
            else:
                skipevt = 0
        except:
            skipevt = 0

        if tag:
            outFileEvtMax = open('evtmax.py','w').write('%sEventSelector.CollectionType="ExplicitROOT"\ntheApp.EvtMax = %d\n' % (versionString, evtmax) )
        else:
            outFileEvtMax = open('evtmax.py','w').write('theApp.EvtMax = %d\n' %evtmax)

        if skipevt != 0:
            outFileEvtMax = open('evtmax.py','w').write('ServiceMgr.EventSelector.SkipEvents = %d\n' %skipevt)
            
    # loop over all files
    flatFile = 'input.txt'
    outFlatFile = open(flatFile,'w')
    
    for lfn in lfns:
        filename = files[lfn]['pfn']
##         if tag:
##             if atlas_release_major <= 12:
##                 filename = re.sub('\.root\.\d+$','',filename)
##                 filename = re.sub('\.root$','',filename)
##             else:
##                 filename = re.sub('root\.\d+$','root',filename)
        # write PFN
        outFile.write('"%s",' % filename)
        outFlatFile.write('%s\n' %filename)
        
    if 'RECEXTYPE' not in os.environ or os.environ['RECEXTYPE'] == '':
        outFile.write(']\n')
        outFile.write('except:\n')
        outFile.write('    pass\n')

    else:
        outFile.write(']\n')
        outFile.write('athenaCommonFlags.Pool%sInput.set_Value_and_Lock(ganga_input_files)\n' %
                      os.environ['RECEXTYPE'])
        outFile.write('athenaCommonFlags.FilesInput.set_Value_and_Lock(ganga_input_files)\n')
        outFile.write('athenaCommonFlags.EvtMax.set_Value_and_Lock(%d)\n' % evtmax)

    ## setting for event picking
    if 'ATHENA_RUN_EVENTS' in os.environ:
        revt = eval(os.environ['ATHENA_RUN_EVENTS'])
        run_evt = []
        for i in range(len(revt)):
            run_evt.append((revt[i][0], revt[i][1]))
        
        outFile.write('\n#EventPicking\n')
        outFile.write('from AthenaCommon.AlgSequence import AthSequencer\n')
        outFile.write("seq = AthSequencer('AthFilterSeq')\n")
        outFile.write('from GaudiSequencer.PyComps import PyEvtFilter\n')
        outFile.write("seq += PyEvtFilter('alg', evt_info='',)\n")
        outFile.write('seq.alg.evt_list = %s\n' % run_evt)
        outFile.write("seq.alg.filter_policy = '%s'\n"  % os.environ['ATHENA_FILTER_POLICY'])
        outFile.write('for tmpStream in theApp._streams.getAllChildren():\n')
        outFile.write('\t fullName = tmpStream.getFullName()\n')
        outFile.write("\t if fullName.split('/')[0] == 'AthenaOutputStream':\n")
        outFile.write("\t\t tmpStream.AcceptAlgs = [seq.alg.name()]\n")
   
    # close
    outFile.close()
    outFlatFile.close()

########################################################################
# extract PFN and LFN from PoolFileCatalog
def _getFNsPFC(stringValue,fromFile=True):
    lfns = []
    pfns = []
    guids = []
    # instantiate parser
    try:

        if fromFile:
            root  = xml.dom.minidom.parse(stringValue)
        else:
            root  = xml.dom.minidom.parseString(stringValue)
        files = root.getElementsByTagName('File')
        for file in files:
            # GUID
            guid = str(file.getAttribute('ID'))
            # get PFN node
            physical = file.getElementsByTagName('physical')[0]
            pfnNode  = physical.getElementsByTagName('pfn')[0]
            # convert UTF8 to Raw
            pfn = str(pfnNode.getAttribute('name'))
            # remove protocol
            pfn = re.sub('^[^:]+:','',pfn)
            # get LFN node
            try:
                logical  = file.getElementsByTagName('logical')[0]
                lfnNode  = logical.getElementsByTagName('lfn')[0]
                # convert UTF8 to Raw            
                lfn = str(lfnNode.getAttribute('name'))
            except:
                lfn = pfn.split('/')[-1]
            # append
            lfns.append(lfn)
            pfns.append(pfn)
            guids.append(guid)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR : could not parse XML - %s %s" % (type, value)
        #sys.exit(EC_STAGEOUT)

    # return
    return (lfns,pfns,guids)
########################################################################
def hexify(str):
    # a function to turn a string of non-printable characters into a string of
    # hex characters

    hexStr = string.hexdigits
    r = ''
    for ch in str:
        i = ord(ch)
        r = r + hexStr[(i >> 4) & 0xF] + hexStr[i & 0xF]
    return r


def __adler32(filename):
    import zlib
    #adler starting value is _not_ 0L
    adler=1

    try:
        openFile = open(filename, 'rb')

        for line in openFile:
            adler=zlib.adler32(line, adler)

    except:
        raise Exception('Could not get checksum of %s'%filename)

    openFile.close()

    #backflip on 32bit
    if adler < 0:
        adler = adler + 2**32

    return str('%08x'%adler) #return as padded hexified string


def getLocalFileMetadata_adler32(file):
    # check file exists
    if not os.access(file,os.R_OK):
        return -1,-1
    size=os.stat(file)[6]
    # get adler32
    try:
        adler32 =  __adler32(file)
    except MemoryError:
        cmd = 'adler32 %s' % file
        rc, out = commands.getstatusoutput(cmd)
        if rc != 0:
            print 'ERROR during execution of %s' %cmd
            print rc, out
            adler32 = -1
        else:
            adler32 = out.split('')[-1]
        
    return size, adler32


def getLocalFileMetadata(file):
    # check file exists
    if not os.access(file,os.R_OK):
        return -1,-1
    size=os.stat(file)[6]
    # get md5sum
    try:
        #m = md.new()
        md5sum = hexify(md.digest())
        mf = open(file, 'r')
        for line in mf.readlines():
            md.update(line)
        mf.close()
        md5sum=hexify(md.digest())
    except MemoryError:
        cmd = 'md5sum %s' % file
        rc, out = commands.getstatusoutput(cmd)
        if rc != 0:
            print 'ERROR during execution of %s' %cmd
            print rc, out
            md5sum = -1
        else:
            md5sum = out.split(' ')[0]
        
    return size,md5sum

####################################
def addFileMetadata(guid, fsize= None, checksum = None, csumtype = 'AD' ):

    try:
        stat = lfc.lfc_filestatg()
        rc = lfc.lfc_statg("", guid, stat)
    except:
        return -1

    if rc != 0:
        err_num = lfc.cvar.serrno
        errstr = lfc.sstrerror(err_num)
        print err_num, errstr
        return -1

    if fsize:
        filesize = long(fsize)
    else:
        filesize = stat.filesize

    try:
        rc = lfc.lfc_setfsizeg( guid, filesize, csumtype, checksum)
    except:
        return -1

    if rc != 0:
        err_num = lfc.cvar.serrno
        errstr = lfc.sstrerror(err_num)
        print err_num, errstr
        return -1

    return 0

########################################################################
# Save outfile file on SE
def save_file(count, griddir, dest, gridlfn, output_lfn, filename, poolguid, siteID, tokenname=''):

    # Calc timeout
    timeout = int(300 * 2**count)
    if timeout<300:
        timeout = 300
    
    # Create LFC directory
    cmd = "lfc-mkdir -p %s" %(griddir) 
    rc, out = commands.getstatusoutput(cmd)
    if rc != 0:
        print 'ERROR during execution of %s' %cmd
        print rc, out
        return -1, -1, -1

    # check which version of lcg-utils we're on
    if 'lcgutil_num' in os.environ and os.environ['lcgutil_num']!='' and eval(os.environ['lcgutil_num']) >= 1007002:
        t = timeout / 2
        cmd = "lcg-cr --connect-timeout %i --sendreceive-timeout %i --srm-timeout %i --bdii-timeout %i " % ( t, t, t, t )
    else:
        cmd = "lcg-cr -t %i" % timeout
    
    # Create file replica
    #cmd = "lcg-cr --vo atlas -t 300 -d %s -l %s -P %s file://%s" %(dest, gridlfn, output_lfn, filename)
    if tokenname:
        cmd = cmd + " --vo atlas -s %s " %tokenname
    else:
        cmd = cmd + " --vo atlas "
    if poolguid != '':
        cmd = cmd + " -d %s -g %s -l %s file://%s" %(dest, poolguid, gridlfn, filename)
    else:
        cmd = cmd + " -d %s -l %s file://%s" %(dest, gridlfn, filename)
    rc, out = commands.getstatusoutput(cmd)
    
    if rc == 0:
        match = re.search('([\w]+-[\w]+-[\w]+-[\w]+-[\w]+)', out)
        if match:
            guid = match.group(1)
        else:
            guid = out
        # Open output_guids to transfer guids back to GANGA
        f = open('output_guids','a')
        print >>f, '%s,%s' %(guid,siteID)
        f.close()
        if globalVerbose:
            print cmd
            print out
            print guid
        guid = re.sub('^guid:','',guid)
    else:
        print 'ERROR during execution of %s' %cmd
        print rc, out
        return -1, -1, -1

    # size and md5sum
    size, md5sum = getLocalFileMetadata_adler32(filename)

    ret = addFileMetadata(guid, size, md5sum, 'AD')
    if ret!=0:
        print 'Error adding file checksum to LFC'
    
    return guid, size, md5sum

########################################################################
def dataset_exists(datasetname, siteID):
    """Does Dataset already exist and is frozen?"""

    state = -1

    try:
        dq2_lock.acquire()
        try:
            datasetinfo = dq2.listDatasets(datasetname)
        except:
            datasetinfo = {}
    finally:
        dq2_lock.release()

    if datasetinfo=={}:
        print 'Dataset %s is not defined in DQ2 database !' %datasetname
        return -1

    try:
        dq2_lock.acquire()
        try:
            state = dq2.getMetaDataAttribute(datasetname,['state'])
            state = state['state']
        except:
            print 'Problem retrieving state of dataset %s !' %datasetname
            return -1
    finally:
        dq2_lock.release()

    return state

########################################################################
def register_dataset_location(datasetname, siteID):
    """Register location of dataset into DQ2 database"""

    alllocations = []

    try:
        dq2_lock.acquire()
        try:
            datasetinfo = dq2.listDatasets(datasetname)
        except:
            datasetinfo = {}
    finally:
        dq2_lock.release()

    if datasetinfo=={}:
        print 'Dataset %s is not defined in DQ2 database !' %datasetname
        return -1

    try:
        dq2_lock.acquire()
        try:
            locations = dq2.listDatasetReplicas(datasetname)
        except:
            locations = {}
    finally:
        dq2_lock.release()

    if locations != {}: 
        try:
            datasetvuid = datasetinfo[datasetname]['vuids'][0]
        except KeyError:
            print 'Dataset %s not found' %datasetname
            return -1
        if datasetvuid not in locations:
            print 'Dataset %s not found' %datasetname
            return -1
        alllocations = locations[datasetvuid][0] + locations[datasetvuid][1]

    try:
        dq2_lock.acquire()
        if not siteID in alllocations:
            try:
                dq2.registerDatasetLocation(datasetname, siteID)
            except DQInvalidRequestException as Value:
                print 'Error registering location %s of dataset %s: %s' %(datasetname, siteID, Value) 
    finally:
        dq2_lock.release()

    # Verify registration
    try:
        dq2_lock.acquire()
        try:
            locations = dq2.listDatasetReplicas(datasetname)
        except:
            locations = {}
    finally:
        dq2_lock.release()

    if locations != {}: 
        datasetvuid = datasetinfo[datasetname]['vuids'][0]
        alllocations = locations[datasetvuid][0] + locations[datasetvuid][1]
    else:
        alllocations = []

    return alllocations


########################################################################
def register_file_in_dataset(datasetname,lfn,guid, size, checksum):
    """Add file to dataset into DQ2 database"""
    # Check if dataset really exists

    val = -1
    try:
        dq2_lock.acquire()
        content = dq2.listDatasets(datasetname)
    finally:
        dq2_lock.release()

    if content=={}:
        print 'Dataset %s is not defined in DQ2 database !' %datasetname
        return
    # Add file to DQ2 dataset
    ret = []
    try:
        dq2_lock.acquire()
        try:
            ret = dq2.registerFilesInDataset(datasetname, lfn, guid, size, checksum) 
            val = 0
        except (DQInvalidFileMetadataException, DQInvalidRequestException, DQFileExistsInDatasetException) as Value:
            print 'Warning, some files already in dataset: %s' %Value
            pass
    finally:
        dq2_lock.release()

    return val

########################################################################
def register_datasets_details(datasets,outdata):

    reglines=[]
    for line in outdata:
        try:
            #[dataset,lfn,guid,siteID]=line.split(",")
            [dataset,lfn,guid,size,md5sum,siteID]=line.split(",")
        except ValueError:
            continue
        size = long(size)
        adler32='ad:'+md5sum
        if len(md5sum)==32:
            adler32='md5:'+md5sum
        
        siteID=siteID.strip() # remove \n from last component
        regline=dataset+","+siteID
        if regline in reglines:
            print "Attempting to register of %s in %s already done, skipping" % (dataset,siteID)
            #continue
        else:
            reglines.append(regline)
            print "Attempting to register dataset %s in %s" % (dataset,siteID)
            # use another version of register_dataset_location, as the "secure" one does not allow to keep track of datafiles saved in the fall-back site (CERNCAF)
            try:
                dq2_lock.acquire()
                try:
                    content = dq2.listDatasets(dataset)
                except:
                    content = {}
            finally:
                dq2_lock.release()

            # Register new dataset
            if content=={}:
                try:
                    dq2_lock.acquire()
                    try:
                        datasetinfo = dq2.registerNewDataset(dataset)
                    except (DQDatasetExistsException,Exception) as Value:
                        print 'Error registering new dataset %s: %s' %(dataset,Value)
                finally:
                    dq2_lock.release()
            else:
                print "Dataset %s already registered." % dataset
            # Register dataset location 
            attempt = 0
            while attempt < 3:
                location = register_dataset_location(dataset, siteID)
                if siteID in location:
                    break
                else:
                    attempt = attempt + 1
                    time.sleep(30)

        attempt = 0
        ret = 0
        while attempt < 3:     
            ret = register_file_in_dataset(dataset,[lfn],[guid],[size],[adler32])
            if ret==0:
                attempt = 3
            else:
                attempt = attempt + 1
                time.sleep(30)

    return
########################################################################
def register_datasets_in_container(container, dataset):
    """Register dataset in container"""

    if not container.endswith('/'):
        containerName = container+'/'

    # Check if container already exists
    containerinfo = {}
    try:
        dq2_lock.acquire()
        try:
            containerinfo = dq2.listDatasets(containerName)
        except:
            containerinfo = {}
    finally:
        dq2_lock.release()

    if containerinfo!={}:
        print 'Container %s is already defined in DQ2 database' %containerName

    # Create output container
    attempt = 0
    while containerinfo=={} and attempt < 3:
        try:
            dq2_lock.acquire()
            try:
                dq2.registerContainer(containerName)
                print 'Registered container %s' %containerName
                attempt = 3
            except:
                print 'Problem registering container %s - might already exist ? Please check with dq2-ls containername' %containerName
                attempt = attempt + 1
                time.sleep(30)
        finally:
            dq2_lock.release()   
    # Register dataset in container
    attempt = 0
    while attempt < 3:
        try:
            dq2_lock.acquire()
            try:
                dq2.registerDatasetsInContainer(containerName, [ dataset ])
                attempt = 3
            except:
                print 'Problem registering dataset %s in container %s - might already be registered ? Please check with dq2-ls -f datasetname or containername' %(dataset, containerName)
                attempt = attempt + 1
                time.sleep(30)
        finally:
            dq2_lock.release()

    return
########################################################################
def check_duplicates_in_dataset(datasetname, output_files):
    """Checks for duplicate output files in outputdataset"""
    
    try:
        dq2_lock.acquire()
        try:
            contents = dq2.listFilesInDataset(out_datasetname)
        except:
            print 'Problem retrieving content info dataset %s from DQ2! ' %datasetname
            return
    finally:
        dq2_lock.release()

    if not contents:
        print 'Dataset %s is empty.' %datasetname
        return

    contents = contents[0]
    fileNames = []

    for guid, keys in contents.iteritems():
        fileNames.append(keys['lfn'])

    filePattern = []
    for fileName in fileNames:
        for outFile in output_files:
            patName = '\._(\w+)\.%s$'%outFile
            match = re.search(patName,fileName)
            if match:
                if match.group(0) in filePattern:
                    print '!!!!!!!!!!! ATTENTION !!!!!!!!!!!!!!'
                    print 'Possible duplicated output file %s in output dataset %s' %(fileName, datasetname)
                    print 'After all subjobs have finished run j.outputdata.clean_duplicates_in_dataset() or j.outputdata.clean_duplicates_in_container()'
                    print '!!!!!!!!!!! ATTENTION !!!!!!!!!!!!!!'
                else:
                    filePattern.append(match.group(0))

    return
       
########################################################################

if __name__ == '__main__':

    directory = os.getcwd()
    retry = 3
    timeout = 600
    input = None
    output = None
    inputguid = None
    returnvalue = 0
    detsetype = False 
    detsename = ''

    dq2tracertime = []
    dq2tracertime.append(time.time())
    
    try:
        opts, args = getopt(sys.argv[1:],'hvt:d:r:i:g:o',['help','verbose','directory=','input=','output=','guid=','timeout=','retry=','setype','se='])
    except GetoptError:
        usage()
        sys.exit(EC_Configuration)

    for opt, val in opts:

        if opt in ['-h','--help']:
            usage()
            sys.exit(EC_Configuration)

        if opt in ['-d','--directory']:
            directory = val 

        if opt in ['-i','--input']:
            input = val

        if opt in ['-o','--output']:
            output = val

        if opt in ['-g','--guid']:
            inputguid = val

        if opt in ['-t','--timeout']:
            timeout = int(val)

        if opt in ['-r','--retry']:
            retry = int(val)

        if opt in ['--setype']:
            detsetype = True

        if opt in ['--se']:
            detsename = val

        if opt in ['-v','--verbose']:
            globalVerbose = True

    # Determine atlas release
    try:
        atlas_release = os.environ['ATLAS_RELEASE']
    except:
        atlas_release = '15.5.1'
        if not detsetype:
            print "ERROR : ATLAS_RELEASE not defined, using %s" %(atlas_release)

        pass

    atlas_release_major = int(atlas_release.split('.')[0])

    # Determine dataset type
    try:
        datasettype = os.environ['DATASETTYPE']
    except:
        datasettype = 'DQ2_LOCAL'
        if not detsetype:
            print "ERROR : DATASETTYPE not defined, using %s" %(datasettype)
        pass

    # use DQ2_LOCAL as default
    if not datasettype in [ 'DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TAG_REC', 'DQ2_OUT', 'TNT_LOCAL', 'TNT_DOWNLOAD', 'TIER3' ]:
        datasettype = 'DQ2_LOCAL'

    # Determine data type
    try:
        datatype = os.environ['DATASETDATATYPE']
    except:
        if not detsetype:
            print "ERROR : DATASETDATATYPE not defined, using MC"
        datatype = 'MC'
        pass
    if not datatype in [ 'DATA', 'MC', 'MuonCalibStream' ]:
        datatype = 'MC'

    # Set DQ2 server
    try:
        dq2urlserver = os.environ['DQ2_URL_SERVER']
    except:
        if not detsetype:        
            print "ERROR: Environment variable DQ2_URL_SERVER not set"
        #sys.exit(EC_Configuration)
    try:
        dq2urlserverssl = os.environ['DQ2_URL_SERVER_SSL']
    except:
        if not detsetype:        
            print "ERROR: Environment variable DQ2_URL_SERVER_SSL not set"
        #sys.exit(EC_Configuration)

    if datasettype in [ 'DQ2_DOWNLOAD', 'DQ2_LOCAL', 'TAG', 'TAG_REC', 'DQ2_OUT', 'TNT_LOCAL', 'TNT_DOWNLOAD' ]:

        # Determine Hostname and local DQ2 settings        
        localsiteid = ''
        siteID = ''
        cmd =  "grep DQ2_LOCAL_SITE_ID $VO_ATLAS_SW_DIR/ddm/latest/setup.sh |  tr '=' '\n' | tail -1"
        rc, out = commands.getstatusoutput(cmd)
        if not rc and not out.startswith('grep') and out.endswith('DISK'):
            dq2localsiteid = out
        else:
            dq2localsiteid = DQ2LOCALSITEID

        localsiteid = dq2localsiteid
        siteID = dq2localsiteid
        outFile = open('dq2localid.txt','w')
        outFile.write('%s\n' %dq2localsiteid )
        outFile.close()

        # Determine srm and Remove token info
        localsitesrm = TiersOfATLAS.getSiteProperty(localsiteid,'srm')
        localsitesrm = re.sub('token:*\w*:','', localsitesrm)
        localsitesrm = re.sub(':*\d*/srm/managerv2\?SFN=','', localsitesrm)
        
        # Determine local protocol and SEType
        configLOCALPROTOCOL = ''
        configLOCALPREFIX = ''

        dq2alternatename = TiersOfATLAS.getSiteProperty(localsiteid,'alternateName')
        for sitename in TiersOfATLAS.getAllSources():
            if TiersOfATLAS.getSiteProperty(sitename,'alternateName'):
                if TiersOfATLAS.getSiteProperty(sitename,'alternateName')==dq2alternatename and \
                (TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:ATLASMCDISK') or \
                 TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:ATLASDATADISK') or \
                 TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:ATLASSCRATCHDISK') or \
                 TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:T2ATLASMCDISK') or \
                 TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:T2ATLASDATADISK') or \
                 TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:T2ATLASSCRATCHDISK')) and \
                 (TiersOfATLAS.getSiteProperty(sitename,'seinfo')): 
                    try:
                        configLOCALPROTOCOL = TiersOfATLAS.getProtocols(sitename)
                        configLOCALPREFIX = configLOCALPROTOCOL[0] + ':'
                        configSETYPE = TiersOfATLAS.getSEType(sitename)
                    except:
                        seinfo = TiersOfATLAS.getSiteProperty(sitename, 'seinfo')
                        if seinfo and 'protocols' in seinfo:
                            configLOCALPROTOCOL = [p[0] for p in seinfo['protocols']]
                            configLOCALPREFIX = configLOCALPROTOCOL[0] + ':'
                            configSETYPE = seinfo['setype']
                        

                    break

        if not detsetype:
            print 'localsiteid: %s' %(localsiteid)
            print 'DQ2_LOCAL_SITE_ID: %s' %(localsiteid)
            print 'localsitesrm: %s' %(localsitesrm) 
            print 'configSETYPE: %s' %(configSETYPE)
            print 'configLOCALPROTOCOL: %s' %(configLOCALPROTOCOL)
            print 'configLOCALPREFIX: %s' %(configLOCALPREFIX)
            
        else:
            print configSETYPE.upper()
            sys.exit(0)

        # Find LFC Catalog host and set LFC_HOST 
        lfccat = TiersOfATLAS.getRemoteCatalogs(localsiteid)
        if lfccat:
            lfc_host = re.sub('[/:]',' ',lfccat[0]).split()[1]
        else:
            lfc_host = ''
        os.environ[ 'LFC_HOST' ] = lfc_host
        print 'LFC_HOST: %s' %(lfc_host)

        # Get location list of dataset
        try:
            datasetlocation = os.environ['DATASETLOCATION'].split(":")
        except:
            if not detsetype:
                print "ERROR : DATASETLOCATION not defined"
            datasetlocation = []
            pass

        for sitename in datasetlocation:
            if TiersOfATLAS.getSiteProperty(sitename,'alternateName')==dq2alternatename:
                print 'detected DQ2_LOCAL_SITE_ID: %s' %(sitename)
        
        if localsitesrm!='':
            defaultSE = _getDefaultStorage(localsitesrm)
        else:
            defaultSE = ''
        
        print 'defaultSE: %s' %(defaultSE)

    ######################################################################
    # Start input configuration
    # Do TAG first as it needs to get the AOD info and change the input_files
    if datasettype!='DQ2_OUT':

        tag_files = {}
        
        # TAG DATASET ###########################################################
        if 'TAG_TYPE' in os.environ:
            
            print "Preparing TAG Datasets..."
            files = {}

            if os.environ['TAG_TYPE'] == 'LOCAL':
                if os.access('./tag_file_list',os.R_OK):

                    print "TAG list file found in input sandbox. Using this as input..."
                    tag_files = {}
                    for tag_file in open("./tag_file_list").readlines():
                        filename = tag_file.strip()
                        if filename[ len(filename)-4:] == '.dat':
                            # uncompress the data files
                            print "UNCOMPRESSING TAG FILES..."
                            cmd = "export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH ; ./CollInflateEventInfo.exe " + filename
                            rc, out = getstatusoutput(cmd)
                            print out
                            
                            if (rc!=0):
                                print "ERROR: error during CollInflateEventInfo.exe. Retrying..."
                                cmd = "export LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH_BACKUP_ATH; export PATH=$PATH_BACKUP_ATH; export PYTHONPATH=$PYTHONPATH_BACKUP_ATH;./CollInflateEventInfo.exe " + filename
                                rc, out = getstatusoutput(cmd)
                                
                                print out
                                if (rc!=0):
                                    print "ERROR: error during CollInflateEventInfo.exe. Giving up..."
                                    sys.exit(-1)

                            os.system("mv outColl.root %s" % filename+".root")
                            filename = filename+".root"
                            
                        item = {'pfn': filename,'guid':''}
                        tag_files[filename] = item

                    print "Creating JO file with this file list:"
                    print tag_files
                else:
                    print "ERROR: Local TAG selected but no local file list."
                    sys.exit(-1)
                    
            elif os.environ['TAG_TYPE'] in ['DQ2', 'AUTO']:
                
                # get dataset list
                try:
                    tagdatasetnames = os.environ['DATASETNAME'].split(":")

                except:
                    raise NameError("ERROR: DATASETNAME not defined")

                # compose dq2 command
                dq2setuppath = '$VO_ATLAS_SW_DIR/ddm/latest/setup.sh'
                inputtxt = 'dq2localid.txt'
                try:
                    temp_dq2localsiteid = [ line.strip() for line in file(inputtxt) ]
                    dq2localsiteid = temp_dq2localsiteid[0]
                except:
                    dq2localsiteid = os.environ[ 'DQ2_LOCAL_SITE_ID' ]
                    pass

                taglfns = [ line.strip() for line in file('input_files') ]
                tagguids = [ line.strip() for line in file('input_guids') ]

                tagflist = ','.join(taglfns)

                print "Downloading files: " + tagflist + " from datasets: " + os.environ['DATASETNAME']

                for tagdatasetname in tagdatasetnames:

                    cmd = 'source %s; dq2-get --client-id=ganga --automatic --local-site=%s --no-directories --timeout %s -p lcg -f %s %s' % (dq2setuppath, dq2localsiteid, timeout, tagflist, tagdatasetname)
                    cmdretry = 'source %s; dq2-get --client-id=ganga --automatic --local-site=CERN-PROD_DATADISK --no-directories --timeout %s -p lcg -f %s %s' % (dq2setuppath, timeout, tagflist, tagdatasetname)

                    # execute dq2 command
                    rc, out = getstatusoutput(cmd)
                    print out

                    bad_dq2_get = False

                    for f in taglfns:
                        if not os.path.exists(f):
                            bad_dq2_get = True

                    if (rc!=0) or bad_dq2_get:
                        print taglfns
                        os.system("ls -ltr")
                        print "ERROR: error during dq2-get occured"
                        rc, out = getstatusoutput('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP ; export PATH=$PATH_BACKUP ; export PYTHONPATH=$PYTHONPATH_BACKUP ; ' + cmd)
                        print out
                        if (rc!=0):
                            print "ERROR: error during retry of dq2-get occured"
                            sys.exit(EC_DQ2GET)

                tagddmFileMap = {}
                for i in xrange(0,len(taglfns)):
                    tagddmFileMap[taglfns[i]] = tagguids[i]

                tag_files = {}
                # check if all files have been transfered
                pfnsnew = []
                for lfn, guid in tagddmFileMap.iteritems():
                    name = os.path.basename(lfn)
                    pfn = os.path.join(directory,name)
                    # check if all files exists and if file size greater 0
                    try:
                        open(pfn)
                        fsize = os.stat(pfn).st_size
                    except IOError:
                        print "ERROR %s not found" % pfn
                        rc, out = getstatusoutput('ls -ltr')
                        print out
                        rc, out = getstatusoutput('ls -ltr directory')
                        print out

                        continue
                    if (fsize>0):
                        # append
                        item = {'pfn':pfn,'guid':guid}
                        tag_files[lfn] = item


                if os.environ['TAG_TYPE'] == 'AUTO':

                    print "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
                    print "Identifying TAG links"
                    ref_lkup = {}
                    new_add_files = {}
                    
                    for lfn in tag_files:

                        # create a symbolic link to remove the .root
                        f = tag_files[lfn]['pfn']
                        if not os.path.exists( f + ".root"):
                            print "linking " + f
                            os.symlink(f, f + ".root")

                        # ----------------------------------------------------
                        # now run the Collection Utilities if required
                        # Note: Grabbing both ESD and AOD GUIDs
                        cmd = "CollListFileGUID -src " + f + " RootCollection -queryopt StreamAOD_ref,StreamESD_ref | grep -E [[:alnum:]]{8}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{12} "
                        rc, out = getstatusoutput(cmd)
                        print out

                        if (rc!=0):
                            print "ERROR: error during CollListFileGUID. Retrying..."
                            cmd = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_BACKUP_ATH; export PATH=$PATH_BACKUP_ATH; export PYTHONPATH=$PYTHONPATH_BACKUP_ATH; CollListFileGUID -src " + f + " RootCollection -queryopt StreamAOD_ref,StreamESD_ref | grep -E [[:alnum:]]{8}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{4}'-'[[:alnum:]]{12} "
                            rc, out = getstatusoutput(cmd)

                            print out
                            if (rc!=0):
                                print "ERROR: error during CollListFileGUID. Giving up..."
                                sys.exit(-1)

                        # grab the guids referenced by this file
                        ref_guids = []
                        for ln in out.split('\n'):
                            try:
                                ref_guids.append(ln.split()[0])
                            except:
                                continue

                        print "GUIDs referenced by " + f
                        print repr(ref_guids)
                        if len(ref_guids) == 0:
                            print "ERROR: No GUIDs found for " + f
                            sys.exit(-1)

                        # now find the dataset and file
                        for ref_guid in ref_guids:

                            print "Getting dataset referenced by GUID %s" % ref_guid

                            ref_name = ''
                            ref_dataset = ''

                            # check the cache first
                            for lkup_name in ref_lkup:
                                if ref_guid in ref_lkup[lkup_name][0]:
                                    ref_name = ref_lkup[lkup_name][0][ref_guid]['lfn']
                                    ref_dataset = lkup_name
                                    new_add_files[ref_dataset].append([ref_name, ref_guid])
                                    
                            if ref_name != '' and ref_dataset != '':
                                continue

                            ref_vuids = dq2.contentClient.queryDatasetsWithFileByGUID(ref_guid)
                            if len(ref_vuids) == 0:
                                continue

                            for ref_vuid in ref_vuids:

                                try:
                                    print "Trying VUID %s for GUID %s... " % (ref_vuid, ref_guid)
                                    ref_dataset = dq2.repositoryClient.resolveVUID(ref_vuid)
                                    ref_name = ref_dataset.get('dsn')
                                    if ref_name != '' and len(dq2.listDatasetReplicas(ref_name)) != 0 and (ref_name.find(".ESD.") != -1 or ref_name.find(".AOD.") != -1):
                                        break
                                    else:
                                        ref_name = ''

                                except DQUnknownDatasetException:
                                    print "ERROR Finding dataset for vuid for " + ref_vuid

                            if ref_name == '':
                                continue

                            # store useful stuff
                            ref_files = dq2.listFilesInDataset(ref_name)
                            if ref_guid in ref_files[0]:

                                if ref_name not in new_add_files:
                                    new_add_files[ref_name] = []

                                new_add_files[ref_name].append([ref_files[0][ref_guid]['lfn'], ref_guid])

                                # cache this dataset in case it's referenced elsewhere...
                                ref_lkup[ref_name] = ref_files

                    # store the additional files
                    str = ""
                    for d in new_add_files:
                        for f in new_add_files[d]:
                            str += d + ":" + f[0] + ":" + f[1] + '\n'
                        
                    open("add_files", "w").write(str)
                    print "New add_files:"
                    print str

        # Sort out datasets, create PFC and input.py #####################################
        # Get datasetnames
        new_datasetnames = []
        try:
            datasetnames = os.environ['DATASETNAME'].split(":")

            # add additional files to the poolfilecatalog:
            if os.access('add_files', os.R_OK):
                print "old datasetnames:"
                print datasetnames
                new_datasetnames = [ line.strip().split(':')[0] for line in file('add_files') ]
                for new_datasetname in new_datasetnames:
                    if not new_datasetname in datasetnames:
                        datasetnames.append(new_datasetname)
                print "new datasetnames:"
                print datasetnames
                
        except:
            print "DATASETNAME not defined !"
            if not datasettype == 'TIER3':
                sys.exit(EC_Configuration)
            else:
                datasetnames = []

        # Read input_files 
        lfns = []
        add_lfns = []
        if input:
            lfns = [ line.strip() for line in file(input) ]
        else:
            for line in file('input_files'):
                print "****   " + line
            lfns = [ line.strip() for line in file('input_files') ]
            
            # add additional files to the poolfilecatalog:
            if os.access('add_files', os.R_OK):
                print "Old lfns:"
                print lfns
                add_lfns = [ line.strip().split(':')[1] for line in file('add_files') ]
                lfns = lfns + add_lfns
                print "New lfns:"
                print lfns


        # Get rid of trailing numbers in filenames in LFC datasets
        for datasetname in datasetnames:
            if datasettype=='LFC' and datasetname.find('AOD')>0:
                i = 0;
                for lfn in lfns:
                    lfn_strip = ""
                    ds = (lfn.strip()).split('.')
                    if ds[len(ds)-1] != 'root':
                        lfn_strip = lfn[0:len(lfn)-len(ds[len(ds)-1])-1]
                    else:
                        lfn_strip = lfn
                    lfns[i] = lfn_strip
                    i = i + 1;

        # Get guids from input_guids
        guids = []
        add_guids = []
        if inputguid:
            guids = [ line.strip() for line in file(inputguid) ]
        else:
            guids = [ line.strip() for line in file('input_guids') ]
            
            # add additional files to the poolfilecatalog:
            if os.access('add_files', os.R_OK):
                print "Old guids:"
                print guids
                add_guids = [ line.strip().split(':')[2] for line in file('add_files') ]
                guids = guids + add_guids
                print "New guids:"
                print guids
                                                                            
        # Check if ESD backnavigation files are there
        lfns_esd = []
        guids_esd = []
        try:
            pfn = 'input_esd_files'
            open(pfn)
            lfns_esd = [ line.strip() for line in file('input_esd_files') ]
            lfns = lfns + lfns_esd

            pfn = 'input_esd_guids'
            open(pfn)
            guids_esd = [ line.strip() for line in file('input_esd_guids') ]
            guids = guids + guids_esd
        except IOError:
            print "Warning %s not found - no back navigation" % pfn
            pass

        # Inputfile dictionary
        ddmFileMap = {}
        for i in xrange(0,len(lfns)):
            ddmFileMap[lfns[i]] = guids[i]

        # Abort if no files are requested
        if not len(lfns):
            print 'No files requested.'
            sys.exit(EC_Configuration)
        
    # DQ2_DOWNLOAD or LFC ####################################################
    # DOWNLOAD INPUT FILES TO LOCAL WORKER NODE
    if datasettype in [ 'DQ2_DOWNLOAD', 'LFC', 'TNT_DOWNLOAD']:
        files = {}
        # create PoolFileCatalog.xml on the fly
        pfc = PoolFileCatalog()

        # Is python32 available
        cmd = 'which python32'
        pythoncmd = ''
        rc, out = commands.getstatusoutput(cmd)
        if (rc!=0):
            print 'No python32 found'
            pythoncmd = ''
        else:
            pythoncmd = out.strip()
        if 'pybin' in os.environ:
            pythoncmd = os.environ['pybin']

        # compose dq2 command
        for datasetname in datasetnames: 
            if ((datasettype == 'DQ2_DOWNLOAD') or (datasettype == 'TNT_DOWNLOAD')):
                #cmd = 'DQ2_LOCAL_SITE_ID= ; %s ./dq2_get -rcv -t %s %s ' % (pythoncmd,timeout,datasetname)
                #cmdretry = 'DQ2_LOCAL_SITE_ID= ; %s ./dq2_get -rcv -s BNL -t %s %s ' % (pythoncmd,timeout,datasetname)

                # resolve the pre-installed dq2-get path 
                dq2_get_path = None
                rc, out = getstatusoutput('which dq2-get')
                if rc != 0:
                    print 'ERROR: dq2-get command not found'
                    sys.exit(EC_DQ2GET)
                else:
                    dq2_get_path = out

                # compose dq2-get command 
                flist = ','.join(lfns) 
                #cmd = '%s %s -s %s -t %s -f %s %s' % (pythoncmd, dq2_get_path, os.environ['DQ2_LOCAL_SITE_ID'], timeout, flist, datasetname)
                #cmdretry = '%s %s -s %s -t %s -f %s %s' % (pythoncmd, dq2_get_path, 'CERN-PROD_DATADISK', timeout, flist, datasetname)
                print 'export %s=%s' % ('PYTHONPATH', os.environ['PYTHONPATH'])
                print 'export %s=%s' % ('LD_LIBRARY_PATH', os.environ['LD_LIBRARY_PATH'])
                for key in os.environ.keys():
                    if key.find('DQ2_') == 0:
                        print 'export %s=%s' % (key, os.environ[key])
                cmd = '%s %s --client-id=ganga -s %s -t %s -p lcg -D -H %s -f %s %s' % (pythoncmd, dq2_get_path, os.environ['DQ2_LOCAL_SITE_ID'], timeout, directory, flist, datasetname)
                cmdretry = '%s %s -s %s -t %s -p lcg -D -H %s -f %s %s' % (pythoncmd, dq2_get_path, 'CERN-PROD_DATADISK', timeout, directory, flist, datasetname)

            elif datasettype == 'LFC':
                cmd = '%s ./dq2_get_old -rlv -t %s %s ' % (pythoncmd,timeout,datasetname)
                cmdretry = '%s ./dq2_get_old -rv -s BNL -t %s %s ' % (pythoncmd,timeout,datasetname)
                for lfn in lfns:
                    cmd = cmd + lfn +" "
                    cmdretry = cmdretry + lfn +" "

            print 'INFO: dq2 get command: %s' % cmd
            print 'INFO: dq2 get retry command: %s' % cmdretry

            # execute dq2 command
            rc, out = getstatusoutput(cmd)
            print out
            if (rc!=0):
                print "ERROR: error during dq2-get occured"
                # retry dq2 command
                rc, out = getstatusoutput(cmdretry)
                print out
                if (rc!=0):
                    print "ERROR: error during retry of dq2-get from CERN (or BNL) occured"
                    sys.exit(EC_DQ2GET)

        # check if all files have been transfered
        pfnsnew = []
        for key, value in ddmFileMap.iteritems():
            name = os.path.basename(key)
            pfn = os.path.join(directory,name)
        # check if all files exists and if file size greater 0
            try:
                open(pfn)
                fsize = os.stat(pfn).st_size
            except IOError:
                print "ERROR %s not found" % name
                continue
            if (fsize>0):
                # add to PoolFileCatalog.xml
                pfc.addFile(value,name,pfn)
                pfnsnew.append(pfn)
                # append for input.py 
                item = {'pfn':pfn,'guid':value}
                files[key] = item

        pfc.close()
        # Create input.py
        if datasettype == 'TNT_DOWNLOAD':
             # make PoolFileCatalog
             _makePoolFileCatalog(files)
        else:
             _makeJobO(files, version=atlas_release_major, dtype=datatype)

        if len(pfnsnew)>0:
            returnvalue=0
        else:
            print 'ERROR: Datasets %s are empty at %s' %(datasetnames, localsiteid)
            returnvalue=EC_QueryFiles


    # DQ2_LOCAL or TAG ######################################################

    if datasettype in [ 'DQ2_LOCAL', 'TAG', 'TAG_REC', 'TNT_LOCAL', 'TIER3' ]:

        if globalVerbose:
            print ddmFileMap
            print defaultSE

        dq2tracertime.append(time.time())
        # get list of files from LFC
        if datasettype == 'TIER3':
            sUrlMap = ddmFileMap
            tUrlMap = ddmFileMap
            fsizeMap = {} 
            md5sumMap = {} 
        else:
            sUrlMap, tUrlMap, fsizeMap, md5sumMap = _getPFNsLFC(ddmFileMap, defaultSE, localsitesrm)

        # NIKHEF/SARA special case
        if len(tUrlMap)==0 and (os.environ[ 'DQ2_LOCAL_SITE_ID' ].startswith('NIKHEF') or os.environ[ 'DQ2_LOCAL_SITE_ID' ].startswith('SARA')):
            print 'Special setup at NIKHEF/SARA - re-reading LFC'
            if os.environ[ 'DQ2_LOCAL_SITE_ID' ].startswith('NIKHEF') or os.environ[ 'DQ2_LOCAL_SITE_ID' ].startswith('SARA'):
                localsitesrm = TiersOfATLAS.getSiteProperty('SARA-MATRIX_MCDISK','srm')
                configLOCALPROTOCOL = 'gsidcap'
                configLOCALPREFIX = 'gsidcap:'

            localsitesrm = re.sub('token:*\w*:','', localsitesrm)
            localsitesrm = re.sub(':*\d*/srm/managerv2\?SFN=','', localsitesrm)
            defaultSE = _getDefaultStorage(localsitesrm)
            sUrlMap, tUrlMap, fsizeMap, md5sumMap = _getPFNsLFC(ddmFileMap, defaultSE, localsitesrm)

        dq2tracertime.append(time.time())
        
        # Check md5sum
        if len(tUrlMap)>0 and 'GANGA_CHECKMD5SUM' in os.environ and os.environ['GANGA_CHECKMD5SUM']=='1':
            tUrlMapTemp = tUrlMap
            for lfn, turl in tUrlMap.iteritems():

                if md5sumMap[lfn] == '': continue
                
                if turl.startswith('dcap'):
                    cmd = 'dccp '
                elif turl.startswith('rfio'):
                    cmd = 'rfcp '
                else:
                    break

                md5sum = ''
                cmd = cmd + turl + ' - | md5sum'
                rc, out = commands.getstatusoutput(cmd)
                if rc == 0:
                    for line in out.split("\n"):
                        match = re.search('^(\S*)  -',line)
                        if match:
                            md5sum = match.group(1)
                            if globalVerbose:
                                print lfn, md5sumMap[lfn], md5sum
                            if md5sum != 'd41d8cd98f00b204e9800998ecf8427e' and md5sumMap[lfn] != md5sum:
                                print 'ERROR: %s has wrong md5sum - local: %s, in LFC: %s - removing from input file list' %(turl, md5sum, md5sumMap[lfn])
                                del tUrlMapTemp[lfn]
            
            tUrlMap = tUrlMapTemp

        if globalVerbose:
            print sUrlMap
            print tUrlMap
            print fsizeMap
            print md5sumMap
        
        # append protocol
        if not datasettype == 'TIER3':
            _appendProtocol(sUrlMap,'gfal')
            if globalVerbose:
                print sUrlMap

        # Use GFAL or TURL ?
        if configLOCALPROTOCOL == 'gfal':
            dirPfnMap = sUrlMap
        else:
            dirPfnMap = tUrlMap
            
        # gather LFN,GUID,PFN
        files = {}
        doConfig = True
        name = ''
        for lfn in dirPfnMap.keys():
            # get GUID
            if lfn in ddmFileMap.keys():
                guid = ddmFileMap[lfn]
            else:
                # check generic LFN
                genLFN = re.sub('\.\d+$','',lfn)
                if genLFN in ddmFileMap.keys():
                    guid = ddmFileMap[genLFN]
                else:
                    print "WARNING: %s is not found in %s : ignored" % (lfn,datasetnames)
                    continue
            # get PFN
            pfn = dirPfnMap[lfn]
            surl = sUrlMap[lfn]

            # change the PFN for TAG-referenced files
            if 'TAG_TYPE' in os.environ and add_lfns:
                if lfn in add_lfns:
                    if (configSETYPE == 'dpm'):
                        # remove protocol and host
                        pfn = re.sub('^gfal:','',surl)
                        pfn = re.sub('^[^:]+://[^/]+','',pfn)
                        # remove redundant /
                        pfn = re.sub('^//','/',pfn)
                        pfn = "rfio:" + pfn
                    
            # append
            item = {'pfn':pfn, 'guid':guid, 'surl':surl}
            files[lfn] = item

        if globalVerbose:
            print files

        # make PoolFileCatalog
        if not datasettype == 'TIER3':
            _makePoolFileCatalog(files)

        # make jobO if not already done so (e.g. for local TAG files)
        if datasettype in [ 'DQ2_LOCAL', 'TIER3']:

            # Remove ESD files
            if lfns_esd:
                for lfn in lfns_esd:
                    if lfn in files.keys():
                        files.pop(lfn)

            # remove any tag-referenced files (not the tag files themselves
            tag_flag=False
            if 'TAG_TYPE' in os.environ:
                
                if add_lfns:
                    tag_flag = True
                    for lfn in add_lfns:
                        if lfn in files.keys():
                            files.pop(lfn)

                files = tag_files
                tag_flag = True

            # Configure prependJobO for AutoConfiguration and InputFilePeeker
            prependJobO = True
            #if os.environ.has_key('ATHENA_OPTIONS'):
            #    joboptions = os.environ['ATHENA_OPTIONS'].split(' ')
            #    for jfile in joboptions:
            #        try:
            #            inFile = open(jfile,'r')
            #            # scan jobOptions for AutoConfiguration and InputFilePeeker
            #            allLines = inFile.readlines()
            #            for line in allLines:
            #                if line.find("InputFilePeeker")>0 or line.find("AutoConfiguration")>0 or line.find("RecExCommon/")>0:
            #                    prependJobO = True
            #                    break
            #        
            #        except:
            #            pass

            print 'prependJobO = %s ' %prependJobO
            _makeJobO(files, tag=tag_flag, version=atlas_release_major, dtype=datatype, usePrependJobO = prependJobO)

        if len(files)>0:
            returnvalue=0
        else:
            if datasettype == 'TIER3':
                print 'ERROR: Dataset is/are empty' 
            else:
                print 'ERROR: Dataset(s) %s is/are empty at %s' %(datasetnames, localsiteid)
                returnvalue=EC_QueryFiles

        dq2tracertime.append(time.time())
        outFile = open('dq2tracertimes.txt','w')
        for itime in dq2tracertime:
            outFile.write('%s\n' % itime)
        outFile.close()


    ##########################################################################
    # DQ2_OUT
    # DQ2 output registration
    
    if datasettype=='DQ2_OUT':

        # extract list of files from PoolFileCatalog
        cataloglfns, catalogpfns, catalogguids = _getFNsPFC('PoolFileCatalog.xml')
        catalogguidMap = {}

        for i in xrange(0,len(cataloglfns)):
            catalogguidMap[cataloglfns[i]] = catalogguids[i]

        # Read output_files and check if it exists - if not revert to files of unparsed jobOptions
        output_files = []
        output_files_new = []
        renameflag = False
        if output:
            output_files_new = [ line.strip() for line in file(output) ]
            output_files_orig = [ line.strip() for line in file('output_files') ]
        else:
            output_files = args

        dir = os.listdir('.')
        for i in xrange(0,len(output_files_new)):
            try:
                open(output_files_new[i],'r')
                
                if not output_files_new[i] in output_files:
                    output_files.append(output_files_new[i])
                    
                # Fail over if Athena has produced more than one outputfile due to file size limit 
                filepat = re.sub('\.(\w+)$','', output_files_new[i])
                pat = re.compile(filepat)
                for altfile in dir:
                    found = re.findall(pat, altfile)
                    if found and not altfile in output_files:
                        output_files.append(altfile)

            except IOError:
                try:
                    open(output_files_orig[i],'r')
                    output_files.append(output_files_orig[i])
                    renameflag = True
                except IOError:
                    raise NameError("ERROR: problems in output stage-out. Could not read output file: '%s'" % output_files_orig[i])
                    sys.exit(EC_STAGEOUT)

        if len(output_files)==0:
            print 'ERROR: no output files existing to stage out.'
            sys.exit(EC_STAGEOUT)

        # Get datasetname
        try:
            datasetname = os.environ['OUTPUT_DATASETNAME']
        except:
            raise NameError("ERROR: OUTPUT_DATASETNAME not defined")
            sys.exit(EC_Configuration)

        if not len(output_files):
            print 'No files requested.'
            sys.exit(EC_Configuration)

        # Set siteID
        siteID = os.environ[ 'DQ2_LOCAL_SITE_ID' ]
        #if siteID in [ 'PIC', 'IFIC', 'CNAF', 'RAL', 'SARA', 'FZK', 'ASGC', 'LYON', 'TRIUMF', 'TIER0' ]:
        #    if re.search('DISK$',siteID) == None:
        #        siteID += 'DISK'

        #elif siteID in [ 'CERN', 'TIER0DISK', 'TIER0TAPE', 'CERNPROD']:
        #    siteID = 'CERN'

        # Get output location
        output_locations = { }
        temp_locations = [ ]

        # Get backup output locations
        try:
            backup_locations = os.environ['DQ2_BACKUP_OUTPUT_LOCATIONS'].split(":")
        except:
            print "ERROR : DQ2_BACKUP_OUTPUT_LOCATIONS not defined"
            backup_locations = []
            pass

        try:
            temp_locations = os.environ['OUTPUT_LOCATION'].split(':')
            #if temp_locations==['']:
            #    temp_locations = [ siteID ]
            
        except:
            print "ERROR: OUTPUT_LOCATION not defined or empty srm value"
            print "Using DQ2_BACKUP_OUTPUT_LOCATIONS" 
            temp_locations = [ ]


        # Set siteID to site_SCRATCHDISK
        dq2alternatename = TiersOfATLAS.getSiteProperty(siteID,'alternateName')
        for sitename in TiersOfATLAS.getAllSources():
            if TiersOfATLAS.getSiteProperty(sitename,'alternateName'):
                if TiersOfATLAS.getSiteProperty(sitename,'alternateName')==dq2alternatename and (TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:ATLASSCRATCHDISK') or TiersOfATLAS.getSiteProperty(sitename,'srm').startswith('token:T2ATLASSCRATCHDISK')): 
                    siteID = sitename
                    break

        # Find close backup locations
        close_backup_locations = []
        for sitename in TiersOfATLAS.getCloseSites(siteID):
            if TiersOfATLAS.getSiteProperty(sitename,'domain').find('atlasscratchdisk')>0 and sitename.find('SCRATCHDISK')>0: 
                close_backup_locations.append( sitename )

        # Compile stage out SE sequence 
        temp_locations = temp_locations + [ siteID ] + close_backup_locations + backup_locations

        if 'CERN' in temp_locations:
            temp_locations.remove('CERN')

        print 'Unchecked output locations: %s'  %(temp_locations)
        new_locations = []

        # Get space token names:
        try:
            space_token_names = os.environ['DQ2_OUTPUT_SPACE_TOKENS'].split(":")
        except:
            print "ERROR : DQ2_OUTPUT_SPACE_TOKENS not defined"
            space_token_names = [ 'ATLASSCRATCHDISK', 'ATLASLOCALGROUPDISK' ]
            pass

        if not 'ATLASSCRATCHDISK' in space_token_names or \
           not 'T2ATLASSCRATCHDISK' in space_token_names or \
           not 'ATLASLOCALGROUPDISK' in space_token_names or \
           not 'T2ATLASLOCALGROUPDISK' in space_token_names: 
            print "ERROR : DQ2_OUTPUT_SPACE_TOKENS not well defined"
            print "Adding ['ATLASSCRATCHDISK', 'ATLASLOCALGROUPDISK', 'T2ATLASSCRATCHDISK', 'T2ATLASLOCALGROUPDISK']"
            space_token_names = space_token_names + ['ATLASSCRATCHDISK', 'ATLASLOCALGROUPDISK', 'T2ATLASSCRATCHDISK', 'T2ATLASLOCALGROUPDISK']

        for ilocation in temp_locations:
            temp_location = ilocation

             # Find LFC Catalog host 
            location_info = { }
            lfccat = TiersOfATLAS.getRemoteCatalogs(temp_location)
            if lfccat:
                temp_lfc_host = re.sub('[/:]',' ',lfccat[0]).split()[1]
                temp_lfc_home = lfccat[0].split(':')[2]
                if not temp_lfc_home.endswith('dq2/'):
                    temp_lfc_home = os.path.join(temp_lfc_home, 'dq2/') 
                temp_srm = TiersOfATLAS.getSiteProperty(temp_location,'srm')
                if not temp_srm:
                    continue
                # Determine token name
                pat = re.compile(r'^token:([^:]+)')
                name = re.findall(pat, temp_srm)
                if name:
                    tokenname = name[0]
                    if not tokenname in space_token_names:
                        continue
                else:
                    tokenname = ''
                # Remove token info
                temp_srm = re.sub('token:*\w*:','', temp_srm)
                temp_srm = re.sub(':*\d*/srm/managerv2\?SFN=','', temp_srm)
                # 
                location_info['srm'] = temp_srm
                location_info['lfc_host'] = temp_lfc_host
                location_info['lfc_home'] = temp_lfc_home
                location_info['defaultSE'] = _getDefaultStorage(temp_srm)
                location_info['token'] = tokenname
                output_locations[temp_location] = location_info 
                new_locations.append(temp_location)

        temp_locations = new_locations
        print 'Checked output locations: %s'  %(temp_locations)

        # Get output lfn
        try:
            output_lfn = os.environ['OUTPUT_LFN']
        except:
            raise NameError("ERROR: OUTPUT_LFN not defined")
            sys.exit(EC_Configuration)

        # Get output jobid
        try:
            output_jobid = os.environ['OUTPUT_JOBID']
        except:
            raise NameError("ERROR: OUTPUT_JOBID not defined")
            sys.exit(EC_Configuration)

        try:
            output_number = int(os.environ['OUTPUT_FILE_NUMBER'])
        except:
            i=output_jobid.split('.')
            if len(i)>1:
               output_number = int(i[1])+1
            else:
               output_number = 1

        try:
            use_short_filename = os.environ['GANGA_SHORTFILENAME']
        except:
            raise NameError("ERROR: GANGA_SHORTFILENAME not defined")
            sys.exit(EC_STAGEOUT)
    
        guids = []
        sizes = []
        md5sums = [] 
        dq2lfns = []

        f_data = open('output_data','a')

        # loop to save all output_files
        for file in output_files:
            if renameflag:
                # Rename file
                temptime = time.gmtime()
                output_datasetname = re.sub('\.[\d]+$','',datasetname)
                pattern=output_datasetname+".%04d%02d%02d%02d%02d%02d._%05d."+file

                new_output_file = pattern % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],output_number)
                short_pattern = ".%04d%02d%02d%02d%02d%02d._%05d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5], output_number)
    
                new_short_output_file = re.sub(".root", short_pattern+".root" , file )
                if new_short_output_file == file:
                    new_short_output_file =  short_pattern[1:] + "." + file
                
                #filenew = file+"."+output_jobid
                if use_short_filename:
                    filenew = new_short_output_file
                else:
                    filenew = new_output_file

                filenamenew = os.path.join(os.environ['PWD'], filenew)
                filenameorig = os.path.join(os.environ['PWD'],file)
                os.rename(filenameorig, filenamenew)
                file = filenew

            # Number of retries
            retry = 3
            count = 1
            siteCount = 0 
            # file storage loop
            while count<=retry: 
                siteID = temp_locations[siteCount]
                output_srm = output_locations[siteID]['srm']
                output_lfc_home = output_locations[siteID]['lfc_home']
                output_lfc_host = output_locations[siteID]['lfc_host']
                output_tokenname = output_locations[siteID]['token']
                os.environ[ 'LFC_HOST' ] = output_lfc_host
                os.environ[ 'DQ2_LFC_HOME' ] = output_lfc_home
                print 'LFC_HOST: %s' %os.environ[ 'LFC_HOST' ]
                print 'DQ2_LFC_HOME: %s' %os.environ[ 'DQ2_LFC_HOME' ]
                print 'Storing file %s at: %s' %(file, siteID)

                # Create output info
                dest = os.path.join(output_srm, output_lfn, file)
                gridlfn = os.path.join(output_lfc_home, output_lfn, file)
                griddir = os.path.join(output_lfc_home, output_lfn)
                filename = os.path.join(os.environ['PWD'], file)
                poolguid = ''

                try:
                    poolguid = catalogguidMap[file]
                except KeyError:
                    print 'poolguid not found in PoolFileCatalog.xml'
                    print 'Trying pool_extractFileIdentifier.py ...'
                    poolextr = """#!/bin/bash

if [ n$GANGA_ATHENA_WRAPPER_MODE = n'grid' ]; then
    ATLAS_RELEASE_DIR=$VO_ATLAS_SW_DIR/software/$ATLAS_RELEASE
elif [ n$GANGA_ATHENA_WRAPPER_MODE = n'local' ]; then
    ATLAS_RELEASE_DIR=$ATLAS_SOFTWARE/$ATLAS_RELEASE
fi
                            
if [ ! -z `echo $ATLAS_RELEASE | grep 16.` ]; then
    if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]; then
        source $ATLAS_RELEASE_DIR/cmtsite/asetup.sh $ATLAS_PRODUCTION,$ATLAS_PROJECT,32,setup
    elif [ ! -z $ATLAS_PROJECT ]; then
        source $ATLAS_RELEASE_DIR/cmtsite/asetup.sh $ATLAS_RELEASE,$ATLAS_PROJECT,32,setup
    else
        source $ATLAS_RELEASE_DIR/cmtsite/asetup.sh AtlasOffline,$ATLAS_RELEASE,32,setup
    fi
else
    if [ ! -z $ATLAS_PROJECT ] && [ ! -z $ATLAS_PRODUCTION ]; then
        source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_PRODUCTION,$ATLAS_PROJECT,32,setup
    elif [ ! -z $ATLAS_PROJECT ]; then
        source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=$ATLAS_RELEASE,$ATLAS_PROJECT,32,setup
    else
        source $ATLAS_RELEASE_DIR/cmtsite/setup.sh -tag=AtlasOffline,$ATLAS_RELEASE,32,setup
    fi
fi
# extract GUID
pool_extractFileIdentifier.py %(filename)s 
""" % { 'filename' : file }

                    outFile = open('poolextr.sh','w')
                    outFile.write(poolextr)
                    outFile.close()

                    rc, out = commands.getstatusoutput('chmod +x poolextr.sh; ./poolextr.sh')
                    if rc == 0:
                        for line in out.split():
                            match = re.search('^([\w]+-[\w]+-[\w]+-[\w]+-[\w]+)',line)
                            if match:
                                poolguid = match.group(1)
                    else:
                        print 'No poolguid stored in file or extraction failed.'

                print 'poolguid: %s' %poolguid

                guid, size, md5sum = save_file(count, griddir, dest, gridlfn, output_lfn, filename, poolguid, siteID, output_tokenname)
                if guid!=-1:
                    dq2lfns.append(file)
                    guids.append(guid)
                    sizes.append(size)
                    md5sums.append(md5sum)
                    break
                else:
                    print 'ERROR: file not saved to %s in attempt number %s ...' %(siteID, count)
                    if count == retry:
                        if siteCount<len(temp_locations):
                            siteCount = siteCount + 1
                            count = 0
                            print 'ERROR: file not saved to %s - using now %s ...' %(siteID, temp_locations[siteCount] )
                        else:
                            count = retry
                            
                    count = count + 1
                    time.sleep(120)

            if siteCount==len(temp_locations)-1:
                print 'ERROR: file not saved to any location ...' 
                sys.exit(EC_STAGEOUT)
            
            # Write output_data
            i = 1
            state = 0
            if not datasetname.endswith(siteID):
                out_datasetname = "%s.%s" %(datasetname, siteID)
            else:
                out_datasetname = datasetname
            while i>0 or state!=-1:
                state = dataset_exists(out_datasetname, siteID)
                if state == 2:
                    out_datasetname = "%s.%s.%s" %(datasetname, siteID, i)
                    i = i + 1
                else:
                    break

            f_data_string = '%s,%s,%s,%s,%s,%s' % (out_datasetname, file, guid, size, md5sum, siteID) 
            print >>f_data, f_data_string 

        f_data.close()

        f2 = open('output_location','w')
        print >>f2, siteID 
        f2.close()

        # Register all output file details in DQ2
        f=open("output_data")
        outputInfo = []
        for line in f.readlines():
            outputInfo.append( line.strip() )
        f.close()

        
        register_datasets_details(out_datasetname, outputInfo)

        register_datasets_in_container(datasetname, out_datasetname)

        check_duplicates_in_dataset(out_datasetname, output_files)


    # before we exit, store the return code in case we seg fault...
    open("dq2_retcode.tmp", "w").write( "%d" % returnvalue)
    sys.exit(returnvalue)

