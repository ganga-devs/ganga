#! /usr/bin/env python
###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ganga-stage-in-out-dq2.py,v 1.15 2008-09-29 19:41:14 elmsheus Exp $
###############################################################################
# DQ2 dataset download and PoolFileCatalog.xml generation

import os, sys, imp, re, time, commands, signal, popen2, socket, urllib
import md5, string
import xml.dom.minidom
from dq2.info import TiersOfATLAS 
from dq2.info.TiersOfATLAS import _refreshToACache, ToACache

from getopt import getopt,GetoptError
from commands import getstatusoutput

import lfc

_refreshToACache()

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
    sys.exit(EC_Configuration)
#try:
#    # local site ID
#    DQ2LOCALSITEID = os.environ['DQ2_LOCAL_SITE_ID']
#except:
#    print "ERROR : DQ2_LOCAL_SITE_ID is not defined"
#    sys.exit(EC_Configuration)
try:
    # local access protocol
    configLOCALPROTOCOL = os.environ['DQ2_LOCAL_PROTOCOL']
except:
    configLOCALPROTOCOL = 'rfio'
try:
    # root directory of storage
    configSTORAGEROOT = os.environ['DQ2_STORAGE_ROOT']
except:
    if configLOCALPROTOCOL == 'rfio':
        configSTORAGEROOT = '/castor'
    elif configLOCALPROTOCOL == 'dcap':
        configSTORAGEROOT = '/pnfs'
    else:
        configSTORAGEROOT = ''
try:
    # prefix for local access
    configLOCALPREFIX = os.environ['DQ2_LOCAL_PREFIX']
except:
    configLOCALPREFIX = ''

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
    
    print 'defaultSE: %s' %defaultSE

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

                if (defaultSE and host in defaultSE) or \
                       rep.sfn.startswith(localsitesrm):

                    surl = rep.sfn

                    lfn = mapLFN[rep.guid]
                    guidReplicas[lfn] = surl

                    fsizeMap[lfn] = long(rep.filesize)
                    md5sumMap[lfn] = rep.csumvalue
                    
                    # TURL
                    match = re.search('^[^:]+://([^:/]+):*\d*/', surl)
                    sURLHost = match.group(1)
                    turl = []    
                    if configLOCALPROTOCOL!='gfal' \
                           and not stUrlMap.has_key(sURLHost) \
                           and configLOCALPROTOCOL!='rfio' \
                           and configLOCALPROTOCOL!='file' \
                           and not 'ccsrm.in2p3.fr' in defaultSE:

                        print 'Using lcg-gt for turl retrieval ...'
                        cmd = "lcg-gt -t 60 " + surl + " " + configLOCALPROTOCOL
                        print cmd
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
                            signal.alarm(0)
                        except IOError:
                            pass
                        signal.alarm(0)

                        if turl and turl[0]:
                            match = re.search('^[^:]+://([^:/]+:*\d*)/', turl[0])
                            tURLHost = match.group(1)
                            stUrlMap[sURLHost] = tURLHost
                                        
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
                    guidReplicas[lfn] = surl

                    res = lfc.lfc_statg("",guid,stat)
                    fsizeMap[lfn] = long(stat.filesize)
                    md5sumMap[lfn] = stat.csumvalue

                    # TURL
                    match = re.search('^[^:]+://([^:/]+):*\d*/', surl)
                    sURLHost = match.group(1)
                    turl = []
                    if configLOCALPROTOCOL!='gfal' \
                           and not stUrlMap.has_key(sURLHost) \
                           and configLOCALPROTOCOL!='rfio' \
                           and configLOCALPROTOCOL!='file' \
                           and not 'ccsrm.in2p3.fr' in defaultSE:
                        print 'Using lcg-gt for turl retrieval ...'
                        cmd = "lcg-gt -t 60 " + surl + " " + configLOCALPROTOCOL
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
                            signal.alarm(0)
                        except IOError:
                            pass
                        signal.alarm(0)

                        if turl and turl[0]:
                            match = re.search('^[^:]+://([^:/]+:*\d*)/', turl[0])
                            tURLHost = match.group(1)
                            stUrlMap[sURLHost] = tURLHost

        try:
            lfc.lfc_endsess()
        except NameError:
            pass

    # Create TURL map
    tUrlMap = {}
    for lfn, surl in guidReplicas.iteritems():
        if configLOCALPROTOCOL in [ "dcap", 'Xrootd', 'gsidcap' ]:
            match = re.search('^[^:]+://([^:/]+):*\d*/', surl)
            sURLHost = match.group(1)
            if stUrlMap.has_key(sURLHost):
                pfn = re.sub(sURLHost,stUrlMap[sURLHost],surl)
            else:
                if not 'ccsrm.in2p3.fr' in defaultSE:
                    pfn = 'gfal:'+surl
                else:
                    pfn = surl
                
            if configLOCALPROTOCOL == "dcap" and (stUrlMap.has_key(sURLHost) or 'ccsrm.in2p3.fr' in defaultSE):
                
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
                    
            elif configLOCALPROTOCOL == "Xrootd":
                pfn = re.sub('srm://','root://',pfn)
                # Hack for ccin2p3
                pfn = re.sub('ccsrm','ccxroot',pfn)
                pfn = re.sub('ccdcamli01','ccxroot',pfn)
                pfn = re.sub(':1094',':1094/',pfn)

            elif configLOCALPROTOCOL == "gsidcap":
                pfn = re.sub('srm://','gfal:gsidcap://',pfn)
                pfn = re.sub('22128/pnfs','22128//pnfs',pfn)

        elif configLOCALPROTOCOL == "rfio" and configSTORAGEROOT == '/castor' \
                 and not sURLHost == 'castorsc.grid.sinica.edu.tw':
            # remove protocol and host
            pfn = re.sub('^[^:]+://[^/]+','',surl)
            # remove redundant /
            pfn = re.sub('^//','/',pfn)
            if 'srm.grid.sinica.edu.tw' in defaultSE:
                pfn = "rfio://castor.grid.sinica.edu.tw/?path=" + pfn
            else:
                pfn = "rfio:" + pfn
        elif ( configLOCALPROTOCOL == "rfio" and \
                 ( configSTORAGEROOT == '/dpm' or sURLHost == 'castorsc.grid.sinica.edu.tw')) \
                 or ( configLOCALPROTOCOL == "file" ):
            turl = []
            print 'Using lcg-gt for turl retrieval ...'
            cmd = "lcg-gt -t 60 " + surl + " " + configLOCALPROTOCOL
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
                signal.alarm(0)
            except IOError:
                pass
            signal.alarm(0)

            if turl and turl[0]:
                pfn = turl[0]
            elif 'storm-fe.cr.cnaf.infn.it' in defaultSE:
                pfn = re.sub('srm://storm-fe.cr.cnaf.infn.it/','file:///storage/gpfs_storm/',surl)
            else:
                # If CNAF TURL fails
                if 'storm-fe.cr.cnaf.infn.it' in defaultSE:
                    pfn = re.sub('srm://storm-fe.cr.cnaf.infn.it/','file:///storage/gpfs_storm/',surl)
                else:    
                    # remove protocol and host
                    pfn = re.sub('^[^:]+://[^/]+','',surl)
                    # remove redundant /
                    pfn = re.sub('^//','/',pfn)
                    pfn = "rfio:" + pfn
        # IFAE is a classic SE
        # IFICDISK is lustre but adveritzes rfio
        elif 'ifaese01.pic.es' in defaultSE or 'ifaesrm.pic.es' in defaultSE or 'lsrm.ific.uv.es' in defaultSE:
            # remove protocol and host
            pfn = re.sub('^[^:]+://[^/]+','',surl)
            # remove redundant /
            pfn = re.sub('^//','/',pfn)
        # If all fails use gfal:srm://...
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
# make job option file
def _makeJobO(files, tag=False, type='TAG', version=12, dtype='MC'):
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
    if tag:
        if type == 'TAG_REC':
            if version >= 13:
                outFile.write('PoolTAGInput = [')
            else:
                outFile.write('CollInput = [')
        else:
            outFile.write('%sEventSelector.CollectionType="ExplicitROOT"\n'%versionString)
            outFile.write('%sEventSelector.InputCollections = ['%versionString)
    else:
        try:
            if os.environ.has_key('ATHENA_MAX_EVENTS'):
                evtmax = int(os.environ['ATHENA_MAX_EVENTS'])
            else:
                evtmax = -1
        except:
            evtmax = -1
        outFile.write('theApp.EvtMax = %d\n' %evtmax)
        if dtype == 'DATA':
            outFile.write('ByteStreamInputSvc.FullFileName = [')
        elif dtype == 'MC':
            outFile.write('%sEventSelector.InputCollections = ['%versionString)
        elif dtype == 'MuonCalibStream':
            outFile.write('svcMgr.MuonCalibStreamFileInputSvc.InputFiles = [')
        else:
            outFile.write('%sEventSelector.InputCollections = ['%versionString)
            
    # loop over all files
    flatFile = 'input.txt'
    outFlatFile = open(flatFile,'w')
    
    for lfn in lfns:
        filename = files[lfn]['pfn']
        if tag:
            if atlas_release_major <= 12:
                filename = re.sub('\.root\.\d+$','',filename)
                filename = re.sub('\.root$','',filename)
            else:
                filename = re.sub('root\.\d+$','root',filename)
        # write PFN
        outFile.write('"%s",' % filename)
        outFlatFile.write('%s\n' %filename)
        
    outFile.write(']\n')
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


def getLocalFileMetadata(file):
    # check file exists
    if not os.access(file,os.R_OK):
        return -1,-1
    size=os.stat(file)[6]
    # get md5sum
    m = md5.new()
    md5sum = hexify(m.digest())
    mf = open(file, 'r')
    for line in mf.readlines():
        m.update(line)
        mf.close()
        md5sum=hexify(m.digest())
        
    return size,md5sum

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

    # Create file replica
    #cmd = "lcg-cr --vo atlas -t 300 -d %s -l %s -P %s file://%s" %(dest, gridlfn, output_lfn, filename)
    if tokenname:
        cmd = "lcg-cr --vo atlas -s %s " %tokenname
    else:
        cmd = "lcg-cr --vo atlas "
    if poolguid != '':
        cmd = cmd + " -t %s -d %s -g %s -l %s file://%s" %(timeout, dest, poolguid, gridlfn, filename)
    else:
        cmd = cmd + " -t %s -d %s -l %s file://%s" %(timeout, dest, gridlfn, filename)
    rc, out = commands.getstatusoutput(cmd)
    if rc == 0:
        # Open output_guids to transfer guids back to GANGA
        f = open('output_guids','a')
        print >>f, '%s,%s' %(out,siteID)
        f.close()
        if globalVerbose:
            print cmd
            print out
        guid = re.sub('^guid:','',out)
    else:
        print 'ERROR during execution of %s' %cmd
        print rc, out
        return -1, -1, -1

    # size and md5sum
    size, md5sum = getLocalFileMetadata(filename)
    
    return guid, size, md5sum

########################################################################

def findsetype(sitesrm):
    setype= 'NULL'

    if sitesrm.find('castor')>=0:
        setype = 'CASTOR'
    elif sitesrm.find('dpm')>=0:
        setype = 'DPM'
    elif sitesrm.find('pnfs')>=0:
        setype = 'DCACHE'
    elif sitesrm.find('/nfs/')>=0:
        setype = 'NFS'
    return setype


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
        if not detsetype:
            print "ERROR : ATLAS_RELEASE not defined, using 12"
        atlas_release = '12.0.6'
        pass

    atlas_release_major = int(atlas_release.split('.')[0])

    # Determine dataset type
    try:
        datasettype = os.environ['DATASETTYPE']
    except:
        if not detsetype:
            print "ERROR : DATASETTYPE not defined, using LFC"
        datasettype = 'LFC'
        pass

    # use DQ2_LOCAL as default
    if not datasettype in [ 'DQ2_DOWNLOAD', 'DQ2_LOCAL', 'LFC', 'TAG', 'TAG_REC', 'DQ2_OUT', 'TNT_LOCAL', 'TNT_DOWNLOAD' ]:
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
        sys.exit(EC_Configuration)
    try:
        dq2urlserverssl = os.environ['DQ2_URL_SERVER_SSL']
    except:
        if not detsetype:        
            print "ERROR: Environment variable DQ2_URL_SERVER_SSL not set"
        sys.exit(EC_Configuration)

    # Set default DQ2 environment variables
    os.environ[ 'DQ2_URL_SERVER' ] = dq2urlserver
    os.environ[ 'DQ2_URL_SERVER_SSL'] = dq2urlserverssl
    #os.environ[ 'DQ2_LOCAL_SITE_ID' ] = ''
    #os.environ[ 'DQ2_LOCAL_PROTOCOL' ] = 'dcap'
    os.environ[ 'DQ2_STORAGE_ROOT' ] = ''
    os.environ[ 'DQ2_SRM_HOST' ] = ''
    os.environ[ 'DQ2_GSIFTP_HOST' ] = ''
    os.environ[ 'LCG_CATALOG_TYPE' ] = 'lfc'
    os.environ[ 'DQ2_LFC_HOME' ] = '/grid/atlas'
    os.environ[ 'DQ2_LOCAL_PREFIX' ] = ''

    if datasettype in [ 'DQ2_DOWNLOAD', 'DQ2_LOCAL', 'TAG', 'TAG_REC', 'DQ2_OUT', 'TNT_LOCAL', 'TNT_DOWNLOAD' ]:
        # Determine local domainname
        hostname = None
        domainname = None
        lcgcename = ''

        # test print out
        if globalVerbose:
            print '----------'
            print os.environ
            print '----------'
            if os.environ.has_key('EDG_WL_RB_BROKERINFO'):
                try:
                    f = open(os.environ['EDG_WL_RB_BROKERINFO'], "r")
                    lines = f.readlines()
                    for line in lines:
                        print line
                except:
                    pass
            if os.environ.has_key('GLITE_WMS_RB_BROKERINFO'):
                try:
                    f = open(os.environ['GLITE_WMS_RB_BROKERINFO'], "r")
                    lines = f.readlines()
                    for line in lines:
                        print line
                except:
                    pass

            if os.environ.has_key('GANGA_LCG_CE'):
                print os.environ['GANGA_LCG_CE']
            if os.environ.has_key('VO_ATLAS_DEFAULT_SE'):
                print os.environ['VO_ATLAS_DEFAULT_SE']
            print socket.gethostbyaddr(socket.gethostname())
            print '----------'

        # First choice: EDG_WL_RB_BROKERINFO or GLITE_WMS_RB_BROKERINFO
        if os.environ.has_key('EDG_WL_RB_BROKERINFO'):
            try:
                f = open(os.environ['EDG_WL_RB_BROKERINFO'], "r")
                lines = f.readlines()
                for line in lines:
                    match = re.search('name = "(\S*):2119', line)
                    if match:
                        hostname =  [ match.group(1) ]
            except:
                pass

        if os.environ.has_key('GLITE_WMS_RB_BROKERINFO'):
            try:
                f = open(os.environ['GLITE_WMS_RB_BROKERINFO'], "r")
                lines = f.readlines()
                for line in lines:
                    match = re.search('name = "(\S*):2119', line)
                    if match:
                        hostname =  [ match.group(1) ]
            except:
                pass

        # Second choice: GANGA_LCG_CE
        if not hostname and os.environ.has_key('GANGA_LCG_CE'):
            try:
                hostname = re.findall('(\S*):2119',os.environ['GANGA_LCG_CE'])
                lcgcename = re.findall('(\S*):2119',os.environ['GANGA_LCG_CE'])[0]
                #print hostname, lcgcename
            except:
                pass

        # Third choice: VO_ATLAS_DEFAULT_SE
        if not hostname and os.environ.has_key('VO_ATLAS_DEFAULT_SE'):
            hostname = os.environ['VO_ATLAS_DEFAULT_SE']
            if hostname.find('grid.sara.nl')>=0: hostname = ''
                
        # Fourth choice: local hostname
        if not hostname:
            hostname = socket.gethostbyaddr(socket.gethostname())

        if hostname.__class__.__name__=='list' or hostname.__class__.__name__=='tuple':
            hostname = hostname[0]

        domainname = re.sub('^[\w\-]+\.','',hostname)
        if domainname=='gridka.de':
            domainname = 'fzk.de'

        if not detsetype:
            print 'Hostname: %s, Domainname: %s' % (hostname, domainname)

        sename = ''
        if os.environ.has_key('VO_ATLAS_DEFAULT_SE'):
            sename  = os.environ['VO_ATLAS_DEFAULT_SE']                
        if not detsetype:
            print 'VO_ATLAS_DEFAULT_SE: %s' %sename

        if sename =='srm.cern.ch':
            sename = 'srm-atlas.cern.ch'

        # Pre-Identification by domainname
        dq2localids = [ ]
        celist = TiersOfATLAS.listCEsInCloudByDomain('*'+domainname) 
        for sitename in TiersOfATLAS.getAllSources():
            # First search for domainname
            dq2domain = TiersOfATLAS.getSiteProperty(sitename,'domain')
            if dq2domain and dq2domain.find(domainname)>=0:
                dq2cename = TiersOfATLAS.getSiteProperty(sitename,'ce')
                if dq2cename and dq2cename!=['']:
                    celist = celist + dq2cename
                dq2localids.append(sitename)  
            # Second search for srm
            dq2srm = TiersOfATLAS.getSiteProperty(sitename,'srm')
            #if dq2srm and dq2srm.startswith('token:'):
            #    continue
            if dq2srm and dq2srm.find(sename)>=0:
                dq2cename = TiersOfATLAS.getSiteProperty(sitename,'ce')
                if dq2cename and dq2cename!=['']:
                    celist = celist + dq2cename
                dq2localids.append(sitename)

        for icelist in celist:
            dq2localids.append(TiersOfATLAS.getSiteByCE(icelist))

        # Get location list of dataset
        try:
            datasetlocation = os.environ['DATASETLOCATION'].split(":")
        except:
            print "ERROR : DATASETLOCATION not defined"
            datasetlocation = []
            pass

        if not detsetype:
            print celist
            print dq2localids
        # Comparision with SRM name
        localsiteid = ''
        localsitesrm = ''
        for dq2localid in dq2localids:
            localsitesrm = TiersOfATLAS.getSiteProperty(dq2localid,'srm')
            #if localsitesrm.startswith('token:'):
            #    continue
            if not localsitesrm:
                continue
            if localsitesrm.find(sename)>=0:
                if dq2localid in datasetlocation: 
                    localsiteid = dq2localid
                    break
            
        if localsiteid=='' and dq2localids:
            localsiteid = dq2localids[0]
            localsitesrm = TiersOfATLAS.getSiteProperty(localsiteid,'srm')

        if not detsetype:
            print 'localsiteid: %s' % localsiteid

        if sename in ['dpm01.grid.sinica.edu.tw']:
            localsiteid = 'ASGCDISK'

        if sename in [ 'atlasse.phys.sinica.edu.tw']:
            localsiteid = 'TW-FTT'

        if sename in ['srm-disk.pic.es']:
            localsiteid = 'PIC_DATADISK'
            
        localsitesrm = TiersOfATLAS.getSiteProperty(localsiteid,'srm')
        # Remove token info
        localsitesrm = re.sub('token:*\w*:','', localsitesrm)
        localsitesrm = re.sub(':*\d*/srm/managerv2\?SFN=','', localsitesrm)

        # Get location list of dataset
        try:
            datasetlocation = os.environ['DATASETLOCATION'].split(":")
        except:
            if not detsetype:
                print "ERROR : DATASETLOCATION not defined"
            datasetlocation = []
            pass
        
        # Determine local DQ2 configuration variables (gsiftp, dcap, rfio)
        os.environ[ 'LCG_CATALOG_TYPE' ] = 'lfc'
        os.environ[ 'DQ2_LFC_HOME' ] = '/grid/atlas'

        # Reset localsite id
        if not detsetype:
            print 'localsiteid before getAggName(): %s' % localsiteid
        # TODO: avoiding  getAggName() is just a workaround; should be tuned
        if datasettype not in [ 'DQ2_DOWNLOAD', 'TNT_DOWNLOAD']:
            localsiteid = getAggName(localsiteid)
            if not detsetype:
                print 'localsiteid after getAggName(): %s' % localsiteid

        # Set DQ2_LOCAL_SITE_ID
        try:
            siteID = ''
            cmd =  "grep DQ2_LOCAL_SITE_ID $VO_ATLAS_SW_DIR/ddm/latest/setup.sh |  tr '=' '\n' | tail -1"
            rc, out = commands.getstatusoutput(cmd)
            if not rc:
                dq2localsiteid = out
            else:
                dq2localsiteid = localsiteid
            dq2alternatename = TiersOfATLAS.getSiteProperty(dq2localsiteid,'alternateName')
            for sitename in datasetlocation:
                if TiersOfATLAS.getSiteProperty(sitename,'alternateName'):
                    if TiersOfATLAS.getSiteProperty(sitename,'alternateName')==dq2alternatename:
                        siteID = sitename
                        break
        except:
            siteID = localsiteid
            pass

        outFile = open('dq2localid.txt','w')
        outFile.write('%s\n' % siteID)
        outFile.close()

        os.environ[ 'DQ2_LOCAL_SITE_ID' ] = localsiteid
        if not detsetype:
            print 'DQ2_LOCAL_SITE_ID: %s' %os.environ[ 'DQ2_LOCAL_SITE_ID' ]

        # Find LFC Catalog host and set LFC_HOST 
        if localsiteid.startswith('CERN'):
            localsiteid = 'CERN'
        lfccat = TiersOfATLAS.getRemoteCatalogs(localsiteid)
        if lfccat:
            lfc_host = re.sub('[/:]',' ',lfccat[0]).split()[1]
        else:
            lfc_host = ''
        os.environ[ 'LFC_HOST' ] = lfc_host

        if localsitesrm!='':
            defaultSE = _getDefaultStorage(localsitesrm)
            if localsiteid in [ 'RHUL' ] :
                defaultSE.append('se1.pp.rhul.ac.uk')
            elif localsiteid in [ 'CERN', 'CERNPROD' ] :
                defaultSE.append('srm-atlas.cern.ch')
            elif localsiteid == 'DESY-HH':
                defaultSE.append('srm-dcache.desy.de')
            elif localsiteid in [ 'ASGC', 'ASGCDISK' ] or localsiteid.startswith('TAIWAN-LCG2'):
                defaultSE.append('srm2.grid.sinica.edu.tw')
                defaultSE.append('castorsc.grid.sinica.edu.tw')
                defaultSE.append('srm.grid.sinica.edu.tw')
            elif localsiteid == 'TW-FTT':
                defaultSE.append('f-dpm001.grid.sinica.edu.tw')
            elif localsiteid == 'BEIJING':
                defaultSE.append('atlasse01.ihep.ac.cn')
            elif localsiteid.startswith('MPPMU'):
                defaultSE.append('lcg-lrz-se.lrz-muenchen.de')

        else:
            defaultSE = ''
        
        try:
            if not detsetype:
                #print 'DQ2_LOCAL_SITE_ID: %s' %os.environ[ 'DQ2_LOCAL_SITE_ID' ]
                print 'DQ2_URL_SERVER: %s' %os.environ[ 'DQ2_URL_SERVER' ] 
                print 'DQ2_URL_SERVER_SSL: %s' %os.environ[ 'DQ2_URL_SERVER_SSL']
                print 'localsitesrm: %s' %localsitesrm            
        except:
            if not detsetype:
                print 'ERROR: could not set DQ2_LOCAL_SITE_ID'
            sys.exit(EC_Configuration)

        try:
            if not detsetype:
                print 'LFC_HOST: %s' %os.environ[ 'LFC_HOST' ]
        except:
            if not detsetype:
                print 'Error LFC_HOST not defined'
            pass

        # local access variables
        if globalVerbose:
            print 'config old: %s,%s,%s' %(configLOCALPROTOCOL, configSTORAGEROOT,  configLOCALPREFIX)

            if os.environ.has_key('VO_ATLAS_DEFAULT_SE'):
                if not detsetype:
                    print 'VO_ATLAS_DEFAULT_SE: %s' %os.environ['VO_ATLAS_DEFAULT_SE']

        if os.environ.has_key('VO_ATLAS_DEFAULT_SE') and not os.environ.has_key('DQ2_LOCAL_PROTOCOL'):
            
            cmd = 'lcg-info --list-se --query SE=$VO_ATLAS_DEFAULT_SE --attr Protocol --sed'
            rc, out = commands.getstatusoutput(cmd)
            out2 = out.split('%')
            if len(out2)>1:
                prot = out2[1].split('&')
                if 'rfio' in prot:
                    configLOCALPROTOCOL = 'rfio'
                    configLOCALPREFIX = 'rfio:'
                    if localsitesrm.find('/dpm/')>=0:
                        configSTORAGEROOT = '/dpm'
                    else:
                        configSTORAGEROOT = '/castor'
                elif 'dcap' in prot or 'gsidcap' in prot:
                    configLOCALPROTOCOL = 'dcap'
                    configSTORAGEROOT = '/pnfs'
                    configLOCALPREFIX = 'dcap:'
                else:
                    configLOCALPROTOCOL = ''
                    configSTORAGEROOT = '/'
                    configLOCALPREFIX = ''
            else:
                configLOCALPROTOCOL = ''
                configSTORAGEROOT = '/'
                configLOCALPREFIX = ''
            # Hack for SFU, SNIP, UAM
            if localsiteid in [ 'SFU', 'SINP', 'UAM' ]:
                configLOCALPROTOCOL = 'dcap'
                configSTORAGEROOT = '/pnfs'
                configLOCALPREFIX = 'dcap:'
            if localsiteid in [ 'MANC', 'MANC-2' ] or localsiteid.startswith('MANC'):
                configLOCALPROTOCOL = 'gsidcap'
                configSTORAGEROOT = '/pnfs'
                configLOCALPREFIX = 'dcap:'
            # RAL uses castor
            if localsiteid in [ 'RAL', 'RALDISK' ] or localsiteid.startswith('RAL-LCG2'):
                configLOCALPROTOCOL = 'rfio'
                configSTORAGEROOT = '/castor'
                configLOCALPREFIX = 'rfio:'
            # CNAF uses StoRM, needs extra treatment
            if localsiteid in [ 'CNAF', 'CNAFDISK' ] or localsiteid.startswith('INFN-T1'):
                configLOCALPROTOCOL = 'file'
                configSTORAGEROOT = '/storage'
                configLOCALPREFIX = 'file:'

            # if no info is found
            if configLOCALPROTOCOL == '':
                if localsitesrm.find('/pnfs/')>=0:
                    configLOCALPROTOCOL = 'dcap'
                    configSTORAGEROOT = '/pnfs'
                    configLOCALPREFIX = 'dcap:'
                elif localsitesrm.find('/castor/')>=0:
                    configLOCALPROTOCOL = 'rfio'
                    configSTORAGEROOT = '/castor'
                    configLOCALPREFIX = 'rfio:'
                elif localsitesrm.find('/dpm/')>=0:
                    configLOCALPROTOCOL = 'rfio'
                    configSTORAGEROOT = '/dpm'
                    configLOCALPREFIX = 'rfio:'
                elif localsitesrm.find('/nfs/')>=0:
                    configLOCALPROTOCOL = ""
                    configLOCALPREFIX = ""
                    configSTORAGEROOT = ""
                else:
                    configLOCALPROTOCOL = ""
                    configLOCALPREFIX = ""
                    configSTORAGEROOT = ""

            os.environ['DQ2_LOCAL_PROTOCOL'] = configLOCALPROTOCOL
            os.environ[ 'DQ2_STORAGE_ROOT' ] = configSTORAGEROOT
            os.environ[ 'DQ2_LOCAL_PREFIX' ] = configLOCALPREFIX

        if not detsetype:
            print 'config: %s,%s,%s' %(configLOCALPROTOCOL, configSTORAGEROOT,  configLOCALPREFIX)

    # Determine site SE type
    if detsetype:
        setype = 'NULL'
        if os.environ.has_key('VO_ATLAS_DEFAULT_SE'):
            sitese=os.environ['VO_ATLAS_DEFAULT_SE']
        elif detsename!='':
            sitese=detsename
        else:
            if not detsetype:
                print 'ERROR no SE specified !'
            sys.exit(4)

        setype = 'NULL'

        # Find TiersOfAtlasCache entry
        if sitese:
            for sitename in TiersOfATLAS.getAllSources():
                dq2srm = TiersOfATLAS.getSiteProperty(sitename,'srm')
                #if dq2srm and dq2srm.startswith('token:'):
                #    continue
                if dq2srm and dq2srm.find(sitese)>=0:
                    setype = findsetype(dq2srm)

                if dq2srm and setype == 'NULL':
                    sitesrm_domain = re.sub('^[\w\-]+\.','',dq2srm)
                    sitese_domain = re.sub('^[\w\-]+\.','',sitese)
                    if sitesrm_domain.find(sitese_domain)>=0:
                        setype = findsetype(sitesrm_domain)

        # If not in TiersOfAtlasCache
        if setype == 'NULL':
            cmd = 'lcg-info --list-se --query SE=%s --attr Accesspoint --sed 2>/dev/null' %sitese
            rc, out = commands.getstatusoutput(cmd)
            out2 = out.split('%')
            if len(out2)>1:
                setype = findsetype(out2[1])

        # The setype of NIKHEF and SARA can be detected via ToA query  
        #if localsiteid.startswith('NIKHEF'):
        #    setype = 'DPM'
        #if localsiteid.startswith('SARA'):
        #    setype = 'DCACHE'

        print setype
        sys.exit(0)

    ######################################################################
    # Start input configuration
    if datasettype!='DQ2_OUT':
        # Get datasetnames
        try:
            datasetnames = os.environ['DATASETNAME'].split(":")
        except:
            raise NameError, "ERROR: DATASETNAME not defined"
            sys.exit(EC_Configuration)

        # Read input_files 
        lfns = []
        if input:
            lfns = [ line.strip() for line in file(input) ]
        else:
            lfns = [ line.strip() for line in file('input_files') ]

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
        if inputguid:
            guids = [ line.strip() for line in file(inputguid) ]
        else:
            guids = [ line.strip() for line in file('input_guids') ]

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
        if os.environ.has_key('pybin'):
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
                cmd = '%s %s -s %s -t %s -p lcg -D -H %s -f %s %s' % (pythoncmd, dq2_get_path, os.environ['DQ2_LOCAL_SITE_ID'], timeout, directory, flist, datasetname)
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

    if datasettype in [ 'DQ2_LOCAL', 'TAG', 'TAG_REC', 'TNT_LOCAL']:

        if globalVerbose:
            print ddmFileMap
            print defaultSE

        # get list of files from LFC
        sUrlMap, tUrlMap, fsizeMap, md5sumMap = _getPFNsLFC(ddmFileMap, defaultSE, localsitesrm)

        # NIKHEF/SARA special case
        if os.environ[ 'DQ2_LOCAL_SITE_ID' ].startswith('NIKHEF') and len(tUrlMap)==0:
            print 'Special setup at NIKHEF - re-reading LFC' 
            localsitesrm = TiersOfATLAS.getSiteProperty(os.environ['DQ2_LOCAL_SITE_ID'],'srm')
            defaultSE = _getDefaultStorage(localsitesrm)
            configLOCALPROTOCOL = 'rfio'
            configSTORAGEROOT = '/dpm'
            configLOCALPREFIX = 'rfio:'
            sUrlMap, tUrlMap, fsizeMap, md5sumMap = _getPFNsLFC(ddmFileMap, defaultSE, localsitesrm)
        elif os.environ[ 'DQ2_LOCAL_SITE_ID' ].startswith('SARA') and len(tUrlMap)==0: 
            print 'Special setup at SARA - re-reading LFC' 
            localsitesrm = TiersOfATLAS.getSiteProperty(os.environ['DQ2_LOCAL_SITE_ID'],'srm')
            defaultSE = _getDefaultStorage(localsitesrm)
            configLOCALPROTOCOL = 'gsidcap'
            configSTORAGEROOT = '/pnfs'
            configLOCALPREFIX = 'dcap:'
            sUrlMap, tUrlMap, fsizeMap, md5sumMap = _getPFNsLFC(ddmFileMap, defaultSE, localsitesrm)

        # Check md5sum
        if len(tUrlMap)>0 and os.environ.has_key('GANGA_CHECKMD5SUM') and os.environ['GANGA_CHECKMD5SUM']=='1':
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
                                    
            # append
            item = {'pfn':pfn,'guid':guid}
            files[lfn] = item

        if globalVerbose:
            print files

        # make PoolFileCatalog
        _makePoolFileCatalog(files)

        # make jobO
        if (datasettype == 'DQ2_LOCAL'):
            tag = False
            # Remove ESD files
            if lfns_esd:
                for lfn in lfns_esd:
                    if lfn in files.keys():
                        files.pop(lfn)
                
            _makeJobO(files, version=atlas_release_major, dtype=datatype)

        if len(files)>0:
            returnvalue=0
        else:
            print 'ERROR: Dataset(s) %s is/are empty at %s' %(datasetnames, localsiteid)
            returnvalue=EC_QueryFiles



    # TAG DATASET ###########################################################
    if datasettype in [ 'TAG', 'TAG_REC', 'TNT_DOWNLOAD', 'TNT_LOCAL']:

        # Choice 1: USE tag.tar.gz as CollInput
        if os.access('./tag.tar.gz',os.R_OK):
            cmd = 'tar xvzf tag.tar.gz'
            rc, out = getstatusoutput(cmd)
            if (rc!=0):
                print "ERROR: error during extraction of tar.tar.gz"
                print out
                sys.exit(EC_UNSPEC)
            # make job option file
            dir = "."
            filepat = "\.root"
            pat = re.compile(filepat)
            filelist = os.listdir(dir)
            joName = 'input.py'
            outFile = open(joName,'w')
            if datasettype in [ 'TAG_REC', 'TNT_DOWNLOAD', 'TNT_LOCAL' ]:
                if atlas_release_major >= 13:
                    outFile.write('PoolTAGInput = [')
                else:
                    outFile.write('CollInput = [')
            else:
                if atlas_release_major >= 13:
                    versionString='ServiceMgr.'
                else:
                    versionString = ''
                outFile.write('%sEventSelector.CollectionType="ExplicitROOT"\n'%versionString)
                outFile.write('%sEventSelector.RefName = "StreamAOD"\n'%versionString)
                outFile.write('%sEventSelector.InputCollections = ['%versionString)

            for file in filelist:
                found = re.findall(pat, file)
                if found:
                    filename = re.sub('\.root\.\d+$','',file)
                    if atlas_release_major <= 12:
                        filename = re.sub('\.root$','',file)
                    outFile.write('"%s",' % filename)
            outFile.write(']\n')
            # close
            outFile.close()

        # Choice 2: USE TAGDATASETNAME
        else:
            # Get tagdatasetname
            try:
                tagdatasetnames = os.environ['TAGDATASETNAME'].split(":")
            except:
                raise NameError, "ERROR: TAGDATASETNAME not defined"
                sys.exit(EC_Configuration)

            # Is python32 available
            cmd = 'which python32'
            pythoncmd = ''
            rc, out = commands.getstatusoutput(cmd)
            if (rc!=0):
                print 'No python32 found'
                pythoncmd = ''
            else:
                pythoncmd = out.strip()
            if os.environ.has_key('pybin'):
                pythoncmd = os.environ['pybin']

            # Download tag dataset
            # compose dq2 command
            for tagdatasetname in tagdatasetnames:
                cmd = 'DQ2_LOCAL_ID= ; %s ./dq2_get -rcv -t %s %s ' % (pythoncmd,timeout,tagdatasetname)
                cmdretry = ' DQ2_LOCAL_ID= ; %s ./dq2_get -rcv -s BNL -t %s %s ' % (pythoncmd,timeout,tagdatasetname)
                # execute dq2 command
                rc, out = getstatusoutput(cmd)
                print out
                if (rc!=0):
                    print "ERROR: error during dq2_get occured"
                    rc, out = getstatusoutput(cmdretry)
                    print out
                    if (rc!=0):
                        print "ERROR: error during retry of dq2_get from BNL occured"
                        sys.exit(EC_DQ2GET)
            
            # Create input file list of TAG dataset
            # make jobO

            # Read input_tag_files 
            taglfns = [ line.strip() for line in file('input_tag_files') ]
            # Get guids from input_tag_guids
            tagguids = [ line.strip() for line in file('input_tag_guids') ]

            tagddmFileMap = {}
            for i in xrange(0,len(taglfns)):
                tagddmFileMap[taglfns[i]] = tagguids[i]

            files = {}
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
                    print "ERROR %s not found" % name
                    continue
                if (fsize>0):
                    # append
                    item = {'pfn':pfn,'guid':guid}
                    files[lfn] = item

            _makeJobO(files, tag=True, type=datasettype, version=atlas_release_major, dtype=datatype)

        # Create symlinks of TAG files since CollInput attaches .root to every filename during processing
        dir = "."
        filepat = "pool\.root\.\d+$"
        pat = re.compile(filepat)
        filelist = os.listdir(dir)
        for file in filelist:
            found = re.findall(pat, file)
            if found:
                filenew = re.sub('\.\d+$','',file)
                try:
                    os.symlink(file,filenew)
                except OSError:
                    pass

        if datasettype == 'TAG_REC':
            # Parse jobOptions file to include input.py
            if os.environ.has_key('ATHENA_OPTIONS'):
                joboptions = os.environ['ATHENA_OPTIONS'].split(' ')
                
                if atlas_release_major >= 13:
                    linepat = "^PoolInputQuery="
                else:
                    linepat = "^CollInputQuery="
                pat = re.compile(linepat)

                for jfile in joboptions:
                    try:
                        jolines = [ rline.strip() for rline in open(jfile,'r') ]
                    except IOError:
                        jolines = []
                    newlines = []
                    for l in jolines:
                        found = re.findall(pat, l)
                        if found:
                            newlines.append("include ( \"input.py\" )")
                        newlines.append(l)
                    outFile = open(jfile,'w')
                    for l in newlines:
                        outFile.write(l+'\n')
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

        for i in xrange(0,len(output_files_new)):
            try:
                open(output_files_new[i],'r')
                output_files.append(output_files_new[i])
            except IOError:
                try:
                    open(output_files_orig[i],'r')
                    output_files.append(output_files_orig[i])
                    renameflag = True
                except IOError:
                    pass

        if len(output_files)==0:
            print 'ERROR: no output files existing to stage out.'
            sys.exit(EC_STAGEOUT)

        # Get datasetname
        try:
            datasetname = os.environ['OUTPUT_DATASETNAME']
        except:
            raise NameError, "ERROR: OUTPUT_DATASETNAME not defined"
            sys.exit(EC_Configuration)

        if not len(output_files):
            print 'No files requested.'
            sys.exit(EC_Configuration)

        # Set siteID
        siteID = os.environ[ 'DQ2_LOCAL_SITE_ID' ]
        if siteID in [ 'PIC', 'IFIC', 'CNAF', 'RAL', 'SARA', 'FZK', 'ASGC', 'LYON', 'TRIUMF', 'TIER0' ]:
            if re.search('DISK$',siteID) == None:
                siteID += 'DISK'

        elif siteID in [ 'CERN', 'TIER0DISK', 'TIER0TAPE', 'CERNPROD']:
            siteID = 'CERN'

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


        # Set siteID to site_USERDISK
        dq2alternatename = TiersOfATLAS.getSiteProperty(siteID,'alternateName')
        for sitename in TiersOfATLAS.getAllSources():
            if TiersOfATLAS.getSiteProperty(sitename,'alternateName'):
                if TiersOfATLAS.getSiteProperty(sitename,'alternateName')==dq2alternatename \
                       and TiersOfATLAS.getSiteProperty(sitename,'domain').find('atlasuserdisk')>0:
                    siteID = sitename
                    break

        # Find close backup locations
        close_backup_locations = []
        for sitename in TiersOfATLAS.getCloseSites(siteID):
            if TiersOfATLAS.getSiteProperty(sitename,'domain').find('atlasuserdisk')>0:
                close_backup_locations.append( sitename )

        # Compile stage out SE sequence 
        temp_locations = temp_locations + [ siteID ] + close_backup_locations + backup_locations

        if 'CERN' in temp_locations:
            temp_locations.remove('CERN')

        new_locations = []

        # Get space token names:
        try:
            space_token_names = os.environ['DQ2_OUTPUT_SPACE_TOKENS'].split(":")
        except:
            print "ERROR : DQ2_OUTPUT_SPACE_TOKENS not defined"
            space_token_names = [ 'ATLASUSERDISK', 'ATLASUSERTAPE', 'ATLASLOCALGROUPDISK' ]
            pass
        

        for ilocation in temp_locations:
            temp_location = ilocation

             # Find LFC Catalog host 
            location_info = { }
            lfccat = TiersOfATLAS.getRemoteCatalogs(temp_location)
            if lfccat:
                temp_lfc_host = re.sub('[/:]',' ',lfccat[0]).split()[1]
                temp_lfc_home = lfccat[0].split(':')[2]
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

        # Get output lfn
        try:
            output_lfn = os.environ['OUTPUT_LFN']
        except:
            raise NameError, "ERROR: OUTPUT_LFN not defined"
            sys.exit(EC_Configuration)

        # Get output jobid
        try:
            output_jobid = os.environ['OUTPUT_JOBID']
        except:
            raise NameError, "ERROR: OUTPUT_JOBID not defined"
            sys.exit(EC_Configuration)

        try:
            use_short_filename = os.environ['GANGA_SHORTFILENAME']
        except:
            raise NameError, "ERROR: GANGA_SHORTFILENAME not defined"
            sys.exit(EC_STAGEOUT)
    
        guids = []
        sizes = []
        md5sums = [] 
        dq2lfns = []

        f_data = open('output_data','w')

        # loop to save all output_files
        for file in output_files:
            if renameflag:
                # Rename file
                temptime = time.gmtime()
                output_datasetname = re.sub('\.[\d]+$','',datasetname)
                pattern=output_datasetname+".%04d%02d%02d%02d%02d%02d._%05d."+file
                i=output_jobid.split('.')
                if len(i)>1:
                    new_output_file = pattern % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],int(i[1])+1)
                    short_pattern = ".%04d%02d%02d%02d%02d%02d._%05d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],int(i[1])+1)
                else:
                    new_output_file = pattern % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],1)
                    short_pattern = ".%04d%02d%02d%02d%02d%02d._%05d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5],1)
                    
    
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
                print 'Storing file at: %s' %siteID

                # Create output info
                dest = os.path.join(output_srm, output_lfn, file)
                gridlfn = os.path.join(output_lfc_home, output_lfn, file)
                griddir = os.path.join(output_lfc_home, output_lfn)
                filename = os.path.join(os.environ['PWD'], file)
                poolguid = ''
                try:
                    poolguid = catalogguidMap[file]
                except KeyError:
                    rc, out = commands.getstatusoutput('pool_extractFileIdentifier '+file)
                    if rc == 0:
                        for line in out.split():
                            match = re.search('^([\w]+-[\w]+-[\w]+-[\w]+-[\w]+)',line)
                            if match:
                                poolguid = match.group(1)

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
                        siteCount = siteCount + 1
                        print 'ERROR: file not saved to %s - using now %s ...' %(siteID, temp_locations[siteCount] )
                        count = 0

                    count = count + 1
                    time.sleep(120)

            if siteCount==len(temp_locations):
                print 'ERROR: file not saved to any location ...' 
                sys.exit(EC_STAGEOUT)
            
            # Write output_data
            f_data_string = '%s,%s,%s,%s,%s,%s' % (datasetname, file, guid, size, md5sum, siteID) 
            print >>f_data, f_data_string 

        f_data.close()

        f2 = open('output_location','w')
        print >>f2, siteID 
        f2.close()


    sys.exit(returnvalue)

