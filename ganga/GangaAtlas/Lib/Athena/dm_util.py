import os
#import md5
import re
import sys
import socket

from threading import Thread, Lock
from Queue import Queue, Empty

from dq2.info import TiersOfATLAS
from dq2.common.DQException import *

try:
    import hashlib
    md = hashlib.md5()
except ImportError:
    # for Python << 2.5
    import md5
    md = md5.new()

## Utility functions ##
def get_md5sum(fname):
    ''' Calculates the MD5 checksum of a file '''

    f = open(fname, 'rb')
    #m = md.new()
    while True:
        d = f.read(8096)
        if not d:
            break
        md.update(d)
    f.close()
    return md.hexdigest()

def urisplit(uri):
   """
   Basic URI Parser according to STD66 aka RFC3986

   >>> urisplit("scheme://authority/path?query#fragment")
   ('scheme', 'authority', 'path', 'query', 'fragment')

   """
   # regex straight from STD 66 section B
   regex = '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?'
   p = re.match(regex, uri).groups()
   scheme, authority, path, query, fragment = p[1], p[3], p[4], p[6], p[8]
   #if not path: path = None
   return (scheme, authority, path, query, fragment)

def get_srm_endpoint(dq2_site_id):
    '''
    Gets the SRM endpoint of a site registered in TiersOfATLAS.

    @param dq2_site_id is a DQ2 site id
    @return a dictionary containing the srm endpoint information
    '''

    srm_endpoint_info = {'token':None, 'endpt':None, 'se_host':None, 'se_path':None}
    re_srm2 = re.compile('^token:(.*):(srm:\/\/.*)\s*$')

    srm_endpt = TiersOfATLAS.getSiteProperty(dq2_site_id,'srm')

    if srm_endpt:
        mat = re_srm2.match(srm_endpt)
        if mat:
            # this is a SRMv2 endpoint specification
            srm_endpoint_info['token'] = mat.group(1)
            srm_endpoint_info['endpt'] = mat.group(2)

            endpt_data = urisplit(srm_endpoint_info['endpt'])
            srm_endpoint_info['se_host'] = endpt_data[1].split(':')[0]
            srm_endpoint_info['se_path'] = endpt_data[3].replace('SFN=','')
        else:
            # this is a SRMv1 endpoint specification
            srm_endpoint_info['token'] = None
            srm_endpoint_info['endpt'] = srm_endpt

            endpt_data = urisplit(srm_endpoint_info['endpt'])
            srm_endpoint_info['se_host'] = endpt_data[1].split(':')[0]
            srm_endpoint_info['se_path'] = endpt_data[2]

    return srm_endpoint_info

def get_srm_host(site):
    '''
    Gets the SRM hostname of the given site.
    '''
    srm_endpoint_info = get_srm_endpoint(site)

    return srm_endpoint_info['se_host']

def get_lfc_host(dq2_site_id):
    '''
    Gets the LFC host of a site registered in TiersOfATLAS.
    '''

    lfc_url = TiersOfATLAS.getLocalCatalog(dq2_site_id)
    if lfc_url:
        return lfc_url.split('/')[2][:-1]
    else:
        return None

def get_se_hostname(sename_replacements={}):
    '''
    Tries to determine the hostname of the ATLAS local SE by parsing $VO_ATLAS_DEFAULT_SE.

    The "sename_replacements" dictionary then is applied by replacing the detected sename with
    a right one given by human knowledge. The dictionary is given in the format:

        sename_replacements = { 'old(wrong)_sename_1': 'new(right)_sename_1',
                                'old(wrong)_sename_2': 'new(right)_sename_2',
                                ... ... }

    '''
    sename = ''
    if 'VO_ATLAS_DEFAULT_SE' in os.environ:
        sename  = os.environ['VO_ATLAS_DEFAULT_SE']

    if sename:
        # apply exceptions
        for old,new in sename_replacements.items():
            if sename == old:
                sename = new

    return sename

def get_transfer_protocols(sename=None):
    '''
    Tries to detect the data transfer protocols supported by the specified storage element.
    '''

    backup_bdii = 'lcg-bdii.cern.ch:2170'

    if not sename:
        sename = get_se_hostname()

    outputs = []
    protocols = []

    ## query to default BDII (LCG_GFAL_INFOSYS)
    if 'LCG_GFAL_INFOSYS' in os.environ:
        cmd = 'lcg-info --list-se --vo atlas --query SE=\'%s\' --attr Protocol --sed' % sename

        f = os.popen(cmd)
        outputs = map(lambda x:x.strip(), f.readlines())
        rc = f.close()

        print >> sys.stdout, 'resolving SE protocols with default BDII: %s | %s | %s' % (os.environ['LCG_GFAL_INFOSYS'], cmd, outputs)
    else:
        print >> sys.stdout, 'no default BDII defined by LCG_GFAL_INFOSYS'

    ## query to the backup BDII at CERN
    if not outputs:
        cmd = 'lcg-info --list-se --vo atlas --bdii \'%s\' --query SE=\'%s\' --attr Protocol --sed' % (backup_bdii, sename)
        f = os.popen(cmd)
        outputs = map(lambda x:x.strip(), f.readlines())
        rc = f.close()


        print >> sys.stdout, 'resolving SE protocols with backup BDII: %s | %s | %s' % (os.environ['LCG_GFAL_INFOSYS'], cmd, outputs)

    ## parsing the query results
    if not rc:
        re_se = re.compile('^%s' % sename.lower())

        for line in outputs:
            if re_se.match(line.lower()):
                se, protocol_str = line.split('%')
                if protocol_str:
                    protocols = protocol_str.split('&')
                break

    if protocols:
        ## keep unique protocol names and remove '_UNDEF_' in the list
        protocols = list( set( protocols ) )
        try:
            protocols.remove('_UNDEF_')
            print >> sys.stdout, 'protocol undefined for SE: %s' % sename
        except ValueError:
            pass

    return protocols

def get_site_domain(domain_replacements={}):
    '''
    Tries to determine the site domain from varies approaches:
     - parse EDG_WL_RB_BROKERINFO: available if job is brokered by EDG RB
     - parse GLITE_WMS_RB_BROKERINFO: available if job is brokered by GLITE WMS
     - parse GANGA_LCG_CE: available if job's CE is assigned through Ganga
     - parse VO_ATLAS_DEFAULT_SE: assuming the default SE of ATLAS is sitting inside the same domain
     - parse localhost domain in the worst case

    The "domain_replacements" dictionary then is applied by replacing the detected domain with
    a right one given by human knowledge. The dictionary is given in the format:

        domain_replacementss = { 'old(wrong)_domain_1': 'new(right)_domain_1',
                                 'old(wrong)_domain_2': 'new(right)_domain_2',
                                 ... ... }
    '''

    site_domain = None

    hostname = None

    # First choice: EDG_WL_RB_BROKERINFO or GLITE_WMS_RB_BROKERINFO
    if 'EDG_WL_RB_BROKERINFO' in os.environ:
        try:
            f = open(os.environ['EDG_WL_RB_BROKERINFO'], "r")
            lines = f.readlines()
            for line in lines:
                match = re.search('name = "(\S*):2119', line)
                if match:
                    hostname =  [ match.group(1) ]
        except:
            pass

    if 'GLITE_WMS_RB_BROKERINFO' in os.environ:
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
    if not hostname and 'GANGA_LCG_CE' in os.environ:
        try:
            hostname = re.findall('(\S*):2119',os.environ['GANGA_LCG_CE'])
            #print hostname, lcgcename
        except:
            pass

    # Third choice: VO_ATLAS_DEFAULT_SE
    if not hostname and 'VO_ATLAS_DEFAULT_SE' in os.environ:
        hostname = os.environ['VO_ATLAS_DEFAULT_SE']

    # Fourth choice: local hostname
    if not hostname:
        hostname = socket.gethostbyaddr(socket.gethostname())

    if hostname.__class__.__name__=='list' or hostname.__class__.__name__=='tuple':
        hostname = hostname[0]

    if hostname:
        site_domain = re.sub('^[\w\-]+\.','',hostname)

        # apply exceptions
        for old,new in domain_replacements.items():
            if site_domain == old:
                site_domain = new

    return site_domain

def resolve_dq2_local_site_id(ds_locations, site_domain, se_hostname, force_siteid_domain={}, force_siteid_se={}):
    '''
    resolves the DQ2_LOCAL_SITE_ID based on 3 factors:
     - the dataset location list
     - the site domain
     - the default SE host
     - the dictionaries containing the local_site_id enforcement

    The "force_siteid_domain" gives a list in which the matched site_domain (regx match)
    will be limited to the given list of DQ2 site ids.

    The "force_siteid_se" gives a list in which the matched se_hostname (regx match) 
    will be limited to the given list of DQ2 site ids.

    it returns a final DQ2 site id which is local and containing the dataset replica
    or an empty string if there is no matched DQ2 site id.
    '''
    dq2_local_site_id = ''

    toa_check = True

    # get dq2 site ids matching the site_domain and se_hostname
    dq2_local_ids = []

    # checking if the given site_domain in the enforcement dictionary
    if force_siteid_domain:
        for mydomain, mysites in force_siteid_domain.items():
            re_domain = re.compile(mydomain)
            if re_domain.match(site_domain):
                dq2_local_ids = mysites
                toa_check = False
                break

    # checking if the given se_hostname in the enforcement dictionary
    if force_siteid_se:
        for myse, mysites in force_siteid_se.items():
            re_se = re.compile(myse)
            if re_se.match(se_hostname):
                dq2_local_ids = mysites
                toa_check = False
                break

    # if enforcement is never applied, do the detection for dq2_local_ids via ToA
    if toa_check:
        
        alternateNameDict    = {}
        
        for sitename in TiersOfATLAS.getAllSources():

            # compose the altname:siteid dictionary
            altnames = TiersOfATLAS.getSiteProperty(sitename,'alternateName')
            if altnames:
                for altname in altnames:
                    if altname not in alternateNameDict:
                        alternateNameDict[altname] = []
                    alternateNameDict[altname].append(sitename)
            
            # First search for srm
            dq2srm = TiersOfATLAS.getSiteProperty(sitename,'srm')
            if dq2srm and dq2srm.find(se_hostname)>=0:
                dq2_local_ids.append(sitename)

            # Second search for domainname
            dq2domain = TiersOfATLAS.getSiteProperty(sitename,'domain')
            if dq2domain and dq2domain.find(site_domain)>=0:
                dq2_local_ids.append(sitename)


        # Thirdly search for sites with the same alternateName
        more_dq2_local_ids = []
        for sitename in dq2_local_ids:
            altnames = TiersOfATLAS.getSiteProperty(sitename,'alternateName')
            if altnames:
                for altname in altnames:
                    if altname in alternateNameDict:
                        more_dq2_local_ids += alternateNameDict[altname]

        dq2_local_ids += more_dq2_local_ids


    # resolving the best location according to the dataset locations
    #  - get the common part between dq2_local_ids and ds_locations
    #  - pick the first one in the common part
    candidates = list(set(dq2_local_ids) & set(ds_locations))

    print >> sys.stdout, str(candidates)

    if candidates:
        ## pick up the site whose corresponding se is matching the se_hostname
        for c in candidates:
            srm_info = get_srm_endpoint(c)
            if srm_info['se_host'] == se_hostname:
                dq2_local_site_id = c
                break
                
        ## otherwise, take the first candidate in the list
        if not dq2_local_site_id:
            dq2_local_site_id = candidates[0]

    return dq2_local_site_id

def make_FileStager_jobOption(pfns, gridcopy=True, protocol='lcgcp', maxEvent=-1, skipEvent = 0, fs_cp_cmd='', fs_cp_args=[], fs_of_prefix='file:', ic_jo_path='', fs_jo_path=''):
    '''
    creates the Athena job option file for FileStager.

    @param pfns specifies a list of files in physical paths
    @param gridcopy indicates if appending "gridcopy://" in front of each of pfns
    @param protocol indicates the preferred copy protocol
    @param maxEvent specifies the max. number of events
    @param skipEvent specifies the number of events to skip
    @param fs_cp_cmd specifies the exact copy command to be used by FileStager
    @param fs_cp_args specifies the exact copy command arguments to be used by FileStager
    @param fs_of_prefix specifies the prefix to be added in front of the destination file path in the copy command used by FileStager
    @param ic_jo_path specifies the path of the Athena job option stating the InputCollection configuration for Athena
    @param fs_jo_path specifies the path of the Athena job option stating the FileStager specific configurations
    '''

    jOptionFS = """#################################################################################################
# FileStager job option file generated by Ganga
#################################################################################################

import os

fsCopyCmd  = ###FSCOPYCMD###
fsCopyArgs = ###FSCOPYARGS###
fsOutputPrefix = ###FSOUTPUTPREFIX###

mySampleList = ###SAMPLELIST###

mySampleFile = 'sample.list'

myProtocol = ###GRIDCOPYPROTOCOL###

os.environ['FILE_STAGER_PROTOCOL'] = myProtocol

f = open(mySampleFile,'w')
for l in mySampleList:
    f.write(l + '\\n')
f.close()

## import filestager tool
from FileStager.FileStagerTool import FileStagerTool

## File with input collections
#stagetool = FileStagerTool(sampleList=mySampleList)
stagetool = FileStagerTool(sampleFile=mySampleFile)

## load the copy command wrapper if it exists on the WN
import os, os.path
if not fsCopyCmd:
    fsCopyCmd  = os.path.join( os.getcwd(), 'fs-copy.py' )
    fsCopyArgs = []
    fsOutputPrefix = 'file:'

stagetool.CpCommand = fsCopyCmd
stagetool.CpArguments = fsCopyArgs
stagetool.OutfilePrefix = fsOutputPrefix

print '*******'
print 'FS copy command   : %s' % stagetool.CpCommand
print 'FS copy arguments : %s' % stagetool.CpArguments
print 'FS dest. prefix   : %s' % stagetool.OutfilePrefix
print '*******'

## enforce storing copied files in current working directory
stagetool.tmpDir = os.getcwd()

#################################################################################################
# Configure the FileStager -- no need to change these lines
#################################################################################################

## get Reference to existing Athena job
from AthenaCommon.AlgSequence import AlgSequence
thejob = AlgSequence()

## check if collection names begin with "gridcopy"
print "doStaging?", stagetool.DoStaging()

## Import file stager algorithm
from FileStager.FileStagerConf import FileStagerAlg

## filestageralg needs to be the first algorithm added to the thejob.
if stagetool.DoStaging():
   thejob += FileStagerAlg('FileStager')
   thejob.FileStager.InputCollections = stagetool.GetSampleList()
   #thejob.FileStager.PipeLength = 2
   thejob.FileStager.VerboseStager = True
   thejob.FileStager.BaseTmpdir    = stagetool.GetTmpdir()
   thejob.FileStager.InfilePrefix  = stagetool.InfilePrefix
   thejob.FileStager.OutfilePrefix = stagetool.OutfilePrefix
   thejob.FileStager.CpCommand     = stagetool.CpCommand
   thejob.FileStager.CpArguments   = stagetool.CpArguments
   thejob.FileStager.FirstFileAlreadyStaged = stagetool.StageFirstFile

#################################################################################################
# Pass collection names to EventSelector
#################################################################################################

## set input collections
FileStagerInputCollection = []

if stagetool.DoStaging():
  FileStagerInputCollection = stagetool.GetStageCollections()
else:
  FileStagerInputCollection = stagetool.GetSampleList()

### Prepend AutoConfiguration (JE) #############################
try:
  from EventSelectorAthenaPool.EventSelectorAthenaPoolConf import EventSelectorAthenaPool
  orig_ESAP__getattribute =  EventSelectorAthenaPool.__getattribute__

  def _dummy(self,attr):
    if attr == 'InputCollections':
      return FileStagerInputCollection
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
    return FileStagerInputCollection

  AthenaCommon.AthenaCommonFlags.FilesInput.__call__ = _dummyFilesInput
except:
  pass

try:
  import AthenaCommon.AthenaCommonFlags

  def _dummyGet_Value(*argv):
    return FileStagerInputCollection

  for tmpAttr in dir (AthenaCommon.AthenaCommonFlags):
    import re
    if re.search('^(Pool|BS).*Input$',tmpAttr) is not None:
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
"""

    jOptionInput = """
ic = []

if FileStagerInputCollection:
    ic = FileStagerInputCollection

## assume we're dealing with AODs, else ESDs
poolESDInput = False
if len(ic)>0:
    if ic[0].find('ESD')>0: poolESDInput = True

## get a handle on the ServiceManager
if os.environ.has_key('DATASETDATATYPE') and os.environ['DATASETDATATYPE']=='MuonCalibStream':
    svcMgr.MuonCalibStreamFileInputSvc.InputFiles = ic

elif not os.environ.has_key('RECEXTYPE') or os.environ['RECEXTYPE'] == '':
    ## default: EventSelector
    try:
        svcMgr = theApp.serviceMgr()
        svcMgr.EventSelector.InputCollections = ic
        #svcMgr.EventSelector.SkipBadFiles = True
    except Exception as inst:
        pass

    ## else: athenaCommonFlags
    if not poolESDInput:
        try:
            ## the Input AOD File(s)
            from AthenaCommon.AthenaCommonFlags import athenaCommonFlags
            athenaCommonFlags.FilesInput = ic
        except Exception as inst:
            pass
    else:
        try:
            ## the Input ESD File(s)
            from AthenaCommon.AthenaCommonFlags import athenaCommonFlags
            athenaCommonFlags.FilesInput = ic
        except Exception as inst:
            pass
else:
    try:
        print "Using RECEXTYPE in FILE_STAGER"
        from AthenaCommon.AthenaCommonFlags import athenaCommonFlags
        athenaCommonFlags.Pool###RECEXTYPE###Input.set_Value_and_Lock( ic )
        athenaCommonFlags.FilesInput.set_Value_and_Lock( ic )
        athenaCommonFlags.EvtMax.set_Value_and_Lock(###MAXEVENT###)
        athenaCommonFlags.SkipEvents.set_Value_and_Lock(###SKIPEVENT###)
    except:
        pass

## override the event number
theApp.EvtMax = ###MAXEVENT###
## override the  number of events to skip
ServiceMgr.EventSelector.SkipEvents = ###SKIPEVENT###
    
## setting for event picking
if os.environ.has_key('ATHENA_RUN_EVENTS'):
    from AthenaCommon.AlgSequence import AthSequencer
    seq = AthSequencer('AthFilterSeq')
    from GaudiSequencer.PyComps import PyEvtFilter
    seq += PyEvtFilter('alg',evt_info='',)
    revt = eval(os.environ['ATHENA_RUN_EVENTS'])
    run_evt = []
    for i in range(len(revt)):
        run_evt.append((revt[i][0], revt[i][1]))
    seq.alg.evt_list = run_evt
    seq.alg.filter_policy = str(os.environ['ATHENA_FILTER_POLICY'])
    for tmpStream in theApp._streams.getAllChildren():
        fullName = tmpStream.getFullName()
        if fullName.split('/')[0] == 'AthenaOutputStream':
             tmpStream.AcceptAlgs = [seq.alg.name()]

"""

    ick = False

    ## create option file for FileStager
    if gridcopy:
        pfns = map(lambda x:'gridcopy://'+x, pfns)

    jOptionFS = jOptionFS.replace('###FSCOPYCMD###', repr(fs_cp_cmd))
    jOptionFS = jOptionFS.replace('###FSCOPYARGS###', repr(fs_cp_args))
    jOptionFS = jOptionFS.replace('###FSOUTPUTPREFIX###', repr(fs_of_prefix))
    jOptionFS = jOptionFS.replace('###SAMPLELIST###', repr(pfns))
    jOptionFS = jOptionFS.replace('###GRIDCOPYPROTOCOL###', repr(protocol))
    if 'RECEXTYPE' in os.environ:
        jOptionInput = jOptionInput.replace('###RECEXTYPE###', os.environ['RECEXTYPE'])
    else:
        jOptionInput = jOptionInput.replace('###RECEXTYPE###', 'ESD')
    f = open( fs_jo_path, 'w' )
    f.write(jOptionFS)
    f.close()

    ## create option file for Inputs
    jOptionInput = jOptionInput.replace('###MAXEVENT###', repr(maxEvent))
    jOptionInput = jOptionInput.replace('###SKIPEVENT###', repr(skipEvent))
    f = open( ic_jo_path, 'w' )
    f.write(jOptionInput)
    f.close()

    ick = True

    # Write flatfile input.txt for ARA
    flatFile = 'input.txt'
    outFlatFile = open(flatFile,'w')
    for pfn in pfns:
       outFlatFile.write('%s\n' %pfn)
    outFlatFile.close()

    return ick

def get_pfns(lfc_host, guids, nthread=10, dummyOnly=False, debug=False):
    '''
    getting pfns and checksum type/value corresponding to the given list of files represented
    by guids.

    @param lfc_host specifies the host of the local file catalogue
    @param guids is a list of GUIDs
    @param nthread is the amount of parallel threads for querying the LFC, 10 by default
    @param dummyOnly indicates if this routine returns only the dummy registries, default is False
    @param debug indicates if debugging messages are printed

    @return a dictionary of PFNs in the following format:

        pfns = { guid_1: [replica_1_pfn, replica_2_pfn, ...],
                 guid_2: [replica_1_pfn, replica_2_pfn, ...],
                 ... }

        and a dictionary of checksum type/value in the following format:

        csum = { guid_1: {'csumtype': checksum_type, 'csumvalue': checksum_value},
                 guid_2: {'csumtype': checksum_type, 'csumvalue': checksum_value},
                 ... }

    It uses the LFC multi-thread library: lfcthr, each worker thread works
    on the query of 1000 LFC registries.

    If dummyOnly, then only the pfns doublely copied on the
    the same SE are presented (determinated by SE hostname parsed from
    the PFNs).
    '''

    print >> sys.stdout, 'resolving physical locations of replicas'

    try:
        import lfcthr
    except ImportError as exp:
        print >> sys.stderr, '%s' % str(exp)
        print >> sys.stderr, 'unable to load LFC python module. Please check LCG UI environment.'
        print >> sys.stderr, 'python path: %s' % repr(sys.path)
        return {}

    pfns = {}
    csum = {}

    # divide guids into chunks if the list is too large
    chunk_size = 1000
    num_chunks = len(guids) / chunk_size
    if len(guids) % chunk_size > 0:
        num_chunks += 1

    chunk_offsets = []
    for i in range(num_chunks):
        chunk_offsets.append(i*chunk_size)

    # backup the current LFC_HOST
    lfc_backup = None
    try:
        lfc_backup = os.environ['LFC_HOST']
    except:
        pass

    # set to use a proper LFC_HOST
    os.putenv('LFC_HOST', lfc_host)

    def _resolveDummy(_pfns):
        '''resolving the dummy PFNs based on SE hostname'''
        _pfns_dummy = {}
        for _guid in _pfns.keys():
            _replicas = _pfns[_guid]
            _replicas.sort()
            seCache  = None
            pfnCache = None
            id = -1
            for _pfn in _replicas:
                id += 1
                _se = urisplit(_pfn)[1]
                if _se != seCache:
                    seCache  = _se
                    pfnCache = _pfn
                else:
                    # keep the dummy PFN
                    if _guid not in _pfns_dummy:
                        _pfns_dummy[_guid] = [pfnCache]
                    _pfns_dummy[_guid].append(_pfn)
        return _pfns_dummy

    ## setup worker queue for LFC queries
    wq = Queue(len(chunk_offsets))
    for offset in chunk_offsets:
        wq.put(offset)

    mylock = Lock()
    def worker(id):
        # try to connect to LFC
        if lfcthr.lfc_startsess('', '') == 0:
            while not wq.empty():
                try:
                    idx_beg = wq.get(block=True, timeout=1)
                    idx_end = idx_beg + chunk_size
                    if idx_end > len(guids):
                        idx_end = len(guids)

                    result, list1 = lfcthr.lfc_getreplicas(guids[idx_beg:idx_end],"")

                    if len(list1) > 0:
                        ## fill up global pfns dictionary and global csum dictionary
                        mylock.acquire()
                        for s in list1:
                            if s is not None:
                                if s.sfn:
                                    if s.guid not in pfns:
                                        pfns[s.guid] = []
                                    pfns[s.guid].append(s.sfn)
                                    csum[s.guid] = {'csumtype':'', 'csumvalue':''}
                                if s.csumtype:
                                    csum[s.guid]['csumtype'] = s.csumtype
                                if s.csumvalue:
                                    csum[s.guid]['csumvalue'] = s.csumvalue
                        mylock.release()
                except Empty:
                    pass
            # close the LFC session
            lfcthr.lfc_endsess()
        else:
            print >> sys.stderr, 'cannot connect to LFC'

    # initialize lfcthr
    lfcthr.init()

    # prepare and run the query threads
    threads = []
    for i in range(nthread):
        t = Thread(target=worker, kwargs={'id': i})
        t.setDaemon(False)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    if dummyOnly:
        pfns = _resolveDummy(pfns)

    # roll back to the original LFC_HOST setup in the environment
    if lfc_backup:
        os.putenv('LFC_HOST', lfc_host)

    return pfns, csum

def get_srmv2_sites(cloud=None, token=None, debug=False):
    '''
    Gets a list of SRMV2 enabled DDM sites in a given cloud.

    @param cloud is the ATLAS cloud name
    @param token restricts the output to only certain srmv2 tokens
    @param debug indicates if debugging messages are printed

    @return a list of ATLAS srmv2-enabled site names

    if token is given, only the site with the specific token type
    will be selected.
    '''

    srmv2_sites = []

    ## a better way of getting all sites within a cloud
    ## however, it seems there is a bug in DQ2 API so it
    ## always returns an empty site list.
    # all_sites   = TiersOfATLAS.getSitesInCloud(cloud)

    ## a bit of hack with non-public DQ2 API interface
    cache = TiersOfATLAS.ToACache

    all_sites = []
    if not cloud:
        all_sites = TiersOfATLAS.getAllDestinationSites()
    else:
        if cloud == 'T0':
            return ['CERNPROD']
        if cloud not in cache.dbcloud:
            return []
        all_sites = TiersOfATLAS.getSites(cache.dbcloud[cloud])

    for site in all_sites:
        srm = TiersOfATLAS.getSiteProperty(site,'srm')

        # presuming the srm endpoint looks like:
        #   token:ATLASDATADISK:srm://grid-cert-03.roma1.infn.it ...
        if srm is not None and srm.find('token') != -1:
            if token:
                if srm.split(':')[1] == token:
                    srmv2_sites.append(site)
            else:
                srmv2_sites.append(site)

    return srmv2_sites
