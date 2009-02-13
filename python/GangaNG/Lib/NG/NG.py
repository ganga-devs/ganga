###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: NG.py,v 1.21 2009-02-13 14:23:03 bsamset Exp $
###############################################################################
#
# NG backend
#
# Maintained by the Oslo group (B. Samset, K. Pajchel)
#
# Date:   January 2007

import os, sys, re, tempfile, urllib, imp, commands
from types import *
import time
import inspect

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Credentials import getCredential 
from Ganga.GPIDev.Adapters.IBackend import IBackend 
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core import BackendError
from Ganga.Core import Sandbox
from Ganga.Utility.Shell import Shell

from Ganga.Utility.Config import makeConfig, getConfig, ConfigError
from Ganga.Utility.util import isStringLike
from Ganga.Utility.logging import getLogger

#from GangaNG.Lib.NG.NGShell import getShell
from Ganga.Utility.GridShell import getShell

from xml.dom import minidom
from xml.sax import SAXParseException

from dq2.common.DQException import DQInvalidRequestException
from dq2.clientapi.DQ2 import DQ2
from dq2.common.DQException import *

from dq2.info import TiersOfATLAS

#import LFCTools
from dq2.filecatalog.lfc.lfcconventions import to_native_lfn
#from dq2.filecatalog.lfc.LFCFileCatalog import LFCFileCatalogException
from GangaNG.Lib.NG.LFCTools import *

# Configuration for later
lfchost = 'lfc1.ndgf.org' # This is production server
lfcarchival = 'P'
lfcprefix = ''
LFC_HOME = '/grid/atlas/'
#os.environ['LFC_CONNTIMEOUT'] = '30'
#os.environ['LFC_CONRETRY'] = '1'
#os.environ['LFC_CONRETRYINT'] = '15'

# SRM url for temporary files
SRMurl ='srm://dcache.ijs.si/pnfs/ijs.si/atlas/disk/flat/'

# python pinding not used - command line wrapper
#from arclib import *

def getTidDatasetnames(ds):
  dq2 = DQ2()
  ds_tid = []
  
  for dsn in ds:
    
    if dsn.find('_tid') < 0:
      ds_names = dq2.listDatasets(dsn+'*')
      ds_names = ds_names.keys()

      lds = len(ds_tid)
      
      for d in ds_names:
        if d.find('_tid') > -1:
          ds_tid += [d]

      # no tid names found for the datset and only one set with the name in dq2
      if lds == len(ds_tid) and len(ds_names) == 1:
        ds_tid += [ds_names[0]]
        
    else:
      ds_tid += [dsn]
  
  return ds_tid

def matchLFNtoDataset(ds,lfn,atlasrel):

  tid = lfn.split('.')[1]
  lfnds = None
  at = atlasrel.split('.')
  
  if int(at[0]) > 12:  
    for d in ds:
      if d.find('_tid'+tid) > -1:
        lfnds = d
  else:
    lfn1 = lfn.split('_')
    for l in lfn1:
      if l.startswith('tid'):
        tid = l.strip('.') 
    for d in ds:
      if d.find(tid) > -1:
        lfnds = d

  if lfnds == None:
    slfn = lfn.split('.')
    for d in ds:
      sd = d.split('.')
      if slfn[0] == sd[0] and slfn[1] == sd[1] and slfn[2] == sd[2]:
        lfnds = d
        
  return lfnds

def getGangaLFN(dataset,fname):

  username = dataset.split('.')[1]
  output_lfn = 'users/%s/ganga/%s/' % (username,dataset)
  
  lfn = output_lfn + fname

  return lfn

def getLFCurl(dataset,file):
  """ user datasets are registered under to_natie_lfn convention
  One can use the same procerure for getting the lfn both for production datasets
  as for user datasets.
  """

  ds = dataset.split('.')[0]

  return 'lfc://lfc1.ndgf.org/' + to_native_lfn(dataset,file)

def getTiersOfATLASCache():
    """Download TiersOfATLASCache.py"""
    
    url = 'http://atlas.web.cern.ch/Atlas/GROUPS/DATABASE/project/ddm/releases/TiersOfATLASCache.py'
    local = os.path.join(os.environ['PWD'],'TiersOfATLASCache.py')
    try:
        urllib.urlretrieve(url, local)
    except IOError:
      logger.warning('Failed to download TiersOfATLASCache.py')
        
    try:
        tiersofatlas = imp.load_source('',local)
    except SyntaxError:
        logger.warning('Error loading TiersOfATLASCache.py')
        sys.exit(EC_UNSPEC)
            
    return tiersofatlas

def register_file_in_dataset(datasetname,lfn,guid, size, checksum):
    """Add file to dataset into DQ2 database"""
    # Check if dataset really exists

    dq2=DQ2()

    content = dq2.listDatasets(datasetname)
    
    if content=={}:
        logger.error('Dataset %s is not defined in DQ2 database !',datasetname)
        return

    # Add file to DQ2 dataset
    ret = []
    try:
        ret = dq2.registerFilesInDataset(datasetname, lfn, guid, size, checksum)
    except DQInvalidRequestException, Value:
        logger.warning('Warning, some files already in dataset: %s', Value)
        pass
        
    return 

def getSRMendpoint(sitename):
  # Get the correctly formatted SRM endpoint from ToA

  srm_endpoint = TiersOfATLAS.getSiteProperty(sitename, 'srm')
  srm_endpoint_l = srm_endpoint.split(":")
  if srm_endpoint_l[0]=='token':
    srm_endpoint_s = ''
    for i in range(len(srm_endpoint_l)):
      if i<2:
        continue
      srm_endpoint_s=srm_endpoint_s+srm_endpoint_l[i]+":"
    # Strip tailing ':'
    srm_endpoint_s = srm_endpoint_s[:-1]
    srm_endpoint = srm_endpoint_s
  # print "SRM ENDPOINT: "+srm_endpoint
  return srm_endpoint

def getTurl(lfn,datasetname):

    username = lfn.split('.')[1]
    output_lfn = 'users/%s/ganga/%s/' % (username,datasetname)

    tiersofatlas = getTiersOfATLASCache() 
    # Set a default site name
    sitename = 'NDGFT1DISK'
  
    # See if sitename is in TiersOfAtlasCache.py
    for site, desc in tiersofatlas.sites.iteritems():
        if site!=sitename:
            continue
        srm_endpoint = desc['srm'].strip()
        
    turl = srm_endpoint + output_lfn + lfn

    return turl

def epoch(date_time):
    date_pattern = '%Y-%m-%d %H:%M:%S'
    epoch = time.mktime(time.strptime(date_time, date_pattern))
    return int(epoch)


class Grid:
    '''Helper class to implement grid interaction'''

    middleware  = 'ARC'

    credential = None


    def __init__(self,middleware='ARC'):

        self.active=False

        self.middleware = middleware.upper()

#       check that UI has been set up
#       start up a shell object specific to the middleware

        self.config = getConfig('ARC')
        self.shell = getShell(self.middleware)

        if not self.shell:
            logger.warning('ARC-%s UI has not been configured. The plugin has been disabled.' % self.middleware)
            return

#       create credential for this Grid object
        self.active = self.check_proxy()

    """
    def __setattr__(self,attr,value):
        object.__setattr__(self, attr, value)
        # dynamic update the internal shell object if the config attribute is reset
        if attr == 'config':
            self.shell = getShell(self.middleware)
    """

    def __get_cmd_prefix_hack__(self,binary=False,sbin=False):
        # this is to work around inconsistency of LCG setup script and commands:
        # LCG commands require python2.2 but the setup script does not set this version of python
        # if another version of python is used (like in GUI), then python2.2 runs against wrong python libraries
        # possibly should be fixed in LCG: either remove python2.2 from command scripts or make setup script force
        # correct version of python

        # Try not to use the ARC_LOCATION but rather NORDUGRID_LOCATION which is defined by the middleware
        
        s = ""
        if sbin:
            s="s"
        prefix_hack = "${%s_LOCATION}/%sbin/" % (self.middleware,s)
        #print 'get_cmd_prefix_hack rerurn prefix_hack ', prefix_hack
        #prefix_hack = "${NORDUGRID_LOCATION}/bin/"

        #if not binary:
        #    prefix_hack = 'python '+prefix_hack

        return prefix_hack

    def __print_gridcmd_log__(self,regxp_logfname,cmd_output):
        match_log = re.search(regxp_logfname,cmd_output)

        if match_log:
            logfile = match_log.group(1)
            f = open(logfile,'r')
            for l in f.readlines():
                logger.warning(l.strip())
            f.close()
        else:
            logger.warning('output\n%s\n',cmd_output)
            logger.warning('end of output')

    def __get_proxy_voname__(self):
        '''Check validity of proxy vo'''

        # not needed in NG
        # for ARC, we never check it
        if self.middleware == 'ARC':
            return None

        rc, output, m = self.shell.cmd1('voms-proxy-info -all | awk -F \':\' \'/^VO/ {print $2}\' 2>/dev/null', allowed_exit=[0,1,255])
        #if rc: return -1

        if output == '':
            output = None
        else:
            output = output.strip()

        return output

    def check_proxy(self):
        '''Check the proxy and prompt the user to refresh it'''

        forceInit = False

        if self.credential is None:
            self.credential = getCredential('GridProxy', self.middleware)

        #if not self.credential.hasShell():
        #    self.credential.setShell('ARC')

        if forceInit or not self.credential.isValid():
            status = self.credential.create(maxTry=3)
            if not status:
                logger.warning("Could not get a proxy, giving up after 3 retries")
                return False

        return True

    def upload(self, upfile ):
        '''Upload file to temporary grid storage.'''

        cmd = 'ngcp '
        
        if not self.active:
            logger.warning('NG plugin not active.')
            return

        if not self.credential.isValid():
            logger.warning('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return
        
        file_name = upfile.split('/')[-1]
        temptime = time.gmtime()
        time_pattern = "%04d%02d%02d%02d%02d%02d" % (temptime[0],temptime[1],temptime[2],temptime[3],temptime[4],temptime[5])
        new_file_name = time_pattern + file_name
        new_upfile = SRMurl +'/'+new_file_name
        command = cmd + upfile +' '+ new_upfile
        rc, output, m = self.shell.cmd1('%s%s ' % (self.__get_cmd_prefix_hack__(),command),allowed_exit=[0,500])
        if rc != 0:
            logger.warning(output)

        return new_upfile

    def clean_gridfile(self,gridfile):
        ''' Clean prestaged files in srm'''
        
        cmd = 'ngrm '
        command = cmd + gridfile
        
        rc, output, m = self.shell.cmd1('%s%s ' % (self.__get_cmd_prefix_hack__(),command),allowed_exit=[0,500])
        
        if rc != 0:
            logger.warning(output)

        return
            
    def submit(self,xrslpath,ce=None,rejectedcl=None,timeout=20):
        '''Submit a XRSL file to NG'''

        cmd = 'ngsub -G giises.txt -t %s ' % str(timeout)

        print cmd
        
        if not self.active:
            logger.warning('NG plugin not active.')
            return

        if not self.credential.isValid():
            logger.warning('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return

        # Select/reject clusters
        clusters = ''
        if ce:
            cea=ce.split(',')
            for c in cea:
                clusters += ' -c ' + c.strip()
        elif rejectedcl:
            cea=rejectedcl.split(',')
            for c in cea:
                clusters += ' -c -' + c.strip()

        if ce or rejectedcl :        
            cmd += clusters

        logger.debug('NG submit command: %s%s %s' % (self.__get_cmd_prefix_hack__(),cmd,xrslpath))

        rc, output, m = self.shell.cmd1('%s%s %s' % (self.__get_cmd_prefix_hack__(),cmd,xrslpath),allowed_exit=[0,500])
        
        match = re.search('(gsiftp:\S+)',output)
        if match: return match.group(1)

        logger.warning('Job submission failed.')
        self.__print_gridcmd_log__('(.*-job-submit.*\.log)',output)

        return   

    def native_master_submit(self,xrslpath,ce=None,rejectedcl=None, timeout = 20):
        '''Native bulk submission supported by GLITE middleware.'''
        # Bulk sumission is supported in NG, but the XRSL files need some care.

        print cmd

        cmd = 'ngsub -G giises.txt -t %s ' % str(timeout)
        
        if not self.active:
            logger.warning('NG plugin not active.')
            return

        if not self.credential.isValid():
            logger.debug('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return

        # Select/reject clusters
        clusters = ''
        if ce:
            cea=ce.split(',')
            for c in cea:
                clusters += ' -c ' + c.strip()
        elif rejectedcl:
            cea=rejectedcl.split(',')
            for c in cea:
                clusters += ' -c -' + c.strip()     
        if ce or rejectedcl :        
            cmd += clusters

        logger.debug('NG submit command: %s%s %s' % (self.__get_cmd_prefix_hack__(),cmd,xrslpath))

        rc, output, m = self.shell.cmd1('%s%s %s' % (self.__get_cmd_prefix_hack__(),cmd,xrslpath),allowed_exit=[0,500])

        
        logger.info('Job submission report')
        self.__print_gridcmd_log__('(.*-job-submit.*\.log)',output)
        
        jobids = []
        for line in output.split('\n'):
            match = re.search('(gsiftp:\S+)',line)
            if match:
                jobids += [match.group(1)]
            else:
                jobids += ['None']

        if len(jobids) > 0:
            return jobids

        logger.warning('Job submission failed.')
        self.__print_gridcmd_log__('(.*-job-submit.*\.log)',output)

        return

    def status(self,jobids):
        '''Query the status of jobs on the grid'''

        cmd = 'ngstat -l '

        if not self.active:
            logger.warning('NG plugin not active.')
            return []

        if not self.credential.isValid():
            logger.warning('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return []

        if not jobids:
            logger.warning('No jobs in status')
            return []

        idsfile = tempfile.mktemp('.jids')
        file(idsfile,'w').write('\n'.join(jobids)+'\n')
        # ngstat -i <file> file containing list of job ids  
        rc, output, m = self.shell.cmd1('%s%s -i %s' % (self.__get_cmd_prefix_hack__(),cmd,idsfile), allowed_exit=[0,255])
        os.unlink(idsfile)

        if rc != 0:
            self.__print_gridcmd_log__('(.*-job-status.*\.log)',output)
        
        re_id = re.compile('^\s*Job (gsiftp://.*\S)\s*$')
        re_exit = re.compile('^\s*Exit Code:\s+(.*\S)\s*$')
        re_reason = re.compile('^\s*Error:\s+(.*\S)\s*$')
        re_status = re.compile('^\s*Status:\s+(.*\S)\s*$')
        re_dest = re.compile('^\s*Cluster:\s+(.*\S)\s*$')
        re_name = re.compile('^\s*Queue:\s+(.*\S)\s*$')
        re_failed = re.compile('^\s*Job information not found:\s+(.*\S)\s*$')
        re_ngls = re.compile('^\s*Failed to obtain listing from ftp:\s+(.*\S)\s*$')
        re_usedcpu = re.compile('^\s*Used CPU Time:\s+(.*\S)\s*$')
        re_reqcpu = re.compile('^\s*Required CPU Time:\s+(.*\S)\s*$')
        info = []
          
        for line in output.split('\n'):
            match = re_id.match(line)
            if match:
                info += [{ 'id' : match.group(1),
                           'status' : None,
                           'exit' : None,
                           'reason' : None,
                           'destination' : None,
                           'name' : None,
                           'cputime' : None,
                           'requestedcputime' : None}]
                continue
            #else:
            #    match = re_failed.match(line)
            #    if match:
            #        info += [{ 'id' : match.group(1).rstrip('.'),
            #               'status' : None,
            #               'exit' : None,
            #               'reason' : None,
            #               'destination' : None,
            #               'name' : None }]
            #
            #        jid = match.group(1)
            #        cmdls = 'ngls '
            #        rc, nglsoutput, m = self.shell.cmd1('%s%s %s' % (self.__get_cmd_prefix_hack__(),cmdls,jid), allowed_exit=[0,255])
            #        if rc != 0:
            #            self.__print_gridcmd_log__('(.*-job-status.*\.log)',output)
            #        for l in nglsoutput.split('\n'):
            #            match = re_ngls.match(l)
            #            if match:
            #                info[-1]['status'] = 'FAILED'
            #                info[-1]['reason'] = 'Job was lost.'
            #                continue

            match = re_status.match(line)
            if match:
                info[-1]['status'] = match.group(1)
                continue

            match = re_exit.match(line)
            if match:
                info[-1]['exit'] = match.group(1)
                continue

            match = re_reason.match(line)
            if match:
                info[-1]['reason'] = match.group(1)
                continue

            match = re_dest.match(line)
            if match:
                info[-1]['destination'] = match.group(1)
                continue

            match = re_name.match(line)
            if match:
                info[-1]['name'] = match.group(1)
                continue

            match = re_usedcpu.match(line)
            if match:
                info[-1]['cputime'] = match.group(1)
                continue

            match = re_reqcpu.match(line)
            if match:
                info[-1]['requestedcputime'] = match.group(1)
                continue    
 
        return info

    def get_loginfo(self,jobid,outfile,verbosity=' '):
        '''Fetch the logging info of the given job and save the output in the jobs outputdir'''
        
        cmd = 'ngcat %s' % verbosity
        
        if not self.active:
            logger.warning('ARC plugin not active.')
            return False 

        if not self.credential.isValid():
            logger.warning('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return False

        rc, output, m = self.shell.cmd1('%s%s %s ' % (self.__get_cmd_prefix_hack__(),cmd,jobid),allowed_exit=[0,255])

        if rc != 0:
            self.__print_gridcmd_log__('(.*-logging-info.*\.log)',output)
            return False
        else:
            # returns the path to the saved logging info if success
            f=open( outfile , 'w')
            f.write( output )
            f.close()
            return outfile
        
    def get_output(self,jobid,directory,out,location,wms_proxy=False):
        '''Retrieve the output of a job on the grid'''

        binary = False
        
        cmd = 'ngget'

        if not self.active:
            logger.warning('NG plugin is not active.')
            return False

        if not self.credential.isValid():
            logger.warning('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return False

        # prexix actualy not needed
        logger.debug('NG download command: %s%s -dir %s %s' % (self.__get_cmd_prefix_hack__(binary),cmd,directory,jobid))
        
        rc, output, m = self.shell.cmd1('%s%s -dir %s %s' % (self.__get_cmd_prefix_hack__(binary),cmd,directory,jobid),allowed_exit=[0,255])
        
        if rc != 0:
            logger.warning('Job output fetch failed.')
            #self.__print_gridcmd_log__('(.*-output.*\.log)',output)
            return False

        outdir = directory + '/' + jobid.split('/')[-1]

        if outdir:
            if self.shell.system('mv %s/* %s' % (outdir,directory)) == 0:
                try:
                    os.rmdir(outdir)
                except Exception, msg:
                    logger.warning( "Error trying to remove the empty directory %s:\n%s" % ( outdir, msg ) )
            else:
                logger.warning( "Error moving output from %s to %s.\nOutput is left in %s." % (outdir,directory,outdir) )
        else:
            pass

        Sandbox.getPackedOutputSandbox(directory,directory)
        # if job grid status not FAILED or KILLED nothing to register, stop here:
        if not out:
            return True
            
        outp = True
        try:
            outputxml = minidom.parse (directory + "OutputFiles.xml")
        except IOError, x:
            outp = False
            if not str(x).startswith('[Errno 2]'):
                logger.warning(x)
            pass
        except SAXParseException, x:
            outp = False
            logger.warning(x)
            logger.warning("XML PARSE ERROR: failed to parse OutputFiles.xml for job ", jobid)
            pass
        
        lfn = []
        md5sum = []
        adler32a = []
        date = []
        size = []
        guid = []
        
        # If more files per datset - this can be done more efficiend look at DMSDQ2_PROD.py
        if outp == True:
            files = outputxml.getElementsByTagName ("file")
  
            for file in files:
                try:
                    lfnt = str(file.getElementsByTagName ("lfn")[0].firstChild.data)
                    lfn += [lfnt]
                    #print 'get_output lfn ', lfn 
                except:
                    lfn = None
      
                try:
                    lcn = file.getElementsByTagName ("lcn")[0].firstChild.data
                    #print 'get_output lcn ', lcn 
                except:
                    lcn = None
                
                try:
                    md5sum += ['md5:'+str(file.getElementsByTagName ("md5sum")[0].firstChild.data)]
                    #print 'get_output md5 ', md5sum
                except:
                    md5sum = None

                try:
                  adler32 = file.getElementsByTagName ("ad32")[0].firstChild.data
                  adler32 = str(adler32.lower().rstrip('l')) # the python in Atlas release can append l (lowercase L)!!!
                  adler32 = '%8s' % (adler32)
                  adler32 = adler32.replace(' ','0')
                  adler32a  += ['ad:' + adler32]
                except:
                  adler32 = None
                  print ":-( adler32 missing?",outp
                
                try:
                    tdate = file.getElementsByTagName ("date")[0].firstChild.data
                    date += [str(tdate[0:19])] # On some cluster the date is extended to microseconds or something.
                    #print 'get_output date ', date 
                except:
                    date = None
      
                try:
                    size += [long(file.getElementsByTagName ("size")[0].firstChild.data)]
                    #print 'get_output sise ', size 
                except:
                    size = None
                    
                try:
                    guid += [str(file.getElementsByTagName ("guid")[0].firstChild.data)]
                    #print 'get_output guid ', guid 
                except:
                    guid = None
                    
                try:
                    dataset = str(file.getElementsByTagName ("dataset")[0].firstChild.data)
                    #print 'get_output dataset ', dataset 
                except:
                    dataset = None
                
                if lfn == None or md5sum == None or date == None or size == None or guid == None or dataset == None:
                    outp = False
                    logger.warning("ERROR: File attribute is missing, job outputs not processed! ",jobid)

            if adler32 == None:
              checksum =  md5sum
            else:
              checksum = adler32a

            if outp:
              register_file_in_dataset(dataset,lfn,guid, size, checksum)
            else:
              logger.warning('ERROR could not register file in dq2')

            srm_endpoint =  getSRMendpoint(location)
            usertag = configDQ2['usertag']

            lfcinput = {}
            for i in range(len(lfn)):

                username = str(lfn[i]).split('.')[1]
                turl = srm_endpoint+usertag+"/"+username+"/ganga/"+dataset+"/"+str(lfn[i])
                
                lfcinput[guid[i]] = {'lfn': str(lfn[i]),
                                  'surl': turl,
                                  'dsn': dataset,
                                  'fsize': int(size[i]),
                                  'checksum': str(checksum[i]),
                                  'archival': lfcarchival}

            lfchost = TiersOfATLAS.getLocalCatalog(location)
            lfchost_l = lfchost.split(":")
            lfchost = ""
            for i in range(len(lfchost_l)-1):
              lfchost = lfchost+lfchost_l[i]+":"
            lfchost = lfchost[6:-1]
            
            try:
              result = bulkRegisterFiles(lfchost,lfcinput)
              for guid in result:
                if isinstance(result[guid],LFCFileCatalogException):
                  logger.warning('ERROR: LFC exception during registration: %s' % result[guid])
            except:
              logger.warning('Unclassified exception during LFC registration, put job back to UNKNOWN')
                
        return True

    def cancel(self,jobid):
        '''Cancel a job'''
        # remove -k, info sustem is not updated before cleanup
        cmd = 'ngkill '
        
        if not self.active:
            logger.warning('NG plugin is not active.')
            return False

        if not self.credential.isValid():
            logger.warning('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return False

        rc, output, m = self.shell.cmd1('%s%s %s' % (self.__get_cmd_prefix_hack__(),cmd,jobid),allowed_exit=[0,255])

        if rc == 0:
            return True
        else:
            logger.warning( "Failed to cancel job %s.\n%s" % ( jobid, output ) )
            self.__print_gridcmd_log__('(.*-job-cancel.*\.log)',output)
            return False

    def check_dq2_file_avaiability(self,lfcu,jobid):
        # Check if a file is available on NorduGrid

        #cmd = 'globus-rls-cli query wildcard lrc lfn "'+lfn+'" '+rls
    
        cmd = 'ngls -l ' + lfcu  
        # Returns 0 if file is found, otherwise 
        rc, output, m = self.shell.cmd1('%s%s %s' % (self.__get_cmd_prefix_hack__(),cmd,jobid),allowed_exit=[0,1,255])

        return rc

    def setup(self):
        # Source the setup script in the arc directory
        # print 'Soursing the setup file.'
        cmd = 'cd %s/.. &> /dev/null ; source ./setup.sh &> /dev/null ; cd - &> /dev/null' % self.__get_cmd_prefix_hack__()
        
        rc = self.shell.system(cmd)

        if rc == 0:
            return True
        else:
            logger.warning( "Failed to source arc setup script.")
            return False

    def update_crls(self):
        # Update the list of approved sites for your user

        cmd = 'grid-update-crls'

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(False,True),cmd),allowed_exit=[0,255])

        if rc == 0:
            return True
        else:
            logger.warning( "Failed to run command grid-update-crls: %s" % ( output ) )


class NG(IBackend):
    '''NG backend'''

    _schema = Schema( Version( 1, 1 ), {\
      "requirements" : ComponentItem( category = "ng_requirements",
         defvalue = "NGRequirements",
         doc = "Requirements for selecting execution host" ),
      "CE" : SimpleItem(defvalue="",typelist=['str'], doc='Request specific cluster(s)'),
      "RejectCE" : SimpleItem(defvalue="",typelist=['str'], doc='Reject specific cluster(s)'),
      "submit_options" : SimpleItem( defvalue = [], typelist=['str'], sequence = 1,
         doc = "Options passed to Condor at submission time" ),
      "id" : SimpleItem( defvalue = "", typelist=['str'], protected = 1, copyable = 0,
         doc = "NG jobid" ),
      "status" : SimpleItem( defvalue = "", typelist=['str'], protected = 1, copyable = 0,
         doc = "NG status"),
      "reason" : SimpleItem( defvalue = "", typelist=['str'], protected = 1, copyable = 0,
         doc = "Failure season"),
      "cputime" : SimpleItem( defvalue = "", typelist=['str'], protected = 1, copyable = 0,
         doc = "CPU time used by job"),
      "requestedcputime" : SimpleItem( defvalue = "", typelist=['str'], protected = 1, copyable = 0,
         doc = "Requested CPU time"),
      "actualCE" : SimpleItem( defvalue = "", typelist=['str'], protected = 1, copyable = 0,
         doc = "Machine where job has been submitted" ),
      'monInfo': SimpleItem(defvalue="",typelist=['str'], protected=1,copyable=0,hidden=0,
         doc='Hidden information of the monitoring service.'),
      "queue" : SimpleItem( defvalue = "", typelist=['str'], protected = 1, copyable = 0,
         doc = "Queue where job has been submitted" ),
      'middleware' : SimpleItem(defvalue="",typelist=['str'], protected=0,copyable=1,doc='Middleware type'),
      'check_availability'   : SimpleItem(defvalue = False, typelist=['bool'], 
                                          doc = 'Check availability of DQ2 data on NG before submission'),
      'clean' : SimpleItem( defvalue=[],typelist=['str'],sequence=1, doc= "Files to be cleaned after job")
      } )


    _category = 'backends'
    _name =  'NG'
    _exportmethods = ['check_proxy','peek','update_crls','setup']
    
    def __init__(self):
        super(NG,self).__init__()
        if not self.middleware:
            self.middleware = 'ARC'

        # dynamic requirement object loading 
        try:
            reqName1   = config['Requirements']
            reqName   = config['Requirements'].split('.').pop()
            reqModule = __import__(reqName1, globals(), locals(), [reqName1]) 
            reqClass  = vars(reqModule)[reqName]
            self.requirements = reqClass()

            logger.debug('load %s as NGRequirements' % reqName)
        except:
            logger.debug('load default NGRequirements')
            pass


    def master_submit(self,rjobs,subjobconfigs,masterjobconfig):
        '''Submit the master job to the grid'''

        mt = self.middleware.upper()

        job = self.getJobObject()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return False 

        if len(job.subjobs) == 0:
            gjid = IBackend.master_submit(self,rjobs,subjobconfigs,masterjobconfig)
            return gjid
        else:
            return self.master_bulk_submit(rjobs,subjobconfigs,masterjobconfig)

    def master_resubmit(self,rjobs):
        '''Resubmit the master job to the grid'''

        mt = self.middleware.upper()

        job = self.getJobObject()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return False

        if len(job.subjobs) == 0:
            return IBackend.master_resubmit(self,rjobs)
        else:
            return self.master_bulk_resubmit(rjobs)

    def master_bulk_submit(self,rjobs,subjobconfigs,masterjobconfig):
        '''NG bulk submission'''

        from Ganga.Core import IncompleteJobSubmissionError
        from Ganga.Utility.logging import log_user_exception
        
        assert(implies(rjobs,len(subjobconfigs)==len(rjobs)))

        mt = self.middleware.upper()

        job = self.getJobObject()
        inpw = job.getInputWorkspace() 

        # prepare the master job (i.e. create shared inputsandbox, etc.)
        master_input_sandbox=IBackend.master_prepare(self,masterjobconfig)

        abspath = os.path.abspath(master_input_sandbox[0])
        sandbox_s = os.path.getsize(abspath)
        
        master_input_sandbox_tmp= []
        if sandbox_s > config['BoundSandboxLimit']:
          master_input_sandbox_tmp += [grids[mt].upload(master_input_sandbox[0])]
        else:
          master_input_sandbox_tmp += [abspath]
          
        master_input_sandbox = master_input_sandbox_tmp

        """
        # arclib 
        # prepare the subjobs, jdl repository before bulk submission
        xrslStrings=[]
        i = 0
        for sc,sj in zip(subjobconfigs,rjobs):
            try:
                sj.name=job.name+'_'+str(i)
                i = i+1
                logger.info("preparing subjob %s" % sj.getFQID('.'))
                xrslStrings += [sj.backend.preparejob(sc,master_input_sandbox,True)]
                
            except Exception,x:
                log_user_exception()
                raise IncompleteJobSubmissionError(sj.id,str(x))
            
        """

        # prepare the subjobs, xrsl repository before bulk submission
        xrslStrings=[]
        i = 0
        group_area_srm = None
        for sc,sj in zip(subjobconfigs,rjobs):
          try:
            if sj.application.group_area.name:
              if i == 0 and not sj.application.group_area.name.startswith('http'):
                abspath = os.path.abspath(sj.application.group_area.name)
                groupArea_s = os.path.getsize(abspath)
              
                if groupArea_s > config['BoundSandboxLimit'] and i == 0: 
                  group_area_srm = grids[mt].upload(sj.application.group_area.name)
                  sj.application.group_area.name = group_area_srm
                  sj.backend.clean += [group_area_srm]

              elif group_area_srm:
                # no uploading, sut add the groupArea
                sj.application.group_area.name = group_area_srm
                sj.backend.clean += [group_area_srm]

            sj.name=job.name+'_'+str(i)
            i = i+1
            logger.info("preparing subjob %s" % sj.getFQID('.'))
            xrslStrings += [sj.backend.preparejob(sc,master_input_sandbox,True)]
                
          except Exception,x:
            log_user_exception()
            raise IncompleteJobSubmissionError(sj.id,str(x))

        # Join xrsls in one file
        
        xrslString = '+'
        for x in xrslStrings:
            if x:
                xrslString +='(' + x + ')'
      
        for sj in rjobs:
            sj.updateStatus('submitting')

        if len(xrslStrings) >= 1:
            xrslpath = inpw.writefile(FileBuffer('ganga.xrsl',xrslString))
            master_jid = grids[mt].native_master_submit(xrslpath,self.CE,self.RejectCE)
        else:
            logger.error('No valid xrsl. Try to run j.backend.update_crls()')
            return False
        
        if not master_jid:
            logger.error('Job submission failed not master_jid')
            return False
            #raise IncompleteJobSubmissionError(job.id,'native master job submission failed.')
        else:
            i = 0
            for sj in rjobs:
                i = i + 1
                if i>len(master_jid):
                    logger.warning("Not enough job IDs for subjobs - job submission most likely incomplete.")
                    continue
                sj.backend.id=master_jid[i-1]
                # job submitted update monitorint
                #print 'sending moinitoring info'
                sj.getMonitoringService().submit()
                #print 'update status submitted '
                sj.updateStatus('submitted')
                
        return True

    def peek(self, filename = None ):
        '''Get the jobs logging info'''

        job = self.getJobObject()

        logger.info('Getting logging info of job %s' % job.getFQID('.'))

        mt = self.middleware.upper()

        if not filename:
            filename = 'stdout'
            print 'Getting standard output stdout'
            verbosity = ' '
            outfile = 'tmpstdout.txt'
        elif filename == 'errors':
            print 'Getting the grid manager log file errors'
            verbosity = '-l'
            outfile = 'tmperrors.txt'
        elif filename == 'stdout':
            outfile = 'tmpstdout.txt'
            print 'Getting standard output stdout'
            verbosity = ' '
        else:
            print 'You have specified wrong file name.'
            print 'Use:'
            print 'peek() -> to get the standard output stdout  -  Default '
            print 'peek(\'stdout\') -> to get the standard output stdout'
            print 'peek(\'errors\') -> to get the grid manager logfile'
            return None

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return None 

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return None 

        outfile = job.outputdir + outfile
        
        # successful logging info fetching returns a file path to the information
        loginfo_output = grids[self.middleware.upper()].get_loginfo(self.id,outfile,verbosity)

        if loginfo_output:
            job.viewFile( path = loginfo_output )
            return None
        else:
            logger.info('Getting logging info of job %s failed.' % job.getFQID('.'))
            return None 

    def submit(self,subjobconfig,master_input_sandbox):
        '''Submit the job to the grid'''
        
        mt = self.middleware.upper()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return None
        
        xrslpath = self.preparejob(subjobconfig,master_input_sandbox,False)

        if xrslpath is None:
            logger.warning('Empty xrsl file returned. No data files present for selected dataset?')
            return None
                    
        self.id = grids[mt].submit(xrslpath,self.CE,self.RejectCE,self.requirements.timeout)
        return not self.id is None

    def kill(self):
        '''Kill the job'''

        job   = self.getJobObject()

        logger.info('Killing job %s' % job.getFQID('.'))

        mt = self.middleware.upper()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return False 

        if not self.id:
            logger.warning('Job %s is not running.' % job.getFQID('.'))
            return False

        return grids[self.middleware.upper()].cancel(self.id)


    def preparejob(self,jobconfig,master_input_sandbox,subjob):
      """Prepare NG job description file xrsl"""

      mt = self.middleware.upper()
      
      job = self.getJobObject()

      # prepare monitoring
      mon = job.getMonitoringService()

      self.monInfo = {}

      # set the monitoring file by default to the stdout
      if type(self.monInfo) is type({}):
          self.monInfo['remotefile'] = 'stdout' 
      
      inpw = job.getInputWorkspace()
      packed_files = jobconfig.getSandboxFiles() + Sandbox.getGangaModulesAsSandboxFiles(mon.getSandboxModules())
      inbox = job.createPackedInputSandbox( packed_files ) 
            
      inpDir = job.getInputWorkspace().getPath()
      outDir = job.getOutputWorkspace().getPath()
      
      infileList = []
      arguments = []

      exeCmdString = jobconfig.getExeString()
      exeString = jobconfig.getExeString().strip()
      

      for filePath in inbox:
         if not filePath in infileList:
           infileList.append( filePath )
            
      for filePath in master_input_sandbox:
         if not filePath in infileList:
             infileList.append( filePath )
             if filePath.startswith('srm'):
               self.clean += [filePath]

      fileList = []
      for filePath in infileList:
         fileList.append( os.path.basename( filePath ) )   

      # inputfiles
      if job.inputdata and job.inputdata._name == 'ATLASLocalDataset':
          for filePath in job.inputdata.names:
              infileList.append( filePath )
      elif job.inputdata and job.inputdata._name == 'NGInputData':
          for f in job.inputdata.names:
            infileList.append(f)                    
      elif job.inputdata and job.inputdata._name == 'DQ2Dataset':

          # prepare the dataset namelist with tids - needed for check availability and paths
          ds_tid = getTidDatasetnames(job.inputdata.dataset)
      
          # Check for availability on DQ2? Avoids submitting jobs where dataset is empty
          lfnlist=[]
          if self.check_availability:
            remove_flist=[]
            remove_glist=[]
            for f in range(len(job.inputdata.names)): 
                if len(ds_tid) > 1:
                  lfn_ds = matchLFNtoDataset(ds_tid,job.inputdata.names[f],job.application.atlas_release)
                  lfn = getLFCurl(lfn_ds,job.inputdata.names[f])
                else:
                  lfn = getLFCurl(ds_tid[0],job.inputdata.names[f])

                rc = grids[self.middleware.upper()].check_dq2_file_avaiability(lfn,self.id)
                if rc==1:
                  logger.warning("DQ2 input file %s not present on NG.",job.inputdata.names[f])
                  remove_flist.append(job.inputdata.names[f])
                  remove_glist.append(job.inputdata.guids[f])
                else:
                  lfnlist += [lfn]
                  
            if len(lfnlist)==0:
              logger.warning("No input files available on NG")
              return None
                
            if len(remove_flist)>0:
              logger.warning("Removing input files not present on NG")
              for f in remove_flist:
                job.inputdata.names.remove(f)
              for g in remove_glist:
                job.inputdata.guids.remove(g)

          else:
            for f in range(len(job.inputdata.names)):
              if len(ds_tid) > 1:
                lfn_ds = matchLFNtoDataset(ds_tid,job.inputdata.names[f], job.application.atlas_release)
                lfn = getLFCurl(lfn_ds,job.inputdata.names[f])
                lfnlist += [lfn]
              else:
                lfn = getLFCurl(ds_tid[0],job.inputdata.names[f])
                lfnlist += [lfn]
              
          # number of input files
          arguments += [len(job.inputdata.names)]

          for i in range(len(job.inputdata.names)):
              arguments += [job.inputdata.names[i]]
              arguments += [job.inputdata.guids[i]]

          for f in lfnlist:
            infileList.append(f)

      #print 'job info of monitoring service: %s' % str(self.monInfo)
      #print 'mon.getWrapperScriptConstructorText() ',mon.getWrapperScriptConstructorText()
            
      commandList = [
         "#!/usr/bin/env python",
         "# Interactive job wrapper created by Ganga",
         "# %s" % ( time.strftime( "%c" ) ),
         "",
         inspect.getsource( Sandbox.WNSandbox ),
         "",
         "import os, sys",
         "import time",
         "wdir = os.getcwd()",
         #"print 'wdir ', wdir",
         "sys.path.insert(0,os.path.join(wdir,PYTHON_DIR))",
         #"print 'PYTHON_DIR ',sys.path",
         #"os.environ['PATH'] = '.:'+os.environ['PATH']"
         "",
         "options = []",
         "print len(sys.argv)",
         "for i in range(len(sys.argv)):",
         "   options += [sys.argv[i]]",
         "print options[1:]",
         "opt = \" \".join(options[1:])",
         "print 'opt ', opt",
         "startTime = time.strftime"\
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
         "",
         "for inFile in %s:" % str( fileList ),
         "   getPackedInputSandbox( inFile )",
         "",
         "%s" %mon.getWrapperScriptConstructorText(), 
         "monitor = createMonitoringObject()",
         "monitor.start()",
         "monitor.progress()",
         "",
         "result = os.system( '%s ' + opt )" % exeCmdString,
         "",
         "monitor.progress()",
         "endTime = time.strftime"\
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
         "print '\\nJob start: ' + startTime",
         "print 'Job end: ' + endTime",
         "print 'Exit code: %s\\n' % str( result )",
         "print 'monitoring to be stopped '", 
         "monitor.stop(result)",
         "print 'monitoring stopped'",
         "" ]
      

      commandString = "\n".join( commandList )      
      if job.name:
         name = job.name
      else:
         name = job.application._name 
      wrapperName = "_".join( [ "Ganga", str( job.id ), name ] )
      
      wrapper = job.getInputWorkspace().writefile\
         ( FileBuffer( wrapperName, commandString), executable = 1 )

      infile = []
      for f in infileList:
         inf = f.split("/")[-1]
         infile.append("(" + inf + " " + f + ")")

      # Add wrapper to inputfiles 
      ex = wrapper.split("/")[-1]
      infile.append("(" + ex + " " + wrapper + ")")
      infileString = ''.join(infile)   
      
      xrslDict = \
         { 
         'executable' : ex,
         'stdout' : 'stdout',
         'join'   : 'yes',
         'gmlog' : 'gmlog'         
         }
      
      # pass athena job options through arguments, must come before the output files
      if jobconfig.env.has_key('ATHENA_OPTIONS'):
          sopt = jobconfig.env['ATHENA_OPTIONS'].split(' ')    
          arguments += [len(sopt)]
          for op in sopt:
              arguments += [op]
          #xrslList.append("(ATHENA_OPTIONS \'%s\'" % str( jobconfig.env['ATHENA_OPTIONS'] ) + ")")

          
      if jobconfig.requirements and jobconfig.requirements.runtimeenvironment:
          self.requirements.runtimeenvironment = jobconfig.requirements.runtimeenvironment  

      outfile = []
      if xrslDict['stdout']:
         outfile.append("(" + "stdout.gz" + " \"\")")

      srm_endpoint = ''
      output_lfn = ''
      if job.outputdata and job.outputdata._name=="DQ2OutputDataset":
          # Get TiersOfATLASChache
          #tiersofatlas = getTiersOfATLASCache() 
          # Set a default site name
          #sitename = 'NDGFT1DISK'
          #spacetoken = 'ATLASUSERDISK'
          sitename = 'NDGF-T1_USERDISK'
          spacetoken = 'USERDISK'

          if job.outputdata.location!='':
            sitename = job.outputdata.location

          srm_endpoint =  getSRMendpoint(sitename)
          
          if srm_endpoint=='':
            logger.warning("Couldn't find SRM information for sitename %s in TiersOfAtlasCache, setting NDGF default" % sn)
            srm_endpoint = 'srm://srm.ndgf.org;spacetoken=ATLASUSERDISK/atlas/disk/'       

          if jobconfig.env.has_key('OUTPUT_LFN'):
              output_lfn = jobconfig.env['OUTPUT_LFN']
          
      if job.outputdata and job.outputdata._name=="DQ2OutputDataset":
          arguments += [len(jobconfig.outputbox)]
          for fn in range(len(jobconfig.outputbox)):
              #users/DietrichLiko/ganga/users.DietrichLiko.ganga.20.20080410/
              gridPlacementString = srm_endpoint + output_lfn + job.outputdata.outputdata[fn]
              outfile.append("(" + jobconfig.outputbox[fn] + " \""+gridPlacementString+"\")" )
              arguments += [jobconfig.outputbox[fn]]
              arguments += [job.outputdata.outputdata[fn]]
              arguments += [job.outputdata.datasetname]
              
          outfile.append("(OutputFiles.xml \"\")" )
      #else:
      #    arguments += [0]

      if job.outputdata and job.outputdata._name == "ATLASOutputDataset":
          for f in job.outputdata.outputdata:
              outfile.append("( " + f + " \"\")" ) 

      if job.application._name == 'Executable':
          arguments = job.application.args
          
      arglist = ""
      for arg in arguments:
          arglist += " \"" + str(arg) + "\""

      if len(arguments) > 0:
          xrslDict['arguments'] = arglist   
       
      outfileString = ''.join(outfile)

      # add group area to inputfiles
      if jobconfig.env.has_key('GROUP_AREA_REMOTE'):
        ga = jobconfig.env['GROUP_AREA_REMOTE'].split('/')[-1]
        infileString += "(" + ga + " " + jobconfig.env['GROUP_AREA_REMOTE'] + ")"

      if jobconfig.env.has_key('GROUP_AREA'):       
        infileString += "(" + jobconfig.env['GROUP_AREA'] + " " + job.application.group_area.name + ")"
          
      if infileString:
         xrslDict[ 'inputfiles' ] = infileString

      if outfileString: 
         xrslDict[ 'outputfiles' ] = outfileString         

      if job.name:
          xrslDict[ 'jobname' ] = job.name     
      elif job.inputdata and job.inputdata._name == 'DQ2Dataset' and job.inputdata.dataset:
          xrslDict[ 'jobname' ] = job.inputdata.dataset[0]

      xrslList = [
         "&" 
         "(* XRSL File created by Ganga *)",
         "(* %s" % ( time.strftime( "%c" ) ) + " *)"]
      for key, value in xrslDict.iteritems():
         xrslList.append( "(%s = %s)" % ( key, value ) )
      ## User requiremants   
      xrslList.append( self.requirements.convert() )

      ## Athena max events and other optiions must be specified through
      ## envrionment variables.
      ## Treated specially here.
      if  jobconfig.env: 
          #print "Setting environment in xrsl:"
          #print "ATHENA_MAX_EVENTS: ", jobconfig.env['ATHENA_MAX_EVENTS']
          xrslList.append( "(environment = ")

          # ATHENA stuff
          if jobconfig.env.has_key('ATHENA_MAX_EVENTS') and jobconfig.env['ATHENA_MAX_EVENTS']>-1:
              xrslList.append("(ATHENA_MAX_EVENTS  %s" % str( jobconfig.env['ATHENA_MAX_EVENTS'] ) + ")")
          if jobconfig.env.has_key('USER_AREA'):
              xrslList.append("(USER_AREA  %s" % str( jobconfig.env['USER_AREA'] ) + ")")    
          if jobconfig.env.has_key('ATHENA_USERSETUPFILE') and jobconfig.env['ATHENA_USERSETUPFILE']!="":
              xrslList.append("(ATHENA_USERSETUPFILE  %s" % str( jobconfig.env['ATHENA_USERSETUPFILE'] ) + ")")  
          if jobconfig.env.has_key('ATLAS_RELEASE'):
              xrslList.append("(ATLAS_RELEASE  %s" % str( jobconfig.env['ATLAS_RELEASE'] ) + ")")
          if jobconfig.env.has_key('GROUP_AREA'):
              xrslList.append("(GROUP_AREA  %s" % str( jobconfig.env['GROUP_AREA'] ) + ")")
          if jobconfig.env.has_key('GROUP_AREA_REMOTE'):
              xrslList.append("(GROUP_AREA_REMOTE  %s" % str( jobconfig.env['GROUP_AREA_REMOTE'] ) + ")")                 
          if jobconfig.env.has_key('ATHENA_EXE_TYPE'):
              xrslList.append("(ATHENA_EXE_TYPE  %s" % str( jobconfig.env['ATHENA_EXE_TYPE'] ) + ")")

          # ROOT env
          if jobconfig.env.has_key('ROOTSYS'):
              xrslList.append("(ROOTSYS  %s" % str( jobconfig.env['ROOTSYS'] ) + ")")
          
          xrslList.append(" ) ") 
      
      xrslString = "\n".join( xrslList )

      logger.debug('NG xrslString: %s ' % xrslString)

      
      
      if subjob:
          return xrslString
      else:
          return inpw.writefile(FileBuffer('ganga.xrsl',xrslString))
          #return xrslString
      
    def updateGangaJobStatus(job,status):
        '''map backend job status to Ganga job status'''

        # FIXME FINISHED-> test exitcode
        if status == "INLRMS:Q":
            job.updateStatus('submitted')
        elif status == "ACCEPTING" or \
             status == "ACCEPTED" or \
             status == "PREPARING" or \
             status == "PREPARED" or \
             status == "SUBMITTING":
            job.updateStatus('submitted')
        elif status.startswith("INLRMS") or \
             status == "EXECUTED" or \
             status == "FINISHING" or \
             status == "KILLING":
            job.updateStatus('running')
        elif status == "FINISHED":
            job.updateStatus('completed')
        elif status == "FAILED" or \
             status == "KILLED" or \
             status == "DELETED" or \
             status == "FINISHED":
            job.updateStatus('failed')

        elif status == 'Cleared':
            if job.status in ['completed','failed']:
                logger.warning('Monitoring loop should not have been called for job %d as status is already %s',job.id,job.status)
                return 
            logger.warning('The job %d has reached unexpected the Cleared state and Ganga cannot retrieve the output.',job.id)
            job.updateStatus('failed')
     
        else:
            logger.warning('Unexpected job status "%s"',info['status'])

    updateGangaJobStatus = staticmethod(updateGangaJobStatus)

    def master_updateMonitoringInformation(jobs):
        '''Main Monitoring loop'''
        
        emulated_bulk_jobs = [] 
        native_bulk_jobs   = []

        for j in jobs:
            mt = j.backend.middleware.upper()

            if len(j.subjobs) == 0:
                emulated_bulk_jobs.append(j)
            else:
                native_bulk_jobs.append(j)

        # involk normal monitoring method for normal jobs
        IBackend.master_updateMonitoringInformation(emulated_bulk_jobs)

        # involk special monitoring method for glite bulk jobs
        NG.master_bulk_updateMonitoringInformation(native_bulk_jobs)

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

    def updateMonitoringInformation(jobs):
        '''Monitoring loop for normal jobs'''
        jobdict   = dict([ [job.backend.id,job] for job in jobs if job.backend.id ])

        ## divide jobs into classes based on the middleware type
        jobclass  = {}
        for key in jobdict:
            mt = jobdict[key].backend.middleware.upper()
            if not jobclass.has_key(mt):
                jobclass[mt] = [key]
            else:
                jobclass[mt].append(key)

        ## loop over the job classes 
        for mt in jobclass.keys():

            if not config['%s_ENABLE' % mt]:
                continue 

            ## loop over the jobs in each class
            for info in grids[mt].status(jobclass[mt]):
                job = jobdict[info['id']]
         
                if job.backend.actualCE != info['destination']:
                    logger.info('job %s has been assigned to %s',job.getFQID('.'),info['destination'])
                    job.backend.actualCE = info['destination']

                if job.backend.queue != info['name']:
                    logger.info('job %s has been assigned to queue %s',job.getFQID('.'),info['name'])
                    job.backend.queue = info['name']    

                if job.backend.requestedcputime != info['requestedcputime']:
                    job.backend.requestedcputime = info['requestedcputime']

                if job.backend.cputime != info['cputime']:
                    job.backend.cputime = info['cputime']    
        
                if job.backend.status != info['status']:
                    logger.info('job %s has changed status to %s',job.getFQID('.'),info['status'])
                    job.backend.status = info['status']
                    job.backend.reason = info['reason']
                    job.backend.exitcode = info['exit']

                    pps_check = True

                    # postprocess of getting job output if the job is done 
                    if ( info['status'] == 'FINISHED' or \
                       info['status'] == 'FAILED' or \
                       info['status'] == 'KILLED' or \
                       info['status'] == 'DELETED' ) and job.status != 'completed':

                        output = True
                        if info['status'] == 'FAILED' or info['status'] == 'KILLED':
                            output = False
                        elif job.outputdata==None:
                            output = False
                            
                        # update to 'running' before changing to 'completing'
                        if job.status == 'submitted':
                            job.updateStatus('running')

                        job.updateStatus('completing')
                        outw = job.getOutputWorkspace()
                        # Post processing of a job
                        lfc_location = ""
                        if job.outputdata!=None:
                          lfc_location=job.outputdata.location
                        pps_check = grids[mt].get_output(job.backend.id,outw.getPath(),output,lfc_location,wms_proxy=False)
                
                    if pps_check:
                        #print 'updateMonitoring info staus ', info['status']
                        NG.updateGangaJobStatus(job,info['status'])
                    else:
                        job.updateStatus("failed")

    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

    def master_bulk_updateMonitoringInformation(jobs):
        '''Monitoring loop for ng bulk jobs'''

        grid = grids['ARC']

        if not grid:
            return

        #jobdict = dict([ [job.id, job] for job in jobs if job.id ])

        final = ['completed','failed','killed']
        job = None
        mt = 'ARC'
        jobdict={}
        all_done = True
        
        for job in jobs:
            jobdict[job.id] = job
            subjobdict = {}
            subjobdict = dict([ [str(subjob.backend.id),subjob] for subjob in job.subjobs ])

            sjids = []
            for sj in job.subjobs:
                sjids += [sj.backend.id]

            all_done = True
            check = False
            sjob = None
            ## loop over the jobs in each class
            for info in grids[mt].status( sjids ):
                sjob = subjobdict[info['id']]
                check = True
                if sjob.backend.actualCE != info['destination']:
                    logger.info('job %s has been assigned to %s',sjob.getFQID('.'),info['destination'])
                    sjob.backend.actualCE = info['destination']
                    
                if sjob.backend.queue != info['name']:
                    logger.info('job %s has been assigned to queue %s',sjob.getFQID('.'),info['name'])
                    sjob.backend.queue = info['name']

                if sjob.backend.requestedcputime != info['requestedcputime']:
                    sjob.backend.requestedcputime = info['requestedcputime']

                if sjob.backend.cputime != info['cputime']:
                    sjob.backend.cputime = info['cputime']    

                if sjob.backend.status != info['status']:
                    logger.info('job %s has changed status to %s',sjob.getFQID('.'),info['status'])
                    sjob.backend.status = info['status']
                    sjob.backend.reason = info['reason']
                    sjob.backend.exitcode = info['exit']

                    pps_check = True
                    if ( info['status'] == 'FINISHED' or \
                       info['status'] == 'FAILED' or \
                       info['status'] == 'KILLED' or \
                       info['status'] == 'DELETED' ) and sjob.status != 'completed':

                        
                        output = True
                        if info['status'] == 'FAILED' or info['status'] == 'KILLED':
                            output = False
                        elif sjob.outputdata==None:
                            output = False
                                                      
                        # update to 'running' before changing to 'completing'
                        if sjob.status == 'submitted':
                            sjob.updateStatus('running')
    
                        sjob.updateStatus('completing')
                        outw = sjob.getOutputWorkspace()
                        # Post processing of a job
                        lfc_location = ""
                        if sjob.outputdata!=None:
                            lfc_location=sjob.outputdata.location                                                
                        pps_check = grids[mt].get_output(sjob.backend.id,outw.getPath(),output,lfc_location,wms_proxy=False)

                    if pps_check:
                        NG.updateGangaJobStatus(sjob,info['status'])
                    else:
                        sjob.updateStatus("failed")

                if sjob:
                    if sjob.status not in final:
                        all_done = False
                    
            if all_done and sjob and check:
              for f in sjob.backend.clean:
                grids[mt].clean_gridfile(f)

        # update master job status
        for jid in jobdict.keys():
            jobdict[jid].updateMasterJobStatus()

    master_bulk_updateMonitoringInformation = staticmethod(master_bulk_updateMonitoringInformation)

    def check_proxy(self):
        '''Update the proxy'''

        mt = self.middleware.upper()
        return grids[mt].check_proxy()

    def setup(self):
        mt = self.middleware.upper()
        return grids[mt].setup()
                    
    def update_crls(self):
        # Update crls list
        mt = self.middleware.upper()
        return grids[mt].update_crls()
    

class NGJobConfig(StandardJobConfig):
    '''Extends the standard Job Configuration with additional attributes'''
   
    def __init__(self,exe=None,inputbox=[],args=[],outputbox=[],env={},inputdata=[],requirements=None):

        self.inputdata=inputdata
        self.requirements=requirements

        StandardJobConfig.__init__(self,exe,inputbox,args,outputbox,env)

    def getArguments(self):
        return ' '.join(self.getArgStrings())
    
    def getExecutable(self):
        
        exe=self.getExeString()
        if os.path.dirname(exe) == '.':
            return os.path.basename(exe)
        else:
            return exe

# initialisation
# function for parsing VirtualOrganisation from ConfigVO
def __getVOFromConfigVO__(file):
    re_vo = re.compile(r'.*VirtualOrganisation\s*=\s*"(.*)"')
    try:
        f = open(file)
        for l in f.readlines():
            m = re_vo.match(l.strip())
            if m:
                f.close()
                return m.groups()[0]
    except:
        raise Ganga.Utility.Config.ConfigError('ConfigVO %s does not exist.' % file )

# configuration preprocessor : avoid VO switching
def __avoidVOSwitch__(opt,val):

    if not opt in ['VirtualOrganisation','ConfigVO']:
        # bypass everything irrelevant to the VO 
        return val
    elif opt == 'ConfigVO' and val == '':
        # accepting '' to disable the ConfigVO
        return val
    else:
        # try to get current value of VO
        if config['ConfigVO']:
            vo_1 = __getVOFromConfigVO__(config['ConfigVO'])
        else:
            vo_1 = config['VirtualOrganisation']

        # get the VO that the user trying to switch to
        if opt == 'ConfigVO':
            vo_2 = __getVOFromConfigVO__(val)
        else:
            vo_2 = val
 
        # if the new VO is not the same as the existing one, raise ConfigError
        if vo_2 != vo_1:
            raise Ganga.Utility.Config.ConfigError('Changing VirtualOrganisation is not allowed in GANGA session.')

    return val

# configuration preprocessor : enabling middleware 
def __enableMiddleware__(opt,val):

    if opt in ['ARC_ENABLE'] and val:
        mt = opt.split('_')[0]
        try:
            if config[opt]:
                logger.info('ARC-%s was already enabled.' % mt)
            else:
                grids[mt] = Grid(mt)
                return grids[mt].active
        except:
            raise Ganga.Utility.Config.ConfigError('Failed to enable NG-%s.' % mt)

    return val

# configuration preprocessor : disabling middleware 
def __disableMiddleware__(opt,val):

    if opt in ['ARC_ENABLE'] and not val:
        mt = opt.split('_')[0]
        grids[mt] = None
        if not config['ARC_ENABLE']:
            logger.warning('No middleware is enabled. NG handler is disabled.')

    return

# configuration postprocessor 
def __preConfigHandler__(opt,val):
    val = __avoidVOSwitch__(opt,val)
    val = __enableMiddleware__(opt,val)
    return val

# configuration postprocessor 
def __postConfigHandler__(opt,val):
    logger.info('%s has been set to %s' % (opt,val))
    __disableMiddleware__(opt,val)
    return

# global variables
logger = getLogger()
config = getConfig('ARC')

# Is this correct ????

config = makeConfig('ARC','NG configuration parameters')
config.addOption('ARC_ENABLE',True,'Turn ON/OFF the ARC middleware support')
arcloc = os.environ['ARC_LOCATION']
config.addOption('ARC_SETUP', arcloc + '/setup.sh','FIXME Environment setup script for ARC middleware')

config.addOption('Requirements','GangaNG.Lib.NG.NGRequirements','FIXME under testing sets the full qualified class name forother specific NG job requirements')

config.addOption('BoundSandboxLimit', 5 * 1024 * 1024,'sets the size limitation of the input sandbox, oversized input sandbox will be pre-uploaded to rls')

# set default values for the configuration parameters
#config['ARC_ENABLE'] = True

# apply preconfig and postconfig handlers
config.attachUserHandler(__preConfigHandler__,__postConfigHandler__)

# a grid list - in case we want more than one middleware, like LCG/gLITE
grids = {'ARC':None}


# Kat test 
if config['ARC_ENABLE']:
    logger.info('ARC_ENABLE in config grid = ARC')
    grids['ARC'] = Grid('ARC')
    config.setSessionValue('ARC_ENABLE',grids['ARC'].active)

configDQ2 = getConfig('DQ2')
    
"""
if config['ARC_ENABLE']:
    grids['ARC'] = Grid('ARC')
    config.addOption('ARC_ENABLE', grids['ARC'].active, 'FIXME')
"""
# $Log: not supported by cvs2svn $
# Revision 1.20  2009/01/13 12:22:06  bsamset
# Added support for giis list, fixed bug in assigning subjob IDs when some jobs failed to submit
#
# Revision 1.19  2008/12/16 14:06:35  pajchel
# stdout.txt -> stdout
#
# Revision 1.18  2008/12/16 13:07:07  bsamset
# Fixed bad lfc registration on NG backend
#
# Revision 1.16  2008/12/08 21:32:53  pajchel
# stdout.txt gzipped
#
# Revision 1.15  2008/12/07 17:22:30  bsamset
# Fixed a buggy equality
#
# Revision 1.14  2008/12/05 20:46:11  pajchel
# dataset name list fix
#
# Revision 1.13  2008/12/05 11:26:23  bsamset
# Take lfc, srm info from ToA, allow for writing to remote storage, add timing info for HammerCloud
#
# Revision 1.12  2008/11/07 13:53:13  pajchel
# file name fix
#
# Revision 1.11  2008/10/27 16:33:38  pajchel
# old file name convention compatibility
#
# Revision 1.10  2008/10/23 11:05:47  bsamset
# Removed debugging message
#
# Revision 1.9  2008/10/22 13:35:08  pajchel
# min. limit for sandbox/groupArea upload 5M
#
# Revision 1.8  2008/10/21 09:28:32  bsamset
# Added ARA support, setup of local databases
#
# Revision 1.7  2008/10/21 09:15:58  pajchel
# creativ bug fixing
#
# Revision 1.4  2008/09/29 11:16:35  bsamset
# Fixed type checking for NG.py and NGRequirements.py
#
# Revision 1.3  2008/07/29 13:19:30  bsamset
# Removed some remaining info messages
#
# Revision 1.2  2008/07/29 09:50:41  bsamset
# Added backup atlasgiis server, fixed autonaming of jobs with DQ2 datasets
#
# Revision 1.1  2008/07/17 16:41:29  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.22  2008/06/06 19:24:25  pajchel
# prestage of tar balls
#
# Revision 1.21  2008/05/20 18:58:47  pajchel
# Minitoring updates
#
# Revision 1.19  2008/05/05 12:09:56  pajchel
# updates
#
# Revision 1.17  2008/04/29 15:33:29  pajchel
# dq2 output dataset
#
# Revision 1.1  2008/03/31 20:22:53  bsamset
# Added some DQ2 code, made for 4.4.0 release, needs updates!
#
# Revision 1.14  2007/05/31 19:07:01  bsamset
# Catch error when submission crashes some way into subjobs. Needs more work, but now at least doesn't give a crash.
#
# Revision 1.13  2007/05/30 14:39:32  pajchel
# Subjob names and error message when invalig xrsl.
#
# Revision 1.12  2007/05/03 07:47:52  bsamset
# Changed Use j.backend.check_proxy() to Use gridProxy.renew()
#
# Revision 1.11  2007/04/21 15:44:59  pajchel
# Fix runtimeenvironment and cputime.
#
# Revision 1.10  2007/04/19 09:56:37  bsamset
# Added function update_crls
#
# Revision 1.9  2007/04/13 12:05:01  pajchel
# Added backend.peek()
#
# Revision 1.8  2007/04/13 11:31:48  bsamset
# Removed ngls functionality temporarily
#
# Revision 1.7  2007/04/10 06:16:44  pajchel
# work on loginfo (ngcat)
#
# Revision 1.6  2007/03/30 14:11:20  pajchel
# Bulk submission added.
#
# Revision 1.5  2007/03/22 19:19:13  bsamset
# Fixed a problem with no dataset class jobs in NG.py
#
# Revision 1.4  2007/03/22 14:58:18  bsamset
# Added default job name for nameless DQ2 jobs.
#
# Revision 1.3  2007/03/19 18:09:36  bsamset
# Several bugfixes, added arc middleware as external package
#
# Revision 1.1  2007/02/28 13:45:11  bsamset
# Initial relase of GangaNG
#

