###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: NG_DQ2.py,v 1.1 2008-07-17 16:41:29 moscicki Exp $
###############################################################################
#
# NG backend
#
# Maintained by the Oslo group (B. Samset, K. Pajchel)
#
# Date:   January 2007

import os, sys, re, tempfile, urllib, imp
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

from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.GridShell import getShell
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import isStringLike

#from GangaNG.Lib.NG.NGShell import getShell 

def getTiersOfATLASCache():
    """Download TiersOfATLASCache.py"""
    
    url = 'http://atlas.web.cern.ch/Atlas/GROUPS/DATABASE/project/ddm/releases/TiersOfATLASCache.py'
    local = os.path.join(os.environ['PWD'],'TiersOfATLASCache.py')
    try:
        urllib.urlretrieve(url, local)
    except IOError:
        print 'Failed to download TiersOfATLASCache.py'
        
    try:
        tiersofatlas = imp.load_source('',local)
    except SyntaxError:
        print 'Error loading TiersOfATLASCache.py'
        sys.exit(EC_UNSPEC)
            
    return tiersofatlas
                                   
class Grid:
    '''Helper class to implement grid interaction'''

    middleware  = 'ARC'

    credential = None


    def __init__(self,middleware='ARC'):

        self.active=False

        self.middleware = middleware.upper()

#       check that UI has been set up
#       start up a shell object specific to the middleware
        self.shell = getShell(self.middleware)

        if not self.shell:
            logger.warning('ARC-%s UI has not been configured. The plugin has been disabled.' % self.middleware)
            return

#       create credential for this Grid object
        # print "Middleware active, check proxy"
        self.active = self.check_proxy()

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

    def submit(self,xrslpath,ce=None,rejectedcl=None):
        '''Submit a XRSL file to NG'''

        cmd = 'ngsub '
        
        if not self.active:
            logger.warning('NG plugin not active.')
            return

        if not self.credential.isValid():
            logger.warning('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return

        # Select/reject clusters
        clusters = ''
        if ce:
            #print "Making cluster list"
            #print ce
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

    def native_master_submit(self,xrslpath,ce=None,rejectedcl=None):
        '''Native bulk submission supported by GLITE middleware.'''
        # Bulk sumission is supported in NG, but the XRSL files need some care.

        cmd = 'ngsub '
        if not self.active:
            logger.warning('NG plugin not active.')
            return

        if not self.credential.isValid():
            logger.debug('GRID proxy not valid. Use gridProxy.renew() to renew it.')
            return

        # Select/reject clusters
        clusters = ''
        if ce:
            #print "Making cluster list"
            #print ce
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
            print "No jobs in status"
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

    def get_output(self,jobid,directory,wms_proxy=False):
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

#       some versions of LCG middleware create an extra output directory (named <uid>_<jid_hash>) 
#       inside the job.outputdir. Try to match the jid_hash in the outdir. Do output movememnt
#       if the <jid_hash> is found in the path of outdir.

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

        return True

    def cancel(self,jobid):
        '''Cancel a job'''
        # ngkill -k keep output so it can be downloaded.
        cmd = 'ngkill -k'
        
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

    def check_dq2_file_avaiability(self,lfn,rls,jobid):
        # Check if a file is available on NorduGrid

        cmd = 'globus-rls-cli query wildcard lrc lfn "'+lfn+'" '+rls

        # Returns 0 if file is found, otherwise 1
        rc, output, m = self.shell.cmd1('%s%s %s' % (self.__get_cmd_prefix_hack__(),cmd,jobid),allowed_exit=[0,1,255])

        # Use return code instead
        #match = re.search('(gsiftp:\S+)',output)
        #if match: return match.group(1)

        return rc

    def setup(self):
        # Source the setup script in the arc directory
        cmd = 'cd %s/.. ; source ./setup.sh ; cd -' % self.__get_cmd_prefix_hack__()
        
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
            return False

class NGRequirements(GangaObject):
   '''Helper class to group requirements'''

   _schema = Schema(Version(1,0), {
      "runtimeenvironment": SimpleItem(defvalue=[],sequence=1,doc='Runtimeenvironment'),
      "cputime" : SimpleItem( defvalue = "30", doc = "Requested cpu time" ),
      "walltime" : SimpleItem( defvalue = "30", doc = "Requested wall time" ),
      "memory" : SimpleItem( defvalue = 500, doc = "Mininum virtual  memory" ),
      "disk" : SimpleItem( defvalue = 500, doc = "Minimum memory" ),
      "other" : SimpleItem( defvalue=[], sequence=1, doc= "Other requirements" )
      } )

   _category = 'ng_requirements'
   _name = 'NGRequirements'

   def __init__(self):
      
      super(NGRequirements,self).__init__()

   def convert( self):
      '''Convert the condition(s) to a xrsl specification'''
      requirementList = []

      if self.cputime:
         requirementList.append( "(cputime = %smin" % str( self.cputime ) + ")")
         
      if self.walltime:
         requirementList.append( "(walltime = %smin" % str( self.walltime ) + ")")   

      if self.memory:
         requirementList.append( "(memory = %s" % str( self.memory ) + ")")

      if self.disk:
         requirementList.append\
            ( "(disk = %s" % str( self.disk ) + ")" )

      if self.other:
         requirementList.extend( self.other )

         
      if self.runtimeenvironment:
          for re in self.runtimeenvironment:
              requirementList.append( '(runtimeenvironment = %s' % str( re ) + ')')

      requirementString = "\n".join( requirementList )

      logger.debug('NG requirement string: %s' % requirementString)

      return requirementString

class NG(IBackend):
    '''NG backend'''

    _schema = Schema( Version( 1, 1 ), {\
      "requirements" : ComponentItem( category = "ng_requirements",
         defvalue = "NGRequirements",
         doc = "Requirements for selecting execution host" ),
      "CE" : SimpleItem(defvalue="",doc='Request specific cluster(s)'),
      "RejectCE" : SimpleItem(defvalue="",doc='Reject specific cluster(s)'),
      "submit_options" : SimpleItem( defvalue = [], sequence = 1,
         doc = "Options passed to Condor at submission time" ),
      "id" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "NG jobid" ),
      "status" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "NG status"),
      "reason" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Failure season"),
      "cputime" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "CPU time used by job"),
      "requestedcputime" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Requested CPU time"),
      "actualCE" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Machine where job has been submitted" ),
      "queue" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Queue where job has been submitted" ),
      'middleware' : SimpleItem(defvalue=None,protected=0,copyable=1,doc='Middleware type'),
      "RLS" : SimpleItem( defvalue = "rls://atlasrls.nordugrid.org:39281",
         doc = "RLS dserver" ),
      'check_availability'   : SimpleItem(defvalue = False,
                                          doc = 'Check availability of DQ2 data on NG before submission')
      } )

    _category = 'backends'
    _name =  'NG'
    _exportmethods = ['check_proxy','peek','update_crls','setup']
    
    def __init__(self):
        super(NG,self).__init__()
        if not self.middleware:
            self.middleware = 'ARC'

    def master_submit(self,rjobs,subjobconfigs,masterjobconfig):
        '''Submit the master job to the grid'''

        mt = self.middleware.upper()

        job = self.getJobObject()

        if not config['%s_ENABLE' % mt]:
            logger.warning('Operations of %s middleware are disabled.' % mt)
            return False 

        if len(job.subjobs) == 0:
            return IBackend.master_submit(self,rjobs,subjobconfigs,masterjobconfig)
        else:
            return self.master_bulk_submit(rjobs,subjobconfigs,masterjobconfig)

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

        # Join xrsls in one file
        xrslString = '+'
        for x in xrslStrings:
            if x:
                xrslString +='(' + x + ')'

        for sj in rjobs:
            sj.updateStatus('submitting')

        if len(xrslString) > 1:        
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
                    continue
                sj.backend.id=master_jid[i-1]
                sj.updateStatus('submitted')
                
        return True

    def peek(self, filename = None ):
        '''Get the jobs logging info'''

        job = self.getJobObject()

        logger.info('Getting logging info of job %s' % job.getFQID('.'))

        mt = self.middleware.upper()

        if not filename:
            filename = 'stdout.txt'
            print 'Getting standard output stdout.txt'
            verbosity = ' '
            outfile = 'tmpstdout.txt'
        elif filename == 'errors':
            print 'Getting the grid manager log file errors'
            verbosity = '-l'
            outfile = 'tmperrors.txt'
        elif filename == 'stdout.txt':
            outfile = 'tmpstdout.txt'
            print 'Getting standard output stdout.txt'
            verbosity = ' '
        else:
            print 'You have specified wrong file name.'
            print 'Use:'
            print 'peek() -> to get the standard output stdout.txt  -  Default '
            print 'peek(\'stdout.txt\') -> to get the standard output stdout.txt'
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
                    
        self.id = grids[mt].submit(xrslpath,self.CE,self.RejectCE)
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
      inpw = job.getInputWorkspace()
      inbox = job.createPackedInputSandbox( jobconfig.getSandboxFiles() )
      inpDir = job.getInputWorkspace().getPath()
      outDir = job.getOutputWorkspace().getPath()
      #print 'inpDir ', inpDir
      #print 'outDir ', outDir
      #print 'jobconfig.inputbox '

      #for f in jobconfig.inputbox:
      #    print f
          
      infileList = []

      exeCmdString = jobconfig.getExeCmdString()
      #print "exeCmdString " + exeCmdString
      exeString = jobconfig.getExeString().strip()
       
      for filePath in inbox:
         if not filePath in infileList:
            #print 'inbox ', filePath
            infileList.append( filePath )
            
      for filePath in master_input_sandbox:
         if not filePath in infileList:
            #print  'master_input_sandbox', filePath
            infileList.append( filePath )

      fileList = []
      for filePath in infileList:
         #print 'infileList', filePath
         fileList.append( os.path.basename( filePath ) )   

      # inputfiles
      if job.inputdata and job.inputdata._name == 'ATLASLocalDataset':
          for filePath in job.inputdata.names:
              infileList.append( filePath )
      elif job.inputdata and job.inputdata._name == 'DQ2Dataset':
          # Check for availability on DQ2? Avoids submitting jobs where dataset is empty
          if self.check_availability:
              remove_list=[]
              for f in job.inputdata.names:
                  rc = grids[self.middleware.upper()].check_dq2_file_avaiability(f,self.RLS,self.id)
                  if rc==1:
                      logger.warning("DQ2 input file %s not present on NG",f)
                      remove_list.append(f)
              if len(remove_list)>0:
                  logger.warning("Removing input files not present on NG")
                  for f in remove_list:
                      job.inputdata.names.remove(f)

              if len(job.inputdata.names)==0:
                  logger.warning("No input files available on NG")
                  return None

          for f in job.inputdata.names:
                   infileList.append( getRLSurl(f) )


      commandList = [
         "#!/usr/bin/env python",
         "# Interactive job wrapper created by Ganga",
         "# %s" % ( time.strftime( "%c" ) ),
         "",
         inspect.getsource( Sandbox.WNSandbox ),
         "",
         "import os",
         "import time",
         "",
         "startTime = time.strftime"\
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
         "",
         "for inFile in %s:" % str( fileList ),
         "   getPackedInputSandbox( inFile )",
         "",
         "result = os.system( '%s' )" % exeCmdString,
         "",
         "endTime = time.strftime"\
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
         "print '\\nJob start: ' + startTime",
         "print 'Job end: ' + endTime",
         "print 'Exit code: %s\\n' % str( result )",
         "" ]

# removed lines from Ganga wrapper
#         "if os.path.isfile( '%s' ):" % os.path.basename( exeCmdString ),
#         "   os.chmod( '%s', 0755 )" % exeCmdString,

      commandString = "\n".join( commandList )      
      if job.name:
         #print "Job name defined", job.name
         name = job.name
      else:
         #print "Job name not defined", job.application._name  
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
      #print 'ex after split ', ex
      infile.append("(" + ex + " " + wrapper + ")")
      infileString = ''.join(infile)   
      
      xrslDict = \
         { 
         'executable' : ex,
         'stdout' : 'stdout.txt',
         'join'   : 'yes',
         'gmlog' : 'gmlog'         
         }

          
      if jobconfig.requirements and jobconfig.requirements.runtimeenvironment:
          self.requirements.runtimeenvironment = jobconfig.requirements.runtimeenvironment  
      
      outfile = []
      if xrslDict['stdout']:
         outfile.append("(" + xrslDict['stdout'] + " \"\")")

      srm_endpoint = ''
      output_lfn = ''
      if job.outputdata and job.outputdata._name=="DQ2OutputDataset":
          # Get TiersOfATLASChache
          tiersofatlas = getTiersOfATLASCache()
          # Set a default site name
          sitename = 'NDGFT1DISK1'
          # ...but then check if the user has set one
          if job.outputdata.location!='':
              sitename = job.outputdata.location
          # See if sitename is in TiersOfAtlasCache.py
          for site, desc in tiersofatlas.sites.iteritems():
              if site!=sitename:
                  continue
              srm_endpoint = desc['srm'].strip()
          # environment['OUTPUT_DATASETNAME'] = output_datasetname
          if jobconfig.env.has_key('OUTPUT_LFN'):
              output_lfn = jobconfig.env['OUTPUT_LFN']


      for f in jobconfig.outputbox:
         #print 'outputfile ', f

         gridPlacementString = ""
         if job.outputdata and job.outputdata._name=="DQ2OutputDataset":
             gridPlacementString="rls://"+srm_endpoint+output_lfn.strip("/")+"@"+job.backend.RLS.strip("rls://")+"/"+f
             print gridPlacementString

         outfile.append("(" + f + " \""+gridPlacementString+"\")" )

       
      outfileString = ''.join(outfile)
      #print 'outputfileString ', outfileString
      
      if infileString:
         xrslDict[ 'inputfiles' ] = infileString

      if outfileString:
         #print 'add outfileString' 
         xrslDict[ 'outputfiles' ] = outfileString         

      if job.name:
          xrslDict[ 'jobname' ] = job.name     
      elif job.inputdata and job.inputdata._name == 'DQ2Dataset' and job.inputdata.dataset:
          xrslDict[ 'jobname' ] = job.inputdata.dataset

      #print "############## This is xrslDict"
      #print xrslDict

#     if outfileString:
#        xrslDict[ 'transfer_output_files' ] = outfileString

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
          #print "ATHENA_OPTIONS: ", jobconfig.env['ATHENA_OPTIONS']
          xrslList.append( "(environment = ")

          # ATHENA stuff
          if jobconfig.env.has_key('ATHENA_MAX_EVENTS') and jobconfig.env['ATHENA_MAX_EVENTS']>-1:
              xrslList.append("(ATHENA_MAX_EVENTS  %s" % str( jobconfig.env['ATHENA_MAX_EVENTS'] ) + ")")
          if jobconfig.env.has_key('ATHENA_OPTIONS'):
                  xrslList.append("(ATHENA_OPTIONS  %s" % str( jobconfig.env['ATHENA_OPTIONS'] ) + ")")
          if jobconfig.env.has_key('USER_AREA'):
              xrslList.append("(USER_AREA  %s" % str( jobconfig.env['USER_AREA'] ) + ")")    
          if jobconfig.env.has_key('ATHENA_USERSETUPFILE') and jobconfig.env['ATHENA_USERSETUPFILE']!="":
              xrslList.append("(ATHENA_USERSETUPFILE  %s" % str( jobconfig.env['ATHENA_USERSETUPFILE'] ) + ")")  

          # ROOT env
          if jobconfig.env.has_key('ROOTSYS'):
              xrslList.append("(ROOTSYS  %s" % str( jobconfig.env['ROOTSYS'] ) + ")")    
          
          xrslList.append(" ) ") 
      
      xrslString = "\n".join( xrslList )

      # print xrslString

      logger.debug('NG xrslString: %s ' % xrslString)

      # return ""
      
      if subjob:
          return xrslString
      else:
          return inpw.writefile(FileBuffer('ganga.xrsl',xrslString))

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
                    #print 'backend status ', job.backend.status
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

                        # update to 'running' before changing to 'completing'
                        if job.status == 'submitted':
                            job.updateStatus('running')

                        job.updateStatus('completing')
                        outw = job.getOutputWorkspace()
                        pps_check = grids[mt].get_output(job.backend.id,outw.getPath(),wms_proxy=False)
                
                    if pps_check:
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
        
        job = None
        mt = 'ARC'
        jobdict={}
        
        for job in jobs:
            jobdict[job.id] = job
            subjobdict = {}
            subjobdict = dict([ [str(subjob.backend.id),subjob] for subjob in job.subjobs ])

            sjids = []
            for sj in job.subjobs:
                sjids += [sj.backend.id]
                
                ## loop over the jobs in each class
            for info in grids[mt].status( sjids ):
                sjob = subjobdict[info['id']]

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
                    #print 'backend status ', job.backend.status
                    logger.info('job %s has changed status to %s',sjob.getFQID('.'),info['status'])
                    sjob.backend.status = info['status']
                    sjob.backend.reason = info['reason']
                    sjob.backend.exitcode = info['exit']

                    pps_check = True
                    
                    # postprocess of getting job output if the job is done
                    if ( info['status'] == 'FINISHED' or \
                       info['status'] == 'FAILED' or \
                       info['status'] == 'KILLED' or \
                       info['status'] == 'DELETED' ) and sjob.status != 'completed':

                        # update to 'running' before changing to 'completing'
                        if sjob.status == 'submitted':
                            sjob.updateStatus('running')
    
                        sjob.updateStatus('completing')
                        outw = sjob.getOutputWorkspace()
                        pps_check = grids[mt].get_output(sjob.backend.id,outw.getPath(),wms_proxy=False)

                    if pps_check:
                        NG.updateGangaJobStatus(sjob,info['status'])
                    else:
                        sjob.updateStatus("failed")

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

        #print 'In NG, arguments in NGJobConfig'
        #print 'exe', exe
        #print 'inputbox'
        #for f in inputbox:
        #    print f
        #print 'arg'    
        #for a in args:
        #    print a
        #print 'env ', env
        #if requirements:
        #    print 'requirements', requirements.runtimeenvironment

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

def getRLSurl (lfn):
    ''' Help function to get the RLSurl'''
    
    if lfn.startswith('dc2.') or lfn.startswith('rome.'):
        return 'rls://atlasrls.nordugrid.org:39282/' + lfn
    else:
        return 'rls://atlasrls.nordugrid.org:39281/' + lfn

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
config = getConfig('NG')

# set default values for the configuration parameters
config['ARC_ENABLE'] = True

# apply preconfig and postconfig handlers
config.attachUserHandler(__preConfigHandler__,__postConfigHandler__)

# a grid list - in case we want more than one middleware, like LCG/gLITE
grids = {'ARC':None}

if config['ARC_ENABLE']:
    grids['ARC'] = Grid('ARC')
    config['ATC_ENABLE'] = grids['ARC'].active

# $Log: not supported by cvs2svn $
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

