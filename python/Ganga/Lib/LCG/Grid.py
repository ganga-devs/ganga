import os, sys, md5, re, tempfile, time, errno, socket
from types import *
from urlparse import urlparse

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Credentials import getCredential 
from Ganga.GPIDev.Adapters.IBackend import IBackend 
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core import BackendError
from Ganga.Utility.Shell import Shell

from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import isStringLike

from Ganga.Utility.GridShell import getShell
from Ganga.Lib.LCG.ElapsedTimeProfiler import ElapsedTimeProfiler

# global variables
logger = getLogger()

class Grid(object):
    '''Helper class to implement grid interaction'''

    # attributes
    _attributes = ('middleware', 'credential', 'config', 'active', 'perusable')

    def __init__(self,middleware='EDG'):

        self.active = False

        self.re_token = re.compile('^token:(.*):(.*)$')

        self.credential = None

        self.middleware = middleware.upper()

        self.perusable = False

        self.config = getConfig('LCG')

#       check that UI has been set up
#       start up a shell object specific to the middleware
        self.shell = getShell(self.middleware)

        if not self.shell:
            logger.warning('LCG-%s UI has not been configured. The plugin has been disabled.' % self.middleware)
            return
        else:
            lfc_host = self.__get_default_lfc__()
            if lfc_host:
                self.shell.env['LFC_HOST'] = lfc_host
                logger.debug('set LFC_HOST of %s UI to %s.' % (self.middleware,lfc_host))

#       create credential for this Grid object
        self.active = self.check_proxy()

    def __setattr__(self,attr,value):
        object.__setattr__(self, attr, value)
        # dynamic update the internal shell object if the config attribute is reset
        if attr == 'config':
            self.shell = getShell(self.middleware)

    def __get_cmd_prefix_hack__(self,binary=False):
        # this is to work around inconsistency of LCG setup script and commands:
        # LCG commands require python2.2 but the setup script does not set this version of python
        # if another version of python is used (like in GUI), then python2.2 runs against wrong python libraries
        # possibly should be fixed in LCG: either remove python2.2 from command scripts or make setup script force
        # correct version of python
        prefix_hack = "${%s_LOCATION}/bin/" % self.middleware

        if not binary:
            prefix_hack = 'python '+prefix_hack

        return prefix_hack

    def __set_submit_option__(self):

#       find out how the VO has been specified

        self.submit_option = ''

        msg = 'using the VO defined '

        # VO specific WMS options (no longer used by glite-wms-job-submit command)
        if self.config['ConfigVO']: # 1. vo specified in the configuration file
            if self.middleware == 'EDG':
                self.submit_option = '--config-vo %s' % self.config['ConfigVO']
                if not os.path.exists(self.config['ConfigVO']):
                    raise Ganga.Utility.Config.ConfigError('')
                else:
                    msg += 'in %s.' % self.config['ConfigVO']
            else:
                logger.warning('ConfigVO configuration ignored by %s middleware. Set Config instead.' % self.middleware)

        elif self.__get_proxy_voname__(): # 2. vo attached in the voms proxy
            msg += 'as %s.' % self.__get_proxy_voname__()
        elif self.config['VirtualOrganisation']: # 3. vo is given explicitely 
            self.submit_option = '--vo %s' % self.config['VirtualOrganisation']
            msg += 'as %s.' % self.config['VirtualOrganisation']
        else: # 4. no vo information is found
            logger.warning('No Virtual Organisation specified in the configuration. The plugin has been disabeled.')
            return False

        # general WMS options
        # NB. please be aware the config for gLite WMS is NOT compatible with the config for EDG RB
        #     although both shares the same command option: '--config'
        if self.config['Config']:
            self.submit_option += ' --config %s' % self.config['Config']

        self.submit_option = ' %s ' % self.submit_option

        logger.debug(msg)

        return True

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

        # for EDG, we never check it
        if self.middleware == 'EDG':
            return None
        else:
            logger.debug('voms of credential: %s' % self.credential.voms)
            return self.credential.voms

    def __get_default_lfc__(self):
        '''Gets the default lfc host from lcg-infosites'''

        cmd = 'lcg-infosites'

        rc, output, m = self.shell.cmd1('%s --vo %s lfc' % (cmd,self.config['VirtualOrganisation']),allowed_exit=[0,255])

        if rc != 0:
            self.__print_gridcmd_log__('lcg-infosites',output)
            return None
        else:
            lfc_list = output.strip().split('\n')
            return lfc_list[0]

    def check_proxy(self):
        '''Check the proxy and prompt the user to refresh it'''

        if self.credential is None:
            self.credential = getCredential('GridProxy',self.middleware)

        if self.middleware == 'GLITE':
            self.credential.voms = self.config['VirtualOrganisation'];
            self.credential = getCredential('GridProxy', 'GLITE')

        status = self.credential.renew(maxTry=3)

        if not status:
            logger.warning("Could not get a proxy, giving up after 3 retries")
            return False

        return True

    def submit(self,jdlpath,ce=None):
        '''Submit a JDL file to LCG'''

        if self.middleware == 'EDG':
            cmd = 'edg-job-submit'
        else:
            cmd = 'glite-wms-job-submit -a'

        if not self.active:
            logger.warning('LCG plugin not active.')
            return

        if not self.credential.isValid('01:00'):
            logger.warning('GRID proxy lifetime shorter than 1 hour')
            return

        if not self.__set_submit_option__():
            return

        if ce:
            cmd = cmd + ' -r %s' % ce

        cmd += self.submit_option

        cmd = '%s --nomsg %s < /dev/null' % (cmd,jdlpath)

        logger.debug('job submit command: %s' % cmd)

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(binary=True),cmd),allowed_exit=[0,255])

        if output: output = "%s" % output.strip()

        #match = re.search('^(https:\S+)',output)
        match = re.search('.*(https:\/\/\S+:9000\/[0-9A-Za-z_\.\-]+)',output)

        if match:
            logger.debug('job id: %s' % match.group(1))
            if self.middleware == 'GLITE' and self.perusable:
                logger.info("Enabling perusal")
                per_rc, per_out, per_m=self.shell.cmd1("glite-wms-job-perusal --set -f stdout %s" % match.group(1))
            return match.group(1)

        logger.warning('Job submission failed.')
        self.__print_gridcmd_log__('(.*-job-submit.*\.log)',output)

        return

    def native_master_cancel(self,jobids):
        '''Native bulk cancellation supported by GLITE middleware.'''

        idsfile = tempfile.mktemp('.jids')
        file(idsfile,'w').write('\n'.join(jobids)+'\n')

        if self.middleware == 'EDG':
            logger.warning('EDG middleware doesn\'t support bulk cancellation.')
            return False
        else:
            cmd = 'glite-wms-job-cancel'
      
        if not self.active:
            logger.warning('LCG plugin not active.')
            return False

        if not self.credential.isValid('01:00'):
            logger.warning('GRID proxy lifetime shorter than 1 hour')
            return False

        if not self.__set_submit_option__():
            return False

        cmd = '%s --noint -i %s' % (cmd, idsfile)

        logger.debug('job cancel command: %s' % cmd)

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(binary=True),cmd),allowed_exit=[0,255])

        if rc != 0:
            logger.warning('Job cancellation failed.')
            self.__print_gridcmd_log__('(.*-job-cancel.*\.log)',output)
            return False
        else:
            return True

    def status(self,jobids,is_collection=False):
        '''Query the status of jobs on the grid'''

        if not jobids: return []

        #do_node_mapping = False

        #if node_map and os.path.exists(node_map):
        #    do_node_mapping = True

        idsfile = tempfile.mktemp('.jids')
        file(idsfile,'w').write('\n'.join(jobids)+'\n')

        if self.middleware == 'EDG':
            cmd = 'edg-job-status'
        else:
            cmd = 'glite-wms-job-status'
            if is_collection:
                cmd = '%s -v 3' % cmd

        if not self.active:
            logger.warning('LCG plugin not active.')
            return []
        if not self.credential.isValid('01:00'):
            logger.warning('GRID proxy lifetime shorter than 1 hour')
            return []

        cmd = '%s --noint -i %s' % (cmd,idsfile)
        logger.debug('job status command: %s' % cmd)

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(binary=True),cmd), allowed_exit=[0,255])
        os.unlink(idsfile)

        if rc != 0:
            self.__print_gridcmd_log__('(.*-job-status.*\.log)',output)

        re_id = re.compile('^\s*Status info for the Job : (https://.*\S)\s*$')
        re_status = re.compile('^\s*Current Status:\s+(.*\S)\s*$')

        ## from glite UI version 1.5.14, the attribute 'Node Name:' is no longer available
        ## for distinguishing master and node jobs. A new way has to be applied.
        #re_name = re.compile('^\s*Node Name:\s+(.*\S)\s*$')
        re_exit = re.compile('^\s*Exit code:\s+(.*\S)\s*$')
        re_reason = re.compile('^\s*Status Reason:\s+(.*\S)\s*$')
        re_dest = re.compile('^\s*Destination:\s+(.*\S)\s*$')

        ## pattern to distinguish master and node jobs
        re_master = re.compile('^BOOKKEEPING INFORMATION:\s*$')
        re_node   = re.compile('^- Nodes information.*\s*$')

        ## pattern for node jobs
        re_nodename = re.compile('^\s*NodeName\s*=\s*"(gsj_[0-9]+)";\s*$')
      
        info = []
        is_master = False 
        is_node   = False
        #node_cnt  = 0
        for line in output.split('\n'):

            match = re_master.match(line)
            if match:
                is_master = True
                is_node   = False
                #node_cnt  = 0
                continue

            match = re_node.match(line)
            if match:
                is_master = False
                is_node   = True
                continue

            match = re_id.match(line)
            if match:
                info += [{ 'id'     : match.group(1),
                           'name'   : '',
                           'is_node': False,
                           'status' : '',
                           'exit'   : '',
                           'reason' : '',
                           'destination' : '' }]
                if is_node:
                    info[-1]['is_node'] = True 
                #if is_node:
                #    info[-1]['name'] = 'node_%d' % node_cnt
                #    node_cnt = node_cnt + 1
                continue

            match = re_nodename.match(line)
            if match and is_node:
                info[-1]['name'] = match.group(1)
                #logger.debug('id: %s, name: %s' % (info[-1]['id'],info[-1]['name']))
                continue

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

        return info

    def get_loginfo(self,jobids,directory,verbosity=1):
        '''Fetch the logging info of the given job and save the output in the job's outputdir'''

        idsfile = tempfile.mktemp('.jids')
        file(idsfile,'w').write('\n'.join(jobids)+'\n')

        if self.middleware == 'EDG':
            cmd = 'edg-job-get-logging-info -v %d' % verbosity
        else:
            cmd = 'glite-wms-job-logging-info -v %d' % verbosity

        if not self.active:
            logger.warning('LCG plugin not active.')
            return False 
        if not self.credential.isValid('01:00'):
            logger.warning('GRID proxy lifetime shorter than 1 hour')
            return False

        log_output = directory+'/__jobloginfo__.log'

        cmd = '%s --noint -o %s -i %s' % (cmd, log_output, idsfile)

        logger.debug('job logging info command: %s' % cmd)

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(binary=True),cmd),allowed_exit=[0,255])

        if rc != 0:
            self.__print_gridcmd_log__('(.*-logging-info.*\.log)',output)
            return False
        else:
            # returns the path to the saved logging info if success  
            return log_output 

    def get_output(self,jobid,directory,wms_proxy=False):
        '''Retrieve the output of a job on the grid'''

        if self.middleware == 'EDG':
            cmd = 'edg-job-get-output'
        else:
            cmd = 'glite-wms-job-output'
            # general WMS options (somehow used by the glite-wms-job-output command)
            if self.config['Config']:
                cmd += ' --config %s' % self.config['Config']

        if not self.active:
            logger.warning('LCG plugin is not active.')
            return (False,None)
        if not self.credential.isValid('01:00'):
            logger.warning('GRID proxy lifetime shorter than 1 hour')
            return (False,None)

        cmd = '%s --noint --dir %s %s' % (cmd,directory,jobid)

        logger.debug('job get output command: %s' % cmd)

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(binary=True),cmd),allowed_exit=[0,255])

        match = re.search('directory:\n\s*(\S+)\s*\n',output)

        if not match:
            logger.warning('Job output fetch failed.')
            self.__print_gridcmd_log__('(.*-output.*\.log)',output)
            return (False, 'cannot fetch job output')

        outdir = match.group(1)

#       some versions of LCG middleware create an extra output directory (named <uid>_<jid_hash>) 
#       inside the job.outputdir. Try to match the jid_hash in the outdir. Do output movememnt
#       if the <jid_hash> is found in the path of outdir.
        import urlparse
        jid_hash = urlparse.urlparse(jobid)[2][1:]

        if outdir.count(jid_hash):
            if self.shell.system('mv %s/* %s' % (outdir,directory)) == 0:
                try:
                    os.rmdir(outdir)
                except Exception, msg:
                    logger.warning( "Error trying to remove the empty directory %s:\n%s" % ( outdir, msg ) )
            else:
                logger.warning( "Error moving output from %s to %s.\nOutput is left in %s." % (outdir,directory,outdir) )
        else:
            pass

        import Ganga.Core.Sandbox as Sandbox
        Sandbox.getPackedOutputSandbox(directory,directory)

        ## check the application exit code
        app_exitcode = -1
        runtime_log  = os.path.join(directory,'__jobscript__.log')
        pat = re.compile(r'.*exit code (\d+).')

        if not os.path.exists(runtime_log):
            logger.warning('job runtime log not found: %s' % runtime_log)
            return (False, 'job runtime log not found: %s' % runtime_log)
      
        f = open(runtime_log,'r')
        for line in f.readlines():
            mat = pat.match(line)
            if mat:
                app_exitcode = eval(mat.groups()[0])
                break
        f.close()

        ## returns False if the exit code of the real executable is not zero
        ## the job status of GANGA will be changed to 'failed' if the return value is False
        if app_exitcode != 0:
            logger.debug('job\'s executable returns non-zero exit code: %d' % app_exitcode)
            return (False, app_exitcode)
        else:
            return (True, 0)

    def cancelMultiple(self, jobids):
        '''Cancel multiple jobs in one LCG job cancellation call'''

        # compose a temporary file with job ids in it
        if not jobids: return True

        idsfile = tempfile.mktemp('.jids')
        file(idsfile,'w').write('\n'.join(jobids)+'\n')

        # do the cancellation using a proper LCG command
        if self.middleware == 'EDG':
            cmd = 'edg-job-cancel'
        else:
            cmd = 'glite-wms-job-cancel'

        if not self.active:
            logger.warning('LCG plugin is not active.')
            return False
        if not self.credential.isValid('01:00'):
            logger.warning('GRID proxy lifetime shorter than 1 hour')
            return False

        # compose the cancel command
        cmd = '%s --noint -i %s' % (cmd,idsfile)

        logger.debug('job cancel command: %s' % cmd)

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(binary=True),cmd),allowed_exit=[0,255])

        if rc == 0:
            return True
        else:
            logger.warning( "Failed to cancel jobs.\n%s" % output )
            self.__print_gridcmd_log__('(.*-job-cancel.*\.log)',output)
            return False

    def cancel(self,jobid):
        '''Cancel a job'''

        if self.middleware == 'EDG':
            cmd = 'edg-job-cancel'
        else:
            cmd = 'glite-wms-job-cancel'

        if not self.active:
            logger.warning('LCG plugin is not active.')
            return False
        if not self.credential.isValid('01:00'):
            logger.warning('GRID proxy lifetime shorter than 1 hour')
            return False

        cmd = '%s --noint %s' % (cmd,jobid)

        logger.debug('job cancel command: %s' % cmd)

        rc, output, m = self.shell.cmd1('%s%s' % (self.__get_cmd_prefix_hack__(binary=True),cmd),allowed_exit=[0,255])

        if rc == 0:
            return True
        else:
            logger.warning( "Failed to cancel job %s.\n%s" % ( jobid, output ) )
            self.__print_gridcmd_log__('(.*-job-cancel.*\.log)',output)
            return False

    def expandjdl(items):
        '''Expand jdl items'''

        text = ""   
        for key, value in items.iteritems():

            if key == 'Requirements':
                if value: text += 'Requirements = \n   %s;\n' % ' &&\n   '.join(value)

            elif key in ['ShallowRetryCount','RetryCount','NodeNumber','ExpiryTime', 'PerusalTimeInterval']:
                try:
                    value = int(value)
                    if value<0: raise ValueError
                    text+='%s = %d;\n' % (key,value)
                except ValueError:
                    logger.warning('%s is not positive integer.' % key)

            elif key == 'Environment':
                if value: text += 'Environment = {\n   "%s"\n};\n' % '",\n   "'.join(['%s=\'%s\'' % var for var in value.items()])

            elif type(value) == ListType:
                if value: text += '%s = {\n   "%s"\n};\n' % (key,'",\n   "'.join(value)) 

            elif key == 'Rank':
                text += 'Rank = ( %s );\n' % value

            elif key == 'Nodes':
                text += 'Nodes = %s;\n' % value

            elif key == 'PerusalFileEnable':
                text += 'PerusalFileEnable = %s;\n' % value                
            else:
                text += '%s = "%s";\n' % (key,value)  

        return text

    expandjdl=staticmethod(expandjdl)
