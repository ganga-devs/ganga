from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Core import BackendError

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import Ganga.Utility.Config

## FIXME: experimental code
## class BatchFilter(Ganga.Utility.logging.logging.Filter):
##     def __init__(self):
##         Ganga.Utility.logging.logging.Filter.__init__(self)

##     def filter(self,logrecord):
##         #print logrecord
##         #for l in dir(logrecord):
##         #    print l,getattr(logrecord,l)
##         return 1

## logger.addFilter(BatchFilter())

from Ganga.Core import FileWorkspace
import os


# A trival implementation of shell command with stderr/stdout capture
# This is a self-contained function (with logging).
#
# return (exitcode,soutfile,exeflag)
# soutfile - path where the stdout/stderr is stored
# exeflag - 0 if the command failed to execute, 1 if it executed
def shell_cmd(cmd,soutfile=None,allowed_exit=[0]):

    if not soutfile:
        import tempfile
        soutfile = tempfile.mktemp()
        
    # FIXME: garbbing stdout is done by shell magic and probably should be implemented in python directly
    cmd = "%s > %s 2>&1" % (cmd,soutfile)

    logger.debug("running shell command: %s",cmd)
    rc = os.system(cmd)  

    if not rc in allowed_exit:
        logger.debug('exit status [%d] of command %s',rc,cmd)
        logger.debug('full output is in file: %s',soutfile)
        logger.debug('<first 255 bytes of output>\n%s',file(soutfile).read(255))
        logger.debug('<end of first 255 bytes of output>')

    m = None
    
    if rc != 0:
        logger.debug('non-zero [%d] exit status of command %s ',rc,cmd)
        import re
        m = re.compile(r"command not found$", re.M).search(file(soutfile).read())

    return rc,soutfile,m is None

            
class Batch(IBackend):
    """ Batch submission backend.

    It  is  assumed  that   Batch  commands  (bjobs,  bsub  etc.)  setup
    correctly. As  little assumptions as  possible are made  about the
    Batch configuration but  at certain sites it may  not work correctly
    due  to a  different  Batch setup.  Tested  with CERN  and CNAF  Batch
    installations.
    
    """
    _schema = Schema(Version(1,0), {'queue' : SimpleItem(defvalue='',doc='queue name as defomed in your local Batch installation'),
                                    'extraopts' : SimpleItem(defvalue='',doc='extra options for Batch'),
                                    'id' : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Batch id of the job'),
                                    'exitcode' : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Process exit code'),
                                    'status' : SimpleItem(defvalue=None,protected=1,hidden=1,copyable=0,doc='Batch status of the job'),
                                    'actualqueue' : SimpleItem(defvalue='',protected=1,copyable=0,doc='queue name where the job was submitted.')
				    })
    _category = 'backends'
    _name = 'Batch'
    _hidden = 1

    def __init__(self):
        super(Batch,self).__init__()

    def command(klass,cmd,soutfile=None,allowed_exit=[0]):
        rc,soutfile,ef = shell_cmd(cmd,soutfile,allowed_exit)
        if not ef:
            raise BackendError(klass._name,'It seems that %s commands are not installed properly:%s'%(klass._name,file(soutfile).readline()))
        return rc,soutfile

    command = classmethod(command)
    
    def submit(self,jobconfig, master_input_sandbox):

        job = self.getJobObject()
        
        inw = job.getInputWorkspace() 
        outw = job.getOutputWorkspace()

        #scriptpath = self.preparejob(jobconfig,inw,outw)
        scriptpath=self.preparejob(jobconfig,master_input_sandbox)

        # FIX from Angelo Carbone
        stderr_option = '-e '+str(outw.getPath())+'stderr'
        stdout_option = '-o '+str(outw.getPath())+'stdout'

        queue_option = ''
        if self.queue:
            queue_option = '-q '+str(self.queue)

        try:
            jobnameopt = "-"+self.config['jobnameopt']
        except:
            jobnameopt = False

        if self.extraopts:
            import re
            for opt in re.compile(r'(-\w+)').findall(self.extraopts):
                if opt in ('-o','-e','-oo','-eo'):
                    logger.warning("option %s is forbidden",opt)
                    return False
                if self.queue and opt == '-q':
                    logger.warning("option %s is forbidden if queue is defined ( queue = '%s')",opt,self.queue)
                    return False
                if jobnameopt and opt == jobnameopt:
                    jobnameopt = False
                        
            queue_option = queue_option + " " + self.extraopts
		
        if jobnameopt and job.name != '':
            queue_option = queue_option + " " + jobnameopt + " " + "'%s'"%(job.name) 
        
        # bugfix #16646 
        if self.config['shared_python_executable']:
            import sys
            script_cmd = "%s %s" % (sys.executable,scriptpath)
        else:
            script_cmd = scriptpath
        
        command_str=self.config['submit_str'] % (inw.getPath(),queue_option,stderr_option,stdout_option,script_cmd)
	self.command_string = command_str
        rc,soutfile = self.command(command_str)
        logger.debug('from command get rc: "%d"',rc)
        if rc == 0:
            sout = file(soutfile).read()
            import re
            m = re.compile(self.config['submit_res_pattern'], re.M).search(sout)
            if m is None:
                logger.warning('could not match the output and extract the Batch job identifier!')
                logger.warning('command output \n %s ',sout)
            else:
                self.id = m.group('id')
		try:
                    queue = m.group('queue')
                    if self.queue != queue:
                        if self.queue:
                            logger.warning('you requested queue "%s" but the job was submitted to queue "%s"',self.queue,queue)
                            logger.warning('command output \n %s ',sout)
                        else:
                            logger.info('using default queue "%s"',queue)
                        self.actualqueue = queue
                except IndexError:
                    logger.info('could not match the output and extract the Batch queue name')
        else:
            logger.warning(file(soutfile).read())
		        
        return rc == 0

    def resubmit(self):

        job = self.getJobObject()
        
        inw = job.getInputWorkspace() 
        outw = job.getOutputWorkspace()

	statusfilename = outw.getPath('__jobstatus__')
	try:
	    os.remove(statusfilename)
	except OSError,x:
            if x.errno!=2:
	        logger.warning("OSError:"+str(x))
	    
        scriptpath = inw.getPath('__jobscript__')
        stderr_option = '-e '+str(outw.getPath())+'stderr'
        stdout_option = '-o '+str(outw.getPath())+'stdout'

        queue_option = ''
        if self.queue:
            queue_option = '-q '+str(self.queue)

        try:
            jobnameopt = "-"+self.config['jobnameopt']
        except:
            jobnameopt = False

        if self.extraopts:
            import re
            for opt in re.compile(r'(-\w+)').findall(self.extraopts):
                if opt in ('-o','-e','-oo','-eo'):
                    logger.warning("option %s is forbidden",opt)
                    return False
                if self.queue and opt == '-q':
                    logger.warning("option %s is forbidden if queue is defined ( queue = '%s')",opt,self.queue)
                    return False
                if jobnameopt and opt == jobnameopt:
                    jobnameopt = False

            queue_option = queue_option + " " + self.extraopts
		
        if jobnameopt and job.name != '':
            queue_option = queue_option + " " + jobnameopt + " " + "'%s'"%(job.name) 
		
        # bugfix #16646 
        if self.config['shared_python_executable']:
            import sys
            script_cmd = "%s %s" % (sys.executable,scriptpath)
        else:
            script_cmd = scriptpath
        
        command_str=self.config['submit_str'] % (inw.getPath(),queue_option,stderr_option,stdout_option,script_cmd)
	self.command_string = command_str
        rc,soutfile = self.command(command_str)
        logger.debug('from command get rc: "%d"',rc)
        if rc == 0:
            sout = file(soutfile).read()
            import re
            m = re.compile(self.config['submit_res_pattern'], re.M).search(sout)
            if m is None:
                logger.warning('could not match the output and extract the Batch job identifier!')
                logger.warning('command output \n %s ',sout)
            else:
                self.id = m.group('id')
		try:
                    queue = m.group('queue')
                    if self.queue != queue:
                        if self.queue:
                            logger.warning('you requested queue "%s" but the job was submitted to queue "%s"',self.queue,queue)
                            logger.warning('command output \n %s ',sout)
                        else:
                            logger.info('using default queue "%s"',queue)
                        self.actualqueue = queue
                except IndexError:
                    logger.info('could not match the output and extract the Batch queue name')
        else:
            logger.warning(file(soutfile).read())
		        
        return rc == 0


    def kill(self):
        rc,soutfile = self.command(self.config['kill_str'] % (self.id))

        sout = file(soutfile).read()
        logger.debug('while killing job %s: rc = %d',self.getJobObject().getFQID('.'),rc)
        if rc == 0:
	    return True
	else:
            import re
            m = re.compile(self.config['kill_res_pattern'],re.M).search(sout)
            logger.warning('while killing job %s: %s',self.getJobObject().getFQID('.'), sout)
	
            return not m==None
        
    def preparejob(self,jobconfig,master_input_sandbox):

        job = self.getJobObject()
                    
        subjob_input_sandbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles())
	appscriptpath = [jobconfig.getExeString()] + jobconfig.getArgStrings()
        sharedoutputpath=job.getOutputWorkspace().getPath()
        outputpatterns = jobconfig.outputbox
	environment = jobconfig.env

        text = """#!/usr/bin/env python
import shutil
import os
import time
import popen2

############################################################################################

###INLINEMODULES###

############################################################################################

input_sandbox = ###INPUT_SANDBOX###
sharedoutputpath = ###SHAREDOUTPUTPATH###
outputpatterns = ###OUTPUTPATTERNS###
appscriptpath = ###APPSCRIPTPATH###
environment = ###ENVIRONMENT###

# jobid is a string
jobid = ###JOBID###

###PREEXECUTE###

statusfilename = os.path.join(sharedoutputpath,'__jobstatus__')

try:
  statusfile=file(statusfilename,'w')
except IOError,x:
  print 'ERROR: not able to write a status file: ', statusfilename
  print 'ERROR: ',x
  raise

line='START: '+ time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
line+='PID: ' + os.getenv('###JOBIDNAME###') + os.linesep
line+='QUEUE: ' + os.getenv('###QUEUENAME###') + os.linesep
statusfile.writelines(line)
statusfile.flush()

import sys
sys.path.insert(0, ###GANGADIR###)

try:
    import subprocess
except ImportError,x:
    sys.path.insert(0,###SUBPROCESS_PYTHONPATH###)
    import subprocess
try:
    import tarfile
except ImportError,x:
    sys.path.insert(0,###TARFILE_PYTHONPATH###)
    import tarfile

for f in input_sandbox:
  getPackedInputSandbox(f)

for key,value in environment.iteritems():
    os.environ[key] = value

sysout2 = os.dup(sys.stdout.fileno())
syserr2 = os.dup(sys.stderr.fileno())

print >>sys.stdout,"--- GANGA APPLICATION OUTPUT BEGIN ---"
print >>sys.stderr,"--- GANGA APPLICATION ERROR BEGIN ---"
sys.stdout.flush()
sys.stderr.flush()

sys.stdout=file('./__syslog__','w')
sys.stderr=sys.stdout

###MONITORING_SERVICE###
monitor = createMonitoringObject()
monitor.start()

child = subprocess.Popen(appscriptpath, shell=False, stdout=sysout2, stderr=syserr2)

result = -1

try:
  while 1:
    result = child.poll()
    if result is not None:
        break
    monitor.progress()
    time.sleep(0.3)
finally:
    monitor.progress()
    sys.stdout=sys.__stdout__
    sys.stderr=sys.__stderr__
    print >>sys.stdout,"--- GANGA APPLICATION OUTPUT END ---"
    print >>sys.stderr,"--- GANGA APPLICATION ERROR END ---"

monitor.stop(result)

###POSTEXECUTE###

try:
    filefilter
except:
    filefilter = None

from Ganga.Utility.files import multi_glob, recursive_copy

createOutputSandbox(outputpatterns,filefilter,sharedoutputpath)

for fn in ['__syslog__']:
    try:
        recursive_copy(fn,sharedoutputpath)
    except Exception,x:
        print 'ERROR: (job %s) %s'%(jobid,str(x))

line='EXITCODE: ' + repr(result) + os.linesep
line+='STOP: '+time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
statusfile.writelines(line)

sys.exit(result)
"""

        import inspect
	import Ganga.Core.Sandbox as Sandbox
        text = text.replace('###INLINEMODULES###',inspect.getsource(Sandbox.WNSandbox))
        text = text.replace('###APPSCRIPTPATH###',repr(appscriptpath))
        #text = text.replace('###SHAREDINPUTPATH###',repr(sharedinputpath))
	
        logger.debug('subjob input sandbox %s ',subjob_input_sandbox)
        logger.debug('master input sandbox %s ',master_input_sandbox)
	
	text = text.replace('###INPUT_SANDBOX###',repr(subjob_input_sandbox+master_input_sandbox))
        text = text.replace('###SHAREDOUTPUTPATH###',repr(sharedoutputpath))
        text = text.replace('###OUTPUTPATTERNS###',repr(outputpatterns))
        text = text.replace('###JOBID###',repr(self.getJobObject().getFQID('.')))
	text = text.replace('###ENVIRONMENT###',repr(environment))
        text = text.replace('###PREEXECUTE###',self.config['preexecute'])
        text = text.replace('###POSTEXECUTE###',self.config['postexecute'])
        text = text.replace('###JOBIDNAME###',self.config['jobid_name'])
        text = text.replace('###QUEUENAME###',self.config['queue_name'])

        text = text.replace('###MONITORING_SERVICE###',job.getMonitoringService().getWrapperScriptConstructorText())

        from Ganga.Utility.Config import getConfig
        text = text.replace('###GANGADIR###',repr(getConfig('System')['GANGA_PYTHONPATH']))

        import Ganga.PACKAGE
        text = text.replace('###SUBPROCESS_PYTHONPATH###',repr(Ganga.PACKAGE.setup.getPackagePath2('subprocess','syspath',force=True)))
        text = text.replace('###TARFILE_PYTHONPATH###',repr(Ganga.PACKAGE.setup.getPackagePath2('tarfile','syspath',force=True)))

        from Ganga.GPIDev.Lib.File import FileBuffer
        
        return job.getInputWorkspace().writefile(FileBuffer('__jobscript__',text),executable=1)

    def updateMonitoringInformation(jobs):

        def get_exit_code(f):
            import re
            statusfile=file(f)
            stat = statusfile.read()
            m = re.compile(r'^EXITCODE: (?P<exitcode>\d+)',re.M).search(stat)

            if m is None:
                return None
            else:
                return int(m.group('exitcode'))

        def get_pid(f):
            import re
            statusfile=file(f)
            stat = statusfile.read()
            m = re.compile(r'^PID: (?P<pid>\d+)',re.M).search(stat)

            if m is None:
                return None
            else:
                return int(m.group('pid'))          

        def get_queue(f):
            import re
            statusfile=file(f)
            stat = statusfile.read()
            m = re.compile(r'^QUEUE: (?P<queue>\S+)',re.M).search(stat)

            if m is None:
                return None
            else:
                return str(m.group('queue'))          


        for j in jobs:
            outw=j.getOutputWorkspace()

            # try to get the application exit code from the status file
            try:
                statusfile = os.path.join(outw.getPath(),'__jobstatus__')
                if j.status == 'submitted':
                    pid = get_pid(statusfile)
                    queue = get_queue(statusfile)
                    if pid or queue:
                        j.updateStatus('running')

			if pid:
	                    j.backend.id = pid
                        if queue and queue != j.backend.actualqueue:
                            j.backend.actualqueue = queue
                exitcode = get_exit_code(statusfile)
                logger.debug('status file: %s \n%s',statusfile,file(statusfile).read())
            except IOError,x:
                logger.debug('problem reading status file: %s (%s)',statusfile,str(x))
                exitcode=None
            except Exception,x:
                logger.critical('problem during monitoring: %s',str(x))
                raise x

          
            if not exitcode is None:
                # status file indicates that the application finished
                j.backend.exitcode = exitcode

                if exitcode == 0:
                    j.updateStatus('completed')
                else:
                    j.updateStatus('failed')
                        
    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

#____________________________________________________________________________________
        
config = Ganga.Utility.Config.makeConfig('LSF','internal LSF command line interface')

#fix bug #21687
config.addOption('shared_python_executable', False, "Shared PYTHON")

config.addOption('jobid_name', 'LSB_BATCH_JID', "Name of environment with ID of the job")
config.addOption('queue_name', 'LSB_QUEUE', "Name of environment with queue name of the job")

config.addOption('submit_str', 'cd %s; bsub %s %s %s %s', "String used to submit job to queue")
config.addOption('submit_res_pattern', '^Job <(?P<id>\d*)> is submitted to .*queue <(?P<queue>\S*)>',
                  "String pattern for replay from the submit command")

config.addOption('kill_str', 'bkill %s', "String used to kill job")
config.addOption('kill_res_pattern', 
                 '(^Job <\d+> is being terminated)|(Job <\d+>: Job has already finished)|(Job <\d+>: No matching job found)',
                 "String pattern for replay from the kill command")

tempstr = '''
'''
config.addOption('preexecute',tempstr,"String contains commands executing before submiting job to queue")

tempstr = '''
def filefilter(fn):
  # FILTER OUT Batch INTERNAL INPUT/OUTPUT FILES: 
  # 10 digits . any number of digits . err or out
  import re
  internals = re.compile(r'\d{10}\.\d+.(out|err)')
  return internals.match(fn) or fn == '.Batch.start'
'''
config.addOption('postexecute', tempstr,"String contains commands executing before submiting job to queue")
config.addOption('jobnameopt', 'J', "String contains option name for name of job in batch system")


class LSF(Batch):
    ''' LSF backend - submit jobs to Load Sharing Facility.'''
    _schema = Batch._schema.inherit_copy()
    _category = 'backends'
    _name = 'LSF'

    config = Ganga.Utility.Config.getConfig('LSF')

    def __init__(self):
        super(LSF,self).__init__()

	
		
#____________________________________________________________________________________
        
config = Ganga.Utility.Config.makeConfig('PBS','internal PBS command line interface')

config.addOption('shared_python_executable', False, "Shared PYTHON")

config.addOption('jobid_name', 'PBS_JOBID', "Name of environment with ID of the job")
config.addOption('queue_name', 'PBS_QUEUE', "Name of environment with queue name of the job")

config.addOption('submit_str', 'cd %s; qsub %s %s %s %s', "String used to submit job to queue")
config.addOption('submit_res_pattern', '^(?P<id>\d*)\.pbs\s*', "String pattern for replay from the submit command")

config.addOption('kill_str', 'qdel %s', "String used to kill job")
config.addOption('kill_res_pattern', '(^$)|(qdel: Unknown Job Id)', "String pattern for replay from the kill command")

tempstr='''
env = os.environ
jobnumid = env["PBS_JOBID"]
os.system("mkdir /tmp/%s/" %jobnumid)
os.chdir("/tmp/%s/" %jobnumid)
os.environ["PATH"]+=":."
'''
config.addOption('preexecute', tempstr, "String contains commands executing before submiting job to queue")

tempstr='''
'''
config.addOption('postexecute', tempstr, "String contains commands executing before submiting job to queue")
config.addOption('jobnameopt', 'N', "String contains option name for name of job in batch system")


class PBS(Batch):
    ''' PBS backend - submit jobs to Portable Batch System.
    '''
    _schema = Batch._schema.inherit_copy()
    _category = 'backends'
    _name = 'PBS'

    config = Ganga.Utility.Config.getConfig('PBS')
    def __init__(self):
        super(PBS,self).__init__()
		

#____________________________________________________________________________________
        
config = Ganga.Utility.Config.makeConfig('SGE','internal SGE command line interface')

config.addOption('shared_python_executable', False, "Shared PYTHON")

config.addOption('jobid_name', 'JOB_ID', "Name of environment with ID of the job")
config.addOption('queue_name', 'QUEUE', "Name of environment with queue name of the job")

#the -V options means that all environment variables are transferred to the batch job (ie the same as the default behaviour on LSF at CERN)
config.addOption('submit_str', 'cd %s; qsub -cwd -V %s %s %s %s', "String used to submit job to queue")
config.addOption('submit_res_pattern', 'Your job (?P<id>\d+) (.+)', "String pattern for replay from the submit command")

config.addOption('kill_str', 'qdel %s', "String used to kill job")
config.addOption('kill_res_pattern', '(has registered the job +\d+ +for deletion)|(denied: job +"\d+" +does not exist)', 
                 "String pattern for replay from the kill command")

#From the SGE man page on qsub
#
#===========================
#Furthermore, Grid Engine sets additional variables into the job's
#environment, as listed below.
#:
#:
#TMPDIR
#   The absolute path to the job's temporary working directory.
#=============================

config.addOption('preexecute', 'os.chdir(os.environ["TMPDIR"])\nos.environ["PATH"]+=":."', 
                 "String contains commands executing before submiting job to queue")
config.addOption('postexecute', '', "String contains commands executing before submiting job to queue")
config.addOption('jobnameopt', 'N', "String contains option name for name of job in batch system")


class SGE(Batch):
    ''' SGE backend - submit jobs to Sun Grid Engine.
    '''
    _schema = Batch._schema.inherit_copy()
    _category = 'backends'
    _name = 'SGE'

    config = Ganga.Utility.Config.getConfig('SGE')
    def __init__(self):
        super(SGE,self).__init__()
		
