import datetime
import time
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
    
    Each batch system supports an 'extraopts' field, which allows customisation
    of way the job is submitted.

    PBS:
    Take environment settings on submitting machine and export to batch job:
    backend.extraopts = "-V"

    Request minimum walltime of 24 hours and minimum memory of 2GByte:
    backend.extraopts = "-l walltime=24:00:00 mem=2gb"

    The above can be combined as:
    backend.extraopts = "-V -l walltime=24:00:00 mem=2gb" 

    LSF:
    Sends mail to you when the job is dispatched and begins execution.
    backend.extraopts = "-B"

    Assigns the Ganga job name to the batch job. The job name does not need to 
    be unique.
    backend.extraopts = "-J "+ j.name

    Run the job on a host that meets the specified resource requirements.
    A resource requirement string describes the resources a job needs.
    E.g request 2Gb of memory ans 1Gb of swap space
    backend.extraopts = '-R "mem=2048" -R "swp=1024"'

    Kill job if it has exceeded the deadline (i.e. for your presentation)
    backend.extraopts = '-t 07:14:12:59' #Killed if not finished by 14 July before 1 pm
    """
    _schema = Schema(Version(1,0), {'queue' : SimpleItem(defvalue='',doc='queue name as defomed in your local Batch installation'),
                                    'extraopts' : SimpleItem(defvalue='',doc='extra options for Batch. See help(Batch) for more details'),
                                    'id' : SimpleItem(defvalue='',protected=1,copyable=0,doc='Batch id of the job'),
                                    'exitcode' : SimpleItem(defvalue=None,typelist=['int','type(None)'],protected=1,copyable=0,doc='Process exit code'),
                                    'status' : SimpleItem(defvalue='',protected=1,hidden=1,copyable=0,doc='Batch status of the job'),
                                    'actualqueue' : SimpleItem(defvalue='',protected=1,copyable=0,doc='queue name where the job was submitted.'),
                                    'actualCE' : SimpleItem(defvalue='',protected=1,copyable=0,doc='hostname where the job is/was running.')
                                    })
    _category = 'backends'
    _name = 'Batch'
    _hidden = 1

    def __init__(self):
        super(Batch,self).__init__()

    def command(klass,cmd,soutfile=None,allowed_exit=[0]):
        rc,soutfile,ef = shell_cmd(cmd,soutfile,allowed_exit)
        if not ef:
            logger.warning('Problem submitting batch job. Maybe your chosen batch system is not available or you have configured it wrongly')
            logger.warning(file(soutfile).read())
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
        # stderr_option = '-e '+str(outw.getPath())+'stderr'
        # stdout_option = '-o '+str(outw.getPath())+'stdout'

        # FIX from Alex Richards - see Savannah #87477
        stdout_option = self.config['stdoutConfig'] % str(outw.getPath())
        stderr_option = self.config['stderrConfig'] % str(outw.getPath())

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
        #stderr_option = '-e '+str(outw.getPath())+'stderr'
        #stdout_option = '-o '+str(outw.getPath())+'stdout'

        # FIX from Alex Richards - see Savannah #87477
        stdout_option = self.config['stdoutConfig'] % str(outw.getPath())
        stderr_option = self.config['stderrConfig'] % str(outw.getPath())

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

    def getStateTime(self, status):
        """Obtains the timestamps for the 'running', 'completed', and 'failed' states.

           The __jobstatus__ file in the job's output directory is read to obtain the start and stop times of the job.
           These are converted into datetime objects and returned to the user.
        """
        j = self.getJobObject()
        end_list = ['completed', 'failed']
        d = {}
        checkstr=''

        if status == 'running': checkstr='START:'
        elif status == 'completed': checkstr='STOP:'
        elif status == 'failed': checkstr='FAILED:'
        else:
            checkstr=''

        if checkstr=='':
            logger.debug("In getStateTime(): checkstr == ''")
            return None

        try:
            p = os.path.join(j.outputdir, '__jobstatus__')
            logger.debug("Opening output file at: %s", p)
            f = open(p)
        except IOError:
            logger.debug('unable to open file %s', p)
            return None

        for l in f.readlines():
            if checkstr in l:
                pos=l.find(checkstr)
                timestr=l[pos+len(checkstr)+1:pos+len(checkstr)+25]
                try:
                    t = datetime.datetime(*(time.strptime(timestr, "%a %b %d %H:%M:%S %Y")[0:6]))
                except ValueError:
                    logger.debug("Value Error in file: '%s': string does not match required format.", p)
                    return None
                return t

        logger.debug("Reached the end of getStateTime('%s'). Returning None.", status)
        return None

    def timedetails(self):
        """Return all available timestamps from this backend.
        """
        j = self.getJobObject()
        try: ## check for file. if it's not there don't bother calling getSateTime (twice!)
            p = os.path.join(j.outputdir, '__jobstatus__')
            logger.debug("Opening output file at: %s", p)
            f = open(p)
            f.close()
        except IOError:
            logger.error('unable to open file %s', p)
            return None
        del f
        r = self.getStateTime('running')
        c = self.getStateTime('completed')
        d = {'START' : r, 'STOP' : c}

        return d

    def preparejob(self,jobconfig,master_input_sandbox):

        job = self.getJobObject()
        mon = job.getMonitoringService()
        import Ganga.Core.Sandbox as Sandbox
        subjob_input_sandbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles()
            + Sandbox.getGangaModulesAsSandboxFiles(Sandbox.getDefaultModules())
            + Sandbox.getGangaModulesAsSandboxFiles(mon.getSandboxModules()))
        
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
###INLINEHOSTNAMEFUNCTION###

############################################################################################

input_sandbox = ###INPUT_SANDBOX###
sharedoutputpath = ###SHAREDOUTPUTPATH###
outputpatterns = ###OUTPUTPATTERNS###
appscriptpath = ###APPSCRIPTPATH###
environment = ###ENVIRONMENT###

# jobid is a string
jobid = ###JOBID###

###PREEXECUTE###

## system command executor with subprocess
def execSyscmdSubprocess(cmd):

    exitcode = -999
    mystdout = ''
    mystderr = ''

    try:
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (mystdout, mystderr) = child.communicate()
        exitcode = child.returncode
    finally:
        pass

    return (exitcode, mystdout, mystderr)

def postprocessoutput():

    massStorageList = []          

    inpfile = os.path.join(###INPUT_DIR###, '__postprocessoutput__')
    
    if not os.path.exists(inpfile):
        return None
                
    for line in open(inpfile, 'r').readlines(): 
        line = line.strip()     
        if line.startswith('massstorage'):
            massStorageList.append(line)        

    return [massStorageList]


def flush_file(f):
   f.flush()
   os.fsync(f.fileno()) #this forces a global flush (cache synchronization on AFS)

def open_file(fname):
  try:
    filehandle=file(fname,'w')
  except IOError,x:
    print 'ERROR: not able to write a status file: ', fname
    print 'ERROR: ',x
    raise
  return filehandle

statusfilename = os.path.join(sharedoutputpath,'__jobstatus__')
heartbeatfilename = os.path.join(sharedoutputpath,'__heartbeat__')

statusfile=open_file(statusfilename)
heartbeatfile=open_file(heartbeatfilename)

line='START: '+ time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
try:
  line+='PID: ' + os.getenv('###JOBIDNAME###') + os.linesep
  line+='QUEUE: ' + os.getenv('###QUEUENAME###') + os.linesep
  line+='ACTUALCE: ' + hostname() + os.linesep
except:
  pass
statusfile.writelines(line)
flush_file(statusfile)

try:
    import tarfile
except ImportError,x:
    sys.path.insert(0,###TARFILE_PYTHONPATH###)
    import tarfile

# -- WARNING: get the input files including the python modules BEFORE sys.path.insert()
# -- SINCE PYTHON 2.6 THERE WAS A SUBTLE CHANGE OF SEMANTICS IN THIS AREA

for f in input_sandbox:
  getPackedInputSandbox(f)

# -- END OF MOVED CODE BLOCK

import sys
sys.path.insert(0, ###GANGADIR###)
sys.path.insert(0,os.path.join(os.getcwd(),PYTHON_DIR))

try:
    import subprocess
except ImportError,x:
    sys.path.insert(0,###SUBPROCESS_PYTHONPATH###)
    import subprocess

for key,value in environment.iteritems():
    os.environ[key] = value

sysout2 = os.dup(sys.stdout.fileno())
syserr2 = os.dup(sys.stderr.fileno())

print >>sys.stdout,"--- GANGA APPLICATION OUTPUT BEGIN ---"
print >>sys.stderr,"--- GANGA APPLICATION ERROR BEGIN ---"
flush_file(sys.stdout)
flush_file(sys.stderr)

sys.stdout=file('./__syslog__','w')
sys.stderr=sys.stdout

###MONITORING_SERVICE###
monitor = createMonitoringObject()
monitor.start()

result = 255



try:
  child = subprocess.Popen(appscriptpath, shell=False, stdout=sysout2, stderr=syserr2)

  while 1:
    result = child.poll()
    if result is not None:
        break
    monitor.progress()
    heartbeatfile.write('.')
    flush_file(heartbeatfile)
    time.sleep(###HEARTBEATFREQUENCE###)
except Exception,x:
  print 'ERROR: %s'%str(x)

monitor.progress()
flush_file(sys.stdout)
flush_file(sys.stderr)
sys.stdout=sys.__stdout__
sys.stderr=sys.__stderr__
print >>sys.stdout,"--- GANGA APPLICATION OUTPUT END ---"


monitor.stop(result)

try:
    filefilter
except:
    filefilter = None

from Ganga.Utility.files import multi_glob, recursive_copy

createOutputSandbox(outputpatterns,filefilter,sharedoutputpath)

postprocesslocations = file(os.path.join(sharedoutputpath, '__postprocesslocations__'), 'w')         

postProcessOutputResult = postprocessoutput()

#code here for upload to castor
if postProcessOutputResult is not None:
    for massStorageLine in postProcessOutputResult[0]:
        massStorageList = massStorageLine.split(' ')

        filenameWildChar = massStorageList[1]
        cm_mkdir = massStorageList[2]
        cm_cp = massStorageList[3]
        cm_ls = massStorageList[4]
        path = massStorageList[5]

        pathToDirName = os.path.dirname(path)
        dirName = os.path.basename(path)

        (exitcode, mystdout, mystderr) = execSyscmdSubprocess('nsls %s' % pathToDirName)
        if exitcode != 0:
            print >>sys.stderr, 'Error while executing nsls %s command, be aware that Castor commands can be executed only from lxplus, also check if the folder name is correct and existing' % pathToDirName
            print >>sys.stderr, mystderr
            continue

        directoryExists = False 
        for directory in mystdout.split('\\n'):
            if directory.strip() == dirName:
                directoryExists = True
                break

        if not directoryExists:
            (exitcode, mystdout, mystderr) = execSyscmdSubprocess('%s %s' % (cm_mkdir, path))
            if exitcode != 0:
                print >>sys.stderr, 'Error while executing %s %s command, check if the ganga user has rights for creating directories in this folder' % (cm_mkdir, path)
                print >>sys.stderr, mystderr
                continue
            
        import glob 
        for currentFile in glob.glob(filenameWildChar):
            (exitcode, mystdout, mystderr) = execSyscmdSubprocess('%s %s %s' % (cm_cp, currentFile, os.path.join(path, currentFile)))
            if exitcode != 0:
                print >>sys.stderr, 'Error while executing %s %s %s command, check if the ganga user has rights for uploading files to this mass storage folder' % (cm_cp, currentFile, os.path.join(path, currentFile))    
                print >>sys.stderr, mystderr        
            else:
                postprocesslocations.write('massstorage %s %s\\n' % (filenameWildChar, os.path.join(path, currentFile)))
                #remove file from output dir
                os.system('rm %s' % currentFile)        

postprocesslocations.close()    

print >>sys.stderr,"--- GANGA APPLICATION ERROR END ---"

for fn in ['__syslog__']:
    try:
        recursive_copy(fn,sharedoutputpath)
    except Exception,x:
        print 'ERROR: (job %s) %s'%(jobid,str(x))

###POSTEXECUTE###

line='EXITCODE: ' + repr(result) + os.linesep
line+='STOP: '+time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
statusfile.writelines(line)

statusfile.close()
heartbeatfile.close()
os.unlink(heartbeatfilename)

sys.exit(result)
"""

        import inspect
        import Ganga.Core.Sandbox as Sandbox
        import Ganga.Utility as Utility
        text = text.replace('###INLINEMODULES###',inspect.getsource(Sandbox.WNSandbox))
        text = text.replace('###INLINEHOSTNAMEFUNCTION###',inspect.getsource(Utility.util.hostname))
        text = text.replace('###APPSCRIPTPATH###',repr(appscriptpath))
        #text = text.replace('###SHAREDINPUTPATH###',repr(sharedinputpath))
        
        logger.debug('subjob input sandbox %s ',subjob_input_sandbox)
        logger.debug('master input sandbox %s ',master_input_sandbox)
        
        text = text.replace('###INPUT_SANDBOX###',repr(subjob_input_sandbox+master_input_sandbox))
        text = text.replace('###SHAREDOUTPUTPATH###',repr(sharedoutputpath))

        if '__postprocessoutput__' in os.listdir(job.getStringInputDir()):
            
            fullFilePath = os.path.join(job.getStringInputDir(), '__postprocessoutput__')
            fileRead = open(fullFilePath, 'r')
            for line in fileRead.readlines(): 
                line = line.strip()     
                if line.startswith('lcgse'):
                    filenameWildChar = line.split(' ')[1]
                    if filenameWildChar not in outputpatterns:
                        outputpatterns += [filenameWildChar]
            fileRead.close()


        text = text.replace('###OUTPUTPATTERNS###',repr(outputpatterns))
        text = text.replace('###JOBID###',repr(self.getJobObject().getFQID('.')))
        text = text.replace('###ENVIRONMENT###',repr(environment))
        text = text.replace('###PREEXECUTE###',self.config['preexecute'])
        text = text.replace('###POSTEXECUTE###',self.config['postexecute'])
        text = text.replace('###JOBIDNAME###',self.config['jobid_name'])
        text = text.replace('###QUEUENAME###',self.config['queue_name'])
        text = text.replace('###HEARTBEATFREQUENCE###',self.config['heartbeat_frequency'])
        text = text.replace('###INPUT_DIR###',repr(job.getStringInputDir()))

        text = text.replace('###MONITORING_SERVICE###',job.getMonitoringService().getWrapperScriptConstructorText())

        from Ganga.Utility.Config import getConfig
        
        text = text.replace('###GANGADIR###',repr(getConfig('System')['GANGA_PYTHONPATH']))

        import Ganga.PACKAGE
        text = text.replace('###SUBPROCESS_PYTHONPATH###',repr(Ganga.PACKAGE.setup.getPackagePath2('subprocess','syspath',force=True)))
        text = text.replace('###TARFILE_PYTHONPATH###',repr(Ganga.PACKAGE.setup.getPackagePath2('tarfile','syspath',force=True)))

        from Ganga.GPIDev.Lib.File import FileBuffer
        
        return job.getInputWorkspace().writefile(FileBuffer('__jobscript__',text),executable=1)

    def postprocess(self, outputfiles, outputdir):    

        if len(outputfiles) > 0:
            for outputFile in outputfiles:
                if outputFile.__class__.__name__ in ['OutputSandboxFile', 'LCGStorageElementFile']:
                    outputFile.put()    

        def findOutputFile(className, pattern):
            for outputfile in outputfiles:
                if outputfile.__class__.__name__ == className and outputfile.name == pattern:
                    return outputfile

            return None 

        postprocessLocationsPath = os.path.join(outputdir, '__postprocesslocations__')

        if not os.path.exists(postprocessLocationsPath):
            return

        postprocesslocations = open(postprocessLocationsPath, 'r')
        
        for line in postprocesslocations.readlines():
            lineParts = line.split(' ') 
            outputType = lineParts[0] 
            outputPattern = lineParts[1]
            outputPath = lineParts[2]           

            if line.startswith('massstorage'):
                outputFile = findOutputFile('MassStorageFile', outputPattern)
                if outputFile is not None:
                    outputFile.setLocation(outputPath.strip('\n'))
            else:
                pass
                #to be implemented for other output file types
                
        postprocesslocations.close()
  
        os.system('rm %s' % postprocessLocationsPath)

    def updateMonitoringInformation(jobs):

        import re
        repid = re.compile(r'^PID: (?P<pid>\d+)',re.M)
        requeue = re.compile(r'^QUEUE: (?P<queue>\S+)',re.M)
        reactualCE = re.compile(r'^ACTUALCE: (?P<actualCE>\S+)',re.M)
        reexit = re.compile(r'^EXITCODE: (?P<exitcode>\d+)',re.M)

        def get_last_alive(f):
            """Time since the statusfile was last touched in seconds"""
            import os.path,time
            talive = 0
            try:
                talive = time.time()-os.path.getmtime(f)
            except OSError,x:
                logger.debug('Problem reading status file: %s (%s)',f,str(x))
                
            return talive

        def get_status(f):
            """Give (pid,queue,actualCE,exit code) for job"""

            pid,queue,actualCE,exitcode=None,None,None,None

            import re
            try:
                statusfile=file(f)
                stat = statusfile.read()
            except IOError,x:
                logger.debug('Problem reading status file: %s (%s)',f,str(x))
                return pid,queue,actualCE,exitcode

            mpid = repid.search(stat)
            if mpid:
                pid = int(mpid.group('pid'))          
            
            mqueue = requeue.search(stat)
            if mqueue:
                queue = str(mqueue.group('queue'))          

            mactualCE = reactualCE.search(stat)
            if mactualCE:
                actualCE = str(mactualCE.group('actualCE'))          

            mexit = reexit.search(stat)
            if mexit:
                exitcode = int(mexit.group('exitcode'))
            
            return pid,queue,actualCE,exitcode

        from Ganga.Utility.Config import getConfig
        for j in jobs:
            outw=j.getOutputWorkspace()

            statusfile = os.path.join(outw.getPath(),'__jobstatus__')
            heartbeatfile = os.path.join(outw.getPath(),'__heartbeat__')
            pid,queue,actualCE,exitcode = get_status(statusfile)

            if j.status == 'submitted':
                if pid or queue:
                    j.updateStatus('running')

                    if pid:
                        j.backend.id = pid
                        
                    if queue and queue != j.backend.actualqueue:
                        j.backend.actualqueue = queue

                    if actualCE:
                        j.backend.actualCE = actualCE

            if j.status == 'running':
                if exitcode != None:
                    # Job has finished
                    j.backend.exitcode = exitcode
                    if exitcode == 0:
                        j.updateStatus('completed')
                    else:
                        j.updateStatus('failed')
                else:
                    # Job is still running. Check if alive
                    time = get_last_alive(heartbeatfile)
                    config = getConfig(j.backend._name)
                    if time>config['timeout']:
                        logger.warning('Job %s has disappeared from the batch system.', str(j.getFQID('.')))
                        j.updateStatus('failed')

    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

#____________________________________________________________________________________
        
config = Ganga.Utility.Config.makeConfig('LSF','internal LSF command line interface')

#fix bug #21687
config.addOption('shared_python_executable', False, "Shared PYTHON")

config.addOption('jobid_name', 'LSB_BATCH_JID', "Name of environment with ID of the job")
config.addOption('queue_name', 'LSB_QUEUE', "Name of environment with queue name of the job")
config.addOption('heartbeat_frequency', '30', "Heartbeat frequency config variable")

config.addOption('submit_str', 'cd %s; bsub %s %s %s %s', "String used to submit job to queue")
config.addOption('submit_res_pattern', '^Job <(?P<id>\d*)> is submitted to .*queue <(?P<queue>\S*)>',
                  "String pattern for replay from the submit command")

config.addOption('stdoutConfig', '-o %s/stdout', "String pattern for defining the stdout")
config.addOption('stderrConfig', '-e %s/stderr', "String pattern for defining the stderr")

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
config.addOption('timeout',600,'Timeout in seconds after which a job is declared killed if it has not touched its heartbeat file. Heartbeat is touched every 30s so do not set this below 120 or so.')

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
config.addOption('heartbeat_frequency', '30', "Heartbeat frequency config variable")

config.addOption('submit_str', 'cd %s; qsub %s %s %s %s', "String used to submit job to queue")
config.addOption('submit_res_pattern', '^(?P<id>\d*)\.pbs\s*', "String pattern for replay from the submit command")

config.addOption('stdoutConfig', '-o %s/stdout', "String pattern for defining the stdout")
config.addOption('stderrConfig', '-e %s/stderr', "String pattern for defining the stderr")

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
env = os.environ
jobnumid = env["PBS_JOBID"]
os.chdir("/tmp/")
os.system("rm -rf /tmp/%s/" %jobnumid) 
'''
config.addOption('postexecute', tempstr, "String contains commands executing before submiting job to queue")
config.addOption('jobnameopt', 'N', "String contains option name for name of job in batch system")
config.addOption('timeout',600,'Timeout in seconds after which a job is declared killed if it has not touched its heartbeat file. Heartbeat is touched every 30s so do not set this below 120 or so.')


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
config.addOption('heartbeat_frequency', '30', "Heartbeat frequency config variable")

#the -V options means that all environment variables are transferred to the batch job (ie the same as the default behaviour on LSF at CERN)
config.addOption('submit_str', 'cd %s; qsub -cwd -V %s %s %s %s', "String used to submit job to queue")
config.addOption('submit_res_pattern', 'Your job (?P<id>\d+) (.+)', "String pattern for replay from the submit command")

config.addOption('stdoutConfig', '-o %s/stdout', "String pattern for defining the stdout")
config.addOption('stderrConfig', '-e %s/stderr', "String pattern for defining the stderr")

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
config.addOption('timeout',600,'Timeout in seconds after which a job is declared killed if it has not touched its heartbeat file. Heartbeat is touched every 30s so do not set this below 120 or so.')


class SGE(Batch):
    ''' SGE backend - submit jobs to Sun Grid Engine.
    '''
    _schema = Batch._schema.inherit_copy()
    _category = 'backends'
    _name = 'SGE'

    config = Ganga.Utility.Config.getConfig('SGE')
    def __init__(self):
        super(SGE,self).__init__()
                
