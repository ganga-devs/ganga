"""
Dirac.py: The Ganga backendhandler for the Dirac system
"""
__revision__ = 0.2
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Credentials import GridProxy
from Ganga.GPIDev.Lib.File import  FileBuffer, File
from Ganga.GPIDev.Schema import *
from Ganga.Utility.tempfile_compatibility import *
from Ganga.Core import BackendError
from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper

import DiracShared

import Ganga.Utility.Config
import Ganga.Utility.logging
from GangaLHCb.Lib.LHCbDataset import LHCbDataset,LHCbDataFile,string_dataset_shortcut,string_datafile_shortcut
logger = Ganga.Utility.logging.getLogger()

configLHCb = Ganga.Utility.Config.getConfig('LHCb')

configDirac = Ganga.Utility.Config.getConfig('DIRAC')

try:
    import os

    # get the environment
    from Ganga.Utility.GridShell import getShell
    gShell=getShell()
    os.environ['X509_CERT_DIR'] = gShell.env['X509_CERT_DIR']
    logger.debug('Setting the envrionment variable X509_CERT_DIR to ' + os.environ['X509_CERT_DIR']+'.')

except:
    pass

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from ExeDiracRunTimeHandler import *
from RootDiracRunTimeHandler import *
allHandlers.add('Executable','Dirac', ExeDiracRunTimeHandler)
allHandlers.add('Root','Dirac', RootDiracRunTimeHandler)

class Dirac(IBackend):
    """The backend that submits LHCb jobs to the Grid via DIRAC

The backend for LHCb jobs to be submitted to the Grid. Jobs are
submitted through the DIRAC WMS system and then in turn submitted to the
Grid. A few examples of usage are given below

#Run a DaVinci job on the Grid

# Create a DaVinci application object. See DaVinci help text for instructions
# on how to configure this.
app = DaVinci()

# Create Dirac backend object with CPU time limit of 10 hours
b = Dirac(CPUTime=36000)

# Create and submit job.
j = Job(application=app,backend=b)
j.submit()


# Run a Root job on the Grid if in LHCb VO

# Create a Root application object. See Root help text for instructions
# on how to configure this.
app = Root()

# Create and submit job to Dirac using default options
j = Job(application=app,backend=Dirac())
j.submit()
    
    """
    _schema = Schema(Version(2, 0), 
            {'id': SimpleItem(defvalue = None, protected = 1, copyable = 0, typelist=['int','type(None)'],
                              doc='''The id number assigned to the job by the
DIRAC WMS. If seeking help on jobs with the Dirac backend, please always
report this id number in addition to a full description of your problem.
The id can also be used to further inspect the job at
https://lhcbweb.pic.es/DIRAC/info/general/diracOverview '''),
            'status':SimpleItem(defvalue = None, protected = 1, copyable = 0,typelist=['str','type(None)'],
                                doc='''The detailed status as reported by
the DIRAC WMS'''),
            'CPUTime': SimpleItem(defvalue=86400,checkset='_checkset_CPUTime',
                                  doc='''The requested CPU time in seconds'''),
            'actualCE': SimpleItem(defvalue=None, protected=1, copyable=0,typelist=['str','type(None)'],
                                   doc='''The location where the job ran'''),
            'statusInfo' : SimpleItem(defvalue='', protected=1, copyable=0,typelist=['str','type(None)'],
                                   doc='''Minor status information from Dirac''')})

    _exportmethods = ['getOutput','getOutputData','peek']
    _packed_input_sandbox = True

    _category="backends"
    _name = 'Dirac'
    def __init__(self):
        super(Dirac, self).__init__()
  

    def _checkset_CPUTime(self,value):
        """Adjust the CPUTime if needed"""
        if not value:
            return
        
        from bisect import bisect
        queues = [ 300, 900, 3600, 18000, 43200, 86400, 129600, 172800, 216000 ]
        time = queues[bisect(queues,value-1e-9)]
        if value>queues[-1]: time=queue[-1]
        if time != value:
            logger.info("Adjusting CPU time request from %d to the Dirac queue %d" % (value, time))
            self.CPUTime = time
        return 
    
    def _handleGridProxy(self):
        """Check the Grid Proxy validity"""
        proxy = GridProxy()
        # Time on top of CPU time should be adjustable
        mintime = float(configDirac['extraProxytime'])
        try:
            timeleft = float(proxy.timeleft("hours"))*3600
        except ValueError:
            timeleft = 0.0

        if timeleft < mintime:
            logger.warning("Failed to submit job. Grid proxy validity %s s, while %s s required" % (str(timeleft),str(mintime)))
            raise BackendError("Dirac", "Failed to submit job. Grid proxy validity %s s, while %s s required" % (str(timeleft),str(mintime)))
        
    def _diracverbosity(self):
        if configDirac['DiracLoggerLevel']=='DEBUG':
            return ''
        else:
            return 'quiet'

    def submit(self, subjobconfig, master_input_sandbox):
        """Submit a DIRAC job"""
        # Get the reference to a the job object
        job=self.getJobObject()
        fqid=job.getFQID('.')

        # Append to the DIRACscript items which are shared by all applications
        from DiracScript import DiracScript
        diracScript = subjobconfig.script
        self._handleApplication(subjobconfig,master_input_sandbox)
        self._handleInputSandbox(subjobconfig,master_input_sandbox)
        self._handleOutputSandbox(subjobconfig,master_input_sandbox)
        self._handleGridProxy()
        if self.CPUTime:
            diracScript.append("setCPUTime("+str(self.CPUTime)+")")

        #set the destination - can use 'localhost' here for agent mode
        diracScript.setDestination(configDirac['DIRACsite'])

        # Write script into input sandbox and submit
        diracScript.write(job)
        return self._diracsubmit()

    def resubmit(self):
        """Resubmit a DIRAC job"""
        self._handleGridProxy()
        return self._diracsubmit()

    def _diracsubmit(self):
        """Perform the actual submission to DIRAC"""
        job=self.getJobObject()
        fqid=job.getFQID('.')

        from DiracScript import DiracScript
        diracScript = DiracScript(job)
        logger.debug(diracScript.commands())
        self.id=None
        self.actualCE=None
        self.status=None

        # There seems to be a problem with the DIRAC API accepting a full path for
        # input sandbox files. Implement a workaround for this where everything is
        # copied into a single directory.
        def tempcopy(src,dst):
            import shutil,os, os.path
            if not os.path.isdir(dst):
                os.mkdir(dst)
            import shutil
            names = os.listdir(src)
            for name in names:
                srcname = os.path.join(src, name)
                dstname = os.path.join(dst, name)
                if os.path.isdir(srcname):
                    tempcopy(srcname, dstname)
                else:
                    shutil.copy2(srcname, dstname)

        tempdir=mkdtemp()

        if job.master:
            tempcopy(job._getParent().getInputWorkspace().getPath(),tempdir)
        tempcopy(job.getInputWorkspace().getPath(),tempdir)

        import os
        pwd=os.getcwd()
        try:
            os.chdir(tempdir)
            self.id = diracScript.execute()
        finally:
            os.chdir(pwd)

        import shutil
        shutil.rmtree(tempdir)

        if self.id==None or self.id == -1:
            raise BackendError("Dirac","Ganga Job %s submitted as Dirac job id %s" % (fqid,str( self.id )))
            
        logger.info( "Ganga Job %s submitted as Dirac job id %s" , fqid,str( self.id )) 
        return type(self.id)==int

    def kill(self ):
        """ Kill a Dirac jobs"""

        command = """
result = dirac.kill(%i)
if not result.get('OK',False): rc = -1
        """ % self.id
        if diracwrapper(command).returnCode != 0:
            job=self.getJobObject()
            fqid=job.getFQID('.')
            raise BackendError('Dirac', "Could not kill job %s. Try with a higher Dirac Log level set." % fqid)
        return 1
    
    def peek(self):
        """Peek at the output of a job"""
        command = """
result = dirac.peek(%i)
if not result.get('OK',False): rc = -1
storeResult(result)        
        """ % self.id
        
        dw = diracwrapper(command)
        result = dw.getOutput()

        if result is not None and result.get('OK',False):
            print result['Value']
        else:
            logger.error("No peeking available for Dirac job '%i'.", self.id)
    
    
    def getOutput(self,dir=os.curdir):
        """Retrieve the outputsandbox from the DIRAC WMS. The dir argument gives the directory the the sandbox will be retrieved into"""
        
        command = """
def getOutput(dirac, num):

    outputDir = os.path.join('%s',str(num))
    id = %d
    
    if not os.path.exists(outputDir):
        os.mkdir(outputDir)

    pwd = os.getcwd()
    result = None
    try:
        #call now downloads oversized sandboxes if there are there
        result = dirac.getOutputSandbox(id,outputDir=outputDir)
    finally:
        if os.getcwd() != pwd: os.chdir(pwd)
        
    files = []
    if result is not None and result.get('OK',False):
        outdir = os.path.join(outputDir,str(id))
        files = listdirs(outdir)
        
        if files:
            result['Value'] = files
        else:
            result['OK'] = False
            result['Message'] = 'Failed to find downloaded files on the local file system.'
    
    return result

for i in range(3):
    result = getOutput(dirac, i)
    if (result is None) or (result is not None and not result.get('OK', False)):
            import time
            time.sleep(5)
            rc = 1
    else:
        storeResult(result)
        rc = 0
        break
""" % (dir, self.id)
        
        dw = diracwrapper(command)
        result = dw.getOutput()
        
        if dw.returnCode != 0 or (result is None) or (result is not None and not result.get('OK',False)):
            job = self.getJobObject()
            fqid = job.getFQID('.')
            if result is None: result = {}
            logger.warning("Job %s with Dirac id %s at %s: Problems retrieving output. Message was '%s'.",\
                               fqid,str(job.backend.id),job.backend.actualCE, result.get('Message','None'))
            return []
        return result.get('Value',[])

    def master_prepare(self,masterjobconfig):
        """Add Python files specific to the DIRAC backend to the master inputsandbox"""

        import Ganga.Core.Sandbox as Sandbox
        packed_files = masterjobconfig.getSandboxFiles()
        import Ganga.PACKAGE
        Ganga.PACKAGE.setup.getPackagePath2('subprocess','syspath',force=True)
        import subprocess
        packed_files.extend(Sandbox.getGangaModulesAsSandboxFiles(
            [subprocess]))
        import Ganga.Utility.files
        packed_files.extend(Sandbox.getGangaModulesAsSandboxFiles(Sandbox.getDefaultModules()))

        return IBackend.master_prepare(self,masterjobconfig)

    def _add_outputdata(self,data):
        import re
        list=[]

        job=self.getJobObject()
        ds=LHCbDataset()
        for i in data:
            notefile=file(os.path.join(job.inputsandbox,str(self.id),i+'.note'))
            note=notefile.read()
            m=re.compile(r'^LFN: (?P<lfn>.*)',re.M).search(note)
            if m in None: 
                logger.debug('Could not find lfn in note file %s',notefile.name)
            else:
                list.append('LFN:'+m.group('lfn'))
        
        job.outputdata=string_dataset_shortcut(list,None)

    def getOutputData(self, dir='', names = None):
        """Retrieve data created by job and stored on SE

        dir   Copy the output to this directory. The output workspace of the job
              is the default.
        names The list of files to retrieve. If empty all files registered as
              outputdata will be retrieved. If names are simple file names the default
              LFN will be prepended. If names start with a / it is assumed to be the
              complete path. Do never add an "LFN:" to the name.
        """
        if names is None:
            names = []
            
        downloadedFiles = []
        
        if type(names)!=type([]):
            logger.error('Aborting. The names argument has to be a list of strings')
            return

        job=self.getJobObject()
        if not dir:
            dir = job.getOutputWorkspace().getPath()

        try:
            if len(names)==0 and job.outputdata:
                names = [f.name for f in job.outputdata.files]
                
            if names:
                logger.debug('Retrieving the files '+str(names)+' from the Storage Element')            
                command = """
id = %(ID)d
files = %(FILES)s
outputdir = '%(OUTPUTDIR)s'

def getFiles():
    pwd = os.getcwd()
    result = None
    try:
        #call now downloads oversized sandboxes if there are there
        os.chdir(outputdir)
        result = dirac.getJobOutputData(id,outputFiles=files)
    finally:
        os.chdir(pwd)
    return result

result = None
for i in range(3): #retry
    if not hasattr(dirac,'getJobOutputData'):
        result = {'OK':False,'Message':'The version of the Dirac client needs to be upgraded for this to work!'}
        break
    result = getFiles()
    if result is not None and result.get('OK',False):
        rc = 0
        break
if result is None:
    result = {'OK':False,'Message':'Failed to download the outputdata files. The reason is not known'}
storeResult(result)
                """ % {'FILES':str(names),'OUTPUTDIR':dir,'ID':self.id}
                
                dw = diracwrapper(command)
                result = dw.getOutput()

                if result is not None:
                    if result.get('OK',False):
                        if result.has_key('Value'):
                            files = result['Value']
                            
                            import os
                            for f in files:
                                downloadedFiles.append(os.path.basename(f))
                            for n in names:
                                if not n in downloadedFiles:
                                    logger.warning("Output download failed for file: '%s'", str(n))
                            
                    elif result.has_key('Message'):
                         logger.error("Output download failed: '%s'", str(result['Message']))
                         
        except Exception, e:
            pass
        
        return downloadedFiles

  
    def _handleInputSandbox(self,subjobconfig,master_input_sandbox):
        """Default implementation. Just deals with input sandbox"""
        job=self.getJobObject()
        sboxname=job.createPackedInputSandbox(subjobconfig.getSandboxFiles())
        from os.path import basename
        input_sandbox = ['jobscript.py',basename(sboxname[0])]+\
                        [basename(f) for f in master_input_sandbox]
        logger.debug('Defining input sandbox as %s',str(input_sandbox))
        subjobconfig.script.append("setInputSandbox("+
                                str(input_sandbox)+
                                ")")
    
    def _handleOutputSandbox(self,subjobconfig,master_input_sandbox):
        """Default implementation. Just deals with output sandbox"""
        
        logger.debug('Defining output sandbox')
        outputsandbox = ['_output_sandbox.tgz','__jobstatus__','stdout','stderr' ]
        try:
            outputsandbox.append(subjobconfig.logfile)
        except AttributeError:
            pass
        subjobconfig.script.append("setOutputSandbox("+str(outputsandbox)+")")

    def _handleApplication(self,subjobconfig,master_input_sandbox):
        """Runs the specified script with enviroment variables set"""
       
        def quote_arguments(args):
            quoted = ""
            for a in args:
                quoted += '"'+a+'" '
            return quoted

        job=self.getJobObject()

        appscriptpath = [subjobconfig.getExeString()+' '+\
                         quote_arguments(subjobconfig.getArgStrings())]

        outputpatterns=subjobconfig.outputbox
        environment=subjobconfig.env

        script= """#!/usr/bin/env python

import os,os.path,shutil,tempfile
from os.path import join
import sys,time

import sys

wdir = os.getcwd()

if len(sys.argv)>1 and sys.argv[1] == 'subprocess':
 os.setsid()

############################################################################################

###INLINEMODULES###

############################################################################################

sys.path.insert(0,os.path.join(wdir,PYTHON_DIR))

input_sandbox = ###INPUT_SANDBOX###
outputpatterns = ###OUTPUTPATTERNS###
appscriptpath = ###APPSCRIPTPATH###
environment = ###ENVIRONMENT###

statusfilename = join(wdir,'__jobstatus__')

try:
  statusfile=file(statusfilename,'w')
except IOError,x:
  print 'ERROR: not able to write a status file: ', statusfilename
  print 'ERROR: ',x
  raise


#for f in input_sandbox:
#  getPackedInputSandbox(f)

try:
  for key,value in environment.iteritems():
    os.environ[key] = value
except AttributeError:
  pass

outfile=file('stdout','w')
errorfile=file('stderr','w')

###MONITORING_SERVICE###
#monitor = createMonitoringObject()
#monitor.start()

print >> statusfile, 'START: '+time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time()))

import subprocess
try:
  subprocess.Popen("chmod +x "+appscriptpath[0].split()[0], shell=True)
except:
  pass
child = subprocess.Popen(appscriptpath, shell=True, stdout=outfile, stderr=errorfile)

print >> statusfile, 'PID: %d'%child.pid
statusfile.flush()

result = -1

try:
  while 1:
    result = child.poll()
    if result is not None:
        break
    outfile.flush()
    errorfile.flush()
#    monitor.progress()
    time.sleep(0.3)
finally:
  pass
#  monitor.progress()

#monitor.stop(result)

outfile.flush()
errorfile.flush()

createPackedOutputSandbox(outputpatterns,None,wdir)

outfile.close()
errorfile.close()

subprocess.Popen("ls -l", shell=True)

line="EXITCODE: " + repr(result) + os.linesep
line+='STOP: '+time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
statusfile.writelines(line)
"""

        import Ganga.Core.Sandbox as Sandbox
        from inspect import getsource
        from os.path import basename
        script = script.replace('###INLINEMODULES###',getsource(Sandbox.WNSandbox))
        name='_input_sandbox_'+job.getFQID('_')+'.tgz'
        script = script.replace('###INPUT_SANDBOX###',
                                repr([name]+
                                     [basename(f) for f in master_input_sandbox]))
        script = script.replace('###APPLICATION_NAME###',repr(job.application._name))
        script = script.replace('###APPSCRIPTPATH###',repr(appscriptpath))
        script = script.replace('###OUTPUTPATTERNS###',repr(outputpatterns))
        script = script.replace('###JOBID###',repr(job.getFQID('.')))
        script = script.replace('###ENVIRONMENT###',repr(environment))
      
        # These files are the files DIRAC eventually will retrieve
        outputpatterns=['_output_sandbox.tgz','stdout','stderr','__jobstatus__']

#        script = script.replace('###MONITORING_SERVICE###',job.getMonitoringService().getWrapperScriptConstructorText())

        job.getInputWorkspace().writefile(FileBuffer('jobscript.py',script),executable=1)
  
    def updateMonitoringInformation( jobs ):
        """Check the status of jobs and retrieve output sandboxes"""

        def _get_DIRACstatus(jobs):
            """Retrieve status information from Dirac and return as list"""

            # Translate between the many statuses in DIRAC and the few in Ganga
            statusmapping = {'Checking' : 'submitted',
                             'Completed' : 'completed',
                             'Deleted' : 'failed',
                             'Done' : 'completed',
                             'Failed' : 'failed',
                             'Killed' : 'killed',
                             'Matched' : 'submitted',
                             'Received' : 'submitted',
                             'Running' : 'running',
                             'Staging' : 'submitted',
                             'Stalled' : 'failed',
                             'Waiting' : 'submitted'}

            # Get status information from DIRAC in bulk operation
            djobids=[j.backend.id for j in jobs]
                    
            command = """
result = dirac.status(%s)
if not result.get('OK',False): rc = -1
storeResult(result)
        """ % str(djobids)
            
            dw = diracwrapper(command)
            result = dw.getOutput()
            
            statusList = []
            if result is None or dw.returnCode != 0:
                logger.warning('No monitoring information could be obtained, and no reason was given.')
                return statusList
            
            if not result['OK']:
                msg = result.get('Message',None)
                if msg is None:
                    msg = result.get('Exception',None)
                logger.warning("No monitoring information could be obtained. The Dirac error message was '%s'", str(msg))
                return statusList
            
            bulkStatus = result['Value']

            for j in jobs:
                diracid=j.backend.id
                try:
                    minorStatus=bulkStatus[diracid]['MinorStatus']
                    diracStatus=bulkStatus[diracid]['Status']
                    diracSite=bulkStatus[diracid]['Site']
                except KeyError:
                    logger.info('No monitoring information for job %s with Dirac id %s',
                                str(j.id),str(j.backend.id))
                    continue
                try:
                    gangaStatus=statusmapping[diracStatus]
                except KeyError:
                    logger.warning('Unknown DIRAC status %s for job %s',
                                   diracStatus,str(j.id))
                    continue
                statusList.append((j,diracStatus,diracSite,gangaStatus,minorStatus))
            return statusList

        def get_exit_code(f):
            import re
            statusfile=file(f)
            stat = statusfile.read()
            m = re.compile(r'^EXITCODE: (?P<exitcode>\d*)',re.M).search(stat)

            if m is None:
                return None
            else:
                return int(m.group('exitcode'))

        for j,diracStatus,diracSite,gangaStatus,minorStatus in _get_DIRACstatus(jobs):

            # Update backend information
            if diracStatus != j.backend.status or\
               diracSite != j.backend.actualCE or\
               minorStatus != j.backend.statusInfo:
                j.backend.status=diracStatus
                j.backend.statusInfo = minorStatus
                j.backend.actualCE=diracSite
                logger.debug("Ganga Job %s: Dirac job %s at %s: %s" ,
                             str(j.id),str(j.backend.id),
                             j.backend.actualCE, j.backend.status) 

                if ("completed" != gangaStatus) and (gangaStatus != j.status):
                    j.updateStatus(gangaStatus)

            # Retrieve output while in completing state
            if ("completed" == gangaStatus):

                from Ganga.Core import Sandbox
                
                j.updateStatus("completing")
                outputsandboxname = Sandbox.OUTPUT_TARBALL_NAME
                outw=j.getOutputWorkspace()
                jobDir = outw.getPath()
                tmpdir = tempfile.mkdtemp()

                # Get output sandbox from DIRAC WMS
                filelist = j.backend.getOutput(tmpdir)

                import os
                import shutil
                
                for f in filelist:
                    try:
                        # Handle the untaring of the sandbox
                        if os.path.basename(f) == outputsandboxname:
                            Sandbox.getPackedOutputSandbox(os.path.dirname(f),jobDir)
                        else:
                            shutil.copy2(f,jobDir)
                        os.unlink(f)
                    except OSError:
                        logger.warning("Failed to move file '%s' file  from '%s'", f, tmpdir)
                
                # Get sandbox from SE if uploaded there
                if outputsandboxname+'.note' in filelist:
                    j.backend.getOutputData(names=[outputsandboxname])

                # try to get the application exit code from the status file
                try:
                  statusfile = os.path.join(outw.getPath(),'__jobstatus__')
                  exitcode = get_exit_code(statusfile)
                  logger.debug('status file: %s %s',statusfile,file(statusfile).read())
                except IOError:
                  logger.debug('Problem reading status file: %s',statusfile)
                  exitcode=None
                except Exception:
                    logger.critical('Problem during monitoring')
                    raise

                if exitcode is not None and exitcode == 0 and gangaStatus != 'failed':
                    j.updateStatus('completed')
                else:
                    j.updateStatus('failed')
                    
    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

#
#
## $Log: not supported by cvs2svn $
## Revision 1.11  2008/12/08 11:56:20  wreece
## Improves the overlarge sandbox handling to make use of the Dirac OutputSandboxLFN parameter and do the untarring.
##
## Revision 1.10  2008/12/04 12:00:36  wreece
## tip from Andrei on multiple lfns
##
## Revision 1.9  2008/12/04 11:24:09  wreece
## savannah 44923: use the dirac job parameters to construct the lfn rather than guessing
##
## Revision 1.8  2008/11/19 09:17:04  wreece
## Improves the error reporting for monitoring
##
## Revision 1.7  2008/10/30 16:05:04  wreece
## Adds a warning if the sandbox was oversized.
##
## Revision 1.6  2008/10/27 14:46:17  wreece
## Updates the URL for the monitoring page
##
## Revision 1.5  2008/10/27 14:42:23  wreece
## stops failed jobs from downloading their sandboxes
##
## Revision 1.4  2008/10/27 11:06:53  wreece
## Dirac 3 merged to HEAD. Need to perform lots of testing
##
## Revision 1.3  2008/09/26 12:09:42  wreece
## Updates the schema definitions for Ganga 5.0.9-pre. Just adds a few type(None)s to the allowed types.
##
## Revision 1.2  2008/08/07 22:07:47  uegede
## Added a GaudiPython application handler with runtime handlers for Local/Batch
## and for DIRAC.
##
## Revision 1.1  2008/07/17 16:41:23  moscicki
## migration of 5.0.2 to HEAD
##
## the doc and release/tools have been taken from HEAD
##
## Revision 1.71.6.12  2008/05/08 14:55:07  uegede
## Fixed problem with comparison of float and string types.
##
## Revision 1.71.6.11  2008/05/07 15:33:59  uegede
## Updated 5.0 branch from trunk.
##
## Revision 1.71.6.10  2008/05/07 14:31:26  uegede
## Merged from trunk
##
## Revision 1.84  2008/05/01 15:47:45  uegede
## - Removed forced 32 bit running
## - Took away modifications to LD_LIBRARY_PATH
## - Introduced "diracwrapper"to execute DIRAC commands in separate
##   process with a different environment. Updates to LHCbDataset and
##   Dirac backend to make use of this feature.
##
## Revision 1.71.6.9  2008/04/15 08:06:20  wreece
## savannah 35488 - default modules not making it into the sandbox.
##
## Revision 1.71.6.8  2008/04/04 15:11:17  andrew
## Schema changes:
##   * make optsfile a list
##   * rename cmt_user_path to user_release_area
##   * rename cmt_release_area to lhcb_release_area
##
## Add type info to Gaudi schema
##
## Adapt code for schema changes
##
## Revision 1.71.6.7  2008/04/04 10:01:01  andrew
## Merge from head
##
## Revision 1.71.6.6  2008/04/03 11:33:07  wreece
## fix for savannah 33823 - dirac changing the working dir (merge from head)
##
## Revision 1.71.6.5  2008/03/17 12:08:04  andrew
## Tried to fix the worst problems after merging from head
##
## Revision 1.71.6.4  2008/03/17 11:08:26  andrew
## Merge from head
##
## Revision 1.71.6.3  2007/12/13 09:30:59  wreece
## Initial pass at porting GangaLHCb to the new config system. Also sorts out some of the warnings in the files.
##
## Revision 1.71.6.2  2007/12/12 19:54:02  wreece
## Merge in changes from HEAD.
##
## Revision 1.76  2007/12/04 14:30:36  andrew
## change the Gridshell import
##
## Revision 1.75  2007/11/20 12:13:52  uegede
## Fix to allow non-DaVinci Gaudi jobs. Implementation of quiet feature
## for DIRAC API.
##
## Revision 1.74  2007/10/15 15:30:20  uegede
## Merge from Ganga_4-4-0-dev-branch-ulrik-dirac with new Dirac backend
##
## Revision 1.73  2007/10/12 11:55:20  andrew
## Added the setPlatform string to the Dirac script
##
## Revision 1.72  2007/10/12 11:46:42  andrew
## Add platfom string
##
## Revision 1.71.2.7  2007/10/15 13:59:03  uegede
## Bugfix in location of Shell module
##
## Revision 1.71.2.6  2007/10/15 13:58:00  uegede
## Updated Dirac backend to use new wrappers for lcg commands used inside DIRAC.
##
## Revision 1.71.2.5  2007/10/08 11:36:01  uegede
## - Added test cases
## - Debugged retrieval of outputdata in Dirac backend. New exported function
##   getOutputData
## - Detects oversized  sandboxes gone to SE and download them
## - Made cmt_release_areanon-hidden.
##
## Revision 1.71.2.4  2007/09/18 15:42:27  uegede
## Root using the pre-installed versions on the Grid implemented for Dirac backend.
## Test cases written
## Updates to division between master and subjob sandboxes.
##
## Revision 1.71.2.3  2007/09/13 20:54:19  uegede
## Gaudi jobs now have correct XML slice for file catalogue and automatic detection
## of output files re-enabled.
##
## Revision 1.71.2.2  2007/09/07 15:08:37  uegede
## Dirac backend and runtime handlers updated to be controlled by a Python script.
## Gaudi jobs work with this as well now.
## Some problems with the use of absolute path in the DIRAC API are still unsolved.
## See workaround implemented in Dirac.py
##
## Revision 1.71.2.1  2007/08/15 15:50:14  uegede
## Develop a new dirac backend handler. Work in progress. The Executable and Root appliaction
## should work but Gaudi doesn't.
##
## Revision 1.81  2008/03/07 17:01:18  uegede
## Lib/Dirac/Dirac.py: Extratime check now require only 10 min proxy validity
## test/GPI/Dirac/ExeDiracErrorCode.gpi: Bug fixed in definition of job
##
## Revision 1.80  2008/03/07 16:19:46  andrew
## Fix for bug #21546: Force use of DiracTopDir, even when LHCBPRODROOT is
##       defined
##
## Revision 1.79  2008/03/06 22:27:21  uegede
## Fix for failing to return error code correctly for ROOT scripts.
##
## Revision 1.78  2008/03/05 21:50:40  uegede
## Fixed missing check of proxy validity in Dirac backend when resubmitting.
##
## Added test case for this, but it currently fails due to bug in
## state change when resubmission fails.
##
## Removed some old test files that are no longer relevant.
##
## Revision 1.77  2008/01/23 23:15:42  uegede
## - Changed default DIRAC version to v2r18
## - Changed magic line in python script for DIRAC to have
##   "/bin/env python". this ensures that python version which is in PATH
##   is started.
## - Removed Panoramix application type as it never worked
## - Removed GaudiLCG runtime handler as it is not functional.
##
## Revision 1.76  2007/12/04 14:30:36  andrew
## change the Gridshell import
