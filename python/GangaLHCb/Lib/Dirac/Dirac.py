#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""Dirac.py: The Ganga backendhandler for the Dirac system."""

import os
import os.path
import shutil
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
from GangaLHCb.Lib.LHCbDataset import LHCbDataset,LHCbDataFile,\
     string_dataset_shortcut,string_datafile_shortcut
import DiracUtils

try:
    # get the environment
    from Ganga.Utility.GridShell import getShell
    gShell=getShell()
    os.environ['X509_CERT_DIR'] = gShell.env['X509_CERT_DIR']
    logger.debug('Setting the envrionment variable X509_CERT_DIR to ' \
                 + os.environ['X509_CERT_DIR']+'.')
except:
    pass

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from ExeDiracRunTimeHandler import *
from RootDiracRunTimeHandler import *

logger = Ganga.Utility.logging.getLogger()
configLHCb = Ganga.Utility.Config.getConfig('LHCb')
configDirac = Ganga.Utility.Config.getConfig('DIRAC')

allHandlers.add('Executable','Dirac', ExeDiracRunTimeHandler)
allHandlers.add('Root','Dirac', RootDiracRunTimeHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Dirac(IBackend):
    """The backend that submits LHCb jobs to the Grid via DIRAC

    The backend for LHCb jobs to be submitted to the Grid. Jobs are
    submitted through the DIRAC WMS system and then in turn submitted to the
    Grid. A few examples of usage are given below

    #Run a DaVinci job on the Grid
    
    # Create a DaVinci application object. See DaVinci help text for
    # instructions on how to configure this.
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

    schema = {}
    docstr = 'The id number assigned to the job by the DIRAC WMS. If ' \
             'seeking help on jobs with the Dirac backend, please always ' \
             'report this id number in addition to a full description of ' \
             'your problem. The id can also be used to further inspect the ' \
             'job at https://lhcbweb.pic.es/DIRAC/info/general/diracOverview '
    schema['id'] = SimpleItem(defvalue=None, protected=1, copyable=0,
                              typelist=['int','type(None)'], doc=docstr)

    docstr = 'The detailed status as reported by the DIRAC WMS'    
    schema['status'] = SimpleItem(defvalue=None, protected=1, copyable=0,
                                  typelist=['str','type(None)'],doc=docstr)
    schema['CPUTime'] = SimpleItem(defvalue=86400,
                                   checkset='_checkset_CPUTime',
                                   doc='The requested CPU time in seconds')
    schema['actualCE'] = SimpleItem(defvalue=None, protected=1, copyable=0,
                                    typelist=['str','type(None)'],
                                    doc='The location where the job ran')
    docstr = 'Minor status information from Dirac'
    schema['statusInfo'] = SimpleItem(defvalue='', protected=1, copyable=0,
                                      typelist=['str','type(None)'],doc=docstr)
    
    _schema = Schema(Version(2, 0),schema)

    _exportmethods = ['getOutput','getOutputData','getOutputSandbox','getOutputDataLFNs','peek']
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
        queues = [300, 900, 3600, 18000, 43200, 86400, 129600, 172800, 216000]
        if value > queues[-1]: time=queues[-1]
        else: time = queues[bisect(queues,value-1e-9)]

        if time != value:
            logger.info("Adjusting CPU time request from %d to the Dirac " \
            "queue %d" % (value, time))
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
            logger.warning("Failed to submit job. Grid proxy validity %s s, " \
                           "while %s s required" \
                           % (str(timeleft),str(mintime)))
            raise BackendError("Dirac", "Failed to submit job. Grid proxy " \
                               "validity %s s, while %s s required" % \
                               (str(timeleft),str(mintime)))
        
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

        # There seems to be a problem with the DIRAC API accepting a full path
        # for input sandbox files. Implement a workaround for this where
        # everything is copied into a single directory.
        def tempcopy(src,dst):
            if not os.path.isdir(dst):
                os.mkdir(dst)
                
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

        pwd=os.getcwd()
        try:
            os.chdir(tempdir)
            self.id = diracScript.execute()
        finally:
            os.chdir(pwd)

        shutil.rmtree(tempdir)

        if self.id==None or self.id == -1:
            raise BackendError("Dirac","Ganga Job %s submitted as Dirac job " \
                               "id %s" % (fqid,str( self.id )))
        
        logger.info( "Ganga Job %s submitted as Dirac job id %s" ,
                     fqid,str( self.id ))
        
        return type(self.id)==int

    def kill(self ):
        """ Kill a Dirac jobs"""        
        command = DiracUtils.kill_command(self.id)

        if diracwrapper(command).returnCode != 0:
            job=self.getJobObject()
            fqid=job.getFQID('.')
            raise BackendError('Dirac', "Could not kill job %s. Try with a " \
                               "higher Dirac Log level set." % fqid)
        return 1
    
    def peek(self):
        """Peek at the output of a job"""
        command = DiracUtils.peek_command(self.id)
        dw = diracwrapper(command)
        result = dw.getOutput()

        if result is not None and result.get('OK',False):
            print result['Value']
        else:
            logger.error("No peeking available for Dirac job '%i'.", self.id)
    
    
    def getOutput(self,dir=os.curdir):
        """Retrieve the outputsandbox from the DIRAC WMS. The dir argument
        gives the directory the the sandbox will be retrieved into."""

        command = DiracUtils.getOutput_command(dir,self.id)        
        dw = diracwrapper(command)
        result = dw.getOutput()
        
        if dw.returnCode != 0 or (result is None) or \
               (result is not None and not result.get('OK',False)):
            job = self.getJobObject()
            fqid = job.getFQID('.')
            if result is None: result = {}
            msg = "Job %s with Dirac id %s at %s: Problems retrieving " \
                  "output. Message was '%s'."
            logger.warning(msg,fqid,str(job.backend.id),job.backend.actualCE, 
                           result.get('Message','None'))
            return []
        return result.get('Value',[])

    def master_prepare(self,masterjobconfig):
        """Add Python files specific to the DIRAC backend to the master
        inputsandbox"""

        import Ganga.Core.Sandbox as Sandbox
        packed_files = masterjobconfig.getSandboxFiles()
        import Ganga.PACKAGE
        Ganga.PACKAGE.setup.getPackagePath2('subprocess','syspath',force=True)
        import subprocess
        packed_files.extend(Sandbox.getGangaModulesAsSandboxFiles( \
            [subprocess]))
        import Ganga.Utility.files
        packed_files.extend(Sandbox.getGangaModulesAsSandboxFiles( \
            Sandbox.getDefaultModules()))

        return IBackend.master_prepare(self,masterjobconfig)

    def _add_outputdata(self,data):
        import re
        list=[]

        job=self.getJobObject()
        ds=LHCbDataset()
        for i in data:
            notefile=file(os.path.join(job.inputsandbox,str(self.id),
                                       i+'.note'))
            note=notefile.read()
            m=re.compile(r'^LFN: (?P<lfn>.*)',re.M).search(note)
            if m in None: 
                logger.debug('Could not find lfn in note file %s',
                             notefile.name)
            else:
                list.append('LFN:'+m.group('lfn'))
        
        job.outputdata=string_dataset_shortcut(list,None)

    def getOutputData(self, dir='', names = None):
        """Retrieve data created by job and stored on SE

        dir   Copy the output to this directory. The output workspace of the
              job is the default.
              
        names The list of files to retrieve. If empty all files registered as
              outputdata will be retrieved. If names are simple file names the
              default LFN will be prepended. If names start with a / it is
              assumed to be the complete path. Do never add an \"LFN:\" to the
              name.
        """
        if names is None:
            names = []
            
        downloadedFiles = []
        
        if type(names)!=type([]):
            logger.error('Aborting. The names argument has to be a list of ' \
                         'strings')
            return

        job=self.getJobObject()
        if not dir:
            dir = job.getOutputWorkspace().getPath()

        try:
            if len(names)==0 and job.outputdata:
                names = [f.name for f in job.outputdata.files]
                
            if names:
                logger.debug('Retrieving the files ' + str(names) + \
                             ' from the Storage Element')

                command = DiracUtils.getOutputData_command(names,dir,
                                                              self.id)
                
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
                                if not os.path.basename(n) in downloadedFiles:
                                    logger.warning("Output download failed ' \
                                                   'for file: '%s'", str(n))

                    elif result.has_key('Message'):
                         logger.error("Output download failed: '%s'",
                                      str(result['Message']))
                         
        except Exception, e:
            pass
        
        return downloadedFiles

    def getOutputDataLFNs(self):
        """Get a list of outputdata that has been uploaded by Dirac. Excludes the outputsandbox if it is there."""
        
        job = self.getJobObject()
        if not job.status == 'completed':
            logger.warning('LFN query will only work for completed jobs')
            return []
        
        command = DiracUtils.getOutputDataLFNs_command(self.id)
        dw = diracwrapper(command)
        result = dw.getOutput()
        lfns = []
        if result is not None:
            if not result.get('OK',False):
                logger.warning("LFNs not found - Message was '%s'.",result.get('Message',''))
            else:
                lfns = result.get('Value',[])
        else:
            logger.warning('LFN query failed for an unknow reason')
        
        return lfns
      
    def _handleInputSandbox(self,subjobconfig,master_input_sandbox):
        """Default implementation. Just deals with input sandbox"""
        job=self.getJobObject()
        sboxname=job.createPackedInputSandbox(subjobconfig.getSandboxFiles())
        from os.path import basename
        input_sandbox = ['jobscript.py',basename(sboxname[0])]+\
                        [basename(f) for f in master_input_sandbox]
        logger.debug('Defining input sandbox as %s',str(input_sandbox))
        subjobconfig.script.append("setInputSandbox("+
                                   str(input_sandbox)+")")
    
    def _handleOutputSandbox(self,subjobconfig,master_input_sandbox):
        """Default implementation. Just deals with output sandbox"""
        
        logger.debug('Defining output sandbox')
        outputsandbox = ['_output_sandbox.tgz','__jobstatus__','stdout',
                         'stderr' ]
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

        script = DiracUtils.application_script()

        import Ganga.Core.Sandbox as Sandbox
        from inspect import getsource
        from os.path import basename
        script = script.replace('###INLINEMODULES###',
                                getsource(Sandbox.WNSandbox))
        name = '_input_sandbox_' + job.getFQID('_') + '.tgz'
        r = repr([name] + [basename(f) for f in master_input_sandbox])
        script = script.replace('###INPUT_SANDBOX###', r)            
        script = script.replace('###APPLICATION_NAME###',
                                repr(job.application._name))
        script = script.replace('###APPSCRIPTPATH###',repr(appscriptpath))
        script = script.replace('###OUTPUTPATTERNS###',repr(outputpatterns))
        script = script.replace('###JOBID###',repr(job.getFQID('.')))
        script = script.replace('###ENVIRONMENT###',repr(environment))
      
        # These files are the files DIRAC eventually will retrieve
        outputpatterns=['_output_sandbox.tgz','stdout','stderr',
                        '__jobstatus__']

        #script = script.replace('###MONITORING_SERVICE###',\
        #job.getMonitoringService().getWrapperScriptConstructorText())

        job.getInputWorkspace().writefile(FileBuffer('jobscript.py',script),
                                          executable=1)
  
    def getOutputSandbox(self):
        """Downloads the outputsandbox into the job outputdir."""
        import Ganga.Core.Sandbox as Sandbox

        j = self.getJobObject()
        outputsandboxname = Sandbox.OUTPUT_TARBALL_NAME
        outw = j.getOutputWorkspace()
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
                    Sandbox.getPackedOutputSandbox(os.path.dirname(f),
                                                           jobDir)
                else:
                    shutil.copy2(f,jobDir)
                    os.unlink(f)
            except OSError:
                logger.warning("Failed to move file '%s' file from " \
                                       "'%s'", f, tmpdir)
                
  
    def updateMonitoringInformation( jobs ):
        """Check the status of jobs and retrieve output sandboxes"""

        for j,diracStatus,diracSite,gangaStatus,minorStatus in \
                DiracUtils.get_DIRAC_status(jobs):

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
                j.backend.getOutputSandbox()
                outw = j.getOutputWorkspace()
                
                # try to get the application exit code from the status file
                try:
                  statusfile = os.path.join(outw.getPath(),'__jobstatus__')
                  exitcode = DiracUtils.get_exit_code(statusfile)
                  logger.debug('status file: %s %s',statusfile,
                               file(statusfile).read())
                except IOError:
                  logger.debug('Problem reading status file: %s',statusfile)
                  exitcode=None
                except Exception:
                    logger.critical('Problem during monitoring')
                    raise

                if exitcode is not None and exitcode == 0 and \
                       gangaStatus != 'failed':
                    j.updateStatus('completed')
                else:
                    j.updateStatus('failed')
                    
    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
