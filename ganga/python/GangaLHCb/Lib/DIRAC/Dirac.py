 #\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""The Ganga backendhandler for the Dirac system."""

import os
import Ganga.Utility.Config
import Ganga.Utility.logging
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.Core import BackendError
from DiracUtils import *
from DiracServer import DiracServer
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
from Ganga.Utility.util import unique

logger = Ganga.Utility.logging.getLogger()
configLHCb = Ganga.Utility.Config.getConfig('LHCb')
configDirac = Ganga.Utility.Config.getConfig('DIRAC')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

dirac_ganga_server = DiracServer()
dirac_monitoring_server = DiracServer()
dirac_monitoring_is_active = True

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
# Dirac class schema
schema = {}
docstr = 'The id number assigned to the job by the DIRAC WMS. If seeking help'\
         ' on jobs with the Dirac backend, please always report this id ' \
         'number in addition to a full description of your problem. The id '\
         'can also be used to further inspect the job at ' \
         'https://lhcbweb.pic.es/DIRAC/info/general/diracOverview'
schema['id'] = SimpleItem(defvalue=None, protected=1, copyable=0,
                          typelist=['int','type(None)'], doc=docstr)
docstr = 'The detailed status as reported by the DIRAC WMS'    
schema['status'] = SimpleItem(defvalue=None, protected=1, copyable=0,
                              typelist=['str','type(None)'],doc=docstr)
schema['actualCE'] = SimpleItem(defvalue=None, protected=1, copyable=0,
                                typelist=['str','type(None)'],
                                doc='The location where the job ran')
docstr = 'The normalized CPU time reported by the DIRAC WMS'
schema['normCPUTime'] = SimpleItem(defvalue=None, protected=1, copyable=0,
                                   typelist=['str','type(None)'], doc=docstr)
docstr = 'Minor status information from Dirac'
schema['statusInfo'] = SimpleItem(defvalue='', protected=1, copyable=0,
                                  typelist=['str','type(None)'],doc=docstr)
docstr = 'DIRAC API commands to add the job definition script. Only edit ' \
         'if you *really* know what you are doing'
schema['diracOpts'] = SimpleItem(defvalue='',doc=docstr)
docstr = 'Settings for DIRAC job (e.g. CPUTime, BannedSites, etc.)'
schema['settings'] = SimpleItem(defvalue={'CPUTime':2*86400},doc=docstr)
docstr = 'LFNs to be downloaded into the work dir on the grid node. Site '\
         'matching is *not* performed on these files; they are downloaded.'\
         'I.e., do not put prod data here'
types = ['GangaLHCb.Lib.LHCbDataset.LogicalFile.LogicalFile']
schema['inputSandboxLFNs'] = ComponentItem(category='datafiles',defvalue=[],
                                           typelist=types,sequence=1,
                                           doc=docstr)
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Dirac(IBackend):
    """The backend that submits LHCb jobs to the Grid via DIRAC.

    The backend for LHCb jobs to be submitted to the Grid. Jobs are
    submitted through the DIRAC WMS system and then in turn submitted to the
    Grid. A few examples of usage are given below

    # Run a DaVinci job on the Grid
    
    # Create a DaVinci application object. See DaVinci help text for
    # instructions on how to configure this.
    app = DaVinci()

    # Create Dirac backend object
    b = Dirac()

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

    # Using the 'settings' attribute
    j.backend.settings['BannedSites'] = ['LCG.CERN.ch']
    j.resubmit()

    # settings can be set at any time but are only 'respected' during
    # submit and resubmit.
    
    """    
    _schema = Schema(Version(3, 2),schema)
    _exportmethods = ['getOutputData','getOutputSandbox','checkSites',
                      'getOutputDataLFNs','peek','reset','debug','checkTier1s']
    _packed_input_sandbox = True
    _category = "backends"
    _name = 'Dirac'

    def _getDiracScript(self):
        '''Gets the DIRAC API script for the job.'''
        j = self.getJobObject()
        return os.path.join(j.getInputWorkspace().getPath(),'dirac-script.py')

    def _submit(self):
        '''Submit the job via the Dirac server.'''
        self.id = None
        self.actualCE = None
        self.status = None
        global dirac_ganga_server
        dirac_cmd = "execfile('%s')" % self._getDiracScript()
        result = dirac_ganga_server.execute(dirac_cmd)        
            
        err_msg = 'Error submitting job to Dirac: %s' % str(result)
        if not result_ok(result) or not result.has_key('Value'):
            logger.error(err_msg)
            raise BackendError('Dirac',err_msg)
        
        self.id = result['Value']        
        return type(self.id) == int
   
    def submit(self, subjobconfig, master_input_sandbox):
        """Submit a DIRAC job"""
        j = self.getJobObject()

        inputdata = subjobconfig.script.inputdata
        if (not inputdata or len(inputdata.data) == 0) and not \
               self.settings.has_key('Destination'):
            t1_sites = configLHCb['noInputDataBannedSites']
            msg = 'Job has no inputdata (T1 sites will be banned to help '\
                  'avoid overloading them).'
            logger.info(msg)
            if self.settings.has_key('BannedSites'):
                banned = self.settings['BannedSites']
                banned.extend(t1_sites)
                self.settings['BannedSites'] = unique(banned)
            else:
                self.settings['BannedSites'] = t1_sites[:]
        
        dirac_script = subjobconfig.script
        dirac_script.name = mangle_job_name(j)
        dirac_script.settings = self.settings
        dirac_script.dirac_opts = self.diracOpts

        sboxname = j.createPackedInputSandbox(subjobconfig.getSandboxFiles())
        script_file = self._getDiracScript()
        dirac_script.input_sandbox = [sboxname[0],master_input_sandbox[0],
                                      script_file]
        for lfn in self.inputSandboxLFNs:
            from GangaLHCb.Lib.LHCbDataset.PhysicalFile import PhysicalFile
            if type(lfn) is PhysicalFile:
                msg = 'Dirac.inputSandboxLFNs cannot contain a PhysicalFile.'
                logger.error(msg)
                raise BackendError('Dirac',msg)            
            dirac_script.input_sandbox.append('LFN:'+lfn.name)
        dirac_script.write(script_file)
        return self._submit()
 
    def resubmit(self):
        """Resubmit a DIRAC job"""
        script = self._getDiracScript()
        cur_script = open(script)
        tmp_script = open(script + '.tmp','w')
        skip = False
        for line in cur_script.readlines():
            if line.find('<-- user settings') >= 0:
                skip = True
                # write new settings
                tmp_script.write('# <-- user settings \n')
                for key in self.settings:
                    value = self.settings[key]
                    if type(value) == type(''):
                        tmp_script.write('j.set%s("%s")\n' % (key,value))
                    else:
                        tmp_script.write('j.set%s(%s)\n' % (key,str(value)))
            if line.find('user settings -->') >= 0: skip = False
            if not skip: tmp_script.write(line)
        cur_script.close()
        tmp_script.close()
        os.system('mv -f %s %s' % (script+'.tmp',script))
        return self._submit()

    def reset(self):
        """Resets the state of a job back to 'submitted' so that the
        monitoring will run on it again."""        
        j = self.getJobObject()
        
        if j.status == 'submitting' or j.status == 'killed':
            logger.warning("Can not reset a job in status '%s'." % j.status)
        else:
            j.getOutputWorkspace().remove(preserve_top=True)
            j.updateStatus('submitted')        
            if j.master: j.master.updateMasterJobStatus()
    
    def kill(self):
        """ Kill a Dirac jobs"""         
        global dirac_ganga_server
        if not self.id: return None
        dirac_cmd = 'result = DiracCommands.kill(%d)' % self.id
        result = dirac_ganga_server.execute(dirac_cmd)
        if not result_ok(result):
            raise BackendError('Dirac','Could not kill job: %s' % str(result))
        return result['OK']

    def peek(self,filename=None,command=None):
        """Peek at the output of a job (Note: filename/command are ignored)."""
        global dirac_ganga_server
        dirac_cmd = 'result = DiracCommands.peek(%d)' % self.id
        result = dirac_ganga_server.execute(dirac_cmd)
        if result_ok(result): print result['Value']
        else: logger.error("No peeking available for Dirac job '%i'.", self.id)

    def getOutputSandbox(self,dir=None):
        """Retrieve the outputsandbox from the DIRAC WMS. The dir argument
        gives the directory the the sandbox will be retrieved into."""
        global dirac_ganga_server
        return self._getOutputSandbox(dirac_ganga_server,dir)

    def _getOutputSandbox(self,server,dir=None):
        j = self.getJobObject()
        if dir is None: dir = j.getOutputWorkspace().getPath()
        tmpdir = j.getDebugWorkspace().getPath()
        if os.path.exists("%s/%d" % (tmpdir,self.id)):
            os.system('rm -f %s/%d/*' % (tmpdir,self.id))
            os.system('rmdir --ignore-fail-on-non-empty %s/%d' \
                      % (tmpdir,self.id))
        dirac_cmd = 'result = DiracCommands.getOutputSandbox(%d,"%s","%s")' \
                    % (self.id,tmpdir,dir)
        result = server.execute(dirac_cmd)
        if not result_ok(result):
            msg = 'Problem retrieving output: %s' % str(result)
            logger.warning(msg)
            return False

        return True

    def getOutputData(self,dir=None,names=None):
        """Retrieve data stored on SE to dir (default=job output workspace).
        If names=None, then all outputdata is downloaded otherwise names should
        be a list of files to download."""
        j = self.getJobObject()
        if not names: names = ''
        if not dir: dir = j.getOutputWorkspace().getPath()

        global dirac_ganga_server
        cmd = 'result = DiracCommands.getOutputData("%s","%s",%d)' \
              % (names,dir,self.id)
        result = dirac_ganga_server.execute(cmd)

        downloaded_files = []
        if not result_ok(result):
            logger.error('Output download failed: %s' % str(result))
        else: downloaded_files = result.get('Value',[])
        ds = LHCbDataset()
        for f in downloaded_files: ds.files.append(LogicalFile(f))
        return GPIProxyObjectFactory(ds)
        
    def _getOutputDataLFNs(self,server,force_query):
        j = self.getJobObject()
        lfns = []
        fname = j.getOutputWorkspace().getPath() + '/lfns.lst'
        if not force_query:
            if os.path.exists(fname):
                file = open(fname)
                lfns = file.read().strip().split('\n')
                file.close()                
        if not lfns:        
            if not j.status == 'completed' and not force_query:
                logger.warning('LFN query will only work for completed jobs')
                return []
            cmd = 'result = DiracCommands.getOutputDataLFNs(%d)' % self.id
            result = server.execute(cmd)
            if not result_ok(result):
                logger.warning('LFN query failed: %s' % str(result))
                return []
            lfns = result.get('Value',[])
            file = open(fname,'w')
            for lfn in lfns: file.write(lfn.replace(' ','')+'\n')
            file.close()
        return lfns

    def getOutputDataLFNs(self,force_query=False):
        """Get a list of outputdata that has been uploaded by Dirac. Excludes
        the outputsandbox if it is there."""        
        global dirac_ganga_server
        lfns = self._getOutputDataLFNs(dirac_ganga_server,force_query)
        ds = LHCbDataset()
        for f in lfns: ds.files.append(LogicalFile(f))
        return GPIProxyObjectFactory(ds)

    def checkSites(self):
        global dirac_ganga_server
        cmd = 'result = DiracCommands.checkSites()'
        result = dirac_ganga_server.execute(cmd)
        if not result_ok(result):
            logger.warning('Could not obtain site info: %s' % str(result))
            return
        return result.get('Value',{})

    def checkTier1s(self):
        global dirac_ganga_server
        cmd = 'result = DiracCommands.checkTier1s()'
        result = dirac_ganga_server.execute(cmd)
        if not result_ok(result):
            logger.warning('Could not obtain Tier-1 info: %s' % str(result))
            return
        return result.get('Value',{})

    def debug(self):
        '''Obtains some (possibly) useful DIRAC debug info. '''
        global dirac_ganga_server
        # check services
        cmd = 'result = DiracCommands.getServicePorts()'
        result = dirac_ganga_server.execute(cmd)
        if not result_ok(result):
            logger.warning('Could not obtain services: %s' % str(result))
            return
        services = result.get('Value',{})
        for category in services:
            system,service = category.split('/')
            cmd = 'result = DiracCommands.ping("%s","%s")' % (system,service)
            result = dirac_ganga_server.execute(cmd)
            msg = 'OK.'
            if not result_ok(result): msg = '%s' % result['Message']
            print '%s: %s' %  (category,msg)
        # get pilot info for this job
        if type(self.id) != int: return
        j = self.getJobObject()
        cwd = os.getcwd()
        debug_dir = j.getDebugWorkspace().getPath()
        cmd = 'result = DiracCommands.getJobPilotOutput(%d,"%s")' % \
              (self.id, debug_dir)
        result = dirac_ganga_server.execute(cmd)
        #print 'result =', result
        if result_ok(result):
            print 'Pilot Info: %s/pilot_%d/std.out.'%(debug_dir,self.id)
        else:
            print result.get('Message','')

    def getStateTime(self, status):
        """Returns the timestamps for 'running' or 'completed' by extracting
        their equivalent timestamps from the loggingInfo."""
        global dirac_monitoring_server
        if not self.id: return None
        logger.debug("Accessing getStateTime() in diracAPI")
        dirac_cmd = "result = DiracCommands.getStateTime(%d,\'%s\')" % \
                    (self.id, status)
        result = dirac_monitoring_server.execute(dirac_cmd)
        return result
     
    def timedetails(self):
        """Prints contents of the loggingInfo from the Dirac API."""
        global dirac_ganga_server
        if not self.id: return None
        logger.debug("Accessing getStateTime() in diracAPI")
        dirac_cmd = 'result = DiracCommands.timedetails(%d)' % self.id
        result = dirac_ganga_server.execute(dirac_cmd)
        return result
                
    def updateMonitoringInformation(jobs):
        """Check the status of jobs and retrieve output sandboxes"""
        from Ganga.Core import monitoring_component
        dirac_job_ids = []
        for j in jobs: dirac_job_ids.append(j.backend.id)
        global dirac_monitoring_server
        global dirac_monitoring_is_active
        if not dirac_monitoring_server.proxy.isValid():
            if dirac_monitoring_is_active:
                logger.warning('DIRAC monitoring inactive (no valid proxy '\
                               'found).')
            dirac_monitoring_is_active = False
            return
        else:
            dirac_monitoring_is_active = True
        cmd = 'result = DiracCommands.status(%s)' % str(dirac_job_ids)
        result = dirac_monitoring_server.execute(cmd)
        if type(result) != type([]):
            logger.warning('DIRAC monitoring failed: %s' % str(result))
            return
                
        for i in range(0,len(jobs)):
            if monitoring_component:
                if monitoring_component.should_stop(): break
            j = jobs[i]
            j.backend.statusInfo = result[i][0]
            j.backend.status = result[i][1]
            j.backend.actualCE = result[i][2]
            cmd = 'result = DiracCommands.normCPUTime(%d)' % j.backend.id
            j.backend.normCPUTime = dirac_monitoring_server.execute(cmd)
            if result[i][3] != 'completed' and result[i][3] != j.status:
                j.updateStatus(result[i][3])
            if result[i][3] == 'completed':
                j.updateStatus('completing')
                ok = j.backend._getOutputSandbox(dirac_monitoring_server)
                if ok and j.outputdata:
                    j.backend._getOutputDataLFNs(dirac_monitoring_server,True)
                if not ok: j.updateStatus('failed')
                else: j.updateStatus('completed')
            if result[i][3] == 'failed':
                if configLHCb['failed_sandbox_download']:
                    j.backend._getOutputSandbox(dirac_monitoring_server)
                    
    updateMonitoringInformation = staticmethod(updateMonitoringInformation)

    def execAPI(cmd,timeout=None):
        """Executes DIRAC API commands.  If variable 'result' is set, then
        it is returned by this method. """
        global dirac_ganga_server
        return dirac_ganga_server.execute(cmd,timeout)

    execAPI = staticmethod(execAPI)

    def killServer():
        '''Kills the DIRAC server child process.'''
        global dirac_ganga_server
        return dirac_ganga_server.disconnect()

    killServer = staticmethod(killServer)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from ExeDiracRTHandler import *
from RootDiracRTHandler import *

allHandlers.add('Executable','Dirac', ExeDiracRTHandler)
allHandlers.add('Root','Dirac', RootDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
