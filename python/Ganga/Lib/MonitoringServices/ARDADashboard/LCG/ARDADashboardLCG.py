# benjamin.gaidioz@cern.ch
# the DashboardAPI module is loaded without any URL for the monalisa
# host, configuration is the default one. This is a bit bad.

import os
import popen2

from Ganga.GPIDev.Adapters.IMonitoringService import IMonitoringService
from types import DictionaryType, IntType
from Ganga.Lib.MonitoringServices.ARDADashboard.DashboardAPI import DashboardAPI

#out = open(os.path.join(os.getcwd(), 'dashboard.log'),'w')
def printInfo(s):
    pass
#    out.write(str(s) + os.linesep)
#    out.flush()


def printError(s):
    pass
#    out.write(str(s) + os.linesep)
#    out.flush()


def safe_getenv(varname):
    try:
        return os.environ[varname]
    except:
        return 'unknown'

def get_output(commands):
    for command in commands:
        printInfo("running %s" % command)
        p = popen2.Popen3(command)
        retcode = p.wait()
        if retcode == 0:
            lines = p.fromchild.readlines()
            s = ''
            for l in lines:
                if s != '':
                        s += ' '
                s += l.split('\n')[0]
            return s
        else:
            lines = p.fromchild.readlines()
            printInfo("get_output: %s" % lines)
    return 'unknown'


class ARDADashboardLCG(IMonitoringService):
    
    gridJobId = None
    gangaJobId = None
    gangaTaskId = None
    dashboard = None
    gridBackend = None
    gridCertificate = None
    VO = None
    taskPrefix = 'ganga'
    _complete = False
    
    _logger = None
        
    def __init__(self, job_info):
        IMonitoringService.__init__(self, job_info)
        
        if type(job_info) is DictionaryType:
            # we are on the worker node. We just need
            # to get values from the dictionary
            
            try:
                self.gangaJobId = job_info['gangaJobId']
                self.gangaTaskId = job_info['gangaTaskId']
                self.gridBackend = job_info['gridBackend']
                self.gridCertificate = job_info['gridCertificate']
                self.VO = job_info['VO']
                self._complete = True
            except KeyError,msg:
                # too bad, we will not monitor the job         
                return
            # on the WN, we get the job ID from envar
            self.gridJobId = safe_getenv('EDG_WL_JOBID')
            if self.gridJobId == 'unknown':
                self.gridJobId = safe_getenv('GLITE_WL_JOBID')
                if self.gridJobId == 'unknown':
                    self._complete = False

            
        else:
            # we are in the client session. job_info is a Job()
            
            from Ganga.Utility.logging import getLogger
            self._logger = getLogger()
            
            job = job_info
            
            self.gridBackend = getattr(job,'backend')._name    
            if self.gridBackend not in ['LCG']:
                self._logger.debug('not sending monitoring because not in LCG')
                return
            
            self._logger.debug(job.backend)
            self._logger.debug(job.backend.id)
            self.gridJobId = job.backend.id
            
            # we compute the "jobID" and "taskID"
            # (which is the gangaJobId followed by the user@repository
            
            # the repository unique ID:
            from Ganga.Utility.Config import getConfig, ConfigError
            config = getConfig('Configuration')
            rep_type = config['repositorytype']
            rep_login = config['user']
            if 'Local' in rep_type:
                from Ganga.Runtime import Repository_runtime
                rep_dir = Repository_runtime.getLocalRoot()
                sys_config = getConfig('System')
                rep_hostname = sys_config['GANGA_HOSTNAME']
                rep_location = rep_hostname + ':' + rep_dir
            elif 'Remote' in rep_type:
                remote_config = getConfig(rep_type+"_Repository")
                rep_host = remote_config['host']
                rep_port = remote_config['port']
                rep_location = rep_host + ':' + rep_port
            else:
                return
            repository_id = rep_login + '@' + rep_location
            
            master_job = job.master

            if master_job is not None:
                master_id = master_job.id
                self._logger.debug('found master: %d' % master_id)
                self.gangaTaskId = self.taskPrefix + '_' + str(master_id) + '_' + repository_id
                self.gangaJobId = str(job.id)
            else:
                self.gangaTaskId = self.taskPrefix + '_' + str(job.id) + '_' + repository_id
                self.gangaJobId = '0'
                
            self._logger.debug('task_id = %s' % self.gangaTaskId)
            self._logger.debug('task_job_id = %s' % self.gangaJobId)
            
            backendConfig = getConfig(self.gridBackend)
            try:
                self.VO = backendConfig['VirtualOrganisation']
            except KeyError:
                self._logger.debug('VirtualOrganisation not configured')
                # we need it, it's too dangerous if we are not sure
                return
            
            from Ganga.GPIDev.Credentials import getCredential

            proxy = getCredential('GridProxy')
            self.gridCertificate = proxy.info('-subject')
            if self.gridCertificate is None:
                self._logger.debug('error: grid certificate not known')
                return
            
            if self.gridJobId is None:
                self._logger.debug('normal: grid job ID is None')
            
            self._logger.debug('job is complete')
            self._complete = True        
        
        # we can now initialize the dashboard communication thing
        if self.gridJobId is not None:
            try:
                self.dashboard = DashboardAPI(self.gangaTaskId, self.gangaJobId + '_' + self.gridJobId)
            except TypeError:
                self.dashboard = DashboardAPI(self.gangaTaskId, self.gangaJobId + '_' + '_'.join(self.gridJobId))
                

                
    def getJobInfo(self):
        
        # the method which returns useful info to have on the WN. We send basically
        # everything, just in case.
        if self._complete:
            dict = {
                'gangaJobId':self.gangaJobId,
                'gangaTaskId':self.gangaTaskId,
                'gridBackend':self.gridBackend,
                'gridCertificate':self.gridCertificate,
                'VO':self.VO
            }
            self._logger.debug(dict)
            return dict
        else:
            return {}
        
    def getSandboxModules(self):
        
        # it would be nice if this would be more readable.
        
        import Ganga.Lib.MonitoringServices.ARDADashboard.LCG
        import ApMon
        import ApMon.apmon
        import ApMon.Logger
        return IMonitoringService.getSandboxModules(self) + [Ganga,
                Ganga.Lib,
                Ganga.Lib.MonitoringServices,
                Ganga.Lib.MonitoringServices.ARDADashboard,
                Ganga.Lib.MonitoringServices.ARDADashboard.DashboardAPI,
                Ganga.GPIDev,
                Ganga.GPIDev.Adapters,
                Ganga.GPIDev.Adapters.IMonitoringService,
                ApMon,
                ApMon.apmon,
                ApMon.Logger,
                ApMon.ProcInfo,
                Ganga.Lib.MonitoringServices.ARDADashboard.LCG,
                Ganga.Lib.MonitoringServices.ARDADashboard.LCG.ARDADashboardLCG,
                ]
    
    def submit(self,**opts):
        raise Exception('not implemented')
    
    def start(self, **opts):
        # we are in principle in the WN. We need the CE (we try both possibilities)
        printInfo("monitor start event")
        if self._complete:
            hostqueue = get_output(['edg-brokerinfo getCE','glite-brokerinfo getCE'])
            
            if hostqueue == 'unknown':
                printInfo("brokerinfo command returns nothing")
                hostqueue = safe_getenv('GANGA_LCG_CE')
            self.dashboard.publish(SyncCE=hostqueue)
    
            #FIXED: A TYPO? replaced executable by application
            # optional
            if self.application is not None:
                self.dashboard.publish(ExeStart=self.application)
    
    def progress(self, **opts):
        # we are in the WN. But it's application dependent
        return
    
    def stop(self,exitcode,**opts):
        if self._complete:
            if type(exitcode) is IntType:
                self.dashboard.publish(JobExitCode=exitcode)
        

