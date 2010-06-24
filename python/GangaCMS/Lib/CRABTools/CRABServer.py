from GangaCMS.Lib.CRABTools import CRABServerError
from GangaCMS.Lib.CRABTools import Telltale

from Ganga.Core import ApplicationConfigurationError, BackendError
from Ganga.GPIDev.Credentials import getCredential
from Ganga.Utility.logging import getLogger

import os.path
import datetime

from subprocess import Popen, PIPE

logger = getLogger()

class CRABServer:

    def __init__(self):
        pass

    def _send(self,cmd,type):

        code = 'x'
        stdout, stderr = '',''

        try:        
            init = datetime.datetime.now()
            p = Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE)
            stdout, stderr = p.communicate()
            code = p.returncode
            end = datetime.datetime.now()
            time = (end-init).seconds
            logger.info('%s took %d seconds'%(type,time))
        except OSError,e:
            logger.error(stdout)
            logger.error(stderr)
            raise CRABServerError(e,'OSError %s crab job(s).'%(type))

        if code != 0:
            logger.info(stdout)
            logger.info(stderr) 
            raise CRABServerError('CRAB %s exit code %s'%(type,code))  

    def create(self, job):

        cfgfile = '%scrab.cfg'%(job.inputdir)
        if not os.path.isfile(cfgfile):
            raise CRABServerError('File "%s" not found.'%(cfgfile))     

        cmd = 'crab -create -cfg %s'%(cfgfile)
        self._send(cmd,'creating')
#        msg = telltale.create('%slog/crab.log'%(job.outputdir))
        return 1

    def submit(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        cmd = 'crab -submit -c %s'%(workdir)
        self._send(cmd,'submitting')
#        msg = telltale.submit('%slog/crab.log'%(job.outputdir))   
        return 1    

    def status(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        cmd = 'crab -status -c %s'%(workdir)
        self._send(cmd,'checking status')
#        msg = telltale.submit('%slog/crab.log'%(job.outputdir))
        return 1

    def kill(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        if not job.master:
            cmd = 'crab -kill all -c %s'%(workdir)
        else:
            index = int(job.id) + 1
            cmd = 'crab -kill %d -c %s'%(index,workdir)
        self._send(cmd,'killing')
#        msg = telltale.kill('%slog/crab.log'%(job.outputdir))
        return 1

    def resubmit(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        index = int(job.id) + 1
        cmd = 'crab -resubmit %d -c %s'%(index,workdir)
        self._send(cmd,'resubmitting')
#        msg = telltale.resubmit('%slog/crab.log'%(job.outputdir))
        return 1

    def getOutput(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        index = int(job.id) + 1
        cmd = 'crab -getoutput %d -c %s'%(index,workdir)
        self._send(cmd,'getting Output')
#        msg = telltale.resubmit('%slog/crab.log'%(job.outputdir))
        return 1
