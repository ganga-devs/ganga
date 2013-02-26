from GangaCMS.Lib.CRABTools import CRABServerError
from GangaCMS.Lib.CRABTools import Telltale

from Ganga.Core import ApplicationConfigurationError, BackendError
from Ganga.GPIDev.Credentials import getCredential
from Ganga.Utility.logging import getLogger

import os,os.path
import datetime
import shlex
import shutil
import time

from subprocess import Popen, PIPE

import Ganga.Utility.Config

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

logger = getLogger()


class CRABServer(GangaObject):

    _schema =  Schema(Version(0,0), {})
    _hidden = 1

    def _send(self,cmd,type,env):

        code = 'x'
        stdout, stderr = '',''

        try:
            cmd = shlex.split(cmd)
            logger.debug('Launching a CRAB command: %s' %str(cmd))
            init = datetime.datetime.now()
            p = Popen(cmd,bufsize=-1,stdout=PIPE,stderr=PIPE,env=env)
            stdout, stderr = p.communicate()
            code = p.returncode
            logger.debug('Command ended with code %d' % code)
            # Remove zombie processes.
            p.wait()
            # Really wait for everything to finish.
            while True:
                try:
                    os.waitpid(-1, os.WNOHANG)
                except OSError:
                    break
            logger.debug('Finished CRAB command: %s' %str(cmd))
            end = datetime.datetime.now()

            time = (end-init).seconds
            logger.info('%s took %d seconds'%(type,time))
        except OSError,e:
            logger.error(stdout)
            logger.error(stderr)
            raise CRABServerError.CRABServerError(e,'OSError %s crab job(s).'%(type))

        if code != 0:
            logger.info(stdout)
            logger.info(stderr)
            raise CRABServerError.CRABServerError('CRAB %s exit code %s'%(type,code))

    def _send_with_retry(self,cmd,type,env,retries=3,delay=60):
        assert retries > 0
        assert delay >= 0

        for i in range(retries):
            try:
                self._send(cmd,type,env)
                return
            except:
                time.sleep(delay) # Introduce a delay to avoid flooding

        # If we reach this code, all the retries failed.
        raise CRABServerError.CRABServerError('CRAB %s failed on all retries (%d)'%(type,retries))

    def create(self, job):

        cfgfile = '%scrab.cfg'%(job.inputdir)
        if not os.path.isfile(cfgfile):
            raise CRABServerError('File "%s" not found.'%(cfgfile))

        # Clean up the working dir for the CRAB UI.
        shutil.rmtree(job.inputdata.ui_working_dir, ignore_errors=True)

        cmd = 'crab -create -cfg %s'%(cfgfile)
        self._send_with_retry(cmd,'creating',job.backend.crab_env)
#        msg = telltale.create('%slog/crab.log'%(job.outputdir))
        return 1

    def submit(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        cmd = 'crab -submit -c %s'%(workdir)
        self._send_with_retry(cmd,'submitting',job.backend.crab_env)
#        msg = telltale.submit('%slog/crab.log'%(job.outputdir))
        return 1

    def status(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        cmd = 'crab -status -c %s'%(workdir)
        self._send_with_retry(cmd,'checking status',job.backend.crab_env)
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
        self._send_with_retry(cmd,'killing',job.backend.crab_env)
#        msg = telltale.kill('%slog/crab.log'%(job.outputdir))
        return 1

    def resubmit(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        index = int(job.id) + 1
        cmd = 'crab -resubmit %d -c %s'%(index,workdir)
        self._send_with_retry(cmd,'resubmitting',job.backend.crab_env)
#        msg = telltale.resubmit('%slog/crab.log'%(job.outputdir))
        return 1

    def getOutput(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        index = int(job.id) + 1
        cmd = 'crab -getoutput %d -c %s'%(index,workdir)
        self._send_with_retry(cmd,'getting Output',job.backend.crab_env)
#        msg = telltale.resubmit('%slog/crab.log'%(job.outputdir))
        return 1

    def postMortem(self, job):

        workdir = job.inputdata.ui_working_dir
        if not os.path.exists(workdir):
            raise CRABServerError('Workdir %s not found.'%(workdir))

        index = int(job.id) + 1
        cmd = 'crab -postMortem %d -c %s'%(index,workdir)
        self._send_with_retry(cmd,'getting postMortem',job.backend.crab_env)
#        msg = telltale.resubmit('%slog/crab.log'%(job.outputdir))
        return 1

