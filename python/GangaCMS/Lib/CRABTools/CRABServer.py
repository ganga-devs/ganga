from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
from Ganga.Utility.logging import getLogger
from GangaCMS.Lib.CRABTools.CRABServerError import CRABServerError

import datetime
import os
import shlex
import shutil
import subprocess
import time


logger = getLogger()


class CRABServer(GangaObject):
    """Helper class to launch CRAB commands."""
    _schema = Schema(Version(0, 0), {})
    _hidden = True

    def _send(self, cmd, operation, env):
        """Launches a command and waits for output."""
        try:
            logger.debug('Launching a CRAB command: %s' % cmd)
            init = datetime.datetime.now()
            p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, bufsize=-1, env=env)
            stdout, stderr = p.communicate()
            p.wait()
            end = datetime.datetime.now()
            logger.debug('Finished CRAB command: %s' % cmd)
            logger.info('%s took %d sec.' % (operation, (end - init).seconds))

            if p.returncode != 0:
                raise CRABServerError('CRAB %s returned %s' % (operation,
                                                               p.returncode))
        except OSError as e:
            logger.error(stdout)
            logger.error(stderr)
            raise CRABServerError(e, 'OSError %s crab job(s).' % operation)

    def _send_with_retry(self, cmd, operation, env, retries=3, delay=60):
        """Wrapper to add some retries to the CRAB command launching."""
        assert retries > 0
        assert delay >= 0

        for _ in range(retries):
            try:
                self._send(cmd, operation, env)
                return
            except CRABServerError:
                time.sleep(delay)

        raise CRABServerError('CRAB %s failed %d times' % (operation, retries))

    def create(self, job):
        """Create a new CRAB jobset."""
        cfgfile = os.path.join(job.inputdir, 'crab.cfg')
        if not os.path.isfile(cfgfile):
            raise CRABServerError('File "%s" not found.' % cfgfile)

        # Clean up the working dir for the CRAB UI.
        shutil.rmtree(job.inputdata.ui_working_dir, ignore_errors=True)

        cmd = 'crab -create -cfg %s' % cfgfile
        self._send_with_retry(cmd, 'create', job.backend.crab_env)
        return True

    def submit(self, job):
        """Submit an already created jobset."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -submit -c %s' % job.inputdata.ui_working_dir
        self._send_with_retry(cmd, 'submit', job.backend.crab_env)
        return True

    def status(self, job):
        """Get the status of a jobset."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -status -c %s' % job.inputdata.ui_working_dir
        self._send_with_retry(cmd, 'status', job.backend.crab_env)
        return True

    def kill(self, job):
        """Kill all the jobs on the task."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        if not job.master:
            cmd = 'crab -kill all -c %s' % job.inputdata.ui_working_dir
        else:
            cmd = 'crab -kill %d -c %s' % (int(job.id) + 1,
                                           job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'kill', job.backend.crab_env)
        return True

    def resubmit(self, job):
        """Resubmit an already created job."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -resubmit %d -c %s' % (int(job.id) + 1,
                                           job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'resubmit', job.backend.crab_env)
        return True

    def getOutput(self, job):
        """Retrieve the output of the job."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -getoutput %d -c %s' % (int(job.id) + 1,
                                            job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'getoutput', job.backend.crab_env)
        # Make output files coming from the WMS readable.
        for root, _, files in os.walk(os.path.join(job.inputdata.ui_working_dir,
                                                   'res')): # Just 'res'.
            for f in files:
                os.chmod(os.path.join(root, f), 0o644)
        return True

    def postMortem(self, job):
        """Retrieves the postmortem information."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir %s not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -postMortem %d -c %s' % (int(job.id) + 1,
                                             job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'postMortem', job.backend.crab_env)
        return True
