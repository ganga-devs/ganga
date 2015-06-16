# !/usr/bin/env python
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TimeTest.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
##########################################################################

import sys
import os
import time
import tempfile

import TimeTest
_thisDir = os.path.dirname(TimeTest.__file__)
if not _thisDir:
    _thisDir = os.getcwd()
_root = os.path.dirname(os.path.dirname(os.path.dirname(_thisDir)))
sys.path.append(_root)


from Ganga.GPIDev.Lib.Job.Job import Job
#from GangaLHCb.Lib.Gaudi.Gaudi import Gaudi
from Ganga.Lib.Executable.Executable import Executable
from Ganga.Core.JobRepository.ARDA import repositoryFactory
from Ganga.GPIDev.Streamers.SimpleStreamer import SimpleJobStreamer
import Ganga.Runtime.plugins

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)

##########################################################################


def _startText(ff, txt):
    ff.write(txt)
    t1 = time.time()
    ff.write('operation started at %s \n' % time.ctime(t1))
    return t1


def _endText(ff, t1):
    t2 = time.time()
    ff.write('operation finished at %s \n' % time.ctime(t2))
    ff.write('time used: %f seconds \n' % (t2 - t1))
    ff.write('-s->%f<-s-\n\n\n' % (t2 - t1))


def runTest(NJOBS, NRUN, rootDir, output_dir, rep_type):
    logger.debug('from runTest: rootDir %s, output_dir %s' %
                 (rootDir, output_dir))
    if rep_type == "Remote":
        repository = repositoryFactory(repositoryType=rep_type,
                                       root_dir=rootDir,
                                       streamer=SimpleJobStreamer(),
                                       host='gangamd.cern.ch',
                                       port=8822,
                                       login=os.getlogin(),
                                       keepalive=True)
    elif rep_type == "Local":
        repository = repositoryFactory(repositoryType=rep_type,
                                       root_dir=rootDir,
                                       streamer=SimpleJobStreamer(),
                                       local_root='/tmp')
    else:
        logger.error("Wrong type of repository...")
        logger.error("Exiting ...")
        return
    nn = tempfile.mktemp(suffix='.test')
    nn = os.path.join(output_dir, os.path.basename(nn))
    ff = file(nn, 'w')
    try:
        jjj = []
        for n in range(NRUN):
            ff.write("NUMBER of jobs in the repository %d \n" % len(jjj))
            jj = []
            for i in range(NJOBS):
                j = Job()
                #j.application = Gaudi()
                j.name = "MyJob" + str(i)
                jj.append(j)

            jjj.extend(jj)
            t1 = _startText(ff, 'registering %d jobs...' % NJOBS)
            repository.registerJobs(jj)
            logger.debug("--->command status", "OK", "\n")
            _endText(ff, t1)

        t1 = _startText(ff, 'deleting jobs...')
        repository.deleteJobs(map(lambda j: j.id, jjj))
        logger.debug("--->command status", "OK", "\n")
        _endText(ff, t1)

    finally:
        ff.close()

##########################################################################


def NormPath(path):
    if sys.platform == 'win32':
        directory, file = os.path.split(path)
        drive, tail = os.path.splitdrive(directory)
        path_elem = tail.split(os.sep)
        path = drive + os.sep
        for i in range(len(path_elem)):
            elem = path_elem[i]
            if elem:
                sub_elem = elem.split()
                elem = sub_elem[0]
                if len(elem) > 8 or len(sub_elem) > 1:
                    elem = elem[:6] + '~1'
            path = os.path.join(path, elem)

        path = os.path.join(path, file)

    return path

##########################################################################
if __name__ == '__main__':
    NJOBS = int(raw_input('Enter a number of job to test --->'))
    NRUN = int(raw_input('Enter a number of runs to test --->'))
    OUTPUT = raw_input('Enter a name of output dir --->')
    REPTYPE = raw_input(
        'Enter type of repository (Local[1] or Remote[2]) --->')
    if REPTYPE == '1':
        REPTYPE = 'Local'
    elif REPTYPE == '2':
        REPTYPE = 'Remote'
    else:
        logger.error("Unknown type of repository. Exiting")
        sys.exit(1)
    NUSERS = 1
    dname = 'runs_' + str(NRUN) + '__jobs_' + str(NJOBS)
    output_dir = os.path.join(os.getcwd(), OUTPUT, REPTYPE, dname)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    logger.debug("output dir is ", output_dir)

    python_path = NormPath(sys.executable)
    i = 0
    while i < NUSERS:
        rootDir = '/testdir/GangaTest/user' + str(i)
        cmd = '"import sys\nsys.path.append(\'%s\')\nfrom TimeTest import runTest\nrunTest(%d, %d, \'%s\',\'%s\',\'%s\')"' % (
            _thisDir, NJOBS, NRUN, rootDir, output_dir, REPTYPE)
        cmd = cmd[1:-1]
# if sys.version_info[:2] < (2,3):
##            cmd = cmd[1:-1]
        logger.debug(cmd)
        pid = os.spawnl(os.P_NOWAIT, python_path, python_path, "-c", cmd)
        logger.debug("new user process started %d" % pid)
        i += 1
