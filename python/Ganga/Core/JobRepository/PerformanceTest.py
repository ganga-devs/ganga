# !/usr/bin/env python
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PerformanceTest.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
##########################################################################

import sys
import os
import time
import re
import tempfile

import PerformanceTest
_thisDir = os.path.dirname(PerformanceTest.__file__)
if not _thisDir:
    _thisDir = os.getcwd()
_root = os.path.dirname(os.path.dirname(os.path.dirname(_thisDir)))
sys.path.append(_root)


from Ganga.GPIDev.Lib.Job.Job import Job
from Ganga.Lib.Executable.Executable import Executable
from Ganga.Core.JobRepository.ARDA import repositoryFactory
from Ganga.GPIDev.Streamers.SimpleStreamer import SimpleJobStreamer
import Ganga.Runtime.plugins

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)

#DEBUG = False
DEBUG = True

##########################################################################
# memory testing


def getmem():
    """Gives memory used by the calling process in kb"""
    ss = os.popen('pmap %d | tail -1' % os.getpid(), 'r').read()
    if ss:
        m = re.search(r'([0-9]*)K', ss)
        if m:
            return int(m.group(1))


def writerep(n, m, bunch=100):
    """writerep(n, m, bunch = 100) --> (memory increase in kb, creation time)
        register n bunches of GPIDev jobs; each bunch contains 'bunch' jobs;
        each job has name of 'm' characters"""
    m1 = getmem()
    if m1 is not None:
        dt = _writerep(n, m, bunch)
        m2 = getmem()
        if m2 is not None:
            return (m2 - m1, dt)


def _writerep(n, m, bunch=100):
    jj = []
    for i in range(bunch):
        j = Job()
        j.application.args = [m * 'x' + str(i)]
        jj.append(j)
    from Ganga.GPI import jobs
    rep = jobs._impl.repository
    t1 = time.time()
    for i in range(n):
        rep.registerJobs(jj)
    t2 = time.time()
    del jj
    return t2 - t1


def readrep(bunch=100):
    """readrep(bunch = 100) --> (memory increase in kb, retrieval time)
        checkout all jobs in the repository as GPIDev objects by bunches;
        each bunch contains no more than 'bunch' jobs"""
    m1 = getmem()
    if m1 is not None:
        dt = _readrep(bunch)
        m2 = getmem()
        if m2 is not None:
            return (m2 - m1, dt)


def _readrep(bunch=100):
    def _co(ii):
        jj = rep.checkoutJobs(ii)
    from Ganga.GPI import jobs
    rep = jobs._impl.repository
    ids = rep.getJobIds({})
    start = 0
    ii = ids[start: start + bunch]
    t1 = time.time()
    while ii:
        _co(ii)
        start += bunch
        ii = ids[start: start + bunch]
    t2 = time.time()
    return t2 - t1


def readallrep():
    """readallrep() --> (memory increase in kb, retrieval time)
        checkout all jobs in the repository as GPIDev objects."""
    m1 = getmem()
    if m1 is not None:
        dt = _readallrep()
        m2 = getmem()
        if m2 is not None:
            return (m2 - m1, dt)


def _readallrep():
    from Ganga.GPI import jobs
    rep = jobs._impl.repository
    t1 = time.time()
    jj = rep.checkoutJobs({})
    t2 = time.time()
    del jj
    return t2 - t1


def delrep(n, bunch=100):
    """delrep(bunch = 100) --> (memory increase in kb, delete time)
        delete n*bunch jobs in the repository by bunches;
        each bunch contains no more than 'bunch' jobs"""
    m1 = getmem()
    if m1 is not None:
        dt = _delrep(n, bunch)
        m2 = getmem()
        if m2 is not None:
            return (m2 - m1, dt)


def _delrep(n, bunch=100):
    from Ganga.GPI import jobs
    rep = jobs._impl.repository
    ids = rep.getJobIds({})
    start = 0
    ids = ids[start:n * bunch]
    ii = ids[start:start + bunch]
    t1 = time.time()
    while ii:
        rep.deleteJobs(ii)
        start += bunch
        ii = ids[start:start + bunch]
    t2 = time.time()
    return t2 - t1


##########################################################################

ARG_LEN = 100  # controls length of a job


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


def getSplitJob(LEN=10):
    # top level splitting
    mj = Job()
    jj = []
    for i in range(LEN):
        sj = Job()
        sj.application.exe = '/bin/myexe' + str(i)
        sj.application.args = ['/' + ARG_LEN * 'abc' + str(i)]
        sj._setParent(mj)
        jj.append(sj)
    mj.subjobs = jj
    return mj


def registerJobs(jobList, repository):
    for mj in jobList:
        repository.registerJobs([mj])


def runTest(NTEST, LEN, rootDir, output_dir, rep_type):
    logger.debug('from runTest: rootDir %s, output_dir %s' %
                 (rootDir, output_dir))
    if rep_type == "Remote":
        repository = repositoryFactory(repositoryType=rep_type,
                                       root_dir=rootDir,
                                       streamer=SimpleJobStreamer(),
                                       host='gangamd.cern.ch',
                                       #host      = 'lxgate41.cern.ch',
                                       port=8822,
                                       login=os.getlogin(),
                                       keepalive=True)
    elif rep_type == "Local":
        repository = repositoryFactory(repositoryType=rep_type,
                                       root_dir=rootDir,
                                       streamer=SimpleJobStreamer(),
                                       #local_root = os.path.expanduser('~'),
                                       local_root=os.path.join('/afs/cern.ch/sw/ganga/workdir',
                                                               os.getlogin(), 'gangadir/repository'))
    else:
        logger.error("Wrong type of repository...")
        logger.error("Exiting ...")
        return
    nn = tempfile.mktemp(suffix='.test')
    nn = os.path.join(output_dir, os.path.basename(nn))
    ff = file(nn, 'w')
    try:
        jj = []
        for i in range(NTEST):
            j = getSplitJob(LEN)
            j.name = "MyJob" + str(i)
            j.application.args = ['/' + ARG_LEN * 'abcd']
            jj.append(j)

        #----------------------------------------------------
        t1 = _startText(ff, 'registering %d jobs...' % NTEST)
        logger.debug('registering %d jobs...' % NTEST)
        try:
            repository.registerJobs(jj)
        except Exception as e:
            logger.error("EXCEPTION in registerJobs " + str(e))
            logger.debug("--->command status", "FAIL")
        else:
            logger.debug("--->command status", "OK")
        _endText(ff, t1)

        #----------------------------------------------------
        t1 = _startText(ff, 'retrieving info about ALL jobs')
        logger.debug('retrieving info about ALL jobs')
        try:
            #rjj = repository.checkoutJobs(map(lambda j: j.id, jj))
            rjj = repository.checkoutJobs({})
        except Exception as e:
            logger.error("EXCEPTION in checkoutJobs " + str(e))
            logger.debug("--->command status", "FAIL")
        else:
            logger.debug(
                "--->checkout jobs", len(rjj), map(lambda j: j.id, rjj))
        _endText(ff, t1)

        # some job modification
        for j in jj:
            # j.application = Executable()
            j.name = j.name + 'Changed'
            for sj in j.subjobs:
                sj.application.exe = '/bin/ls'

        #----------------------------------------------------
        t1 = _startText(ff, 'commiting %d jobs...' % NTEST)
        logger.debug('commiting %d jobs...' % NTEST)
        try:
            repository.commitJobs(jj)
        except Exception as e:
            logger.error("EXCEPTION in commitJobs " + str(e))
            logger.debug("--->command status", "FAIL")
        else:
            logger.debug("--->command status", "OK")
        _endText(ff, t1)

        #----------------------------------------------------
        t1 = _startText(ff, 'deleting %d jobs...' % NTEST)
        logger.debug('deleting %d jobs...' % NTEST)
        try:
            repository.deleteJobs(map(lambda j: j.id, jj))
        except Exception as e:
            logger.error("EXCEPTION in deleteJobs " + str(e))
            logger.debug("--->command status", "FAIL")
        else:
            logger.debug("--->command status", "OK")
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
    NTEST = int(raw_input('Enter a number of job to test --->'))
    LEN = int(raw_input('Enter a number of subjobs per job --->'))
    NUSERS = int(raw_input('Enter a number of '"users"' to test --->'))
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
    dname = 'users_' + str(NUSERS) + '__jobs_' + \
        str(NTEST) + '__subjobs_' + str(LEN)
    output_dir = os.path.join(os.getcwd(), OUTPUT, REPTYPE, dname)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    logger.debug("output dir is ", output_dir)

    python_path = NormPath(sys.executable)
    i = 0
    while i < NUSERS:
        rootDir = '/users/testframework'
        cmd = '"import sys\nsys.path.append(\'%s\')\nfrom PerformanceTest import runTest\nrunTest(%d, %d, \'%s\',\'%s\',\'%s\')"' % (
            _thisDir, NTEST, LEN, rootDir, output_dir, REPTYPE)
        if sys.version_info[:3] < (2, 3, 0) or sys.version_info[:3] >= (2, 3, 4):
            cmd = cmd[1:-1]
        logger.debug(cmd)
        pid = os.spawnl(os.P_NOWAIT, python_path, python_path, "-c", cmd)
        logger.debug("new user process started %d" % pid)
        i += 1
