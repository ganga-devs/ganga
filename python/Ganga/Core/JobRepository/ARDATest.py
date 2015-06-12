##!/usr/bin/env python
################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ARDATest.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
################################################################################

import sys, os
import time
import tempfile

import ARDATest
_thisDir = os.path.dirname(ARDATest.__file__)
if not _thisDir:
    _thisDir = os.getcwd()
_root = os.path.dirname(os.path.dirname(os.path.dirname(_thisDir)))
sys.path.append(_root)



from Ganga.GPIDev.Lib.Job.Job  import Job
#from GangaLHCb.Lib.Gaudi.Gaudi import Gaudi
from Ganga.Lib.Executable.Executable import Executable
from Ganga.Core.JobRepository.ARDA import repositoryFactory
from Ganga.GPIDev.Streamers.SimpleStreamer import SimpleJobStreamer
import Ganga.Runtime.plugins

#DEBUG = False
DEBUG = True

################################################################################    
def _startText(ff, txt):
    ff.write(txt)
    t1 = time.time()
    ff.write('operation started at %s \n' % time.ctime(t1))
    return t1

def _endText(ff, t1):
    t2 = time.time()
    ff.write('operation finished at %s \n' % time.ctime(t2))
    ff.write('time used: %f seconds \n' % (t2 - t1))
    ff.write('-s->%f<-s-\n\n\n'% (t2 - t1))     


def testSplitting(repository, LEN):
    # top level splitting
    mj = Job()
    jj = []
    for i in range(LEN):
        sj = Job()
        sj._setParent(mj)
        sj.application.exe = '/bin/myexe' + str(i)
        sj.application.args = 1000*['/ab' + str(i)]
        jj.append(sj)
    mj.subjobs = jj
    
    # check registration
    repository.registerJobs([mj])
    for s in mj.subjobs:
        assert(s.master is mj)
        assert(s.id != None)

    # check ci/co
    #repository.commitJobs([j._impl])
    mid = mj.id        
    j = repository.checkoutJobs([mid])[0]
    assert(len(j.subjobs) == LEN)

    # another ci/co check
    j.subjobs[1].application.exe = '/bin/ls'
    j.application.exe = '/bin/pwd'    
    repository.commitJobs([j])
    j = repository.checkoutJobs([mid])[0]
    
    assert(j.subjobs[1].application.exe == '/bin/ls')
    assert(j.application.exe == '/bin/pwd')        

    # check set status
    status_list = [((mid, j.subjobs[1].id),'running'),
                   ((mid, j.subjobs[2].id),'running'),
                   ((mid, j.subjobs[3].id),'running')]
    repository.setJobsStatus(status_list)
    
    # check get status
    md = repository.getJobsStatus(map(lambda x: x[0], status_list))
    for i in range(len(md)):
        assert (md[i][0] == status_list[i][0])
        assert (md[i][1] == status_list[i][1])

    # check getting job status in another way
    ttt = {'table_path':repository._getSubJobPath((mid,)), 'attributes':{}}
    md = repository.getJobsStatus(ttt)
    for i in range(len(status_list)):
        if md[i][0] == status_list[i][0]:
            assert (md[i][1] == status_list[i][1])

    # check delete job
    repository.deleteJobs([mj.id])


def runTest(NTEST, rootDir, output_dir, rep_type):
    logger.debug('from runTest: rootDir %s, output_dir %s'%(rootDir, output_dir))
    if rep_type == "Remote":
        repository = repositoryFactory(repositoryType = rep_type,
                                       root_dir  = rootDir,
                                       streamer  = SimpleJobStreamer(),
                                       host      = 'lxgate41.cern.ch',
                                       port      = 8822,
                                       login     = os.getlogin(),
                                       keepalive = True)
    elif rep_type == "Local":
        repository = repositoryFactory(repositoryType = rep_type,
                                       root_dir   = rootDir,
                                       streamer   = SimpleJobStreamer(),
                                       local_root = os.path.expanduser('~'))
    else:
        logger.error("Wrong type of repository...")
        logger.error("Exiting ...")
        return
    nn = tempfile.mktemp(suffix = '.test')
    nn = os.path.join(output_dir, os.path.basename(nn))
    ff = file(nn, 'w')
    try:
        jj = []
        for i in range(NTEST):
            j = Job()
            #j.application = Gaudi()
            j.name = "MyJob" + str(i)
            j.application.args = 1000*['/abc']
            jj.append(j)
            
        #----------------------------------------------------
        t1 = _startText(ff, 'registering %d jobs...' % NTEST)
        logger.debug('registering %d jobs...' % NTEST)
        try:
            repository.registerJobs(jj)
        except Exception as e:
            logger.error("EXCEPTION in registerJobs " + str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->command status OK")
        _endText(ff, t1)

        #----------------------------------------------------
        t1 = _startText(ff, 'testing splitting of %d jobs...' % NTEST)
        logger.debug('testing splitting of  %d jobs...' % NTEST)
        try:
            for i in range(NTEST):
                testSplitting(repository, LEN = 10)
        except Exception as e:
            logger.error("EXCEPTION in testSplitting "+ str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->command status OK")
        _endText(ff, t1)

        #----------------------------------------------------        
        t1 = _startText(ff, 'retrieving info about first 10 jobs...')
        logger.debug('retrieving info about first 10 jobs...')
        try:
            rjj = repository.checkoutJobs(map(lambda j: j.id, jj[:10]))
        except Exception as e:
            logger.error("EXCEPTION in checkoutJobs " + str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->checkout jobs " + map(lambda j: j.id, rjj))
        _endText(ff, t1)

        #----------------------------------------------------            
        t1 = _startText(ff, 'retrieving info about ALL jobs')
        logger.debug('retrieving info about ALL jobs')
        try:
            rjj = repository.checkoutJobs({})
        except Exception as e:
            logger.error("EXCEPTION in checkoutJobs " + str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->checkout jobs "+ len(rjj), map(lambda j: j.id, rjj))
        _endText(ff, t1)

        for j in jj:
            j.application = Executable()
            try:
                j.updateStatus('submitting')
            except:
                pass
            
        #----------------------------------------------------  
        t1 = _startText(ff, 'commiting %d jobs...' % NTEST)
        logger.debug('commiting %d jobs...' % NTEST)
        try:
            repository.commitJobs(jj)
        except Exception as e:
            logger.error("EXCEPTION in commitJobs " + str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->command status OK")
        _endText(ff, t1)

        #----------------------------------------------------          
        t1 = _startText(ff, 'setting status for %d jobs...' % NTEST)
        logger.debug('setting status for %d jobs...' % NTEST)
        try:
            repository.setJobsStatus(map(lambda j: (j.id, 'submitted'), jj))
        except Exception as e:
            logger.error("EXCEPTION in setJobsStatus " + str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->command status OK")
        _endText(ff, t1)

        #----------------------------------------------------
        t1 = _startText(ff, 'getting status of first 10 jobs...')
        logger.debug('getting status of first 10 jobs...')
        try:
            rjj = repository.getJobsStatus(map(lambda j: j.id, jj[:10]))
        except Exception as e:
            logger.error("EXCEPTION in getJobsStatus " + str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->command output "+ str(len(rjj))+ str(rjj))
        _endText(ff, t1)

        #----------------------------------------------------    
        t1 = _startText(ff, 'getting id of jobs with particular attributes...')
        logger.debug('getting id of jobs with particular attributes...')
        try:
            rjj = repository.getJobIds({'status':'submitted', 'application':'Executable'})
        except Exception as e:
            logger.error("EXCEPTION in getJobIds " + str(e))
            logger.debug("--->command status FAIL")
        else:
            logger.debug("--->command output" + str(len(rjj))+ str(rjj))
        _endText(ff, t1)


        t1 = _startText(ff, 'retrieving info about ALL jobs')
        rjj = repository.checkoutJobs({})
        if DEBUG:
            logger.debug('retrieving info about ALL jobs')
            jj_id = map(lambda j: j.id, jj)
            st_lst = []
            for j in rjj:
                if j.id in jj_id:
                    st_lst.append((j.id, j.status))
            logger.error("--->command output " + str(len(st_lst)) + ' ' + str(st_lst))
        _endText(ff, t1)
        
        
        t1 = _startText(ff, 'deleting %d jobs...' % NTEST)
        logger.debug('deleting %d jobs...' % NTEST)
        try:
            repository.deleteJobs(map(lambda j: j.id, jj))
        except Exception as e:
            logger.error("EXCEPTION in deleteJobs " + str(e))
            logger.debug("--->command status FAIL")
        else:        
            logger.debug("--->command status OK")
        _endText(ff, t1)

    finally:
        ff.close()

################################################################################
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

################################################################################
if __name__ == '__main__':
    NTEST  = int(raw_input('Enter a number of job to test --->'))
    NUSERS = int(raw_input('Enter a number of '"users"' to test --->'))
    OUTPUT = raw_input('Enter a name of output dir --->')
    REPTYPE= raw_input('Enter type of repository (Local[1] or Remote[2]) --->')
    if REPTYPE == '1':
        REPTYPE = 'Local'
    elif REPTYPE == '2':
        REPTYPE = 'Remote'
    else:
        logger.error("Unknown type of repository. Exiting")
        sys.exit(1)
    dname = 'users_' + str(NUSERS) + '__jobs_' + str(NTEST)
    output_dir = os.path.join(os.getcwd(), OUTPUT, REPTYPE, dname)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    logger.debug("output dir is ", output_dir)

    python_path = NormPath(sys.executable)
    i = 0
    while i < NUSERS:
        rootDir  = '/testdir/GangaTest/user'
        cmd =  '"import sys\nsys.path.append(\'%s\')\nfrom ARDATest import runTest\nrunTest(%d, \'%s\',\'%s\',\'%s\')"' % (_thisDir, NTEST, rootDir, output_dir, REPTYPE)
        if sys.version_info[:3] < (2,3,0) or sys.version_info[:3] >=(2,3,4):
            cmd = cmd[1:-1]
        logger.debug(cmd)
        pid = os.spawnl(os.P_NOWAIT, python_path, python_path, "-c", cmd)
        logger.debug("new user process started %d" % pid)
        i+=1

