##!/usr/bin/env python
################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: diskutilsTest.py,v 1.1 2008-07-17 16:41:02 moscicki Exp $
################################################################################

import sys, os
import re
import time
import tempfile

import diskutilsTest
_thisDir = os.path.dirname(diskutilsTest.__file__)
if not _thisDir:
    _thisDir = os.getcwd()

from diskutils import *

#DEBUG = False
DEBUG = True

################################################################################    
# memory testing
def getmem():
    """Gives memory used by the calling process in kb"""
    ss = os.popen('pmap %d | tail -1'%os.getpid(), 'r').read()
    if ss:
        m = re.search(r'([0-9]*)K', ss)
        if m:
            return int(m.group(1))
        
def _startText(ff, txt):
    ff.write(txt)
    t1 = time.time()
    ff.write('operation started at %s \n' % time.ctime(t1))
    m1 = getmem()
    return t1, m1

def _endText(ff, t1, m1):
    t2 = time.time()
    m2 = getmem()
    dt = t2 - t1
    dm = m2 - m1
    ff.write('operation finished at %s \n' % time.ctime(t2))
    ff.write('time used: %f seconds \n' % dt)
    ff.write('-s->%f<-s-\n\n\n'% dt)     
    ff.write('memory used: %f Kb \n' % dm)
    ff.write('-s->%f<-s-\n\n\n'% dm)
    return dt, dm
    
def runTest(NTEST, LEN, rootDir, output_dir):
    if DEBUG:
        print 'from runTest: rootDir %s, output_dir %s'%(rootDir, output_dir)
    nn = tempfile.mktemp(suffix = '.test')
    nn = os.path.join(output_dir, os.path.basename(nn))
    ff = file(nn, 'w')
    lock = RLock()
    try:
        jj = []
        for i in range(NTEST):
            j = []
            for k in range(10):
                j.append(str(i) + '-' + str(k))
            j.append(LEN*'x'+str(i))
            jj.append(j)

        t1, m1 = _startText(ff, 'registering %d jobs...' % NTEST)
        if lock.acquire():
            try:
                write(jj, 'aaaa')
            finally:
                lock.release()
        dt, dm = _endText(ff, t1, m1)
        if DEBUG:
            print 'registering %d jobs...' % NTEST
            print "--->command status", "OK"
            print "(dt, dm):", (dt, dm), "\n"
        

        t1, m1 = _startText(ff, 'retrieving info about ALL jobs')
        if lock.acquire():
            try:
                rjj = readLast('aaaa')
            finally:
                lock.release()
        dt, dm = _endText(ff, t1, m1)
        if DEBUG:
            print 'retrieving info about ALL jobs'
            #print "--->checkout jobs", len(rjj), map(lambda j: j[0], rjj)
            print "(dt, dm):", (dt, dm), "\n"


        t1, m1 = _startText(ff, 'registering %d jobs 10 times...' % NTEST)
        for i in xrange(10):
            if lock.acquire():
                try:
                    for j in jj:
                        j[-1] += '_'+str(i)
                    write(jj, 'aaaa'+str(i))
                finally:
                    lock.release()
        dt, dm = _endText(ff, t1, m1)
        if DEBUG:
            print 'registering %d jobs 10 times...' % NTEST
            print "--->command status", "OK"
            print "(dt, dm):", (dt, dm), "\n"


        t1, m1 = _startText(ff, 'retrieving info about ALL jobs 10 times')
        for i in xrange(10):
            if lock.acquire():
                try:
                    rjj = readLast('aaaa'+str(i), rjj)
                finally:
                    lock.release()
        dt, dm = _endText(ff, t1, m1)
        if DEBUG:
            print 'retrieving info about ALL jobs 10 times'
            #print "--->checkout jobs", len(rjj), map(lambda j: j[0], rjj)
            print "(dt, dm):", (dt, dm), "\n"            

        pid = os.getpid()
        for j in jj:
            j[0] = str(pid) 
            
        t1, m1 = _startText(ff, 'commiting %d jobs...' % NTEST)
        if lock.acquire():
            try:
                rjj = readLast('aaaa', rjj)              
                write(jj, 'aaaa')
            finally:
                lock.release()
        dt, dm = _endText(ff, t1, m1)
        if DEBUG:
            print 'commiting %d jobs...' % NTEST
            print "--->command status", "OK"
            print "(dt, dm):", (dt, dm), "\n"

        t1, m1 = _startText(ff, 'retrieving info about ALL jobs')
        if lock.acquire():
            try:
                rjj = readLast('aaaa', rjj)                   
            finally:
                lock.release()
        dt, dm = _endText(ff, t1, m1)       
        if DEBUG:
            print 'retrieving info about ALL jobs'
            #print "--->checkout jobs", len(rjj), map(lambda j: j[0], rjj)
            print "(dt, dm):", (dt, dm), "\n"
        for j in rjj:
            assert(rjj[0][0] == j[0])
            
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
    NUSERS = int(raw_input('Enter a number of '"processes"' to test --->'))
    LEN    = int(raw_input('Enter a job length --->'))
    OUTPUT = raw_input('Enter a name of output dir --->')
    dname = 'users_' + str(NUSERS) + '__jobs_' + str(NTEST)
    output_dir = os.path.join(os.getcwd(), OUTPUT, dname)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    print "output dir is ", output_dir
    rootDir  = os.path.join(os.getcwd(), 'testdir', 'GangaTest', 'user')

    python_path = NormPath(sys.executable)
    i = 0
    while i < NUSERS:
        cmd =  "import sys\nsys.path.append(\'%s\')\nfrom diskutilsTest import runTest\nrunTest(%d, %d, \'%s\',\'%s\')" % (_thisDir, NTEST, LEN, rootDir, output_dir)
        if DEBUG:
            print cmd
        
        pid = os.spawnl(os.P_NOWAIT, python_path, python_path, "-c", cmd)
        if DEBUG:
            print "new user process started %d" % pid
        i+=1

