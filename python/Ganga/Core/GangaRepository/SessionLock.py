# This file is a bit of a mess at the moment.
# It encapsulates fcntl-based locking that works on NFS, AFS and locally.
import os, time, errno, threading, fcntl, random
from sets import Set

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    from Ganga.Core.GangaThread import GangaThread
    from Ganga.Core.GangaRepository import RepositoryError
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()
except ImportError:
    from threading import Thread
    class GangaThread(Thread):
        def __init__(self,name):
            self.name = name
            super(GangaThread,self).__init__()
        def should_stop(self):
            return False

    class Logger:
        def warning(self,msg):
            print msg
        def debug(self,msg):
            print msg

    class RepositoryError(Exception):
        pass
    logger = Logger() 


session_expiration_timeout = 8 # seconds

def mkdir(dn):
    try:
        os.makedirs(dn)
    except OSError, x:
        if x.errno != errno.EEXIST:
            raise RepositoryError("OSError on directory create: %s" % x)

class SessionLock(GangaThread):
    """ Thread that keeps a global lock file that synchronizes
    ID access across Ganga sessions.
    WARNING: On NFS files that are not locked will not be synchoronized
    across clients, even if a global lock file is used!
    TODO: On OSError it should shutdown the repository!"""

    def __init__(self, dir, name):
        GangaThread.__init__(self, name='LockUpdater.%s' % name)
        realpath = os.path.realpath(dir)
        # Use the hostname (os.uname()[1])  and the current time in ms to construct the session filename.
        # TODO: Perhaps put the username here?
        session_name = os.uname()[1]+"."+str(int(time.time()*1000))+".session"
        self.sdir = os.path.join(realpath,"sessions")
        self.ldir = os.path.join(realpath,"locks")
        self.fn = os.path.join(self.sdir, session_name)
        self.cntfn = os.path.join(realpath,"cnt")

        self.afs = (realpath[:4] == "/afs")
        self.locked = Set()
        self.count = 0
        self.create()

    def create(self):
        mkdir(self.sdir)
        mkdir(self.ldir)
        lockfn = os.path.join(self.sdir,"global_lock")
        file(lockfn,"w").close() # create file
        self.lockfd = os.open(lockfn,os.O_RDWR)
        if not os.path.exists(self.cntfn):
            try:
                fd = os.open(self.cntfn, os.O_EXCL | os.O_CREAT | os.O_WRONLY)
                os.write(fd,"0")
                os.close(fd)
            except OSError, x:
                if x.errno != errno.EEXIST:
                    raise RepositoryError("OSError on count file create: %s" % x)
        else:
            f = file(self.cntfn)
            try:
                self.count = max(self.count,int(f.readline()))
            except ValueError:
                pass
            f.close()
        try:
            fd = os.open(self.fn, os.O_EXCL | os.O_CREAT | os.O_WRONLY)
            os.write(fd,pickle.dumps(Set()))
            os.close(fd)
        except OSError, x:
            raise RepositoryError("Error on file access - session file: %s" % x)

    def global_lock(self):
        fcntl.lockf(self.lockfd,fcntl.LOCK_EX)

    def global_unlock(self):
        fcntl.lockf(self.lockfd,fcntl.LOCK_UN)

    def acquire_next(self,n):
        self.global_lock()
        try:
            # Actualize count
            f = file(self.cntfn)
            try:
                newcount = int(f.readline())
            except ValueError:
                logger.warning("Corrupt job counter! Trying to recover...")
                newcount = self.count
            f.close()
            assert newcount >= self.count
            ids = range(newcount,newcount+n)
            self.locked.update(ids)
            self.count = newcount+n
            f = file(self.cntfn,"w")
            f.write(str(self.count))
            f.close()
            fd = os.open(self.fn,os.O_WRONLY)
            fcntl.lockf(fd,fcntl.LOCK_EX) # ONLY FOR NFS
            os.write(fd,pickle.dumps(self.locked))
            fcntl.lockf(fd,fcntl.LOCK_UN) # ONLY FOR NFS
            os.close(fd)
            return ids
        finally:
            self.global_unlock()

    def acquire(self,ids):
        ids = Set(ids)
        self.global_lock()
        try:
            sessions = [sn for sn in os.listdir(self.sdir) if sn.endswith(".session")]
            slocked = Set()
            for session in sessions:
                sf = os.path.join(self.sdir,session)
                if sf == self.fn:
                    continue
                fd = -1
                if not self.afs:
                    fd = os.open(sf, os.O_RDONLY)
                    fcntl.lockf(fd,fcntl.LOCK_SH) # ONLY NFS
                try:
                    slocked.update(pickle.load(file(sf)))
                except Exception:
                    logger.warning("Corrupt session file - ignoring it: '%s'"% sf)
                if not self.afs and fd > 0:
                    fcntl.lockf(fd,fcntl.LOCK_UN) # ONLY NFS
                    os.close(fd)
            ids.difference_update(slocked)
            self.locked.update(ids)
            fd = os.open(self.fn,os.O_WRONLY)
            fcntl.lockf(fd,fcntl.LOCK_EX) # ONLY FOR NFS
            os.write(fd,pickle.dumps(self.locked))
            fcntl.lockf(fd,fcntl.LOCK_UN) # ONLY FOR NFS
            os.close(fd)
            return ids
        finally:
            self.global_unlock()

    def acquire_block(self,name):
        while not self.acquire(name):
            time.sleep(0.01)

    def release(self,ids):
        self.global_lock()
        try:
            self.locked.difference_update(ids)
            fd = os.open(self.fn,os.O_WRONLY)
            fcntl.lockf(fd,fcntl.LOCK_EX) # ONLY FOR NFS
            os.write(fd,pickle.dumps(self.locked))
            fcntl.lockf(fd,fcntl.LOCK_UN) # ONLY FOR NFS
            os.close(fd)
            return ids
        finally:
            self.global_unlock()

    def run(self):
        while not self.should_stop():
            ## TODO: Check for services active/inactive
            try:
                if not os.path.exists(self.fn):
                    self.create()
                    logger.warning("Session file was deleted externally! Locks may be lost!")
                else:
                    os.utime(self.fn,None)
                # Clear expired session files
                try:
                    now = os.stat(self.fn).st_ctime
                    for sf in os.listdir(self.sdir):
                        if not sf.endswith(".session"):
                            continue
                        if now - os.stat(os.path.join(self.sdir,sf)).st_ctime > session_expiration_timeout:
                            os.unlink(os.path.join(self.sdir,sf))
                except OSError, x:
                    # nothing really important, another process deleted the session before we did.
                    logger.warning("Unimportant OSError in loop: %s" % x)
            except Exception, x:
                logger.warning("Internal exception in session lock thread: %s %s" % (x.__class__.__name__, x))
            time.sleep(1+random.random())

        # Remove session file
        try:
            os.unlink(self.fn)
            # close AFS lock file
            os.close(self.lockfd)
        except OSError, x:
            logger.warning("Session file was deleted externally or removal failed: %s" % (x))
        self.unregister()

class SessionLockManager(object):

    def __init__(self,root_dir,name):
        """ Synchronizes several Ganga processes accessing the same repository
        Uses fcntl to provide locking.
        This means that NO other part/thread/etc. of ganga should do ANY operations
        on these files as the locks will be lost!
        Raise RepositoryError"""
        self.root = root_dir
        self.name = name
        mkdir(self.root)
        self.sessionlock = SessionLock(self.root, self.name)
        self.sessionlock.start()
        
    def make_new_ids(self,n):
        """ make_new_ids(n) --> list of ids
        Generate n consecutive new job ids.
        Raise RepositoryError"""
        return list(self.sessionlock.acquire_next(n))

    def lock_ids(self,ids):
        """ lock_ids(n) --> list of successfully locked ids
        Tries to lock the given ids, returning only the successfully locked ids
        Raise RepositoryError"""
        return list(self.sessionlock.acquire(ids))

    def release_ids(self,ids):
        """release_ids(ids) --> list of successfully released ids
        Raise RepositoryError"""
        return list(self.sessionlock.release(ids))

    def check(self):
        self.sessionlock.global_lock()
        try:
            f = file(self.sessionlock.cntfn)
            newcount = int(f.readline())
            f.close()
            assert newcount >= self.sessionlock.count
            sessions = os.listdir(self.sessionlock.sdir)
            prevnames = Set()
            for session in sessions:
                if not session.endswith(".session"):
                    continue
                try:
                    sf = os.path.join(self.sessionlock.sdir,session)
                    fd = -1
                    if not self.sessionlock.afs:
                        fd = os.open(sf, os.O_RDONLY)
                        fcntl.lockf(fd,fcntl.LOCK_SH) # ONLY NFS
                    names = pickle.load(file(sf))
                    if not self.sessionlock.afs and fd > 0:
                        fcntl.lockf(fd,fcntl.LOCK_UN) # ONLY NFS
                        os.close(fd)
                except Exception, x:
                    logger.warning("CHECKER: session file %s corrupted: %s %s" % (session, x.__class__.__name__, x) )
                    continue
                if not len(names & prevnames) == 0:
                    print "Double-locked stuff:", names & prevnames
                    assert False
                prevnames.union_update(names)

        finally:
            self.sessionlock.global_unlock()

def test1():
    slm = SessionLockManager("locktest","tester")
    while True:
        print "lock  ---", slm.lock_ids(random.sample(xrange(100),3))
        print "unlock---", slm.release_ids(random.sample(xrange(100),3))
        slm.check()

def test2():
    slm = SessionLockManager("locktest","tester")
    while True:
        n = random.randint(1,9)
        print "get %i ids ---"%n, slm.make_new_ids(n)
        slm.check()

import random
if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        print "Usage: python SessionLock.py {1|2}"
        sys.exit(-1)
    if sys.argv[1] == "1":
        test1()
    elif sys.argv[1] == "2":
        test2()

