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

class SessionLockManager(GangaThread):
    """ Thread that keeps a global lock file that synchronizes
    ID access across Ganga sessions.
    WARNING: On NFS files that are not locked will not be synchoronized
    across clients, even if a global lock file is used!
    TODO: On OSError it should shutdown the repository!"""

    def __init__(self, root, name):
        GangaThread.__init__(self, name='LockUpdater.%s' % name)
        mkdir(root)
        realpath = os.path.realpath(root)
        # Use the hostname (os.uname()[1])  and the current time in ms to construct the session filename.
        # TODO: Perhaps put the username here?
        session_name = os.uname()[1]+"."+str(int(time.time()*1000))+".session"
        self.sdir = os.path.join(realpath,"sessions")
        self.fn = os.path.join(self.sdir, session_name)
        self.cntfn = os.path.join(realpath,"cnt")

        self.afs = (realpath[:4] == "/afs")
        self.locked = Set()
        self.count = 0
        self.setup()
        self.start()

    def setup(self):
        mkdir(self.sdir)
        # setup global lock

        self.global_lock_setup()
        self.global_lock_acquire()
        try:
            # setup counter file if it does not exist, read it if it does
            if not os.path.exists(self.cntfn):
                try:
                    fd = os.open(self.cntfn, os.O_EXCL | os.O_CREAT | os.O_WRONLY)
                    os.write(fd,"0")
                    os.close(fd)
                except OSError, x:
                    if x.errno != errno.EEXIST:
                        raise RepositoryError("OSError on count file create: %s" % x)
            self.count = max(self.count,self.cnt_read())
            self.cnt_write()
            # Setup session file
            try:
                fd = os.open(self.fn, os.O_EXCL | os.O_CREAT | os.O_WRONLY)
                os.write(fd,pickle.dumps(Set()))
                os.close(fd)
            except OSError, x:
                raise RepositoryError("Error on file access - session file: %s" % x)
            self.session_write()
        finally:
            self.global_lock_release()

    # Global lock function
    def global_lock_setup(self):
        lockfn = os.path.join(self.sdir,"global_lock")
        file(lockfn,"w").close() # create file (does not interfere with existing sessions)
        self.lockfd = os.open(lockfn,os.O_RDWR)

    def global_lock_acquire(self):
        fcntl.lockf(self.lockfd,fcntl.LOCK_EX)

    def global_lock_release(self):
        fcntl.lockf(self.lockfd,fcntl.LOCK_UN)

    # Session read-write functions
    def session_read(self,fn):
        """ Reads a session file and returns a set of IDs locked by that session.
            The global lock MUST be held for this function to work, although on NFS additional
            locking is done
            Raises RepositoryError if severe access problems occur (corruption otherwise!) """
        try:
            fd = os.open(fn, os.O_RDONLY) # This can fail (thats OK, file deleted in the meantime)
            try:
                if not self.afs: # additional locking for NFS
                    fcntl.lockf(fd,fcntl.LOCK_SH)
                try:
                    return pickle.loads(os.read(fd,104857600)) # read up to 100 MB (that is more than enough...)
                except Exception, x:
                    logger.warning("corrupt or inaccessible session file '%s' - ignoring it (Exception %s %s)."% (fn, x.__class__.__name__, x))
            finally:
                if not self.afs: # additional locking for NFS
                    fcntl.lockf(fd,fcntl.LOCK_UN)
                os.close(fd)
        except OSError, x:
            if x.errno != errno.EEXIST:
                raise RepositoryError("Error on session file access '%s': %s" % (fn,x))
        return Set()

    def session_write(self):
        """ Writes the locked set to the session file. 
            The global lock MUST be held for this function to work, although on NFS additional
            locking is done
            Raises RepositoryError if session file is inaccessible """
        try:
            # If this fails, we want to shutdown the repository (corruption possible)
            fd = os.open(self.fn,os.O_WRONLY)
            if not self.afs:
                fcntl.lockf(fd,fcntl.LOCK_EX)
            os.write(fd,pickle.dumps(self.locked))
            if not self.afs:
                fcntl.lockf(fd,fcntl.LOCK_UN)
            os.close(fd)
        except OSError, x:
            if x.errno != errno.EEXIST:
                raise RepositoryError("Error on session file access '%s': %s" % (self.fn,x))
            else:
                raise RepositoryError("Own session file not found! Possibly deleted by another ganga session - the system clocks on computers running Ganga must be synchronized!")

    # counter read-write functions
    def cnt_read(self):
        """ Tries to read the counter file.
            Raises ValueError (invalid contents)
            Raises IOError (no access/does not exist)"""
        fd = os.open(self.cntfn, os.O_RDONLY)
        try:
            if not self.afs: # additional locking for NFS
                fcntl.lockf(fd,fcntl.LOCK_SH)
            return int(os.read(fd,100)) # 100 bytes should be enough for any ID
        finally:
            if not self.afs: # additional locking for NFS
                fcntl.lockf(fd,fcntl.LOCK_UN)
            os.close(fd)

    def cnt_write(self):
        """ Writes the counter to the counter file. 
            The global lock MUST be held for this function to work correctly
            Raises OSError if count file is inaccessible """
        try:
            # If this fails, we want to shutdown the repository (corruption possible)
            fd = os.open(self.cntfn,os.O_WRONLY)
            if not self.afs:
                fcntl.lockf(fd,fcntl.LOCK_EX)
            os.write(fd,str(self.count))
            if not self.afs:
                fcntl.lockf(fd,fcntl.LOCK_UN)
            os.close(fd)
        except OSError, x:
            if x.errno != errno.EEXIST:
                raise RepositoryError("Error on count file access: %s" % (x))
            else:
                raise RepositoryError("Count file not found! Repository was modified externally")

    # "User" functions
    def make_new_ids(self,n):
        """ Locks the next n available ids and returns them as a list """
        self.global_lock_acquire()
        try:
            # Actualize count
            try:
                newcount = self.cnt_read()
            except ValueError:
                logger.warning("Corrupt job counter! Trying to recover...")
                newcount = self.count
            assert newcount >= self.count
            ids = range(newcount,newcount+n)
            self.locked.update(ids)
            self.count = newcount+n
            self.cnt_write()
            self.session_write()
            return list(ids)
        finally:
            self.global_lock_release()

    def lock_ids(self,ids):
        ids = Set(ids)
        self.global_lock_acquire()
        try:
            sessions = [sn for sn in os.listdir(self.sdir) if sn.endswith(".session")]
            slocked = Set()
            for session in sessions:
                sf = os.path.join(self.sdir,session)
                if sf == self.fn:
                    continue
                try:
                    slocked.update(self.session_read(sf))
                except Exception:
                    logger.warning("Corrupt session file - ignoring it: '%s'"% sf)
            ids.difference_update(slocked)
            self.locked.update(ids)
            self.session_write()
            return list(ids)
        finally:
            self.global_lock_release()

    def release_ids(self,ids):
        self.global_lock_acquire()
        try:
            self.locked.difference_update(ids)
            self.session_write()
            return list(ids)
        finally:
            self.global_lock_release()

    def run(self):
        while not self.should_stop():
            ## TODO: Check for services active/inactive
            try:
                if not os.path.exists(self.fn):
                    raise RepositoryError("Own session file not found! Possibly deleted by another ganga session - the system clocks on computers running Ganga must be synchronized!")
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

        # On shutdown remove session file
        try:
            os.unlink(self.fn)
        except OSError, x:
            logger.warning("Session file was deleted externally or removal failed: %s" % (x))
        self.unregister()

    def check(self):
        self.global_lock_acquire()
        try:
            f = file(self.cntfn)
            newcount = int(f.readline())
            f.close()
            assert newcount >= self.count
            sessions = os.listdir(self.sdir)
            prevnames = Set()
            for session in sessions:
                if not session.endswith(".session"):
                    continue
                try:
                    sf = os.path.join(self.sdir,session)
                    fd = -1
                    if not self.afs:
                        fd = os.open(sf, os.O_RDONLY)
                        fcntl.lockf(fd,fcntl.LOCK_SH) # ONLY NFS
                    names = pickle.load(file(sf))
                    if not self.afs and fd > 0:
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
            self.global_lock_release()

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

