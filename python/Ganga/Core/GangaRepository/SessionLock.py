# This file is a bit of a mess at the moment.
# It encapsulates fcntl-based locking that works on NFS, AFS and locally.
import os, time, pickle, errno, threading, fcntl, random
from sets import Set

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


session_expiration_timeout = 5 # seconds

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

    def __init__(self, dir, session_name, name):
        GangaThread.__init__(self, name='LockUpdater.%s' % name)
        self.sdir = os.path.join(os.path.realpath(dir),"sessions")
        self.ldir = os.path.join(os.path.realpath(dir),"locks")
        mkdir(self.sdir)
        mkdir(self.ldir)
        self.fn = os.path.join(self.sdir, session_name)
        self._sessions = [] # list of other open sessions
        self.create()

    def create(self):
        try:
            fd = os.open(self.fn, os.O_EXCL | os.O_CREAT)
            os.close(fd)
        except OSError, x:
            raise RepositoryError("Error on file access - session file: %s" % x)

    def acquire(self,name):
        lfn = os.path.join(self.ldir,name)
        try:
            os.symlink(self.fn,lfn)
            return True
        except OSError:
            if not os.path.exists(lfn): # Someone else has a valid lock
                # Now there is a broken link - this is somewhat uncomfortable, since
                # we now have to avoid race conditions between two sessions.
                # The solution is to use a dot-lock and only remove+recreate the symlink
                # if the link is still broken after the dot-lock succeeds
                # If this dot-lock stays there, we have a big problem. Therefore make sure it gets unlinked!
                try:
                    fd = os.open(lfn+".lock", os.O_EXCL | os.O_CREAT | os.O_NONBLOCK)
                    try:
                        if not os.path.exists(lfn): # Recheck that this link is still invalid
                            os.unlink(lfn)
                            os.symlink(self.fn,lfn)
                    finally:
                        os.close(fd)
                        os.unlink(lfn+".lock")
                except OSError: # did not get the dot-lock :(
                    pass
                # ... someone else has the lock 
            elif os.readlink(lfn) == self.fn:
                return True # uh, this is already our lock...
            return False

    def acquire_block(self,name):
        while not self.acquire(name):
            time.sleep(0.01)

    def release(self,name):
        try:
            if os.readlink(os.path.join(self.ldir,name)) == self.fn:
                os.unlink(os.path.join(self.ldir,name))
                return True
        except IOError:
            pass
        return False
        
    def run(self):
        while not self.should_stop():
            ## TODO: Check for services active/inactive
            time.sleep(1+random.random())
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
                        if now - os.stat(os.path.join(self.sdir,sf)).st_ctime > session_expiration_timeout:
                            os.unlink(os.path.join(self.sdir,sf))
                except OSError:
                    pass # nothing really important, another process deleted the session before we did.
            except Exception, x:
                logger.warning("Internal exception in session lock thread: %s %s" % (x.__class__.__name__, x))

        # Remove session file
        try:
            os.unlink(self.fn)
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

        self.locked_ids = Set()
        self.foreign_ids = []
        self.root = root_dir
        self.name = name

        mkdir(self.root)

        self.cntfn = os.path.join(self.root, "count")
        if not os.path.exists(self.cntfn):
            try:
                fd = os.open(self.cntfn, os.O_EXCL | os.O_CREAT | os.O_WRONLY)
                os.write(fd,"0")
                os.close(fd)
            except OSError, x:
                if x.errno != errno.EEXIST:
                    raise RepositoryError("OSError on count file create: %s" % x)

        # Use the hostname (os.uname()[1])  and the current time in ms to construct the session filename.
        # TODO: Perhaps put the username here?
        session_name = os.uname()[1]+"."+str(int(time.time()*1000))+".session"
        self.sessionlock = SessionLock(self.root, session_name, self.name)
        self.sessionlock.start()
        
    def make_new_ids(self,n):
        """ make_new_ids(n) --> list of ids
        Generate n consecutive new job ids.
        Raise RepositoryError"""
        ids = []
        self.sessionlock.acquire_block("counter")
        try:
            try:
                cnt = int(file(self.cntfn).readlines()[0])
                nl = 0
                while nl < n:
                    if self.sessionlock.acquire(str(cnt)):
                        ids.append(cnt)    
                        cnt += 1
                        nl += 1
                fd = os.open(self.cntfn, os.O_EXCL | os.O_TRUNC | os.O_SYNC | os.O_WRONLY)
                os.write(fd,str(cnt))
                os.close(fd)
            except OSError, x:
                raise RepositoryError("Error on file access: %s" % x)
            except IOError, x:
                raise RepositoryError("Error on file access: %s" % x)
        finally:
            self.sessionlock.release("counter")
        self.locked_ids.union_update(Set(ids))
        return ids

    def lock_ids(self,ids):
        """ lock_ids(n) --> list of successfully locked ids
        Tries to lock the given ids, returning only the successfully locked ids
        Raise RepositoryError"""
        already_locked_ids = Set()
        ids_to_lock = Set()
        for i in ids:
            assert type(i) == int
            if i in self.locked_ids:
                already_locked_ids.add(i)
            else:
                ids_to_lock.add(i)
        for id in ids_to_lock:
            if self.sessionlock.acquire(str(id)):
                already_locked_ids.add(id)
                self.locked_ids.add(id)
        return list(already_locked_ids)

    def release_ids(self,ids):
        """release_ids(ids) --> list of successfully released ids
        Raise RepositoryError"""
        ids = Set(ids)
        unlocked_ids = ids.intersection(self.locked_ids)
        self.locked_ids.difference_update(unlocked_ids)
        for id in unlocked_ids:
            self.sessionlock.release(str(id))
        return list(unlocked_ids)

    def check(self):
        for id in self.locked_ids:
            if not os.readlink(os.path.join(self.sessionlock.ldir,str(id))) == self.sessionlock.fn:
                print "ID ", id, " NOT LOCKED by ", self.sessionlock.fn, " but by ", os.readlink(os.path.join(self.sessionlock.ldir,str(id))), "!!!!!!"
                assert False

def test1():
    slm = SessionLockManager("locktest","tester")
    while True:
        print "lock  ---", slm.lock_ids(random.sample(xrange(1000),3))
        print "unlock---", slm.release_ids(random.sample(xrange(1000),3))
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

