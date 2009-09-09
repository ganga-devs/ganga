# This file is a bit of a mess at the moment.
# It encapsulates fcntl-based locking that works on NFS, AFS and locally.

import os, time, pickle, errno, threading, fcntl
from sets import Set
from Ganga.Core.GangaThread import GangaThread


import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

def open_file_sync_read(fn):
    fobj = file(fn,"r")
    fcntl.lockf(fobj.fileno(),fcntl.LOCK_SH)
    return fobj

def open_file_sync_write(fn):
    fobj = file(fn,"a+")
    fcntl.lockf(fobj.fileno(),fcntl.LOCK_EX)
    fobj.seek(0)
    fobj.truncate()
    return fobj

class SessionLock(GangaThread):
    """ Thread that keeps a global lock file that synchronizes
    ID access across Ganga sessions.
    WARNING: On NFS files that are not locked will not be synchoronized
    across clients, even if a global lock file is used!
    TODO: On OSError it should shutdown the repository!"""

    def __init__(self, fn, session_name, name):
        GangaThread.__init__(self, name='LockUpdater.%s' % name)
        self.fn = fn
        self.dir = os.path.dirname(fn)
        self.session_name = session_name

        self._intlock = threading.RLock()
        self._fobj = None
        self._sessions = []


    def acquire(self):
        self._intlock.acquire()
        try:
            file(self.fn,"a+").close() # create if not there
            self._fobj = file(self.fn,"r+")
            fcntl.lockf(self._fobj.fileno(),fcntl.LOCK_EX)
            try:
                ctime = int(time.time())
                sessions = [l.strip() for l in self._fobj.readlines()]
                current_sessions = []
                current_session_str = ""
                for s in sessions:
                    if len(s) == 0:
                        continue
                    try:
                        tstamp, name = s.split(" ",1)
                        dtstamp = ctime - int(tstamp)
                    except ValueError:
                        logger.warning("DEBUG: INVALID SESSION LINE %s" % s)
                        continue
                    if dtstamp < 10 and not name == self.session_name: # kill old sessions
                        current_session_str += s + "\n"
                        current_sessions.append(name)
                current_session_str += "%i %s\n" % (ctime, self.session_name)
                self._fobj.seek(0)
                self._fobj.truncate()
                self._fobj.write(current_session_str)
                self._fobj.flush()
                self._sessions = current_sessions
            except Exception:
                self._fobj.close() # This removes the fcntl lock!
                raise
        except Exception:
            self._intlock.release()
            raise

    def release(self):
        try:
            self._fobj.close()
        finally:
            self._intlock.release()

    def run(self):
        while not self.should_stop():
            ## TODO: Check for services active/inactive (treat IOError)
            time.sleep(1)
            self._intlock.acquire()
            try:
                file(self.fn,"a+").close() # create if not there
                fobj = file(self.fn,"r+")
                fcntl.lockf(fobj.fileno(),fcntl.LOCK_EX)
                try:
                    line = "\n"
                    while len(line) != 0:
                        lastpos = fobj.tell()
                        line = fobj.readline()
                        if line.strip().endswith(self.session_name):
                            fobj.seek(lastpos)
                            fobj.write(str(int(time.time())))
                            break
                    if len(line) == 0: # session was not found, append it!
                        fobj.write("%i %s\n" % (int(time.time()), self.session_name))
                    fobj.flush()
                finally:
                    fobj.close() # This removes the fcntl lock!
            finally:
                self._intlock.release()

        # Remove session from lockfile
        self._intlock.acquire()
        try:
            file(self.fn,"a+").close() # create if not there
            fobj = file(self.fn,"r+")
            fcntl.lockf(fobj.fileno(),fcntl.LOCK_EX)
            try:
                lines = [l for l in fobj.readlines() if not l.endswith(self.session_name+"\n")]
                fobj.truncate(0)
                fobj.writelines(lines)
                fobj.flush()
            finally:
                fobj.close() # This removes the fcntl lock!
        finally:
            self._intlock.release()
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
        self.session_dir = os.path.join(self.root,"session")
        # Use the hostname (os.uname()[1])  and the current time in ms to construct the session filename.
        # TODO: Perhaps put the username here?
        session_name = os.uname()[1]+"."+str(int(time.time()*1000))+".sessionlock"
        self.session_fn = os.path.join(self.session_dir, session_name)
        self.cntfn = os.path.join(self.root, "cnt")

        try:
            os.makedirs(self.session_dir)
        except OSError, x:
            if x.errno != errno.EEXIST:
                self._handle_error(x) # usually raises RepositoryError
        
        self._lock = SessionLock(os.path.join(self.session_dir, "lock"), session_name, self.name)
        self._setlocked() # creates session lock file
        self._lock.start()
        self._lock.acquire()
        self._lock.release()
        
    def make_new_ids(self,n):
        """ make_new_ids(n) --> list of ids
        Generate n consecutive new job ids.
        Raise RepositoryError"""
        self._lock.acquire()
        try:
            try:
                cnt = self._getcnt()
                ids = range(cnt,cnt+n)
                cnt += n
                pickle.dump(cnt,file(self.cntfn,'w'))
                self.locked_ids.union_update(Set(ids))
                self._setlocked()
                return ids
            except OSError, x:
                self._handle_error(x) # usually raises RepositoryError
            except IOError, x:
                self._handle_error(x) # usually raises RepositoryError
        finally:
            self._lock.release()


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
        if len(ids_to_lock) == 0:
            return list(already_locked_ids)
        self._lock.acquire()
        try:
            try:
                locked_ids = self._getlocked()
                self.foreign_ids = list(locked_ids)
                lockable_ids = ids_to_lock.difference(locked_ids)
                self.locked_ids.union_update(lockable_ids)
                self._setlocked()
            except OSError, x:
                self._handle_error(x) # usually raises RepositoryError
            except IOError, x:
                self._handle_error(x) # usually raises RepositoryError
        finally:
            self._lock.release()
        return list(already_locked_ids.union(lockable_ids))

    def release_ids(self,ids):
        """release_ids(ids) --> list of successfully released ids
        Raise RepositoryError"""
        self._lock.acquire()
        try:
            try:
                ids = Set(ids)
                locked_ids = self._getlocked()
                unlocked_ids = ids.intersection(self.locked_ids)
                self.locked_ids.difference_update(unlocked_ids)
                self._setlocked()
            except OSError, x:
                self._handle_error(x) # usually raises RepositoryError
            except IOError, x:
                self._handle_error(x) # usually raises RepositoryError
        finally:
            self._lock.release()
        return list(unlocked_ids)

    def sanity(self):
        self._lock.acquire()
        try:
            locked_ids = self._getlocked()
            for id in self.locked_ids:
                assert not id in locked_ids
            for id in locked_ids:
                assert not id in self.locked_ids
        finally:
            self._lock.release()

    def _handle_error(self, x):
        """ Handle the given error, shutdown services if it is fatal"""
        # TODO: Shutdown services
        raise RepositoryError("Error on file access: %s" % x)

    def _getcnt(self):
        """ _getcnt() --> int
        Returns the current counter; the session must be locked
        Raise OSError
        """
        try:
            fobj = open_file_sync_read(self.cntfn)
            try:
                return pickle.load(fobj)
            finally:
                fobj.close()
        except IOError,x:
            import errno
            if x.errno == errno.ENOENT:
                return 1
            else:
                raise OSError(x)
        
    def _getlocked(self):
        """ _getlocked() --> Set()
        Returns the currently locked ids from _other_ sessions
        Raise OSError"""
        locked_ids = Set()
        for s in self._lock._sessions:
            try:
                fobj = open_file_sync_read(os.path.join(self.session_dir,s))
                try:
                    locked_ids.union_update(pickle.load(fobj))
                finally:
                    fobj.close()
            except Exception, e: # Pretty much anything can happen on unpickling
                logger.debug("Unpickle error on session lock unpickling: %s" % e)
        return locked_ids

    def _setlocked(self):
        """Write the currently locked jobs to the session file."""
        f = open_file_sync_write(self.session_fn)
        pickle.dump(self.locked_ids,f)
        f.close()
       
import random
if __name__ == "__main__":
    slm = SessionLockManager("locktest","tester")
    while True:
        print "lock  ---", slm.lock_ids(random.sample(xrange(1000),3))
        print "unlock---", slm.release_ids(random.sample(xrange(1000),3))
        slm.sanity()

