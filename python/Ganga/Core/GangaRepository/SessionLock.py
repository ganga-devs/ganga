# This class (SessionLockManager) encapsulates fcntl-based locking that works on NFS, AFS and locally.
# You can use
# python SessionLock.py {1|2}
# to run locking tests (run several instances in the same directory, from
# different machines)

from __future__ import print_function

import functools
import threading
from collections import namedtuple

import os
import time
import errno
import fcntl
import random
import datetime
import getpass

try:
    import cPickle as pickle
except ImportError:
    import pickle

from Ganga.Utility.logging import getLogger

from Ganga.Utility.Config.Config import getConfig, ConfigError
from Ganga.GPIDev.Base.Proxy import getName

try:
    from Ganga.Core.GangaThread import GangaThread
    from Ganga.Core.GangaRepository import RepositoryError
except ImportError:
    from threading import Thread

    print("IMPORT ERROR SHOULD NOT OCCUR IN PRODUCTION CODE!!!!!!!!!!!!!!!!!!!!!!")
    

    class GangaThread(Thread):
    
        def __init__(self, name):
            self.name = name
            super(GangaThread, self).__init__()

        def should_stop(self):
            return False


    class RepositoryError(Exception):
        pass


logger = getLogger()


session_lock_last = 0
session_expiration_timeout = 0
try:
    session_expiration_timeout = getConfig('Configuration')['DiskIOTimeout']
except ConfigError, err:
    session_expiratrion_timeout = 5

session_lock_refresher = None

def getGlobalLockRef(session_name, sdir, gfn, _on_afs):
    global session_lock_refresher
    if session_lock_refresher is None:
        try:
            os.close(SessionLockManager.delay_init_open(gfn))
            registerGlobalSessionFile(gfn)
        except OSError as err:
            logger.debug("Startup Lock Refresher Exception: %s" % err)
            raise RepositoryError(None, "Error on session file '%s' creation: %s" % (gfn, err))
        session_lock_refresher = SessionLockRefresher(session_name, sdir, gfn, None, _on_afs)
        session_lock_refresher.start()
    return session_lock_refresher

sessionFiles = []
sessionFileHandlers = []

def registerGlobalSessionFile(thisSessionFile):
    global sessionFiles
    if thisSessionFile not in sessionFiles:
        sessionFiles.append(thisSessionFile)


def registerGlobalSessionFileHandler(thisFileHandler):
    global sessionFileHandlers
    if thisFileHandler not in sessionFileHandlers:
        sessionFileHandlers.append(thisFileHandler)


def removeGlobalSessionFiles():
    global sessionFiles
    for i in sessionFiles:
        if os.path.isfile(i):
            if not i.endswith('global_lock'):
                #logger.debug( "Removing: %s" + i )
                os.unlink(i)


def removeGlobalSessionFileHandlers():
    global sessionFileHandlers
    for i in sessionFileHandlers:
        try:
            fcntl.lockf(i, fcntl.LOCK_UN)
        except Exception as err:
            logger.debug("Failed to unlock or close sessionfilehandler")
            logger.debug("%s" % err)


def getGlobalSessionFiles():
    global sessionFiles
    return sessionFiles


class SessionLockRefresher(GangaThread):

    def __init__(self, session_name, sdir, fn, repo, afs):
        GangaThread.__init__(self, name='SessionLockRefresher', critical=True)
        self.session_name = session_name
        self.sdir = sdir
        self.fns = [fn]
        self.repos = [repo]
        self.afs = afs
        self.FileCheckTimes = {}

    # This function attempts to grab the ctime from a file which should exist
    # As we don't want this to fail outright it attempts to re-read every 1s
    # for 30sec before failing
    def delayread(self, filename):
        value = None
        i = 0
        while value is None:
            try:
                value = os.stat(filename).st_ctime
            except OSError as x:
                # print "fail"
                value = None
                i = i + 1
                time.sleep(0.1)
                if i >= 60:  # 3000:
                    raise x
                else:
                    continue

        return value

    def run(self):

        try:
            while not self.should_stop():

                self.updateNow()
                self.checkAndReap()

                sleeptime = 1  # +random.random()

                for i in range(int(sleeptime * 200)):
                    if not self.should_stop():
                        time.sleep(0.05 + random.random() * 0.05)
        finally:
            #logger.debug("Finishing Monitoring Loop")
            self.unregister()

    def checkAndReap(self):
        # TODO: Check for services active/inactive
        try:
            for index in range(len(self.fns)):

                now = self.updateLocks(index)

                # Clear expired session files if monitoring is active
                self.clearDeadLocks(now)

        except Exception as x:
            logger.warning( "Internal exception in session lock thread: %s %s" % (getName(x), x))

    def updateNow(self):
        try:
            for index in range(len(self.fns)):
                now = self.updateLocks(index)
        except Exception as x:
            logger.warning("Internal exception in Updating session lock thread: %s %s" % ( getName(x), x))

    def updateLocks(self, index):

        this_index_file = self.fns[index]
        if this_index_file in self.FileCheckTimes.keys():
            if abs(self.FileCheckTimes[this_index_file]-time.time()) >= 3:
                now = self._reallyUpdateLocks(index)
                self.FileCheckTimes[this_index_file] = now
        else:
            now = self._reallyUpdateLocks(index)
            self.FileCheckTimes[this_index_file] = now

        return self.FileCheckTimes[this_index_file]

    def _reallyUpdateLocks(self, index, failCount=0):
        this_index_file = self.fns[index]
        now = None
        try:
            oldnow = self.delayread(this_index_file)
            os.system('touch %s' % this_index_file)
            now = self.delayread(this_index_file) # os.stat(self.fn).st_ctime
        except OSError as x:
            if x.errno != errno.ENOENT:
                logger.debug("Session file timestamp could not be updated! Locks could be lost!")
                if now is None and failCount < 4:
                    try:
                        logger.debug("Attempting to lock file again, unknown error:\n'%s'" % x)
                        import time
                        time.sleep(0.5)
                        failcount=failCount+1
                        now = self._reallyUpdateLocks(index, failcount)
                    except Exception as err:
                        now = -999.
                        logger.debug("Received another type of exception, failing to update lockfile: %s" % this_index_file)
                else:
                    logger.warning("Failed to update lock file: %s 5 times." % this_index_file)
                    logger.warning("This could be due to a filesystem problem, or multiple versions of ganga trying to access the same file")
                    now = -999.
            else:
                if self.repos[index] != None:
                    raise RepositoryError(self.repos[index],
                    "[SessionFileUpdate] Run: Own session file not found! Possibly deleted by another ganga session.\n\
                    Possible reasons could be that this computer has a very high load, or that the system clocks on computers running Ganga are not synchronized.\n\
                    On computers with very high load and on network filesystems, try to avoid running concurrent ganga sessions for long.\n '%s' : %s" % (this_index_file, x))
                else:
                    from Ganga.Core import GangaException
                    raise GangaException("Error Opening global .session file for this session: %s" % this_index_file)
        return now

    def clearDeadLocks(self, now):
        try:
            # Make list of sessions that are "alive"
            ls_sdir = os.listdir(self.sdir)
            session_files = [f for f in ls_sdir if f.endswith(".session") and f.find(str(os.getpid())) == -1]
            #metadata_lock_files = [f for f in ls_sdir if f.endswith("metadata.locks") and not f.find( str(os.getpid()) ) ]
            #all_lock_files = [f for f in ls_sdir if f.endswith(".locks") and not f.find( str(os.getpid()) ) ]
            # lock_files = [ s for s in all_lock_files and not in
            # metadata_lock_files ]
            lock_files = [f for f in ls_sdir if f.endswith(".locks") and f.find(str(os.getpid())) == -1]

            ## Loop over locks which aren't belonging to this session!
            for sf in session_files:
                joined_path = os.path.join(self.sdir, sf)
                #print("join: %s" % joined_path)
                if joined_path in self.fns:
                    continue
                mtm = os.stat(joined_path).st_mtime
                # print "%s: session %s delta is %s seconds" % (time.time(),
                # sf, now - mtm)
                global session_expiration_timeout
                if abs(float(now) - float(mtm)) > session_expiration_timeout:
                    logger.warning("Removing session %s because of inactivity (no update since %s seconds)" % (sf, now - mtm))
                    os.unlink(joined_path)
                    session_files.remove(sf)
                # elif now - mtm  > session_expiration_timeout/2:
                #    logger.warning("%s: Session %s is inactive (no update since %s seconds, removal after %s seconds)" % (time.time(), sf, now - mtm, session_expiration_timeout))

            # remove all lock files that do not belong to sessions that are
            # alive
            for f in lock_files:
                # Determine the session file which controls this lock file
                asf = f.split(".session")[0] + ".session"
                if not asf in session_files:
                    #logger.debug("Removing dead file %s" % (f) )
                    #logger.debug("Found, %s session files" % (session_files) )
                    #logger.debug("self.fns[%s] = %s " % ( index, self.fns[index] ) )
                    #logger.debug( "%s, %s" % ( self.sdir, f ) )
                    os.unlink(os.path.join(self.sdir, f))
        except OSError as x:
            # nothing really important, another process deleted the session
            # before we did.
            logger.debug("Unimportant OSError in cleaning locks: %s" % x)

        return

    def numberRepos(self):
        assert(len(self.fns) == len(self.repos))
        return len(self.fns)

    def addRepo(self, fn, repo):
        #logger.debug("Adding Repo: %s" % repo )
        self.repos.append(repo)
        #logger.debug("Adding fn: %s" % fn )
        self.fns.append(fn)

    def removeRepo(self, fn, repo):
        #logger.debug("Removing fn: %s" % fn )
        self.fns.remove(fn)
        #logger.debug("Removing fn: %s" % fn )
        self.repos.remove(repo)

        assert(len(self.fns) == len(self.repos))


def synchronised(f):
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
        with self._lock:
            return f(self, *args, **kwargs)
    return decorated


class SessionLockManager(object):

    """ Class with thread that keeps a global lock file that synchronizes
    ID and counter access across Ganga sessions.
    DEVELOPER WARNING: On NFS, files that are not locked with lockf (NOT flock) will 
    NOT be synchronized across clients, even if a global lock file is used!
    Interface:
        * startup() starts the session, automatically called on init
        * shutdown() stops the thread, FREES ALL LOCKS
        * make_new_ids(n) returns n new (locked) ids
        * lock_ids(ids) returns the ids that were successfully locked
        * release_ids(ids) returns the ids that were successfully released (now: all)
    All access to an instance of this class MUST be synchronized!
    Should ONLY raise RepositoryError (if possibly-corrupting errors are found)
    """

    LastCountAccess = namedtuple('LastCountAccess', ['time', 'val'])

    def mkdir(self, dn):
        """Make sure the given directory exists"""
        try:
            os.makedirs(dn)
        except OSError as x:
            if x.errno != errno.EEXIST:
                raise RepositoryError(
                    self.repo, "OSError on directory create: %s" % x)

    def __init__(self, repo, root, name, minimum_count=0):

        self.repo = repo
        self.mkdir(root)
        realpath = os.path.realpath(root)
        # Use the hostname (os.uname()[1])  and the current time in ms to construct the session filename.
        # TODO: Perhaps put the username here?
        global session_lock_refresher
        if session_lock_refresher is None:
            t = datetime.datetime.now()
            this_date = t.strftime("%H.%M_%A_%d_%B_%Y")
            session_name = ".".join(
                [os.uname()[1], str(this_date), "PID", str(os.getpid()), "session"])
            #session_name = ".".join([os.uname()[1], str(int(time.time()*1000)), str(os.getpid()), "session"])
        else:
            session_name = session_lock_refresher.session_name

        self.sdir = os.path.join(realpath, "sessions")
        self.gfn = os.path.join(self.sdir, session_name)
        self.fn = os.path.join(self.sdir, session_name + "." + name + ".locks")
        self.cntfn = os.path.join(realpath, name, "cnt")

        self.afs = (realpath[:4] == "/afs")
        self.locked = set()
        self.count = minimum_count
        self.session_name = session_name
        self.name = name
        self.realpath = realpath
        #logger.debug( "Initializing SessionLockManager: " + self.fn )
        self._lock = threading.RLock()
        self.last_count_access = None

    @staticmethod
    def delay_init_open(filename):
        value = None
        i = 0
        while value is None:
            try:
                value = os.open(filename, os.O_EXCL | os.O_CREAT | os.O_WRONLY)
            except OSError as x:
                time.sleep(0.1)
                if i >= 60:
                    raise x
                i += 1
        return value

    @synchronised
    def startup(self):
        self.last_count_access = None

        # Ensure directories exist
        self.mkdir(os.path.join(self.realpath, "sessions"))
        self.mkdir(os.path.join(self.realpath, self.name))

        # setup global lock
        self.global_lock_setup()
        self.global_lock_acquire()
        try:
            # setup counter file if it does not exist, read it if it does
            if not os.path.exists(self.cntfn):
                try:
                    fd = self.delay_init_open(self.cntfn)
                    try:
                        os.write(fd, "0")
                    finally:
                        os.close(fd)
                    #registerGlobalSessionFile( self.cntfn )
                except OSError as x:
                    if x.errno != errno.EEXIST:
                        raise RepositoryError(
                            self.repo, "OSError on count file create: %s" % x)
            try:
                self.count = max(self.count, self.cnt_read())
            except ValueError, err:
                logger.debug("Startup ValueError Exception: %s" % err)
                logger.error("Corrupt count file '%s'! Trying to recover..." % (self.cntfn))
            except OSError as err:
                logger.debug("Startup OSError Exception: %s" % err)
                raise RepositoryError(self.repo, "OSError on count file '%s' access!" % (self.cntfn))
            self.cnt_write()
            # Setup session file
            try:
                fd = self.delay_init_open(self.fn)
                try:
                    os.write(fd, pickle.dumps(set()))
                finally:
                    os.close(fd)
                registerGlobalSessionFile(self.fn)
            except OSError as err:
                logger.debug("Startup Session Exception: %s" % err)
                raise RepositoryError(self.repo, "Error on session file '%s' creation: %s" % (self.fn, err))

            session_lock_refresher = getGlobalLockRef(self.session_name, self.sdir, self.gfn, self.afs)

            session_lock_refresher.addRepo(self.fn, self.repo)
            self.session_write()
        finally:
            self.global_lock_release()

    def updateNow(self):
        global session_lock_refresher
        session_lock_refresher.updateNow()

    @synchronised
    def shutdown(self):
        """Shutdown the thread and locking system (on ganga shutdown or repo error)"""
        #logger.debug( "Shutting Down SessionLockManager, self.fn = %s" % (self.fn) )
        # print "Shutting Down SessionLock"
        self.locked = set()
        try:
            global session_lock_refresher
            session_lock_refresher.stop()
            if session_lock_refresher is not None:
                # try:
                session_lock_refresher.removeRepo(self.fn, self.repo)
                # except:
                #    pass
                #session_lock_refresher.removeRepo( self.fn, self.repo )
                if session_lock_refresher.numberRepos() <= 1:
                    session_lock_refresher = None
            #logger.debug("Session file '%s' deleted " % (self.fn))
            if not self.afs:
                os.close(self.lockfd)
            os.unlink(self.fn)
            #os.unlink(self.lockfn)
            # os.unlink(self.gfn)
        except OSError as x:
            logger.debug("Session file '%s' or '%s' was deleted already or removal failed: %s" % (self.fn, self.gfn, x))

    @staticmethod
    def delayopen_global(lockfn):
        value = None
        i = 0
        while value is None:
            try:
                value = os.open(lockfn, os.O_RDWR)
            except OSError as x:
                 # print "fail"
                value = None
                i = i + 1
                time.sleep(0.1)
                if i >= 60:  # 3000:
                    raise x
                else:
                    continue
        return value

    # Global lock function
    @synchronised
    def global_lock_setup(self):

        if self.afs:
            self.lockfn = os.path.join(self.sdir, "global_lock")
            lock_path = str(self.lockfn) + '.afs'
            lock_file = os.path.join(lock_path, "lock_file")
            try:
                if not os.path.exists(lock_path):
                    os.makedirs(lock_path)
                if not os.path.isfile(lock_file):
                    with open(lock_file, "w"):
                        pass
            except Exception as err:
                logger.debug("Global Lock Setup Error: %s" % err)
        else:
            try:
                self.lockfn = os.path.join(self.sdir, "global_lock")
                if not os.path.isfile(self.lockfn):
                    with open(self.lockfn, "w"):
                        # create file (does not interfere with existing sessions)
                        pass
                self.lockfd = self.delayopen_global(self.lockfn)
                registerGlobalSessionFile(self.lockfn)
                registerGlobalSessionFileHandler(self.lockfd)
            except IOError as x:
                raise RepositoryError(self.repo, "Could not create lock file '%s': %s" % (self.lockfn, x))
            except OSError as x:
                raise RepositoryError(self.repo, "Could not open lock file '%s': %s" % (self.lockfn, x))

    @staticmethod
    def delay_lock_mod(lockfd, lock_mod):
        i = 0
        num_tries = 35
        while i <= num_tries:
            try:
                fcntl.lockf(lockfd, lock_mod)
            except IOError as x:
                time.sleep(0.1)
                i += 1
                if i == num_tries:  # If we're on the last try
                    raise x
                else:
                    continue
            else:
                break
        return

    def global_lock_acquire(self):
        try:
            if self.afs:

                lock_path = str(self.lockfn) + '.afs'
                lock_file = os.path.join(lock_path, "lock_file")

                def clean_path():
                    oldtime = os.stat(lock_file).st_ctime
                    nowtime = time.time()
                    if abs(int(nowtime) - oldtime) > 10:
                        #logger.debug( "cleaning global lock" )
                        os.system("fs setacl %s %s rlidwka" % (lock_path, getpass.getuser()))

                while True:
                    try:
                        if os.path.isfile(lock_file):
                            clean_path()
                        os.unlink(lock_file)
                        break
                    except Exception as err:
                        logger.debug("Global Lock aquire Exception: %s" % err)
                        time.sleep(0.01)

                os.system("fs setacl %s %s rliwka" % (lock_path, getpass.getuser()))

                while not os.path.isfile(lock_file):
                    with open(lock_file, "w"):
                        pass
                    time.sleep(0.01)

            else:
                self.delay_lock_mod(self.lockfd, fcntl.LOCK_EX)

            #logger.debug("global capture")
        except IOError as x:
            raise RepositoryError(self.repo, "IOError on lock ('%s'): %s" % (self.lockfn, x))

    def global_lock_release(self):
        try:
            if self.afs:
                lock_path = str(self.lockfn) + '.afs'
                os.system("fs setacl %s %s rlidwka" % (lock_path, getpass.getuser()))
            else:
                self.delay_lock_mod(self.lockfd, fcntl.LOCK_UN)

            #logger.debug("global release")
        except IOError as x:
            raise RepositoryError(self.repo, "IOError on unlock ('%s'): %s" % (self.lockfn, x))

    @synchronised
    def delay_session_open(self, filename):
        value = None
        i = 0
        while value is None:
            try:
                value = os.open(filename, os.O_RDONLY)
            except OSError as x:
                time.sleep(0.1)
                i = i + 1
                if i >= 60:
                    raise x
                else:
                    continue
        return value

    # Session read-write functions
    @synchronised
    def session_read(self, fn):
        """ Reads a session file and returns a set of IDs locked by that session.
            The global lock MUST be held for this function to work, although on NFS additional
            locking is done
            Raises RepositoryError if severe access problems occur (corruption otherwise!) """
        try:
            # This can fail (thats OK, file deleted in the meantime)
            fd = self.delay_session_open(fn)
            try:
                os.lseek(fd, 0, 0)
                if not self.afs:  # additional locking for NFS
                    fcntl.lockf(fd, fcntl.LOCK_SH)
                try:
                    # 00)) # read up to 1 MB (that is more than enough...)
                    return pickle.loads(os.read(fd, 1048576))
                except Exception as x:
                    logger.warning("corrupt or inaccessible session file '%s' - ignoring it (Exception %s %s)." % (fn, getName(x), x))
            finally:
                if not self.afs:  # additional locking for NFS
                    fcntl.lockf(fd, fcntl.LOCK_UN)
                os.close(fd)
        except OSError as x:
            if x.errno != errno.ENOENT:
                raise RepositoryError(
                    self.repo, "Error on session file access '%s': %s" % (fn, x))
        return set()

    # This function attempts to grab the ctime from a file which should exist
    # As we don't want this to fail outright it attempts to re-read every 1s
    # for 30sec before failing
    @synchronised
    def delayopen(self, filename):
        value = None
        i = 0
        while value is None:
            try:
                value = os.open(filename, os.O_WRONLY)
            except OSError as x:
                # print "fail"
                value = None
                i = i + 1
                time.sleep(0.1)
                if i >= 60:  # 3000:
                    raise x
                else:
                    continue

        return value

    # This function attempts to grab the ctime from a file which should exist
    # As we don't want this to fail outright it attempts to re-read every 1s
    # for 30sec before failing
    @synchronised
    def delaywrite(self, filename, data):
        value = None
        i = 0
        while value is None:
            try:
                value = os.write(filename, data)
            except OSError as x:
                # print "fail"
                value = None
                i = i + 1
                time.sleep(0.1)
                if i >= 60:  # 3000:
                    raise x
                else:
                    continue

        return value

    @synchronised
    def session_write(self):
        """ Writes the locked set to the session file. 
            The global lock MUST be held for this function to work, although on NFS additional
            locking is done
            Raises RepositoryError if session file is inaccessible """
        #logger.debug("Openining Session File: %s " % self.fn )
        try:
            # If this fails, we want to shutdown the repository (corruption
            # possible)
            fd = self.delayopen(self.fn)
            try:
                if not self.afs:
                    fcntl.lockf(fd, fcntl.LOCK_EX)
                self.delaywrite(fd, pickle.dumps(self.locked))
                if not self.afs:
                    fcntl.lockf(fd, fcntl.LOCK_UN)
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError as x:
            if x.errno != errno.ENOENT:
                raise RepositoryError(
                    self.repo, "Error on session file access '%s': %s" % (self.fn, x))
            else:
                #logger.debug( "File NOT found %s" %self.fn )
                raise RepositoryError(self.repo, "SessionWrite: Own session file not found! Possibly deleted by another ganga session.\n\
                                    Possible reasons could be that this computer has a very high load, or that the system clocks on computers running Ganga are not synchronized.\n\
                                    On computers with very high load and on network filesystems, try to avoid running concurrent ganga sessions for long.\n '%s' : %s" % (self.fn, x))
        except IOError as x:
            raise RepositoryError(
                self.repo, "Error on session file locking '%s': %s" % (self.fn, x))

    # counter read-write functions
    def cnt_read(self):
        """ Tries to read the counter file.
            Raises ValueError (invalid contents)
            Raises OSError (no access/does not exist)
            Raises RepositoryError (fatal)
            """
        try:
            if self.last_count_access is not None:
                last_count_time = self.last_count_access.time
                last_count_val = self.last_count_access.val
                last_time = os.stat(self.cntfn).st_ctime
                if last_time == last_count_time:
                    return last_count_val  # If the file hasn't changed since last check, return the cached value
            _output = None
            fd = os.open(self.cntfn, os.O_RDONLY)
            try:
                if not self.afs:  # additional locking for NFS
                    fcntl.lockf(fd, fcntl.LOCK_SH)
                # 100 bytes should be enough for any ID. Can raise ValueErrorr
                _output = int(os.read(fd, 100).split("\n")[0])
            finally:
                if not self.afs:  # additional locking for NFS
                    fcntl.lockf(fd, fcntl.LOCK_UN)
                os.close(fd)

                if _output != None:
                    self.last_count_access = SessionLockManager.LastCountAccess(os.stat(self.cntfn).st_ctime, _output)
                    return _output

        except OSError as x:
            if x.errno != errno.ENOENT:
                raise RepositoryError(
                    self.repo, "OSError on count file '%s' read: %s" % (self.cntfn, x))
            else:
                # This can be a recoverable error, depending on where it occurs
                raise
        except IOError as x:
            raise RepositoryError(
                self.repo, "Locking error on count file '%s' write: %s" % (self.cntfn, x))

    @synchronised
    def cnt_write(self):
        """ Writes the counter to the counter file. 
            The global lock MUST be held for this function to work correctly
            Raises OSError if count file is inaccessible """
        finished = False
        try:
            # If this fails, we want to shutdown the repository (corruption
            # possible)
            fd = os.open(self.cntfn, os.O_WRONLY)
            try:
                if not self.afs:
                    fcntl.lockf(fd, fcntl.LOCK_EX)
                os.write(fd, str(self.count) + "\n")
                if not self.afs:
                    fcntl.lockf(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
            finished = True
        except OSError as x:
            if x.errno != errno.ENOENT:
                raise RepositoryError(self.repo, "OSError on count file '%s' write: %s" % (self.cntfn, x))
            else:
                raise RepositoryError(self.repo, "Count file '%s' not found! Repository was modified externally!" % (self.cntfn))
        except IOError as x:
            raise RepositoryError(self.repo, "Locking error on count file '%s' write: %s" % (self.cntfn, x))
        finally:
            if finished is True:
                self.last_count_access = SessionLockManager.LastCountAccess(os.stat(self.cntfn).st_ctime, self.count)

    # "User" functions
    @synchronised
    def make_new_ids(self, n):
        """ Locks the next n available ids and returns them as a list 
            Raise RepositoryError on fatal error"""
        self.global_lock_acquire()
        try:
            # Actualize count
            try:
                newcount = self.cnt_read()
            except ValueError:
                logger.warning("Corrupt job counter (possibly due to crash of another session)! Trying to recover...")
                newcount = self.count
            except OSError:
                raise RepositoryError(self.repo, "Job counter deleted! External modification to repository!")
            if not newcount >= self.count:
                #raise RepositoryError(self.repo, "Counter value decreased - logic error!")
                logger.warning("Internal counter increased - probably the count file was deleted.")
                newcount = self.count
            # someone used force_ids (for example old repository imports)
            if self.locked and max(self.locked) >= newcount:
                newcount = max(self.locked) + 1
            ids = range(newcount, newcount + n)
            self.locked.update(ids)
            self.count = newcount + n
            self.cnt_write()
            self.session_write()
            return list(ids)
        finally:
            self.global_lock_release()

    def safe_LockCheck(self):
        global session_lock_last
        this_time = time.time()
        if session_lock_last == 0:
            session_lock_last = this_time
        global session_expiration_timeout
        _diff = abs(session_lock_last - this_time)
        if _diff > session_expiration_timeout * 0.5 or _diff < 1:
            session_expiration_timeout = this_time
            global session_lock_refresher
            if session_lock_refresher is not None:
                session_lock_refresher.checkAndReap()
            else:
                # self.reap_locks()
                pass

    @synchronised
    def lock_ids(self, ids):

        self.safe_LockCheck()

        #logger.debug( "locking: %s" % ids)
        ids = set(ids)
        self.global_lock_acquire()
        try:
            try:
                sessions = [
                    sn for sn in os.listdir(self.sdir) if sn.endswith(self.name + ".locks")]
            except OSError as x:
                raise RepositoryError(
                    self.repo, "Could not list session directory '%s'!" % (self.sdir))

            slocked = set()
            for session in sessions:
                sf = os.path.join(self.sdir, session)
                if sf == self.fn:
                    continue
                slocked.update(self.session_read(sf))
            #logger.debug( "locked: %s" % slocked)
            ids.difference_update(slocked)
            self.locked.update(ids)
            #logger.debug( "stored_lock: %s" % self.locked)
            self.session_write()
            #logger.debug( "list: %s" % list(ids))
            return list(ids)
        finally:
            self.global_lock_release()

    @synchronised
    def release_ids(self, ids):
        #logger.debug( "releasing : %s" % ids)
        self.global_lock_acquire()
        try:
            self.locked.difference_update(ids)
            self.session_write()
            #logger.debug( "list: %s" % list(ids))
            return list(ids)
        finally:
            self.global_lock_release()

    @synchronised
    def check(self):
        self.global_lock_acquire()
        try:
            with open(self.cntfn) as f:
                newcount = int(f.readline())
            assert newcount >= self.count
            sessions = os.listdir(self.sdir)
            prevnames = set()
            for session in sessions:
                if not session.endswith(self.name + ".locks"):
                    continue
                try:
                    sf = os.path.join(self.sdir, session)
                    fd = -1
                    if not self.afs:
                        fd = os.open(sf, os.O_RDONLY)
                        fcntl.lockf(fd, fcntl.LOCK_SH)  # ONLY NFS
                    with open(sf) as sf_file:
                        names = pickle.load(sf_file)
                    if not self.afs and fd > 0:
                        fcntl.lockf(fd, fcntl.LOCK_UN)  # ONLY NFS
                        os.close(fd)
                except Exception as x:
                    logger.warning("CHECKER: session file %s corrupted: %s %s" % (session, getName(x), x))
                    continue
                if not len(names & prevnames) == 0:
                    logger.error("Double-locked stuff: " + names & prevnames)
                    assert False
                # prevnames.union_update(names) Should be alias to update but
                # not in some versions of python
                prevnames.update(names)

        finally:
            self.global_lock_release()

    @synchronised
    def get_lock_session(self, id):
        """get_lock_session(id)
        Tries to determine the session that holds the lock on id for information purposes, and return an informative string.
        Returns None on failure
        """
        self.safe_LockCheck()

        self.global_lock_acquire()
        try:
            sessions = [s for s in os.listdir(self.sdir) if s.endswith(self.name + ".locks")]
            for session in sessions:
                try:
                    sf = os.path.join(self.sdir, session)
                    fd = -1
                    if not self.afs:
                        fd = os.open(sf, os.O_RDONLY)
                        fcntl.lockf(fd, fcntl.LOCK_SH)  # ONLY NFS
                    with open(sf) as sf_file:
                        names = pickle.load(sf_file)
                    if not self.afs and fd > 0:
                        fcntl.lockf(fd, fcntl.LOCK_UN)  # ONLY NFS
                        os.close(fd)
                    if id in names:
                        return self.session_to_info(session)
                except Exception as err:
                    logger.debug("Get Lock Session Exception: %s" % err)
                    continue
        finally:
            self.global_lock_release()

    @synchronised
    def get_other_sessions(self):
        """get_session_list()
        Tries to determine the other sessions that are active and returns an informative string for each of them.
        """
        self.global_lock_acquire()
        try:
            sessions = [s for s in os.listdir(self.sdir) if s.endswith(
                ".session") and not os.path.join(self.sdir, s) == self.gfn]
            return [self.session_to_info(session) for session in sessions]
        finally:
            self.global_lock_release()

    @synchronised
    def reap_locks(self):
        """reap_locks() --> True/False
        Remotely clear all foreign locks from the session.
        WARNING: This is not nice.
        Returns True on success, False on error."""
        failed = False
        self.global_lock_acquire()
        try:
            #sessions = [s for s in os.listdir(self.sdir) if s.endswith(".session") and not os.path.join(self.sdir, s) == self.gfn]
            sessions = [s for s in os.listdir(self.sdir) if s.endswith(
                ".session") and int(str(s).split('.')[-2]) != int(os.getpid())]
            metadata_lock_files = [
                s for s in os.listdir(self.sdir) if s.endswith("metadata.locks")]
            all_lock_files = [
                s for s in os.listdir(self.sdir) if s.endswith(".locks")]
            lock_files = [
                s for s in all_lock_files if s not in metadata_lock_files]
            locks = [s for s in lock_files if int(
                str(s).split('.')[-4]) != int(os.getpid())]
            metadata_locks = [s for s in metadata_lock_files if int(
                str(s).split('.')[-5]) != int(os.getpid())]

            sessions.extend(locks)
            sessions.extend(metadata_locks)

#            for s in os.listdir(self.sdir):
#                if s.endswith(".session"):
#                    logger.debug( "PID: %s " % int(str(s).split('.')[-2]) )
#                    logger.debug( "OS PID: %s " % int(os.getpid()) )

            for session in sessions:
                try:
                    sf = os.path.join(self.sdir, session)
                    #logger.debug( "Modified %s" % os.stat(sf).st_ctime )
                    #logger.debug( "Time %s" % time.time() )
                    #logger.debug( "Diff_allowed %s" % session_expiration_timeout )
                    #logger.debug( "Diff %s" % (time.time() - os.stat(sf).st_ctime ) )
                    global session_expiration_timeout
                    if((time.time() - os.stat(sf).st_ctime) > session_expiration_timeout):
                        if(sf.endswith(".session")):
                            logger.debug("Reaping LockFile: %s" % (sf))
                        os.unlink(sf)
                except OSError as x:
                    failed = True
            return not failed
        finally:
            self.global_lock_release()

    def session_to_info(self, session):
        si = session.split(".")
        try:
            return "%s (pid %s) since %s" % (".".join(si[:-3]), si[-2], ".".join(si[-5:-3]))
        except Exception, err:
            logger.debug( "Session Info Exception: %s" % err)
            return session
