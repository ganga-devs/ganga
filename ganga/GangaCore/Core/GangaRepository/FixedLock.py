# This class (SessionLockManager) encapsulates fcntl-based locking that works on NFS, AFS and locally.
# You can use
# python SessionLock.py {1|2}
# to run locking tests (run several instances in the same directory, from
# different machines)



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
from pipes import quote

import pickle as pickle

from GangaCore.Utility.logging import getLogger

from GangaCore.Utility.Config.Config import getConfig, ConfigError
from GangaCore.GPIDev.Base.Proxy import getName

from GangaCore.Core.GangaThread import GangaThread
from GangaCore.Core.GangaRepository import RepositoryError

logger = getLogger()

def lock_synch(f):
    """  
        This decorator must be attached to a method on a ``Node`` subclass
        It uses the object's lock to make sure that the object is held for the duration of the decorated function
        Args:
        f (function): This is the function which we want to wrap
    """  
    @functools.wraps(f)
    def decorated_lock(self, *args, **kwargs):
        with self.sync_lock:
            return f(self, *args, **kwargs)
    return decorated_lock

class FixedLockManager(object):

    """ Class with thread that keeps a single fixed lockfile which is removed on exit.
    This explicitly forbids multiple parallel Ganga sessions.
    """

    __slots__ = ('sync_lock', 'locked', 'repo', 'cntfn', 'global_lock', 'count')

    def __init__(self, repo, root, name, minimum_count=0):


        # Used for self runtime thread safety
        self.sync_lock = threading.RLock()

        # Required to pretend there are some locked jobs from other sessions
        self.locked = set()

        self.repo = repo
        self.mkdir(root)
        realpath = os.path.realpath(root)

        # Location of the count file for this repo
        self.cntfn = os.path.join(realpath, name, "cnt")

        # Required for lock files
        sessions_folder = os.path.join(realpath, "sessions")
        if not os.path.exists(sessions_folder):
            os.mkdir(sessions_folder) 

        # Location of fixed lock for this repo
        self.global_lock = os.path.join(sessions_folder, '%s_fixed_lock' % name)
        self.count = minimum_count
        if os.path.exists(self.global_lock):
            msg = "\n\nCannot start this registry: %s due to a lock file already existing: '%s'\n" % (name, self.global_lock)
            msg += "If you are trying to run only 1 ganga session please remove this file and re-launch ganga\n"
            raise RepositoryError(repo, msg)

        with open(self.global_lock, 'w'):
            pass

    @lock_synch
    def mkdir(self, dn):                                                                                                                                                                                           
        """Make sure the given directory exists"""
        try:
            if not os.path.exists(dn):
                os.makedirs(dn)
        except OSError as x:
            if x.errno != errno.EEXIST:
                raise RepositoryError(self.repo, "OSError on directory create: %s" % x)

    def startup(self):
        pass

    def finish_startup(self):
        pass

    def updateNow(self):
        pass

    def shutdown(self):
        """Shutdown the thread and locking system (on ganga shutdown or repo error)"""
        os.unlink(self.global_lock)
    
    def session_read(self, fn):
        return set()

    def session_write(self):
        pass

    @lock_synch
    def cnt_read(self):
        """Reads the counter file.
        """
        if not os.path.exists(self.cntfn):
            self.cnt_write()
        with open(self.cntfn) as fd:
            # 100 bytes should be enough for any ID. Can raise ValueErrorr
            _output = int(fd.readline())
        self.count = max(self.count, _output)
        return _output

    @lock_synch
    def cnt_write(self):
        """ Writes the counter to the counter file.
        """
        with open(self.cntfn, 'w') as fd:
            fd.write(str(self.count))

    @lock_synch
    def make_new_ids(self, n):
        """This bumps the registry count by n ids"""
        newcount = self.cnt_read()
        ids = list(range(newcount, newcount + n))
        self.count = ids[-1]+1
        self.cnt_write()
        return list(ids)

    def lock_ids(self, ids):
        """Pretend to lock IDs"""
        return list(ids)

    def release_ids(self, ids):
        """Pretend to release IDs"""
        return list(ids)

    def check(self):
        """ No need to check as we're disk/runtime consistent by construction. i.e. no parallel sessions """
        pass

    def get_lock_session(self, id):
        """Return a dummy str of self as only we can own a session
        """
        return "self"

    def get_other_sessions(self):
        """Return an empty list as No other sessions by definition
        """
        return []

    def reap_locks(self):
        """ Return True to respect checks on SessionLock"""
        return True

