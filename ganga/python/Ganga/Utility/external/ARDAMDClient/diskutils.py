################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: diskutils.py,v 1.1 2008-07-17 16:41:02 moscicki Exp $
################################################################################
import sys
import os
import types
import time
import errno
import atexit
import random


##if time.time() < time.time():
##    timestampMethod = time.time
##elif time.clock() < time.clock():
##    timestampMethod = time.clock
##else:
##    raise OSError("Can not find an appropriate timestamp function in the time module")


#---------------------------------------------------------------------------
def timestampMethod():
    timestamp = '%.7f' % time.time()
    for p in range(3):
        timestamp += str(random.randint(0,9))
    return timestamp
          

def newGuid(value = None):
    """newGUID(value = None) --> guid value - any python object"""
    tt = time.gmtime()[0:6] + (random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),                               
                               id(value))
    return '_' + (''.join(map(str, tt))).replace('-','')


def createLock(lockfile, guid, tries_limit = 200):
    tries = 0
    while True:
        tries = tries +1
        try:
            guid += '\0' #mark the end of string
            fd = os.open(lockfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            try:
                guidlen = os.write(fd, guid)
            finally:
                os.close(fd)
            fd = os.open(lockfile, os.O_RDONLY)
            try:
                f_guid = os.read(fd, guidlen)
            finally:
                os.close(fd)
            if f_guid != guid:
                raise OSError("Guids mismatch in the lock file")
        except OSError, e:
            if tries > tries_limit:
                return False
            time.sleep(0.05)
        else:
            return True


def removeLock(lockfile):
    try:
        os.remove(lockfile)
    except OSError, e:
        if e[0] != errno.ENOENT:
            return False
    return True

    

class RLock(object):
    def __init__(self, lockfile = 'Lock', tries_limit = 200):
        self.lockfile = lockfile
        self.guid = newGuid(self)
        self.counter = 0
        self.tries_limit = tries_limit
        atexit.register(self.remove)

    def acquire(self):
        if self.counter == 0:
            if not createLock(self.lockfile, self.guid, self.tries_limit):
                return False
        self.counter += 1
        return True
    
    def release(self):
        if self.counter > 0:
            if self.counter == 1:
                if not removeLock(self.lockfile):
                    return
            self.counter -= 1

    def remove(self):
        if self.counter == 0:
            try:
                f = file(self.lockfile, 'r')
                try:
                    f_guid = f.read()
                finally:
                    f.close()
            except:
                return
            if f_guid.endswith('\0'): #lock file was correctly written
                if f_guid.rstrip('\0') != self.guid: # by another RLock
                    return
        removeLock(self.lockfile)
        self.counter = 0
          

class Filterer(object):
    def __init__(self, fn, tmp = False):
        self.fn = fn
        self.tmp = tmp
    def __call__(self, fn):
        ff = fn.split('_')
        fflen = len(ff)
        if fflen > 1:
            if ff[0] == self.fn:
                if self.tmp:
                    if fflen == 3:
                        if ff[2] == '':
                            return True
                else:
                    if fflen == 2:
                        if ff[1] != '':
                            return True
        return False
                    

def listFiles(dirname):
    files = []
    for e in os.listdir(dirname):
        if os.path.isfile(os.path.join(dirname, e)):
            files.append(e)
    return files


def getTimeStamp(fn):
    # fn -- precise
    fn = os.path.basename(fn)
    ff = fn.split('_')
    if len(ff) > 1:
        return ff[1].split('.')
    return (0,0)


def sorter(x, y):
    x, y = map(getTimeStamp, (x,y))
    if x[0] < y[0]:
        return -1
    elif x[0] == y[0]:
        if x[1] < y[1]:
            return -1
        elif x[1] == y[1]:
            return 0
        elif x[1] > y[1]:
            return 1
    elif x[0] > y[0]:
        return 1


def getHistory(fn, tmp = False):
    # fn -- generic
    dirname, basename = os.path.split(os.path.abspath(fn))
    ffs = filter(Filterer(basename, tmp), listFiles(dirname))
    ffs.sort(sorter)
    return (dirname, ffs)


def getLast(fn):
    # fn -- generic
    dirname, ffs = getHistory(fn)
    ffs.reverse()
    for hf in ffs:  
        hfn = os.path.join(dirname, hf)
        tmp_fn = hfn + '_'
        if not os.path.isfile(tmp_fn):
            break
    else:
        raise OSError("Can not find file %s" % fn)
    return hfn


def __fillEntry(entries, lentr, i, f):
    # helper function to force garbage collection of temporal lists
    line = f.readline()
    line = line.rstrip('\n')
    if not line:
        return False
    fields = line.split('\0')
    if i < lentr:
        entries[i] = fields
    else:
        entries.append(fields)
    return True

    
def read(fn, entries = None):
    # fn -- precise
    # if entries is not none they will be filled first
    # this is a way to reduce memory leak in Python
    # function returns list of entries and number of read lines,
    # which can be less than length of the entries list
    if entries == None:
        entries = []
    lentr = len(entries)
    i = 0
    f = open(fn, 'r')
    try:
        while __fillEntry(entries, lentr, i, f):
            i += 1
        return (entries, i)
    finally:
        f.close()
        

def readLast(fn, entries = None):
    # fn -- generic
    return read(getLast(fn), entries)[0]

    
def move(tmp_fn, fn, tries_limit = 200):
    # tmp_fn, fn -- precise
    if os.path.isfile(fn):
        raise OSError("File exists %s" % fn) 
    os.rename(tmp_fn, fn)
    # check that file was moved
    tries = 0
    while True:
        tries = tries +1
        if os.path.isfile(tmp_fn) or not os.path.isfile(fn):
            if tries > tries_limit:
                raise OSError("Can not rename file %s" % tmp_fn)
            time.sleep(0.05)
        else:
            return


def remove(fn, tries_limit = 200):
    # fn -- generic
    dirname, ffs = getHistory(fn, tmp = False)
    for hf in ffs:  
        fn = os.path.join(dirname, hf)
        os.remove(fn)
        # check that file was removed
        tries = 0
        while True:
            tries = tries +1
            if os.path.isfile(fn):
                if tries > tries_limit:
                    raise OSError("Can not remove file %s" % fn)
                time.sleep(0.05)
            else:
                return    
    

def write(entries, fn, lines_to_write = -1, tries_limit = 200):
    # fn -- generic
    dirname, ffs = getHistory(fn, tmp = False)
    dirname, hfs = getHistory(fn, tmp = True)
    fn = '_'.join([fn, timestampMethod()])
    tmp_fn = fn + '_'
    f = open(tmp_fn, 'w')
    try:
        for entry in entries:
            if lines_to_write == 0:
                break
            lines_to_write -= 1
            if not len(entry):
                continue
            line=''
            for field in entry:
                if len(line):
                    line += '\0'
                line += str(field)
            f.write(line + '\n')
    finally:
        f.close()
    move(tmp_fn, fn, tries_limit)
    for fls in [ffs, hfs]:
        for hf in fls:
            hf = os.path.join(dirname, hf)
            try:
                os.remove(hf)
            except OSError:
                pass
    return fn
    
