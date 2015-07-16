#!/usr/bin/env python
import re
import time
import random
import gzip
from Ganga.Utility.logging import getLogger
from Ganga.Lib.LCG.ElapsedTimeProfiler import ElapsedTimeProfiler
import hashlib
import socket


def get_uuid(*args):
    ''' Generates a universally unique ID. '''
    t = time.time() * 1000
    r = random.random() * 100000000000000000
    try:
        a = socket.gethostbyname(socket.gethostname())
    except:
        # if we can't get a network address, just imagine one
        a = random.random() * 100000000000000000
    data = str(t) + ' ' + str(r) + ' ' + str(a) + ' ' + str(args)

    md5_obj = hashlib.md5()
    md5_obj.update(data)
    data = md5_obj.hexdigest()

    return data


def urisplit(uri):
    """
    Basic URI Parser according to STD66 aka RFC3986

    >>> urisplit("scheme://authority/path?query#fragment")
    ('scheme', 'authority', 'path', 'query', 'fragment') 

    """
    # regex straight from STD 66 section B
    regex = '^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?'
    p = re.match(regex, uri).groups()
    scheme, authority, path, query, fragment = p[1], p[3], p[4], p[6], p[8]
    #if not path: path = None
    return (scheme, authority, path, query, fragment)


def get_md5sum(fname, ignoreGzipTimestamp=False):
    ''' Calculates the MD5 checksum of a file '''

    profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
    profiler.start()

    # if the file is a zipped format (determined by extension),
    # try to get checksum from it's content. The reason is that
    # gzip file contains a timestamp in the header, which causes
    # different md5sum value even the contents are the same.
    #re_gzipfile = re.compile('.*[\.tgz|\.gz].*$')

    f = None

    if ignoreGzipTimestamp and (fname.find('.tgz') > 0 or fname.find('.gz') > 0):
        f = gzip.open(fname, 'rb')
    else:
        f = open(fname, 'rb')

    m = hashlib.md5()

    while True:
        d = f.read(8096)
        if not d:
            break
        m.update(d)
    f.close()

    md5sum = m.hexdigest()

    profiler.check('md5sum calculation time')

    return md5sum
