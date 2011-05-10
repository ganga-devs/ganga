#!/usr/bin/env python
import re
import time
import random
import gzip
from Ganga.Utility.logging import getLogger
from Ganga.Lib.LCG.ElapsedTimeProfiler import ElapsedTimeProfiler
from Ganga.Lib.LCG.Compatibility import *

def get_uuid(*args):
    ''' Generates a universally unique ID. '''
    t = long( time.time() * 1000 )
    r = long( random.random()*100000000000000000L )
    try:
        a = socket.gethostbyname( socket.gethostname() )
    except:
        # if we can't get a network address, just imagine one
        a = random.random()*100000000000000000L
    data = str(t)+' '+str(r)+' '+str(a)+' '+str(args)

    md5_obj = get_md5_obj()
    md5_obj.update( data )
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

def readStrippedLines(fileName):
    '''reads in a list of strings from a file'''

    lines = []
    f = open(fileName, 'r')
    for l in f.readlines():
        lines.append(l.strip())
    f.close()
    return lines

def filter_string_list(allList, filterList, type=0):
    '''picks a list of strings from allList (mis-)matching the elementes in the filterList
         - type = 0 : including lists given by filterLists
         - type = 1 : excluding lists given by filterLists
    '''

    matchedDict = {}
    allDict     = {}

    for item in allList:
        allDict[item] = True

    if type == 1:
        matchedDict = allDict

    for filter in filterList:
        if filter.find('*') > 0:
            wc = ".*".join(filter.split('*'))
            for item in allDict.keys():
                if re.match(wc, item) != None:
                    if type == 0: matchedDict[item] = True
                    if type == 1: del matchedDict[item]
        else:
            if allDict.has_key(filter):
                if type == 0: matchedDict[filter] = True
                if type == 1: del matchedDict[filter]

    return matchedDict.keys()

def get_md5sum(fname, ignoreGzipTimestamp=False):
    ''' Calculates the MD5 checksum of a file '''

    profiler = ElapsedTimeProfiler(getLogger(name='Profile.LCG'))
    profiler.start()


    ## if the file is a zipped format (determined by extension),
    ## try to get checksum from it's content. The reason is that
    ## gzip file contains a timestamp in the header, which causes
    ## different md5sum value even the contents are the same.
    #re_gzipfile = re.compile('.*[\.tgz|\.gz].*$')

    f = None

    if ignoreGzipTimestamp and (fname.find('.tgz') > 0 or fname.find('.gz') > 0):
        f = gzip.open(fname,'rb')
    else:
        f = open(fname, 'rb')

    m = get_md5_obj()

    while True:
        d = f.read(8096)
        if not d:
            break
        m.update(d)
    f.close()

    md5sum = m.hexdigest()

    profiler.check('md5sum calculation time')

    return md5sum
