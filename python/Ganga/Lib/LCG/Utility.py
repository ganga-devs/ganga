#!/usr/bin/env python
import re, md5

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

def get_md5sum(fname):
    ''' Calculates the MD5 checksum of a file '''

    f = open(fname, 'rb')
    m = md5.new()
    while True:
        d = f.read(8096)
        if not d:
            break
        m.update(d)
    f.close()
    return m.hexdigest()
