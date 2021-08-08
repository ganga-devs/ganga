#!/usr/bin/env python

import pickle, os, sys
from dq2.info.TiersOfATLAS import _refreshToACache, ToACache
from getopt import getopt,GetoptError

_refreshToACache

if __name__ == '__main__':

    type = False
    protocol = False

    try:
        opts, args = getopt(sys.argv[1:],':tp', ['type','protocol'])
    except GetoptError:
        print 'access_info.py -t or -p'
        sys.exit(410100)

    for opt, val in opts:
        if opt in ['-t', '--type']:
            type = True
        if opt in ['-p', '--protocol']:
            protocol = True

    fileName = 'access_info.pickle'
    f = open(fileName,"r")
    access_info = pickle.load(f)
    f.close()

    accessType = 'DQ2_LOCAL'
    accessProtocol = ''

    if 'DQ2_LOCAL_SITE_ID' in os.environ:
        dq2localsiteid = os.environ['DQ2_LOCAL_SITE_ID']
        site_info = ToACache.sites[dq2localsiteid]
        alternateName = site_info['alternateName'][-1].upper()
        try:
            accessType = access_info[alternateName][0]
        except:
            pass
        try:
            accessProtocol = access_info[alternateName][1]
        except:
            pass

    if type:
        print accessType
    elif protocol:
        print accessProtocol
