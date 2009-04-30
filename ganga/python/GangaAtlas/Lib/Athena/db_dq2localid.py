#!/usr/bin/env python

import os
from dq2.clientapi.DQ2 import DQ2
from dq2.info import TiersOfATLAS
from dq2.info.TiersOfATLAS import _refreshToACache, ToACache

_refreshToACache()

filename = 'dq2localid.txt'

try:
    open(filename)
    fsize = os.stat(filename).st_size
    dq2localid = [ line.strip() for line in file(filename) ]
    if dq2localid:
        dq2localid = dq2localid[0]
except IOError:
    dq2localid = os.environ['DQ2_LOCAL_SITE_ID']

dq2localid_alternatename = TiersOfATLAS.getSiteProperty(dq2localid,'alternateName')

db_dataset = os.environ['ATLAS_DBRELEASE']

dq2=DQ2()
db_locations = dq2.listDatasetReplicas(db_dataset).values()[0][1]

db_site = dq2localid
for sitename in TiersOfATLAS.getAllSources():
    if TiersOfATLAS.getSiteProperty(sitename,'alternateName')==dq2localid_alternatename and sitename in db_locations:    
        db_site = sitename

outFile = open('db_dq2localid.txt','w')
outFile.write('%s\n' % db_site)
outFile.close()
