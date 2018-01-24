#! /usr/bin/env python

import os, sys

from getopt import getopt,GetoptError
from threading import Thread
from commands import getstatusoutput

from lfc import *

def usage():

    print 'Name:'
    print '    ganga-stagein.py'
    print
    print 'Arguments:'
    print '    logical names'
    print 
    print 'Options:'
    print '    -h, --help            this prinout'
    print '    -i, --input file      list of logical names'
    print '    -d, --directory path  to stage the input files (default $PWD)'
    print '    -t, --timeout seconds for the staging in (default 900)'
    print '    -r, --retry number    for the staging command (default 3)'
    print '    -v, --verbose         verbosity'

def get_guid(lfn):
    '''Get guid for a lfn
    '''

    statg = lfc_filestatg()
    rc = lfc_statg(lfn,'',statg)
    if not rc: return statg.guid

def get_replicas(lfn):
    '''List replicas and sort the one on close SE first
    '''

    replicas = []

    listp = lfc_list()
    res = lfc_listreplica(lfn,'',CNS_LIST_BEGIN,listp)
    while res:
        if res.host in closeSEs:
            replicas.insert(0,res.sfn)
        else:
            replicas.append(res.sfn)
        res = lfc_listreplica(lfn,'',CNS_LIST_CONTINUE,listp)

    lfc_listreplica(lfn,'',CNS_LIST_END,listp)

    return replicas
    
class PoolFileCatalog:
    '''Helper class to create PoolFileCatalog.xml
    '''
    def __init__(self,name='PoolFileCatalog.xml'):

        self.pfc = open(name,'w')
        print >>self.pfc,'<?xml version="1.0" ?>'
        print >>self.pfc,'<POOLFILECATALOG>'

    def addFile(self,guid,lfn,pfn):

        print >>self.pfc,'    <File ID="%s">' % guid
        print >>self.pfc,'        <logical>'
        print >>self.pfc,'            <lfn name="%s"/>' % lfn
        print >>self.pfc,'        </logical>'
        print >>self.pfc,'        <physical>'
        print >>self.pfc,'            <pfn filetype="ROOT_All" name="%s"/>' % pfn
        print >>self.pfc,'        </physical>'
        print >>self.pfc,'    </File>'

    def close(self):

        print >>self.pfc,'</POOLFILECATALOG>'

class StageIn(Thread):

    def __init__(self,lfn,replicas,file):

        Thread.__init__(self)
        self.lfn = lfn
        self.replicas = replicas
        self.file = file

    def run(self):

        for rep in self.replicas:
            for r in xrange(0,retry):
                if  verbose: print 'INFO LFN: %s Replica: %s Retry: %d' % (lfn,rep,r)
                cmd = 'lcg-cp --vo atlas -t %d %s file:%s' % (timeout,rep,self.file)
                rc, out = getstatusoutput(cmd)
                if not rc: return

                print 'Return code %d from %s' % (rc,cmd)
                print out



if __name__ == '__main__':

    directory = os.getcwd()
    retry = 2
    timeout = 900
    input = None
    verbose = False

    try:
        opts, args = getopt(sys.argv[1:],'ht:d:r:i:v',['help','directory=','input=','timeout=','retry=','verbose'])
    except GetoptError:
        usage()
        sys.exit(1)

    for opt, val in opts:

        if opt in ['-h','--help']:
            usage()
            sys.exit()

        if opt in ['-d','--directory']:
            direcory = val 

        if opt in ['-i','--input']:
            input = val

        if opt in ['-t','--timeout']:
            timeout = int(val)

        if opt in ['-r','--retry']:
            retry = int(val)

        if opt in ['-v','--verbose']:
            verbose = True

    if input:
        lfns = [ line.strip() for line in file(input) ]
    else:
        lfns = args

    if not len(lfns):
        print 'No files requested.'
        sys.exit()


#   determine the closeSEs

    rc, output = getstatusoutput('edg-brokerinfo getCloseSEs')
    if rc:
        print 'ERROR: Could not determine close SEs'
        closeSEs = []
    else:
        closeSEs = output.split()
        print 'INFO: Close SEs are ' + ', '.join(closeSEs)

    pfc = PoolFileCatalog()
    workers=[]
    try: lfc_startsess('','')
    except NameError: pass

    for lfn in lfns:

        if verbose: print 'LFN: %s' % lfn
        guid = get_guid(lfn)
        if not guid:
            print 'ERROR: LFN %s not found.' % lfn 
            continue
        if verbose: print 'GUID: %s' % guid

        name = os.path.basename(lfn)
        pfn = os.path.join(directory,name)

        pfc.addFile(guid,name,pfn)

        replicas = get_replicas(lfn)
        if not replicas:
            print 'ERROR: No replica found for LFN %s' % lfn
            continue

        if verbose:
            print 'Replicas :\n   %s' % '\n   '.join(replicas)   

        s = StageIn(lfn,replicas,pfn)
        s.start()
        workers.append(s)

    pfc.close()

    try: lfc_stopsess()
    except NameError: pass

    for s in workers:
        s.join()
