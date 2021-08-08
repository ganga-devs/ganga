#! /usr/bin/env python

import os, sys, popen2, signal, re

from getopt import getopt,GetoptError
from threading import Thread
from commands import getstatusoutput

from lfc import *

def f(signum, frame):
    print "lcg-gt timeout!"

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
    res = lfc_listreplica('',guid,CNS_LIST_BEGIN,listp)
    while res:
        if res.host in closeSEs:
            replicas.insert(0,res.sfn)
        else:
            replicas.append(res.sfn)
        res = lfc_listreplica('',guid,CNS_LIST_CONTINUE,listp)

    lfc_listreplica(lfn,'',CNS_LIST_END,listp)

    return replicas

########################################################################
# make job option file
def _makeJobO(files,tag):
    # sort
    lfns = files.keys()
    lfns.sort()
    # open jobO
    joName = 'input.py'
    outFile = open(joName,'w')
    if tag:
        outFile.write('CollInput = [')
    else:
        try:
            if 'ATHENA_MAX_EVENTS' in os.environ:
                evtmax = int(os.environ['ATHENA_MAX_EVENTS'])
            else:
                evtmax = -1
        except:
            evtmax = -1
        outFile.write('theApp.EvtMax = %d\n' %evtmax)

        if 'ATHENA_SKIP_EVENTS' in os.environ:
            skipevt = int(os.environ['ATHENA_SKIP_EVENTS'])
            outFile.write('ServiceMgr.EventSelector.SkipEvents = %d\n' %skipevt)

        outFile.write('EventSelector.InputCollections = [')
    # loop over all files
    for lfn in lfns:
        filename = files[lfn]['pfn']
        if tag:
            filename = re.sub('\.root\.\d+$','',filename)
            filename = re.sub('\.root$','',filename)
        # write PFN
        outFile.write('"%s",' % filename)
    outFile.write(']\n')

    ## setting for event picking
    if 'ATHENA_RUN_EVENTS' in os.environ:
        revt = eval(os.environ['ATHENA_RUN_EVENTS'])
        run_evt = []
        for i in range(len(revt)):
            run_evt.append((revt[i][0], revt[i][1]))
        
        outFile.write('\n#EventPicking\n')
        outFile.write('from AthenaCommon.AlgSequence import AthSequencer\n')
        outFile.write("seq = AthSequencer('AthFilterSeq')\n")
        outFile.write('from GaudiSequencer.PyComps import PyEvtFilter\n')
        outFile.write("seq += PyEvtFilter('alg', evt_info='',)\n")
        outFile.write('seq.alg.evt_list = %s\n' % run_evt)
        outFile.write("seq.alg.filter_policy = '%s'\n"  % os.environ['ATHENA_FILTER_POLICY'])
        outFile.write('for tmpStream in theApp._streams.getAllChildren():\n')
        outFile.write('\t fullName = tmpStream.getFullName()\n')
        outFile.write("\t if fullName.split('/')[0] == 'AthenaOutputStream':\n")
        outFile.write("\t\t tmpStream.AcceptAlgs = [seq.alg.name()]\n")
   
    # close
    outFile.close()
    
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
                #cmd = 'lcg-cp --vo atlas -t %d %s file:%s' % (timeout,rep,self.file)
                cmd = 'lcg-gt -t %d %s dcap' % (timeout,rep)
                print cmd
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

    try:
        ganga_lfc_host = os.environ['GANGA_LFC_HOST']
        os.environ['LFC_HOST'] = ganga_lfc_host
    except:
        raise LookupError("ERROR: GANGA_LFC_HOST not defined")
        pass



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
    files = {}

    for lfn in lfns:

        if verbose: print 'LFN: %s' % lfn
        guid = get_guid(lfn)
        if not guid:
            print 'ERROR: LFN %s not found.' % lfn 
            continue
        if verbose: print 'GUID: %s' % guid

        name = os.path.basename(lfn)
        pfn = os.path.join(directory,name)

        #pfc.addFile(guid,name,pfn)

        replicas = get_replicas(lfn)
        if not replicas:
            print 'ERROR: No replica found for LFN %s' % lfn
            continue

        if verbose:
            print 'Replicas :\n   %s' % '\n   '.join(replicas)   

        for rep in replicas:
            if  verbose: print 'INFO LFN: %s Replica: %s' % (lfn,rep)
            cmd = 'lcg-gt -t %d %s dcap' % (timeout,rep)
            try:
                signal.signal(signal.SIGALRM, f)
                signal.alarm(90)
                child = popen2.Popen3(cmd,1)
                child.tochild.close()
                out=child.fromchild
                err=child.childerr
                line=out.readline()
                if line:
                    pat = re.compile(r'://[^/]+')
                    if re.findall(pat,line):
                        pfn = line.strip()
                    
            except IOError:
                pass
            signal.alarm(0)

            pfc.addFile(guid,name,pfn)
            item = {'pfn':pfn,'guid':guid}
            files[name] = item



    pfc.close()

    _makeJobO(files,False)

    try: lfc_stopsess()
    except NameError: pass


