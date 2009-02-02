#!/usr/bin/env python
#-----------------------------------------------------
# fs-copy.py is an wrapper script around the file copy
# command to provide more advanced features:
#
# 1. failure recovery by retring the copy command
# 2. quality check on the downloaded files by comparing the checksum
#-----------------------------------------------------

import os
import os.path
#import shutil
#import tempfile
import md5
import zlib
import sys
#import popen2
import time
#import traceback
import pickle
import re
import getopt

#subprocess.py crashes if python 2.5 is used
#try to import subprocess from local python installation before an
#import from PYTHON_DIR is attempted some time later
try:
    import subprocess
except ImportError:
    pass

## Utility functions ##
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

def get_adler32sum(fname):
    ''' Calculate the Adler32 checksum of a file '''

    f = open(fname,'rb')
    data = f.read()
    cksum = hex( zlib.adler32(data) & 0xffffffff )
    f.close()

    # remove the tailing 'L' charactor
    cksum = re.sub(r'L$','',cksum)

    return cksum

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

def timeString():
    return time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time()))

def printInfo(s, outfile):
    outfile.write(timeString() + '  [Info]' +  ' ' + str(s) + os.linesep)
    outfile.flush()

def printError(s, outfile):
    outfile.write(timeString() + ' [Error]' +  ' ' + str(s) + os.linesep)
    outfile.flush()

## system command executor with subprocess
def execSyscmdSubprocess(cmd, outfile, errfile, wdir=os.getcwd()):

    import os, subprocess

    exitcode = None

    try:
        child = subprocess.Popen(cmd, cwd=wdir, shell=True, stdout=outfile, stderr=errfile)

        while 1:
            exitcode = child.poll()
            if exitcode is not None:
                break
            else:
                outfile.flush()
                errfile.flush()
                time.sleep(0.3)
    finally:
        pass

    outfile.flush()
    errfile.flush()

    printInfo('subprocess exit code: %d' % status, outfile)

    if exitcode != 0:
        return False
    else:
        return True

# Main program
if __name__ == '__main__':
    
    ## open files for command output and error
    outfile = open('FileStager.out','a')
    errfile = open('FileStager.err','a')

    ## default value specification
    supported_protocols = ['lcgcp']

    protocol  = 'lcgcp'
    timeout   = 1200
    vo        = 'atlas'
    max_trial = 3

    src_surl  = None
    dest_surl = None

    ## internal subroutine definition
    def make_copycmd(protocol, vo, timeout, src_surl, dest_surl):
        '''
        routine for composing copy command according to the requested protocol.
        '''

        cmd = ''

        if protocol in [ 'lcgcp' ]:
            cmd = 'lcg-cp -v --vo %s -t %d %s %s' % (vo, timeout, src_surl, dest_surl)
        else:
            pass

        return cmd

    ## parse command-line options/arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'p:t:', ["vo=", "mt="])

        for o,a in opts:
            if o in [ '--vo' ]:
                vo = a
            elif o in [ '--mt' ]:
                max_trial = int(a)
            elif o in [ '-p' ]:
                if a in supported_protocols:
                    protocol = a
                else:
                    printInfo('protocal not supported: %s, trying %s' % (a, protocol), outfile)
            elif o in [ '-t' ]:
                timeout = int(a)

        if len(args) == 2:
            src_surl  = args[0]
            dest_surl = args[1]
        else:
            raise getopt.GetoptError('missing source or destination SURL in command arguments.')

    except getopt.GetoptError, err:
        ## close stdout/err and exit the program
        printError(str(err), errfile)
        outfile.close()
        errfile.close()
        sys.exit(2)

    ## load the checksum pickle if it exists
    csumfile = 'lfc_checksum.pickle'
    csum = None
    if os.path.exists(csumfile):
        f = open(csumfile,'r')
        csum = pickle.load(f)
        f.close()

    ## initialize trial count and flags
    cnt_trial = 0
    isDone    = False
    
    ## initialize the return code
    rc = 0

    ## main copy loop 
    while not isDone and ( cnt_trial < max_trial ):

        status = False

        cnt_trial += 1

        ## compose copy command
        ##  - timeout is increasd for each trial
        copy_cmd = make_copycmd(protocol, vo, timeout*cnt_trial, src_surl, dest_surl)

        try:
            # use subprocess to run the user's application if the module is available on the worker node
            import subprocess
            printInfo('Run copy cmd: "%s"' % copy_cmd, outfile)
            status = execSyscmdSubprocess(copy_cmd, outfile, errfile)
        except ImportError,err:
            # otherwise, use separate threads to control process IO pipes
            printError('Not able to load subprocess module', errfile)
            break

        printInfo( 'copy command status: %s' % repr(status), outfile )

        if status:
            ## try to get the checksum type/value stored in LFC
            ## - the checksum dictionary is produced by 'make_filestager_joption.py'
            ##   and stored in a pickle file.
            if csum and csum.has_key(src_surl):

                csum_type  = csum[src_surl]['csumtype']
                csum_value = csum[src_surl]['csumvalue']

                printInfo( 'csum_type:%s csum_value:%s' % (csum_type, csum_value), outfile )

                if csum_type and csum_value:

                    ## do checksum comparison on the downloaded file
                    dest_file = urisplit(dest_surl)[2]

                    csum_local = ''

                    if csum_type.upper() == 'MD':
                        csum_local = get_md5sum(dest_file)
                    elif csum_type.upper() == 'AD':
                        # slight modification on the hex string to make it compatible with what stored in LFC
                        csum_local = get_adler32sum(dest_file).replace('0x','').zfill(8)
                    else:
                        pass

                    if csum_local.lower() == csum_value.lower():
                        printInfo( '%s checksum matched: %s ( local:%s == lfc:%s )' % (csum_type, src_surl, csum_local, csum_value), outfile )
                        isDone = True
                    else:
                        printInfo( '%s checksum not match: %s ( local:%s != lfc:%s )' % (csum_type, src_surl, csum_local, csum_value), outfile )
                        isDone = False
            else:
                printInfo( 'Ignore checksum comparison: %s' % src_surl, outfile)
                isDone = True

    outfile.close()
    errfile.close()

    # wrapper script return code
    if not isDone:
        rc = 1

    sys.exit(rc)
