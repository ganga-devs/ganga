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
#import md5
import zlib
import sys
#import popen2
import time
#import traceback
import pickle
import re
import getopt

try:
    import hashlib
    md = hashlib.md5()
except ImportError:
    # for Python << 2.5
    import md5
    md = md5.new()

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
    #m = md.new()
    while True:
        d = f.read(8096)
        if not d:
            break
        md.update(d)
    f.close()
    return md.hexdigest()

def get_adler32sum(fname):
    ''' Calculate the Adler32 checksum of a file '''

    cksum = None
    f = open(fname,'rb')
    while True:
        d = f.read(8096)
        if not d:
            break

        if not cksum:
            #cksum = hex( zlib.adler32(d) & 0xffffffff )
            cksum = zlib.adler32(d)
        else:
            #cksum = hex( zlib.adler32(d, cksum) & 0xffffffff )
            cksum = zlib.adler32(d, cksum)
    f.close()

    # remove the tailing 'L' charactor
    cksum_str = re.sub(r'L$','', hex(cksum & 0xffffffff) )

    return cksum_str

#def get_adler32sum(fname):
#    ''' Calculate the Adler32 checksum of a file '''
#
#    f = open(fname,'rb')
#    data = f.read()
#    cksum = hex( zlib.adler32(data) & 0xffffffff )
#    f.close()
#
#    # remove the tailing 'L' charactor
#    cksum = re.sub(r'L$','',cksum)
#
#    return cksum

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
def execSyscmdSubprocess(cmd, wdir=os.getcwd()):

    import os
    import subprocess

    exitcode = -999

    mystdout = ''
    mystderr = ''

    try:

        ## resetting essential env. variables
        my_env = os.environ

        if 'LD_LIBRARY_PATH_ORIG' in my_env:
            my_env['LD_LIBRARY_PATH'] = my_env['LD_LIBRARY_PATH_ORIG']

        if 'PATH_ORIG' in my_env:
            my_env['PATH'] = my_env['PATH_ORIG']

        if 'PYTHONPATH_ORIG' in my_env:
            my_env['PYTHONPATH'] = my_env['PYTHONPATH_ORIG']
        
        child = subprocess.Popen(cmd, cwd=wdir, env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (mystdout, mystderr) = child.communicate()

        exitcode = child.returncode

    finally:
        pass

    return (exitcode, mystdout, mystderr)

## resolving TURL from a given SURL
def resolveTURL(surl, protocol, outfile, errfile, timeout=300):

    ## default lcg-gt timeout: 5 minutes
    if 'lcgutil_num' in os.environ and os.environ['lcgutil_num']!='' and eval(os.environ['lcgutil_num']) >= 1007002:
        cmd = "lcg-gt -v --connect-timeout %d --sendreceive-timeout %d --srm-timeout %d --bdii-timeout %d %s %s" % (timeout, timeout, timeout, timeout, surl, protocol) 
    else:
        cmd = 'lcg-gt -v -t %d %s %s' % (timeout, surl, protocol)

    exitcode = -999

    mystdout = ''
    mystderr = ''

    turl = ''

    try:
        # use subprocess to run the user's application if the module is available on the worker node
        import subprocess
        printInfo('Run lcg-gt cmd: "%s"' % cmd, outfile)
        (exitcode, mystdout, mystderr) = execSyscmdSubprocess(cmd)

        # print out command outputs if the return code is not 0
        if exitcode != 0:
            printInfo(mystdout, outfile)
            printError(mystderr, errfile)

    except ImportError as err:
        # otherwise, use separate threads to control process IO pipes
        printError('Not able to load subprocess module', errfile)

    if exitcode == 0:
        data = mystdout.strip().split('\n')

        try:
            if re.match(protocol, data[0]):
                turl = data[0]
        except IndexError:
            pass

    return turl


# Main program
if __name__ == '__main__':
    
    ## open files for command output and error
    outfile = open('FileStager.out','a')
    errfile = open('FileStager.err','a')

    ## default value specification
    supported_protocols = ['lcgcp','rfio','gsidcap','dcap','gsiftp','file']

    protocol  = 'lcgcp'
    timeout   = 1200
    vo        = 'atlas'
    max_trial = 3

    src_surl  = None
    dest_surl = None

    ## a workaround to avoid passing protocol as a argument to this script
    ## this should be disabled when the bug in Athena/FileStager is fixed
    if 'FILE_STAGER_PROTOCOL' in os.environ:
        if os.environ['FILE_STAGER_PROTOCOL'] in supported_protocols:
            protocol = os.environ['FILE_STAGER_PROTOCOL']

    ## internal subroutine definition
    def make_copycmd(protocol, vo, timeout, src_surl, dest_surl):
        '''
        routine for composing copy command according to the requested protocol.
        '''

        cmd = ''

        if protocol in [ 'lcgcp' ]:
            cmd = 'lcg-cp -v --vo %s -t %d %s %s' % (vo, timeout, src_surl, dest_surl)

        else:

            ## resolve TURL
            src_turl = resolveTURL(src_surl, protocol, outfile, errfile)

            if src_turl:

                dest_fpath = '/' + re.sub(r'^(file:)?\/*','',dest_surl)

                ## for dCache
                if protocol in [ 'gsidcap', 'dcap' ]:
                    cmd = 'dccp -A -d 2 -o %d %s %s' % (timeout, src_turl, dest_fpath)

                ## for DPM/Castor
                elif protocol in [ 'rfio' ]:
                    cmd = 'rfcp \'%s\' %s' % (src_turl, dest_fpath)

                ## for Storm/Luster
                elif protocol in [ 'file' ]:
                    ##src_fpath = '/' + re.sub(r'^(file:)?\/*','',src_surl)
                    cmd = 'curl -v %s -o %s' % (src_turl, dest_fpath)

                ## for classic grid storage
                elif protocol in [ 'gsiftp' ]:
                    ## keep retrying the failed transfer operation within the given timeout
                    ## wait for 30 seconds to the next retry
                    cmd = 'globus-url-copy -rst-interval 30 -rst-timeout %d %s %s' % (timeout, src_turl, dest_surl)

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
                    printInfo('file copy protocol: %s' % a, outfile)
                else:
                    printInfo('protocal not supported: %s, trying %s' % (a, protocol), outfile)
            elif o in [ '-t' ]:
                timeout = int(a)

        if len(args) == 2:
            src_surl  = args[0]
            dest_surl = args[1]
        else:
            raise getopt.GetoptError('missing source or destination SURL in command arguments.')

    except getopt.GetoptError as err:
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
    while not isDone and ( cnt_trial < 2*max_trial ):

        ## fall back to lcgcp if all trials with local protocol were failed
        if ( cnt_trial == max_trial+1 ):
            if protocol == 'lcgcp':
                break
            else:
                printInfo('trying with lcg-cp', outfile)
                protocol = 'lcgcp' 
                pass

        exitcode = -999
        cnt_trial += 1

        ## compose copy command
        ##  - timeout is increasd for each trial
        copy_cmd = make_copycmd(protocol, vo, timeout*cnt_trial, src_surl, dest_surl)

        ## faile to compose the full copy command, give another try for the whole loop
        if not copy_cmd:
            printError('fail to compose copy command', errfile)
            continue

        try:
            # use subprocess to run the user's application if the module is available on the worker node
            import subprocess
            printInfo('Run copy cmd: "%s"' % copy_cmd, outfile)
            (exitcode, mystdout, mystderr) = execSyscmdSubprocess(copy_cmd)

            # print command detail if return code is not 0
            if exitcode != 0:
                printInfo(mystdout, outfile)
                printError(mystderr, errfile)

        except ImportError as err:
            # otherwise, use separate threads to control process IO pipes
            printError('Not able to load subprocess module', errfile)
            break

        printInfo( 'copy command exitcode: %s' % repr(exitcode), outfile )

        if exitcode == 0:
            ## try to get the checksum type/value stored in LFC
            ## - the checksum dictionary is produced by 'make_filestager_joption.py'
            ##   and stored in a pickle file.
            if csum and src_surl in csum:

                csum_type  = csum[src_surl]['csumtype']
                csum_value = csum[src_surl]['csumvalue']

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
                        printInfo( '%s checksum matched: %s' % (csum_type, csum_value), outfile )
                        isDone = True
                    else:
                        printInfo( '%s checksum mismatch: %s ( local:%s != lfc:%s )' % (csum_type, src_surl, csum_local, csum_value), outfile )
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
