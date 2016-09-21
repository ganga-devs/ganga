#!/usr/bin/env python
from __future__ import print_function
import shutil
import os
import time
import popen2
import glob
import mimetypes

############################################################################################

###INLINEMODULES###
###INLINEHOSTNAMEFUNCTION###

############################################################################################

input_sandbox = ###INPUT_SANDBOX###
sharedoutputpath = ###SHAREDOUTPUTPATH###
outputpatterns = ###OUTPUTPATTERNS###
appscriptpath = ###APPSCRIPTPATH###
environment = ###ENVIRONMENT###

# jobid is a string
jobid = ###JOBID###

###PREEXECUTE###

def flush_file(f):
    f.flush()
    os.fsync(f.fileno()) #this forces a global flush (cache synchronization on AFS)

def open_file(fname):
    try:
        filehandle=open(fname,'w')
    except IOError as x:
        print('ERROR: not able to write a status file: ', fname)
        print('ERROR: ',x)
        raise
    return filehandle

statusfilename = os.path.join(sharedoutputpath,'__jobstatus__')
heartbeatfilename = os.path.join(sharedoutputpath,'__heartbeat__')

statusfile=open_file(statusfilename)
heartbeatfile=open_file(heartbeatfilename)

line='START: '+ time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
try:
    line+='PID: ' + os.getenv('###JOBIDNAME###') + os.linesep
    line+='QUEUE: ' + os.getenv('###QUEUENAME###') + os.linesep
    line+='ACTUALCE: ' + hostname() + os.linesep
except:
    pass
statusfile.writelines(line)
flush_file(statusfile)

import tarfile

# -- WARNING: get the input files including the python modules BEFORE sys.path.insert()
# -- SINCE PYTHON 2.6 THERE WAS A SUBTLE CHANGE OF SEMANTICS IN THIS AREA

for f in input_sandbox:
    if mimetypes.guess_type(f)[1] in ['gzip', 'bzip2']:
        getPackedInputSandbox(f)
    else:
        shutil.copy(f, os.path.join(os.getcwd(), os.path.basename(f)))

# -- END OF MOVED CODE BLOCK

#get input files
###DOWNLOADINPUTFILES###

import sys
sys.path.insert(0, ###GANGADIR###)
sys.path.insert(0,os.path.join(os.getcwd(),PYTHON_DIR))

import subprocess

fullenvironment = os.environ.copy()
for key,value in environment.iteritems():
    fullenvironment[key] = value

sysout2 = os.dup(sys.stdout.fileno())
syserr2 = os.dup(sys.stderr.fileno())

print("--- GANGA APPLICATION OUTPUT BEGIN ---", file=sys.stdout)
print("--- GANGA APPLICATION ERROR BEGIN ---", file=sys.stdout)
flush_file(sys.stdout)
flush_file(sys.stderr)

sys.stdout=open('./__syslog__','w')
sys.stderr=sys.stdout

result = 255



try:
    child = subprocess.Popen(appscriptpath, shell=False, stdout=sysout2, stderr=syserr2, env=fullenvironment)

    while 1:
        result = child.poll()
        if result is not None:
            break
        heartbeatfile.write('.')
        flush_file(heartbeatfile)
        time.sleep(###HEARTBEATFREQUENCE###)
except Exception as x:
    print('ERROR: %s'%str(x))

flush_file(sys.stdout)
flush_file(sys.stderr)
sys.stdout=sys.__stdout__
sys.stderr=sys.__stderr__
print("--- GANGA APPLICATION OUTPUT END ---", file=sys.stdout)


try:
    filefilter
except:
    filefilter = None

from files import multi_glob, recursive_copy

createOutputSandbox(outputpatterns,filefilter,sharedoutputpath)

def printError(message):
    print(message, file=sys.stderr)

def printInfo(message):
    print(message, file=sys.stdout)

###OUTPUTUPLOADSPOSTPROCESSING###

print("--- GANGA APPLICATION ERROR END ---", file=sys.stderr)

###OUTPUTSANDBOXPOSTPROCESSING###

###POSTEXECUTE###

line='EXITCODE: ' + repr(result) + os.linesep
line+='STOP: '+time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
statusfile.writelines(line)

statusfile.close()
heartbeatfile.close()
os.unlink(heartbeatfilename)

sys.exit(result)


