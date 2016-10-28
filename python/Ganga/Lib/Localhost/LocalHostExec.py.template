#!/usr/bin/env python
from __future__ import print_function

import os,os.path,shutil,tempfile
import sys,time
import glob
import sys
import mimetypes
import subprocess
import tarfile
import time

# FIXME: print as DEBUG: to __syslog__ file
#print(sys.path)
#print(os.environ['PATH'])
#print(sys.version)

# bugfix #13314 : make sure that the wrapper (spawned process) is detached from Ganga session
# the process will not receive Control-C signals
# using fork  and doing setsid() before  exec would probably  be a bit
# better (to avoid  slim chance that the signal  is propagated to this
# process before setsid is reached)
# this is only enabled if the first argument is 'subprocess' in order to enable
# running this script by hand from outside ganga (which is sometimes useful)
if len(sys.argv)>1 and sys.argv[1] == 'subprocess':
    os.setsid()

###########################################################################################

###INLINEMODULES###

############################################################################################

input_sandbox = ###INPUT_SANDBOX###
sharedoutputpath= ###SHAREDOUTPUTPATH###
outputpatterns = ###OUTPUTPATTERNS###
appscriptpath = ###APPSCRIPTPATH###
environment = ###ENVIRONMENT###
workdir = ###WORKDIR###

statusfilename = os.path.join(sharedoutputpath,'__jobstatus__')

try:
    statusfile=open(statusfilename,'w')
except IOError as x:
    print('ERROR: not able to write a status file: ', statusfilename)
    print('ERROR: ',x)
    raise

line='START: '+ time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
statusfile.writelines(line)
statusfile.flush()

if not os.path.exists(workdir):
    os.makedirs(workdir)
os.chdir(workdir)

##-- WARNING: get the input files including the python modules BEFORE sys.path.insert()
# -- SINCE PYTHON 2.6 THERE WAS A SUBTLE CHANGE OF SEMANTICS IN THIS AREA

for f in input_sandbox:
    if mimetypes.guess_type(f)[1] in ['gzip', 'bzip2']:
        getPackedInputSandbox(f)
    else:
        shutil.copy(f, os.path.join(os.getcwd(), os.path.basename(f)))

# -- END OF MOVED CODE BLOCK

#get input files
###DOWNLOADINPUTFILES###

# create inputdata list
###CREATEINPUTDATALIST###

sys.path.insert(0, ###GANGADIR###)
sys.path.insert(0,os.path.join(os.getcwd(),PYTHON_DIR))

fullenvironment = os.environ.copy()

outfile=open('stdout','w')
errorfile=open('stderr','w')

sys.stdout=open('./__syslog__','w')
sys.stderr=sys.stdout

try:
    child = subprocess.Popen(appscriptpath, shell=False, stdout=outfile, stderr=errorfile, env=fullenvironment)
except OSError as x:
    errfile = open( 'stderr', 'w' )
    errfile.close()
    print('EXITCODE: %d'%-9999, file=statusfile)
    print('FAILED: %s'%time.strftime('%a %b %d %H:%M:%S %Y'), file=statusfile) #datetime.datetime.utcnow().strftime('%a %b %d %H:%M:%S %Y')
    print('PROBLEM STARTING THE APPLICATION SCRIPT: \'%s\' \'%s\''%(appscriptpath,str(x)), file=statusfile)
    print('FILES FOUND ARE: %s' % os.listdir('.'), file=statusfile)
    statusfile.close()
    sys.exit()

print('PID: %d'%child.pid, file=statusfile)
statusfile.flush()

result = -1

try:
    while 1:
        result = child.poll()
        if result is not None:
            break
        outfile.flush()
        errorfile.flush()
        time.sleep(0.3)
finally:
    pass

    sys.stdout=sys.__stdout__
    sys.stderr=sys.__stderr__


outfile.flush()
errorfile.flush()

createOutputSandbox(outputpatterns,None,sharedoutputpath)

def printError(message):
    errorfile.write(message + os.linesep)
    errorfile.flush()

def printInfo(message):
    outfile.write(message + os.linesep)
    outfile.flush()


###OUTPUTUPLOADSPOSTPROCESSING###

outfile.close()
errorfile.close()

###OUTPUTSANDBOXPOSTPROCESSING###

line="EXITCODE: " + repr(result) + os.linesep
line+='STOP: '+time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(time.time())) + os.linesep
statusfile.writelines(line)
statusfile.close()
sys.exit()

