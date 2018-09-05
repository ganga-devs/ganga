# Copyright information
__author__  = "Ole Weidner <oweidner@cct.lsu.edu>"
__date__    = "16 September 2010"
__version__ = "1.0"

##############################################################################
##
class SAGAWrapperScript:

    _attributes = ('wrapper_script_template', 'exe', 'args', 'envi', 'sandbox', 'imods', 'mons', 'out')

    ##########################################################################
    ##    
    def setExecutable(self, exe):
        self.exe = exe

    ##########################################################################
    ##    
    def setOutputPatterns(self, out):
        self.out = out

    ##########################################################################
    ##    
    def setArguments(self, args):
        self.args = args
        
    ##########################################################################
    ##    
    def setEnvironment(self, envi):
        self.envi = envi
        
    ##########################################################################
    ##    
    def setInputSandbox(self, sandbox):
        self.sandbox = sandbox
        
    ##########################################################################
    ##    
    def setInlineModules(self, imods):
        self.imods = imods
      
    ##########################################################################
    ##
    def getScript(self):
        script = self.wrapper_script_template
        script = script.replace('###INLINEMODULES###', self.imods)
        script = script.replace('###EXECUTABLE###', repr(self.exe))
        script = script.replace('###ARGUEMTNS_AS_LIST###', repr(self.args))
        #script = script.replace('###ENVIRONMENT_AS_DICT###', repr(self.envi))
        script = script.replace('###INPUTSANDBOXFILE###', repr(self.sandbox))
        script = script.replace('###OUTPUTPATTERNS###', repr(self.out))
    
        return script

    ##########################################################################
    ##
    def __init__(self):
        self.imods = ''
        
        self.wrapper_script_template  = """#!/usr/bin/env python

import shutil
import os
import time
import sys

###INLINEMODULES###

executable       = ###EXECUTABLE###
arguments        = ###ARGUEMTNS_AS_LIST###
outputpatterns   = ###OUTPUTPATTERNS###


inputsandboxfile = ###INPUTSANDBOXFILE###

#redirect stdout/stderr to file
sys.stdout=file('./__syslog__','w')
sys.stderr=sys.stdout


## First things first. Unpack the input sandbox, since that's where
## all our files are. 
import tarfile

if os.path.exists(inputsandboxfile): 
    tar = tarfile.open(inputsandboxfile)
    tar.extractall()
    tar.close()

## Try to import the subprocess library. If it's not in the 
## PYTHON_PATH, we use the one pre-staged with this job. 
##
try:
    import subprocess
except ImportError as x:
    sys.path.insert(0, PYTHON_DIR)
    import subprocess
    


#sysout2 = os.dup(sys.stdout.fileno())
#syserr2 = os.dup(sys.stderr.fileno())

#sys.stdout.flush()
#sys.stderr.flush()

sys.path.insert(0, PYTHON_DIR)


result = 255

## EXECUTE THE STUFF WE WANT TO RUN
##

outfile = open('out.log', 'w')
errfile = open('err.log', 'w')

try:
  executableandargs = arguments
  executableandargs.insert(0, executable)
  child = subprocess.Popen(executableandargs, shell=False, stdout=outfile, stderr=errfile)

  while 1:
    result = child.poll()
    if result is not None:
        break
    #heartbeatfile.write('.')
    #flush_file(heartbeatfile)
    time.sleep(30)
    
except Exception as x:
  outfile.close()
  errfile.close()
  sys.stdout = sys.__stdout__
  sys.stderr = sys.__stderr__
  

outfile.close()
errfile.close()

## As a last step, we will create an archive of the outputsandbox files. This
## will speed up the post-staging process in many cases
try:
    filefilter
except:
    filefilter = None

from GangaCore.Utility.files import multi_glob, recursive_copy

createPackedOutputSandbox(outputpatterns,filefilter,".")

sys.exit(result)
"""
        
