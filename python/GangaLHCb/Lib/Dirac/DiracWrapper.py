# Make some lcg commands callable from within the DIRAC API
from Ganga.Core import BackendError
import Ganga.Utility.Config
from Ganga.Utility.Shell import Shell
from os.path import dirname,exists,join
from os import pathsep,environ
import inspect
import sys
import tempfile
configDirac = Ganga.Utility.Config.getConfig('DIRAC')
configLHCb = Ganga.Utility.Config.getConfig('LHCb')

import DiracShared

# The LD_LIBRARY_PATH set by Ganga interferes with GridEnv. Hence unset
# it and reset it again afterwards. Same may be true for DIRACROOT

_varKeep = {}
def _checkVar(varKeep,varName):
    """Keep a copy of variables before we change them"""
    keep = environ.get(varName,None)
    if keep is not None:
        del environ[varName]
        varKeep[varName] = keep
    return varKeep

_checkVar(_varKeep,'LD_LIBRARY_PATH')
_checkVar(_varKeep,'DIRACROOT')
environ['DIRACROOT'] = configLHCb['DiracTopDir']

#figure out where our env is to load
# TODO: HACK
class __FindMe(object):
    pass
diracEnvSetup = join(dirname(inspect.getsourcefile(__FindMe)),'setupDiracEnv.sh')
if not exists(diracEnvSetup):
    logger.error("Cannot find the file 'setupDiracEnv.sh' needed by ganga.")
    raise BackendError('Dirac',"Cannot find the file 'setupDiracEnv.sh' needed by ganga.")

s = Shell(diracEnvSetup,setup_args = [configLHCb['DiracTopDir']])

for key, item in _varKeep.iteritems():
    environ[key] = item

class _DiracWrapper(object):
    
    def __init__(self):
        
        import tempfile
        self.outputFile = tempfile.mktemp('.p')
        self.returnCode = 0
        self.stdout = ''
    
    def getOutput(self):
        return DiracShared.getResult(self.outputFile)


def diracwrapper(command):
  """This is a wrapper script for executing DIRAC API commands that
  require a modified environment"""
  import os,os.path,stat

  dwrapper = _DiracWrapper()
  
  content="""#!/bin/env python
import sys, os
import warnings
warnings.filterwarnings(action="ignore", category=RuntimeWarning)

__outputFileName = '%(###OUTPUT###)s'

%(###SHARED###)s

rc=0

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob
from DIRAC import gLogger

#set the logging level
gLogger.setLevel('%(###LEVEL###)s')

dirac = Dirac()
%(###COMMAND###)s

sys.exit(rc)
        """ % {'###COMMAND###':command,'###LEVEL###':configDirac['DiracLoggerLevel'],\
               '###SHARED###':inspect.getsource(DiracShared),'###OUTPUT###':dwrapper.outputFile}
        
  wrapper = """#!/bin/env python
import sys, os

__outputFileName = '%(###OUTPUT###)s'
%(###SHARED###)s
  
command = '''
%(###CONTENT###)s
'''

#write out the script and exec
try:
    import tempfile
    fName = tempfile.mktemp('.py')
    try:
        outfile = file(fName,'w')
        outfile.write(command)
        outfile.close()
        execfile(fName)
    finally:
        outfile.close()
        if os.path.exists(fName):
            os.unlink(fName)
except SystemExit, e:
    #try to exit with the correct return code, but don't worry if not
    try:
        sys.exit(int(str(e)))
    except:
        pass
except Exception, e:
    result = {'OK':False,'Exception':str(e)}
    try:
        result['Type'] = e.__class__.__name__
    except:
        pass
    storeResult(result)
  """ % {'###CONTENT###':content,'###SHARED###':inspect.getsource(DiracShared),'###OUTPUT###':dwrapper.outputFile}

  fname = tempfile.mktemp('.py')
  f = open(fname,'w')
  f.write(wrapper)
  f.close()
  os.chmod(fname,stat.S_IRWXU)
  
  rc,output,m = s.cmd1(fname)
      
  dwrapper.returnCode = rc
  dwrapper.stdout = output
  print 'exec',rc, fname
  if rc==0:
      pass
      #os.unlink(fname)
  return dwrapper
