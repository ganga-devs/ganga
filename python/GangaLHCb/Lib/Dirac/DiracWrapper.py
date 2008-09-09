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

def diracwrapper(command, getoutput = False):
  """This is a wrapper script for executing DIRAC API commands that
  require a modified environment"""
  import os,os.path,stat
  content="""#!/bin/env python
import sys, os
import warnings
warnings.filterwarnings(action="ignore", category=RuntimeWarning)

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
        """ % {'###COMMAND###':command,'###LEVEL###':configDirac['DiracLoggerLevel'],'###SHARED###':inspect.getsource(DiracShared)}

  fname = tempfile.mktemp('.py')
  f = open(fname,'w')
  f.write(content)
  f.close()
  os.chmod(fname,stat.S_IRWXU)
  if getoutput:
      rc,output,m = s.cmd1(fname)
  else:
      rc,output,m = s.cmd(fname)
  
  result = rc
  if getoutput:
      result = (rc,output)
  
  if rc==0:
      os.unlink(fname)
      if not getoutput:
          os.unlink(output)
  return result
