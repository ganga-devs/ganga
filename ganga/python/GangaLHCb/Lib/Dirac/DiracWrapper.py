# Make some lcg commands callable from within the DIRAC API
import Ganga.Utility.Config
from Ganga.Utility.Shell import Shell
from os.path import dirname,join
from os import pathsep,environ
import sys
configLHCb = Ganga.Utility.Config.getConfig('LHCb')

diracPythonDir = join(configLHCb['DiracTopDir'],  "DIRAC", "python")

# The LD_LIBRARY_PATH set by Ganga interferes with GridEnv. Hence unset
# it and reset it again afterwards.
keep=environ['LD_LIBRARY_PATH']
del environ['LD_LIBRARY_PATH']
s = Shell('${LHCBSCRIPTS}/GridEnv.sh')
environ['LD_LIBRARY_PATH']=keep

wrapperpath=dirname(s.wrapper('lcg-cp'))
s.wrapper('lhcb-lcg-cp',
          'export PATH=%s:$PATH; LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH' % \
          (join(configLHCb['DiracTopDir'],'DIRAC','bin'),
           join(configLHCb['DiracTopDir'],'DIRAC','lib')))
environ['PATH']=environ['PATH']+pathsep+wrapperpath

def diracwrapper(command):
  """This is a wrapper script for executing DIRAC API commands that
  require a modified environment"""
  import os,os.path,stat
  content="""#!/bin/env python
import sys
sys.path.append( ###DIRACPYTHONDIR### )
import warnings
warnings.filterwarnings(action="ignore", category=RuntimeWarning)
rc=0
from DIRAC.Client.Dirac import *
dirac = Dirac()
###COMMAND###
sys.exit(rc)
        """
  content = content.replace('###DIRACPYTHONDIR###',repr(diracPythonDir))
  content = content.replace('###COMMAND###',command)
  fname=os.path.join(wrapperpath,'ganga-dirac-command')
  f = open(fname,'w')
  f.write(content)
  f.close()
  os.chmod(fname,stat.S_IRWXU)
  rc,output,m = s.cmd(fname)
  if rc==0: os.unlink(output)
  return rc
