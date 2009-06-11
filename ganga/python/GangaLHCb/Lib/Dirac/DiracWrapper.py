#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Make some lcg commands callable from within the DIRAC API

import inspect
import sys
import tempfile
from Ganga.Core import BackendError
import Ganga.Utility.Config
from Ganga.Utility.Shell import Shell
from os.path import basename,dirname,exists,join,sep
from os import pathsep,environ
import Ganga.Utility.logging
import DiracShared

configDirac = Ganga.Utility.Config.getConfig('DIRAC')
configLHCb = Ganga.Utility.Config.getConfig('LHCb')
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#figure out where our env is to load
class __FindMe(object):
    pass
diracEnvSetup = join(dirname(inspect.getsourcefile(__FindMe)),
                     'setupDiracEnv.sh')
if not exists(diracEnvSetup):
    logger.error("Cannot find the file 'setupDiracEnv.sh' needed by ganga.")
    raise BackendError('Dirac',"Cannot find the file 'setupDiracEnv.sh' "\
                       "needed by ganga.")

#idea here is to use DiracVersion if available, but otherwise use DiracTopDir
configDiracVersion = None
try:
    configDiracVersion = configLHCb['DiracVersion']
    diracTopDir = configLHCb['DiracTopDir']
    if not configDiracVersion and diracTopDir:
        #fall back on DiracTopDir - TODO: Remove this
        #happens when DiracVersion is not set but DiracTopDir is
        if diracTopDir.endswith(sep):
            diracTopDir = diracTopDir[:-1]
        diracName = basename(diracTopDir)
        if diracName:
            d = diracName.split('_')
            if len(d) == 2 and d[0].startswith('DIRAC'):
                configDiracVersion = d[1]
            else:
                logger.warning("Failed to find the DIRAC Version. Taking " \
                               "the default version.")
except:
    pass

if configDiracVersion is None:
    logger.warning("Failed to find the DIRAC Version. Taking the default "
                   "version.")
    configDiracVersion = ''
else:
    logger.info("Dirac version is '%s'",configDiracVersion)
s = Shell(diracEnvSetup,setup_args = [configDiracVersion])

#check that SetupProject Dirac worked
if s.env.get('GANGA_DIRAC_SETUP_STATUS',True):
    try:
        status = int(s.env['GANGA_DIRAC_SETUP_STATUS'])
        if status: raise Exception #just go to printout
    except: #printout if its not a number
        logger.error("Setting up the Dirac client failed. Check your Dirac installation."\
                     " The command 'SetupProject Dirac %s' must work correctly from your command prompt.",configDiracVersion)

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
  
  content="""#!/usr/bin/env python
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
        """ % {'###COMMAND###':command,
               '###LEVEL###':configDirac['DiracLoggerLevel'],
               '###SHARED###':inspect.getsource(DiracShared),
               '###OUTPUT###':dwrapper.outputFile}
        
  wrapper = """#!/usr/bin/env python
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
  """ % {'###CONTENT###':content,
         '###SHARED###':inspect.getsource(DiracShared),
         '###OUTPUT###':dwrapper.outputFile}

  fname = tempfile.mktemp('.py')
  f = open(fname,'w')
  f.write(wrapper)
  f.close()
  os.chmod(fname,stat.S_IRWXU)
  
  rc,output,m = s.cmd1(fname)
      
  dwrapper.returnCode = rc
  dwrapper.stdout = output
  if rc==0:
      os.unlink(fname)
  return dwrapper

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
