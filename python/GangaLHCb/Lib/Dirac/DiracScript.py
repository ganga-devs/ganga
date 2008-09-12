from os.path import join
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('DIRAC')
from Ganga.Core import BackendError

import DiracShared

class DiracScript:
  """Encapsulate the commands for submitting a job to Dirac into an object which is persisted as a simple set of commands in the input sandbox."""

  def __init__(self,job=None):
    """Create new or open existing DIRACscript"""
    self.script=''
    if job:
      inputws=job.getInputWorkspace()
      fname=join(inputws.getPath(),'DIRACscript')
      self.finalised=True
      f = open(fname,'r')
      try:
        self.script=f.read()
      finally:
        f.close()
    else:
        
      import inspect
              
      self.finalised=False
      self.script="""#!/bin/env python
# This file contains the commands to be executed for submitting the job
# to DIRAC. Do not edit unless you ***really*** know what you are doing.
# The variable "id" is passed back to Ganga.

id = None

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob

djob = LHCbJob()
"""  
  def append(self,command):
    """Append a command to the DIRAC script for setting up the job"""
    if self.finalised:
      from Ganga.Core import BackendError
      raise BackendError("DIRAC", "You can't add further lines to the DIRACscript after it has been finalised")
            
    self.script+=("djob."+command+'\n')

  def inputdata(self,dataset):
    """Append the options to the DIRAC script for input data as LFNs."""
    indata = []
    import re
    p = re.compile( 'lfn:.',re.IGNORECASE)
    fnames = [f.name for f in dataset.files]
    for lfn in fnames:
        if p.match( lfn):
            indata.append( lfn[4:])
    if len(indata) > 0:
        self.append( 'setInputData(' +str(indata) + ')')

  def outputdata(self,dataset,SE=None):
    """Append the options to the DIRAC script for output data to be uploaded to SE"""

    if len(dataset) > 0:
      from os.path import basename
      outdata = [basename(f) for f in dataset]
      from Ganga.Utility.util import unique
      self.append("setOutputData("+str(unique(outdata))+")")

  def platform(self,platform):
    whitelist = config['AllowedPlatforms']
    if platform in whitelist:
      self.append("setSystemConfig(%s)"% repr(platform))
    else:
      raise BackendError("Dirac", "Failed to submit to the platform %s. Only the following are allowed: %s. Change the value in your application object." % (platform, str(whitelist)))

  def addPackage(self, appName, appVersion):
    """Adds an package to the Dirac environment"""
    self.append("addPackage('%s','%s')" % (appName,appVersion))
    
  def setName(self, jobName):
    """Adds a name to the dirac job"""
    self.append("setName('%s')" % jobName)

  def setExecutable(self, logFile = None, command = 'chmod +x ./jobscript.py; ./jobscript.py'):
    """Calls the setExecutable method with a default command"""
    if logFile is not None:
        self.append("setExecutable('%s', logFile = '%s')" % (command, logFile))
    else:
        self.append("setExecutable('%s')" % command)
        
  def runApplicationScript(self, appName, appVersion, scriptFile, logFile = None):
    if logFile is not None:
        self.append("setApplicationScript('%s','%s','%s', logFile = '%s')" % (appName,appVersion,scriptFile,logFile))
    else:
        self.append("setApplicationScript('%s','%s','%s')" % (appName,appVersion,scriptFile))

  def finalise(self, submit = True):
    """Write the actual submission bit into the DIRACscript"""

    if not self.finalised:
        self.finalised=True
        self.script+="""mydirac = Dirac()
submit = %(#SUBMIT#)i

result = {}
try:
    if submit: result = mydirac.submit(djob)
except:
    pass

if not result.get('OK',False):
    # We failed first attempt, so retry in 5 seconds
    import time
    time.sleep(5)
    mydirac = Dirac()
    if submit: result = mydirac.submit(djob)
storeResult(result)
""" % {'#SUBMIT#':int(submit)}

  def write(self,job, submit = True):
    """Persist the DIRACscript into the input workspace"""
    inputws=job.getInputWorkspace()
    fname=join(inputws.getPath(),'DIRACscript')

    if not self.finalised:
      self.finalise(submit)
    f = open(fname,'w')
    try:
      f.write(self.script)
    finally:
      f.close()

  def execute(self, submit=True):
    """Execute the DIRACscript and report back id."""
    from Ganga.Core import BackendError
    if not self.finalised:
      self.finalise(submit)
      
    from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper
    id = None
    try:
      dw = diracwrapper(self.script)
      result = dw.getOutput()
      if result is not None:
          if result.get('OK',False):
              id = result['Value']
          else:
              logger.warning("Submission failed: Message from Dirac was '%s'.", result.get('Message',''))
              if result.has_key('Exception'):
                  logger.warning("'%s': %s", result.get('Type',''), result['Exception'])
                  raise BackendError("DIRAC", "Problems executing the DIRAC script.") 

    except Exception, detail:
      logger.warning(str(detail))
      logger.warning(self.commands())
      raise BackendError("DIRAC", "Problems executing the DIRAC script.")
    return id

  def commands(self):
    """Returns the DIRAC commands for setting up the job as a string"""
    return self.script
