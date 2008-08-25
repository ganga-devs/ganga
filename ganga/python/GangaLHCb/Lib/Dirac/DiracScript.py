from os.path import join
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('DIRAC')
from Ganga.Core import BackendError

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
      self.finalised=False
      self.script="""#!/bin/env python
# This file contains the commands to be executed for submitting the job
# to DIRAC. Do not edit unless you ***really*** know what you are doing.
# The variable "id" is passed back to Ganga.

id = None
try:
    submit
except NameError:
    submit=True

import DIRAC.Client.Dirac as dirac
djob = dirac.Job()
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

  def finalise(self):
    """Write the actual submission bit into the DIRACscript"""
    if not self.finalised:
      self.finalised=True
      self.script+="""mydirac = dirac.Dirac()
try:
    if submit: id = mydirac.submit(djob, verbose = 1, mode='quiet')
except:
    pass

if type(id)!=int or id==-1:
    # We failed first attempt, so retry in 5 seconds
    import time
    time.sleep(5)
    mydirac=dirac.Dirac()
    if submit: id = mydirac.submit(djob, verbose = 1, mode='quiet')
"""

  def write(self,job):
    """Persist the DIRACscript into the input workspace"""
    inputws=job.getInputWorkspace()
    fname=join(inputws.getPath(),'DIRACscript')
    if not self.finalised:
      self.finalise()
    f = open(fname,'w')
    try:
      f.write(self.script)
    finally:
      f.close()

  def execute(self, submit=True):
    """Execute the DIRACscript and report back id."""
    from Ganga.Core import BackendError
    scriptdict={'submit':submit}
    if not self.finalised:
      self.finalise()
    try:
      exec(self.script, scriptdict, scriptdict)
    except Exception, detail:
      logger.warning(str(detail))
      logger.warning(self.commands())
      raise BackendError("DIRAC", "Problems executing the DIRAC script.")
    try:
      id = scriptdict['id']
    except KeyError:
      raise BackendError("DIRAC", "DIRAC script failed to define variable 'id'.")
    return id

  def commands(self):
    """Returns the DIRAC commands for setting up the job as a string"""
    return self.script
