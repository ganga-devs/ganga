#######################################################################
# File: OptionsParser.py
# Author: U. Egede
# Date: June 2008
#######################################################################
"""
File: OptionsParser.py
Purpose: Used for parsing the options for a Gaudi job
"""

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class OptionsParser:
  """Class for parsing option files of Gaudi jobs. A Shell object should be
  given to the constructor which should contain the correct CMT environment
  for the job."""

  def __init__(self,shell):
    self.shell=shell

  def to_text(self,options,extension):
    import tempfile
    fd=tempfile.NamedTemporaryFile(suffix=extension)
    fd.write(options)
    fd.flush()
    command='gaudirun.py -n -v  --old-opts %s'% fd.name
    rc,output,m=self.shell.cmd1(command)
    if not rc==0:
      from Ganga.Core import ApplicationConfigurationError
      logger.error(output)
      raise ApplicationConfigurationError(None, 'Problem with syntax in options file')

    lines=output.splitlines()
    options=''
    for line in lines:
      if not line.startswith('#') and not line.startswith('//'):
        options+=line+'\n'
    return options

  def optsfiles_to_text(self,optsfiles,extraopts=''):
    """Take the list of options files from Gaudi and convert into a single options string in old style format"""

    names = [fileitem.name for fileitem in optsfiles]

    # See if we have old text style or new python style.
    type = ''
    import os.path
    for name in names:
      (prefix,ext) = os.path.splitext(name)
      if type=='':
        type = ext
      if not ext == type:
        raise TypeError('You cannot mix different types of options for the same job')

    # Abort if no magic extension
    if not type=='.opts' and not type=='.py':
      raise TypeError('Only extensions of type ".opts" and ".py" allowed')

    # Put all the options together in a single string
    options=''
    for name in names:
      file=open(name,'r')
      options+=file.read()
    if extraopts:
      options+=extraopts
        
    return self.to_text(options,type)
 
