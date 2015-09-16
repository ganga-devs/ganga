# File: Magic.py
# Author: K. Harrison
# Created: 060328

"""Module containing IPython magic functions for Ganga"""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "28 March 2006"
__version__ = "1.0"

from Ganga.Utility.logging import getLogger
from Ganga.Utility.Runtime import getScriptPath
from Ganga.Utility.Runtime import getSearchPath

import sys

logger = getLogger(modulename=1)


def magic_ganga(self, parameter_s=''):
    """
    IPython magic function for executing Python scripts in Ganga namespace

    Usage:
       ganga <script> <arguments>

       <script>    - Python script in Ganga search path
       <arguments> - Arguments to be passed to <script>
    """

  # Obtain list of arguments from input parameter string
    argList = parameter_s.split()

  # Determine path to script, using Ganga search rules
    if argList:
        path = getSearchPath()
        script = getScriptPath(argList[0], path)

      # Use mechanism based on that used in magic_run function of IPython
      # for executing script
        if script:
            save_argv = sys.argv
            sys.argv = [script] + argList[1:]
            prog_ns = self.shell.user_ns
            runner = self.shell.safe_execfile
            runner(script, prog_ns, prog_ns)
            sys.argv = save_argv
        else:
            logger.warning("Script '%s' not found in search path '%s'" %
                           (argList[0], path))
    else:
        logger.info(magic_ganga.__doc__)

    return None
