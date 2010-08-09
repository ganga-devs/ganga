"""
This is a stub object representing the core JEM library in the GangaJEM plugin for Ganga.
If the JEM library can be found in Ganga's external-directory *or* in the directory poin-
ted at by the shell variable $JEM_PACKAGEPATH (this path being priorized), it gets loaded
by the stub and inserted into the python-path.

@author: Tim Muenchen
@date: 20.04.09
@organization: University of Wuppertal,
               Faculty of mathematics and natural sciences,
               Department of physics.
@copyright: 2007-2009, University of Wuppertal, Department of physics.
@license: ::

        Copyright (c) 2007-2009 University of Wuppertal, Department of physics

    Permission is hereby granted, free of charge, to any person obtaining a copy of this
    software and associated documentation files (the "Software"), to deal in the Software
    without restriction, including without limitation the rights to use, copy, modify, merge,
    publish, distribute, sublicense, and/or sell copies of the Software, and to permit
    persons to whom the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all copies
    or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
    INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
    PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
    TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
    OR OTHER DEALINGS IN THE SOFTWARE.
"""
import os
import sys
import traceback

#-----------------------------------------------------------------------------------------------------------------------
from Ganga.Utility.logging import logging, getLogger
logger = getLogger("GangaJEM.Lib.JEM")
outlogger = getLogger("GangaJEM.Lib.JEM.out")
#-----------------------------------------------------------------------------------------------------------------------

# status vars to access from other GangaJEM modules
INITIALIZED = False
JEM_PACKAGEPATH = None
#-----------------------------------------------------------------------------------------------------------------------

import GangaJEM

## try to load the Job Execution Monitor (JEM) for runtime job monitoring data.
try:
    # check if the user provided an own JEM packagepath via the shell variable...
    if not os.environ.has_key('JEM_PACKAGEPATH'):
        # if not, find the JEM package in the external packages...
        JEM_PACKAGEPATH = GangaJEM.PACKAGE.setup.getPackagePath('JEM')[0]
        logger.debug("Got JEM-path from GangaJEM PACKAGE: " + JEM_PACKAGEPATH)

        # set the env var to enable JEM to find itself...
        os.environ['JEM_PACKAGEPATH'] = JEM_PACKAGEPATH
    else:
        JEM_PACKAGEPATH = os.environ['JEM_PACKAGEPATH']

    # ...and prepend it to the python-path (priorizing it)
    if not JEM_PACKAGEPATH in sys.path:
        sys.path = [JEM_PACKAGEPATH] + [JEM_PACKAGEPATH + os.sep + "legacy"] + sys.path

    # import JEM-Ganga-Integration module (that manages the rest of JEMs initialisation)
    initError = None
    userpath = os.path.expanduser("~/.JEMrc")

    os.environ["JEM_Global_mode"] = "Ganga"

    try:
        # try to import JEM configs and submit methods
        from JEMlib.conf import JEMSysConfig as SysConfig
        from JEMui.conf import JEMuiSysConfig as JEMConfig
        from JEMlib.conf import JEMConfig as WNConfig
        from JEMui.conf import JEMuiConfig as UIConfig

        # import needed JEM modules
        from JEMlib.utils.ReverseFileReader import ropen
        from JEMlib.utils.DictPacker import multiple_replace
        from JEMlib.utils import Utils
        from JEMlib import VERSION as JEM_VERSION
    except Exception, e:
        initError = "Wrong JEM_PACKAGEPATH specified. Could not find JEM library. (%s)" % e

    if initError == None:
        logger.debug("Using JEM from: " + JEM_PACKAGEPATH)
        try:
            # disable warnings to avoid startup-annoyance in Ganga.
            lvl = logger.level
            logger.setLevel(logging.ERROR)

            # import JEM 0.3 stuff
            import JEM as JEMmain
            runner = JEMmain.setup(logger=getLogger, logprefix="GangaJEM.Lib.JEM", allconfig=True)

            logger.setLevel(lvl)
        except:
            ei = sys.exc_info()
            initError = "Failed to setup JEMs runtime: " + str(ei[0]) + " - " + str(ei[1])
            import traceback
            initError += "\n" + str(traceback.format_tb(ei[2]))

    # if some error occured during initialization, disable JEM monitoring.
    if initError != None:
        raise Exception(initError)

    INITIALIZED = True
except Exception, err:
    if len(err.args) > 0 and err.args[0] == "disabled":
        outlogger.info("The Job Execution Monitor is disabled by config.")
    else:
        outlogger.warn("unable to initialize the Job Execution Monitor module - realtime job monitoring will be disabled.")
        outlogger.warn("reason: " + ": " + str(sys.exc_info()[1]))
        outlogger.debug("trace: " + str(traceback.extract_tb(sys.exc_info()[2])))
