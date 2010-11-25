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
outlogger = getLogger("GangaJEM.Lib.JEM.info")
#---------
from Ganga.Utility.Config import getConfig
jemconfig = getConfig('JEM')
#-----------------------------------------------------------------------------------------------------------------------

# filter JEM's startup logging, unless we want to debug
JEM_LIBRARY_INIT_LOGLEVEL = logging.ERROR
try:
    if jemconfig['JEM_VERBOSE_LOADER_DEBUG']:
        JEM_LIBRARY_INIT_LOGLEVEL = logging.DEBUG
except:
    pass

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
        # set the env var to enable JEM to find itself...
        os.environ['JEM_PACKAGEPATH'] = JEM_PACKAGEPATH
    else:
        JEM_PACKAGEPATH = os.environ['JEM_PACKAGEPATH']

    # ...and prepend it to the python-path (priorizing it)
    if not JEM_PACKAGEPATH in sys.path:
        sys.path = [JEM_PACKAGEPATH] + sys.path

    # import JEM-Ganga-Integration module (that manages the rest of JEMs initialisation)
    initError = None
    os.environ["JEM_Global_mode"] = "Ganga"

    logger.debug("Using JEM from: " + JEM_PACKAGEPATH)
    try:
        lvl = logger.level
        logger.setLevel(JEM_LIBRARY_INIT_LOGLEVEL)

        # import JEM 0.3 stuff
        import JEM as JEMmain
        GangaJEM.library = JEMmain.setup(logger=getLogger, logprefix="GangaJEM.Lib.JEM", allconfig=True, skip_plugins=["Analysis"])
        
        logger.setLevel(lvl)
    except:
        ei = sys.exc_info()
        initError = "Failed to setup JEMs runtime: " + str(ei[0]) + " - " + str(ei[1])

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
