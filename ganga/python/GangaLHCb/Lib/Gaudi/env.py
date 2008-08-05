#!/usr/bin/env python2
#----------------------------------------------------------------------------
# Name:         env.py
# Purpose:      Set up environment for LHCb packages.
#
# Author:       Alexander Soroko
#
# Created:      09/04/2003
#----------------------------------------------------------------------------

from Ganga.Utility.Shell import Shell
import os
import tempfile
import shutil
import os
from Ganga.Utility.files import expandfilename
from threading import Thread
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

#############################################################################

def _setenv(gaudiapp):

    # generate shell script
    pack=gaudiapp.appname
    ver=gaudiapp.version

    import tempfile
    fd=tempfile.NamedTemporaryFile()
    script = '#!/bin/sh\n'
    script +='User_release_area=%s; export User_release_area\n' % \
             expandfilename(gaudiapp.user_release_area)
    script +='. $LHCBHOME/scripts/SetupProject.sh ' + pack + " " + ver + '\n'
    fd.write(script)
    fd.flush()

    setupstr='$LHCBHOME/scripts/SetupProject.sh ' + pack + " " + ver
    logger.debug(script)
    s=Shell(setup=fd.name)
    import pprint
    logger.debug(pprint.pformat(s.env))
    return s

#############################################################################

def setenv( gaudiapp ):
   _t = Thread( target = _setenv, args = (gaudiapp,) )
   _t.start()
   _t.join()
