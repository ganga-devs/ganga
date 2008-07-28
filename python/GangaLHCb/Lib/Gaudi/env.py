#!/usr/bin/env python
#----------------------------------------------------------------------------
# Name:         env.py
# Purpose:      Set up environment for LHCb packages.
#
# Author:       Alexander Soroko
#
# Created:      09/04/2003
# Modified:     01/07/2008, Greig Cowan
#----------------------------------------------------------------------------

from Ganga.Utility.Shell import Shell
import os
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

def _setenv(gaudiapp):
    # generate shell script
    pack = gaudiapp.appname
    ver = gaudiapp.version
    usestr = ' '
    os.environ['CMTPATH'] = expandfilename(gaudiapp.user_release_area)

    logger.debug('CMTPATH before SetupProject: ' + str(os.environ['CMTPATH']))

    if gaudiapp.masterpackage:
        (pack,alg,ver) = gaudiapp._parseMasterPackage()
        usestr = '''--use='%s %s %s' ''' % (str(alg), str(ver), str(pack))
    setupstr = '$LHCBHOME/scripts/SetupProject.sh  --set-CMTPATH  --ignore-missing ' \
                + usestr + str(gaudiapp.appname) + ' ' + str(gaudiapp.version)
    logger.debug(setupstr)
    s = Shell( setup = setupstr)
    logger.debug("CMTPATH of the cached environment: %s", s.env['CMTPATH'])
    import pprint
    logger.debug(pprint.pformat(s.env))
    return s
