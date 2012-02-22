###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: NGShell.py,v 1.1 2008-07-17 16:41:29 moscicki Exp $
###############################################################################
#
# NG backend shell function
#
#
# Date:   August 2006
import os, sys, re, tempfile
from types import *

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *

from Ganga.Utility.Shell import Shell
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import isStringLike

# Not presently used - will get back to this:
default_setup = {'ARC':'/home/scratch/katarzp/nordugrid-arc-standalone-0.5.56/arc_env.sh'}

def getShell(middleware='ARC'):
    ''' Utility function for getting Grid Shell.
        Caller should take responsiblity of credential checking if proxy is needed.'''

    config = getConfig('NG')

    s = None

    key = '%s_SETUP' % middleware

    try:
        # Have we set config['ARC_SETUP'] already?
        # We may want to do this in the future, to allow the user her own arc installation
        s   = Shell(config[key])
    except Ganga.Utility.Config.ConfigError:
        # If not, we should have ARC_LOCATION set from the external packages
        if os.environ.has_key('%s_LOCATION' % middleware):
            logger.debug('ARC location is %s' % os.environ.get('%s_LOCATION' % middleware))
            #print "ARC_LOCATION OK"
            s = Shell()
            config[key] = ''
        # Otherwise, return an error for now
        else :
            logger.warning("Can't find ARC middleware. Please source ARC setup which sets the environment variable NORDUGRID_LOCATION.")
            config[key] = ''
        #An option for default_setup defined above - not used for now
        #elif os.path.exists(default_setup[middleware]):
        #    s = Shell(default_setup[middleware])
        #    config[key] = default_setup[middleware]
    return s
