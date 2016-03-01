##########################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: GridShell.py,v 1.1 2008-07-17 16:41:00 moscicki Exp $
#
# Copyright (C) 2003 The Ganga Project
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
# LCG backend
#
# ATLAS/ARDA
#
# Date:   August 2006
##########################################################################

import os

from Ganga.Utility.Shell import Shell
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

_allShells = {}


def getShell(middleware='EDG', force=False):
    """
    Utility function for getting Grid Shell.
    Caller should take responsiblity of credential checking if proxy is needed.

    Argumennts:

     middleware - grid m/w used 
     force      - False : if the shell already exists in local cache return the previous created instance
                  True  : recreate the shell and if not None update the cache
    """

    logger = getLogger()

    if not middleware:
        logger.debug('No middleware specified, assuming default EDG')
        middleware = 'EDG'

    if middleware in _allShells.keys() and not force:
        return _allShells[middleware]

    values = {}
    for key in ['X509_USER_PROXY', 'X509_CERT_DIR', 'X509_VOMS_DIR']:
        try:
            values[key] = os.environ[key]
        except KeyError:
            pass

    configname = ""
    if middleware == 'EDG' or middleware == 'GLITE':
        configname = 'LCG'
    else:
        configname = middleware

    config = None
    try:
        config = getConfig(configname)
    except:
        logger.warning(
            '[%s] configuration section not found. Cannot set up a proper grid shell.' % configname)
        return None

    s = None

    key = '%s_SETUP' % middleware

    # 1. check if the *_SETUP is changed by user -> take the user's value as session value
    # 2. else check if *_LOCATION is defined as env. variable -> do nothing (ie. create shell without any lcg setup)
    # 3. else take the default *_SETUP as session value

    MIDDLEWARE_LOCATION = '%s_LOCATION' % middleware

    if config.getEffectiveLevel(key) == 2 and MIDDLEWARE_LOCATION in os.environ:
        s = Shell()
    else:
        if os.path.exists(config[key]):
            # FIXME: Hardcoded rule for ARC middleware setup (pass explicitly
            # the $ARC_LOCATION as argument), this is hardcoded to maintain
            # backwards compatibility (and avoid any side effects) for EDG and
            # GLITE setup scripts which did not take any arguments
            if key.startswith('ARC') and MIDDLEWARE_LOCATION in os.environ:
                s = Shell(
                    config[key], setup_args=[os.environ[MIDDLEWARE_LOCATION]])
            else:
                s = Shell(config[key])
        else:
            logger.warning("Configuration of %s for %s: " %
                           (middleware, configname))
            logger.warning("File not found: %s" % config[key])

    if s:
        for key, val in values.items():
            s.env[key] = val

        # check and set env. variables for default LFC setup
        if 'LFC_HOST' not in s.env:
            try:
                s.env['LFC_HOST'] = config['DefaultLFC']
            except ConfigError:
                pass

        if 'LFC_CONNTIMEOUT' not in s.env:
            s.env['LFC_CONNTIMEOUT'] = '20'

        if 'LFC_CONRETRY' not in s.env:
            s.env['LFC_CONRETRY'] = '0'

        if 'LFC_CONRETRYINT' not in s.env:
            s.env['LFC_CONRETRYINT'] = '1'

        _allShells[middleware] = s

    return s
