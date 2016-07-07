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

shell_cache = None


def getShell(force=False):
    """
    Utility function for getting Grid Shell.
    Caller should take responsibility of credential checking if proxy is needed.

    Arguments:
       force (bool): False: if the shell already exists in local cache return the previous created instance ; True: recreate the shell and if not None update the cache
    """

    global shell_cache

    logger = getLogger()

    if shell_cache is not None and not force:
        return shell_cache

    values = {}
    for key in ['X509_USER_PROXY', 'X509_CERT_DIR', 'X509_VOMS_DIR']:
        try:
            values[key] = os.environ[key]
        except KeyError:
            pass

    try:
        config = getConfig('LCG')
    except:
        logger.warning('[LCG] configuration section not found. Cannot set up a proper grid shell.')
        return None

    s = None

    # 1. check if the GLITE_SETUP is changed by user -> take the user's value as session value
    # 2. else check if GLITE_LOCATION is defined as env. variable -> do nothing (ie. create shell without any lcg setup)
    # 3. else take the default GLITE_SETUP as session value

    if config.getEffectiveLevel('GLITE_SETUP') == 2 and 'GLITE_LOCATION' in os.environ:
        s = Shell()
    else:
        if os.path.exists(config['GLITE_SETUP']):
            s = Shell(config['GLITE_SETUP'])
        else:
            if config['GLITE_ENABLE']:
                logger.warning("Configuration of GLITE for LCG: ")
                logger.warning("File not found: %s" % config['GLITE_SETUP'])

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

        shell_cache = s

    return s
