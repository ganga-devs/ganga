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

from Ganga.GPIDev.Credentials2 import credential_store
from Ganga.GPIDev.Credentials2.exceptions import InvalidCredentialError

_allShells = {}

logger = getLogger()

def getShell(cred_req=None):
    """
    Utility function for getting Grid Shell.
    Caller should take responsibility of credential checking if proxy is needed.

    Arguments:

     proxy - the credential requirement object
    """

    if cred_req is not None:
        if not credential_store[cred_req].is_valid():
            logger.info('GridShell.getShell given credential which is invalid')
            raise InvalidCredentialError()

    if cred_req in _allShells.keys():
        return _allShells[cred_req]

    values = {}
    for key in ['X509_CERT_DIR', 'X509_VOMS_DIR']:
        try:
            values[key] = os.environ[key]
        except KeyError:
            pass

    config = getConfig('LCG')

    key = 'GLITE_SETUP'

    # 1. check if the GLITE_SETUP is changed by user -> take the user's value as session value
    # 2. else check if GLITE_LOCATION is defined as env. variable -> do nothing (ie. create shell without any lcg setup)
    # 3. else take the default GLITE_SETUP as session value

    MIDDLEWARE_LOCATION = 'GLITE_LOCATION'

    if config.getEffectiveLevel(key) == 2 and MIDDLEWARE_LOCATION in os.environ:
        s = Shell()
    else:
        if os.path.exists(config[key]):
            s = Shell(config[key])
        else:
            logger.error("Configuration of GLITE:")
            logger.error("File not found: %s" % config[key])
            return None

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

    if cred_req is not None:
        s.env['X509_USER_PROXY'] = credential_store[cred_req].location

    _allShells[cred_req] = s

    return s
