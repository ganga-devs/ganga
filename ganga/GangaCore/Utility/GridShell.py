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

from GangaCore.Utility.Shell import Shell
from GangaCore.Utility.Config import getConfig, ConfigError
from GangaCore.Utility.logging import getLogger

from GangaCore.GPIDev.Credentials import credential_store
from GangaCore.Core.exceptions import InvalidCredentialError

_allShells = {}

logger = getLogger()

def constructShell():
    """
    Construct a grid shell based upon either the GLITE_SETUP or GLITE_LOCATION as possibly defined by the user
    """

    values = {}
    for key in ['X509_CERT_DIR', 'X509_VOMS_DIR']:
        try:
            values[key] = os.environ[key]
        except KeyError:
            pass

    config = getConfig('LCG')

    # 1. check if the GLITE_SETUP is changed by user -> take the user's value as session value
    # 2. else check if GLITE_LOCATION is defined as env. variable -> do nothing (ie. create shell without any lcg setup)
    # 3. else take the default GLITE_SETUP as session value

    if config.getEffectiveLevel('GLITE_SETUP') == 2 and 'GLITE_LOCATION' in os.environ:
        s = Shell()
    else:
        if os.path.exists(config['GLITE_SETUP']):
            s = Shell(config['GLITE_SETUP'])
        else:
            logger.error("Configuration of GLITE for LCG: ")
            logger.error("File not found: %s" % config['GLITE_SETUP'])
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

    return s

def getShell(cred_req=None):
    """
    Utility function for getting Grid Shell.

    If a cred_req is given then the grid shell which has been cached for this credential requirement is returned.
    If a cred_req is given an the credential doesn't exist in the credential_store then an InvalidCredentialError exception is raised

    If no cred_req is given then a grid shell is contructed based upon either the GLITE_SETUP or GLITE_LOCATION as possibly defined by the user
        THERE IS NO CACHING MADE HERE IN THIS CASE!!!

    Arguments:
        cred_req (ICredentialRequirement): This is the credential requirement required.
    """

    if cred_req is not None:
        if not credential_store[cred_req].is_valid():
            logger.info('GridShell.getShell given credential which is invalid')
            raise InvalidCredentialError()

        if cred_req in _allShells.keys():
            return _allShells[cred_req]

    constructed_shell = constructShell()

    if cred_req is not None:
        constructed_shell.env['X509_USER_PROXY'] = credential_store[cred_req].location

    _allShells[cred_req] = constructed_shell

    return constructed_shell
