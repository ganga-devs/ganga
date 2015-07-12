##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: __init__.py,v 1.1 2008-07-17 16:40:53 moscicki Exp $
##########################################################################
# File: Credentials/__init__.py
# Author: K.Harrison
# Created: 060613
#
# 08/08/2006 KH: Added getCredential() function
#
# 28/08/2006 KH: Don't determine available credentials from allPlugins,
#                as credential plugins are now defined as hidden
#
# 31/08/2006 KH: Corrections to getCredential() function to create
#                only a single instance of each credential type
#
# 23/11/2006 KH: Added check on credential availability with
#                system/configuration used
#
# 25/09/2007 KH:  Changes for compatibility with multi-proxy handling
#                 => "middleware" argument added to getCredential function
#
# 02/11/2007 KH:  Added argument to getCredential() function to allow
#                 or supress creation of new credential

"""Initialisation file for the Credentials package,
   containing classes for working with different types of credential."""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "25 September 2007"
__version__ = "1.5"

from Ganga.GPIDev.Credentials.AfsToken import AfsToken
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Plugin import allPlugins
from Ganga.GPIDev.Credentials.GridProxy import GridProxy

_credentialPlugins = {}
for item in locals().keys():
    if ((hasattr(locals()[item], "_category"))
            and (hasattr(locals()[item], "_name"))):
        _category = getattr(locals()[item], "_category")
        if "credentials" == _category:
            _name = getattr(locals()[item], "_name")
            _credentialPlugins[_name] = locals()[item]
            del _name
        del _category

_allCredentials = {}
_voidCredentials = {}

logger = getLogger()


def getCredential(name="", middleware="", create=True):
    """
    Function to return credential object of requested type

    Arguments:
       middleware - String specifying any middleware used with credential
       name       - String specifying credential type
       create     - Boole specifying:
                    True  - requested credential should be created if
                            it doesn't exist
                    False - no new credential should be created

    Return value: Credential object of requested type if it exists or
                  can be created, or False otherwise
    """

#  if name in allPlugins.allClasses( "credentials" ).keys():
#     if not name in _allCredentials.keys():
#        _allCredentials[ name ] = \
#           allPlugins.find( "credentials", name )._proxyClass()
    if name in _credentialPlugins.keys():
        if ( not name in _allCredentials.keys() ) and \
           ( not name in _voidCredentials.keys() ) and \
           (create is True):
            credential = _credentialPlugins[name](middleware)
            if credential.isAvailable():
                _allCredentials[name] = credential
            else:
                _voidCredentials[name] = credential
    else:
        logger.warning("Credential type '%s' not known" % str(name))
        logger.warning("Returning False")

    if name in _allCredentials.keys():
        credential = _allCredentials[name]
    else:
        credential = False

    return credential
