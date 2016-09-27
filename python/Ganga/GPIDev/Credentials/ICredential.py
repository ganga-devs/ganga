##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ICredential.py,v 1.6 2009/05/20 13:40:22 moscicki Exp $
##########################################################################
#
# File: ICredential.py
# Author: K. Harrison
# Created: 060607
#
# 06/07/2006 KH:  Changed to Ganga.Utility.Shell for shell commands
#                 Redefined create and renew methods
#
# 07/08/2006 KH:  Added isValid() method
#
# 08/08/2006 KH:  Corrected errors in methods isValid() and timeInSeconds()
#                 => thanks to HCL and CLT
#                 Make shell an ICredential property
#
# 06/09/2006 KH:  Changes to allow minimum validity of credential
#                 and validity at creation to be specified independently
#                 Changes for monitoring component from CLT
#
# 25/09/2006 KH:  Changed method isValid(), so that default validity is
#                 value of self.minValidity
#
# 10/10/2006 KH:  Made changes to warnings for credential renewal
#
# 23/11/2006 KH:  Added "pipe" keyword to option dictionary of ICommandSet
#                 Added method to determine if credential is available
#                 with system/configuration used
#                 (requests from CLT)
#
# 28/02/2007 CLT: Modified GUI-specific and CLI-specific routines in create(),
#                 to reflect the use of currentOpts dictionary when building
#                 command options
#                 GUI-Specific code in create() updated to make use of the new
#                 Credentials Manager panel in the GUI
#
# 27/07/2007 AM:  Updated the renew() method to notify the
#                 InternalServices.Coordinator when credentials are
#                 detected by the monitoring loop as being invalid
#                 so the internal services to be disabled
#
# 07/12/2007 KH:  Made ICommandSet a component class
#
# 17/12/2007 KH:  Added function registerCommandSet for registering plugins
#                 of category "credential_commands"
#
# 13/03/2008 KH:  Update for change in configuration system
#                 (use "defaults_" instead of "_Properties")
#
# 18/03/2009 MWS: Added the 'log' option to isValid and shifted the expire
#                 message here
#
# 15/10/2009 MWS: Added possibility to force new check of credentials,
#                 rather than relying on cache

"""Module defining interface class for working with credentials"""

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "05 November 2009"
__version__ = "1.11"

import os
import threading
import time
import tempfile

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility.Config import ConfigError, getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Plugin.GangaPlugin import allPlugins
from Ganga.Utility.Shell import Shell

from Ganga.Core.InternalServices import Coordinator

logger = getLogger()
logTimeStamp = 0.0
logRepeatDuration = 120.0


def registerCommandSet(commandClass=None):

    try:
        pluginLoaded = allPlugins.all_dict\
            ["credential_commands"][commandClass._name]
    except KeyError:
        allPlugins.add(commandClass, "credential_commands", commandClass._name)

    return None


class ICommandSet(GangaObject):

    """
    Class used to define shell commands and options for working with credentials
    """
    _schema = Schema(Version(1, 0), {
        "init": SimpleItem(defvalue="",
                           doc="Command for creating/initialising credential"),
        "info": SimpleItem(defvalue="",
                           doc="Command for obtaining information about credential"),
        "destroy": SimpleItem(defvalue="",
                              doc="Command for destroying credential"),
        "init_parameters": SimpleItem(defvalue={},
                                      doc="Dictionary of parameter-value pairs to pass to init command"),
        "destroy_parameters": SimpleItem(defvalue={},
                                         doc="Dictionary of parameter-value pairs to pass to destroy command"),
        "info_parameters": SimpleItem(defvalue={},
                                      doc="Dictionary mapping from Ganga credential properties to command-line options"),
    })

    _category = "credential_commands"
    _name = "ICommandSet"
    _hidden = 1
    _enable_config = 1

    def __init__(self):
        super(ICommandSet, self).__init__()
        return

    def setConfigValues(self):
        """
        Update properties using values from relevant section of config file.
        """
        section = "defaults_%s" % self._name
        config = getConfig(section)
        for attribute in self._schema.datadict.keys():
            try:
                value = config[attribute]
                try:
                    value = eval(value)
                except:
                    pass
                setattr(self, attribute, value)
            except ConfigError:
                pass

registerCommandSet(ICommandSet)


class ICredential(GangaObject):

    """
    Interface class for working with credentials
    """

    _schema = Schema(Version(1, 0), {
        "maxTry": SimpleItem(defvalue=1, typelist=[int],
                             doc="Number of password attempts allowed when creating credential"),
        "minValidity": SimpleItem(defvalue="00:15", typelist=[str],
                                  doc="Default minimum validity"),
        "validityAtCreation": SimpleItem(defvalue="24:00", typelist=[str],
                                         doc="Default credential validity at creation"),
        "command": ComponentItem(category="credential_commands",
                                 defvalue="ICommandSet",
                                 doc="Set of commands to be used for credential-related operations")
    })

    _category = "credentials"
    _name = "ICredential"
    _hidden = 1

    _exportmethods = ["create", "destroy", "isAvailable", "isValid",
                      "location", "renew", "timeleft"]

    def __init__(self):
        super(ICredential, self).__init__()
        self.shell = Shell()
        self.inputPW_Widget = None
        return

    def create(self, validity="", maxTry=0, minValidity="", check=False):
        """
        Create credential.

        Arguments other than self:
           validity    - Validity with which credential should be created,
                         specified as string of format "hh:mm"
                         [ Defaults to value of self.validityAtCreation ]
           maxTry      - Number of password attempts allowed
                         [ Defaults to value of self.maxTry ]
           minValidity - Minimum validity in case checking of
                         pre-existing credential is performed,
                         specified as strong of format "hh:mm"
                         [ Defaults to value of self.minValidity ]
           check       - Flag to request checking of pre-existing
                         credential; if flag is set to true, then new
                         credential is created only if the validity of
                         any pre-existing credential is less than the
                         value of minValidity
                         [ Default: False ] 

        Note: create is the same as renew, except for the default value of check

        Return value: True if credential is created successfully, and False
        otherwise.
        """
        global logTimeStamp

        dummy = False
        if not self.command.init:
            dummy = True
        if "valid" in self.command.init_parameters:
            if not self.command.init_parameters["valid"]:
                dummy = True

        if dummy:
            logger.warning("Dummy CommandSet used - no credential created")
            return False

        if not maxTry:
            maxTry = self.maxTry

        if not minValidity:
            minValidity = self.minValidity

        if not validity:
            validity = self.validityAtCreation

        validityInSeconds = self.timeInSeconds(validity)

        if not validityInSeconds:
            logger.warning("Problems with requested validity: %s" % str(validity))
            return False
        if check and self.isValid(minValidity):
            return True

        ntry = 0

        while ntry < maxTry:

            ntry = ntry + 1
            # Test if GUI widget is to be used.
            if self.inputPW_Widget:
                # Since self.inputPW_Widget is called, current arguments are
                # ignored since renew() and create() in GUI mode will not be
                # called with any arguments.
                #proxy_obj = self._proxyObject ## This is removed to get rid of ref to _proxyObject
                proxy_obj = self
                if self.inputPW_Widget.ask(proxy_obj):
                    logger.dg("Proceeding to retrieve password from inputPW_Widget.")
                    __pw = self.inputPW_Widget.getPassword(proxy_obj)
                    if not __pw:
                        logger.warning("Password/passphrase expected!")
                        return False
                    try:
                        tFile = tempfile.NamedTemporaryFile()
                        tFile.write(__pw)
                        tFile.flush()
                    except:
                        del __pw
                        logger.warning("Could not create secure temporary file for password!")
                        return False
                    del __pw
                else:
                    # Current credential modification denied for various reasons.
                    # see GangaGUI.customDialogs.ask() method for more details.
                    return False
                # self.inputPW_Widget.ask() may have modified parameters.
                # Calling buildOpts() to take them into account.
                self.buildOpts(self.command.init, False)
                # Create initialisation list with the 'pipe' parameter
                initList = [self.command.init, self.command.init_parameters["pipe"]]
                # Append option value pairs
                for optName, optVal in self.command.currentOpts.iteritems():
                    initList.append("%s %s" % (optName, optVal))
                status = self.shell.system("cat %s|%s" % (tFile.name, " ".join(initList)))
                tFile.close()
                # self.inputPW_Widget dialog postprocessing.
                # E.g. disable autorenew mechanism if status != 0.
                self.inputPW_Widget.renewalStatus(proxy_obj, status)
                if status == 0:
                    logger.info("%s creation/renewal successful." % self._name)
                    return True
                else:
                    logger.warning("%s creation/renewal failed [%s]." % (self._name, status))
                    return False
            else:  # Non-GUI credential renewal/creation
                # Check if renewal is from main process (i.e. by bootstrap or
                # user)
                if threading.currentThread().getName() == 'MainThread' or\
                        threading.currentThread().getName().startswith('GANGA_Update_Thread_Ganga_Worker_'):
                    if "valid" in self.command.init_parameters:
                        self.command.currentOpts[self.command.init_parameters['valid']] = validity
                    initList = [self.command.init]
                    # Append option value pairs
                    for optName, optVal in self.command.currentOpts.iteritems():
                        initList.append("%s %s" % (optName, optVal))
                    status = self.shell.system(" ".join(initList))
                    if status == 0:
                        logger.info("%s creation/renewal successful." % self._name)
                        return True
                    else:
                        logger.warning("%s creation/renewal failed [%s]." % (self._name, status))
                # create initiated from worker thread from monitoring
                # component.
                else:
                    currTime = time.time()
                    if currTime - logTimeStamp >= logRepeatDuration:
                        logTimeStamp = currTime

                        # Check validity but print logging messages this time
                        self.isValid("", True)
                        _credentialObject = self._name[0].lower() + self._name[1:]
                        logger.warning("Renew by typing '%s.renew()' at the prompt." % (_credentialObject))

                        # notify the Core that the credential is not valid
                        _validity = self.timeInSeconds(self.timeleft())
                        _minValidity = self.timeInSeconds(minValidity) / 2.
                        if _validity <= max(120, _minValidity):
                            Coordinator.notifyInvalidCredential(self)

                    return True

        logger.warning("%s creation/renewal attempts exceeded %s tries!" % (self._name, maxTry))
        return False

    def destroy(self, allowed_exit=[0]):
        """
        Destroy credential

        Argument other than self:
           allowed_exit - List of exit codes accepted without error
                          when issuing system command for destroying credential

        Return value: False if command for destroying credential is undefined,
                      or True otherwise
        """

        if not self.command.destroy:
            logger.warning("Dummy CommandSet used - no credential created")
            return False

        destroyList = [self.command.destroy]
        for optName, optVal in self.command.destroyOpts.iteritems():
            destroyList.append("%s %s" % (optName, optVal))

        Coordinator.notifyInvalidCredential(self)

        status, output, message = \
            self.shell.cmd1(" ".join(destroyList), allowed_exit)
        proxyPath = self.location()
        if proxyPath:
            os.remove(proxyPath)
        return True

    def isAvailable(self):
        """
        Check whether credential is available with system/configuration used

        No arguments other than self

        Return value: True if credential is available, false otherwise
        """

        logger.warning("Dummy method used - this always returns True")

        return True

    def isValid(self, validity="", log=False, force_check=False):
        """
        Check validity

        Arguments other than self:
           validity    - Minimum time for which credential should be valid,
                         specified as string of format "hh:mm"
                         [ Defaults to valud of self.minValidity ]

           log         - Print logger messages if credential not valid 

           force_check - Force credential check, rather than relying on cache

        Return value: True if credential is valid for required time, False
        otherwise.
        """

        valid = True

        if not validity or validity is None:
            validity = self.minValidity
        validityInSeconds = self.timeInSeconds(validity)
        timeleft = self.timeleft(force_check=force_check)

        if not timeleft:
            valid = False
        else:
            timeleftInSeconds = self.timeInSeconds(timeleft)
            if timeleftInSeconds <= validityInSeconds:
                valid = False

        if not valid and log:
            _tl = self.timeleft(force_check=force_check)
            if _tl == "-1" or _tl == "0:00:00":
                _expiryStatement = "has expired!"
            else:
                _expiryStatement = "will expire in %s!" % _tl

            itemList = []
            text = self._name[0]
            for i in range(len(self._name) - 1):
                character = self._name[i + 1]
                if character.isupper():
                    itemList.append(text)
                    text = character.lower()
                else:
                    text = "".join([text, character])
            itemList.append(text)
            _credentialName = " ".join(itemList)

            logger.warning("%s %s" %
                           (_credentialName, _expiryStatement))

        return valid

    def location(self):
        """
        Determine credential location

        No arguments other than self

        Return value: Path to credential if found, or empty string otherwise
        """

        return ""

    def renew(self, validity="", maxTry=0, minValidity="",
              check=True):
        """
        Renew credential.

        Arguments other than self:
           validity    - Validity with which credential should be created,
                         specified as string of format "hh:mm"
                         [ Defaults to value of self.validityAtCreation ]
           maxTry      - Number of password attempts allowed
                         [ Defaults to value of self.maxTry ]
           minValidity - Minimum validity in case checking of
                         pre-existing credential is performed,
                         specified as strong of format "hh:mm"
                         [ Defaults to value of self.minValidity ]
           check       - Flag to request checking of pre-existing
                         credential; if flag is set to true, then new
                         credential is created only if the validity of
                         any pre-existing credential is less than the
                         value of minValidity
                         [ Default: True ] 

        Note: renew is the same as create, except for the default value of check

        Return value: True if new credential is created successfully, and False
        otherwise.
        """
        status = self.create(validity, maxTry, minValidity, check)

        return status

    def timeInSeconds(self, timeString=""):
        """
        Convert time string to time in seconds

        Arguments other than self:
           timeString - Time specified as string of format "hh:mm:ss"

        Return value: Time in seconds (integer)
        """

        totalTime = 0
        timeList = timeString.split(":")
        if len(timeList) >= 1:
            totalTime = totalTime + int(timeList[0]) * 60 * 60
        if len(timeList) >= 2:
            totalTime = totalTime + int(timeList[1]) * 60
        if len(timeList) >= 3:
            totalTime = totalTime + int(timeList[2])

        return totalTime

    def timeleft(self, units="hh:mm:ss", force_check=False):
        """
        Check time for which credential is valid.

        Arguments other than self:
           units       - String specifying units in which time is returned

           force_check - Force credential check, rather than relying on cache

        Allowed values for units are:
           "hours"              - time returned as in hours
           "minutes"            - time returned in minutes
           "seconds"            - time returned in seconds
           "hh:mm:ss" [default] - time returned as hours, minutes seconds


        Return value: Credential validity as string giving time in requested
           units, or empty string if command for querying credential validity
           is unavailable
        """

        timeRemaining = self.timeleftInHMS(force_check=force_check)
        if timeRemaining not in ["", "-1"]:
            if units in ["hours", "minutes", "seconds"]:
                timeleftInSeconds = self.timeInSeconds(timeRemaining)
                if "seconds" == units:
                    timeRemaining = "%.2f" % (timeleftInSeconds)
                elif "minutes" == units:
                    timeRemaining = "%.2f" % (timeleftInSeconds / 60.)
                elif "hours" == units:
                    timeRemaining = "%.2f" % (timeleftInSeconds / (60. * 60.))

        return timeRemaining

    def timeleftInHMS(self, force_check=False):
        """
        Determine remaining validity of credential in hours, minutes and seconds

        Argument other than self:
           force_check - Force credential check, rather than relying on cache

        Return value: String giving credential validity, or empty string
           if command for querying credential validity is unavailable
        """
        logger.warning("Dummy method used - no information returned")
        return ""
