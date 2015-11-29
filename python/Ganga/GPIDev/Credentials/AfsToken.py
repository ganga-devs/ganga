##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AfsToken.py,v 1.4 2009/03/18 18:28:15 karl Exp $
##########################################################################
#
# File: AfsToken.py
# Author: K. Harrison
# Created: 060608
#
# 06/07/2006 KH:  Changed to Ganga.Utility.Shell for shell commands
#
# 07/08/2006 KH:  Added isValid() method
#
# 08/08/2006 KH:  Modified for shell being made an ICredential property
#
# 25/08/2006 KH:  Declare AfsToken class as hidden
#
# 06/09/2006 KH:  Argument minValidity added to methods create() and renew()
#
# 25/09/2006 KH:  Changed method isValid(), so that default validity is
#                 value of self.minValidity
#
# 23/11/2006 KH:  Added "pipe" keyword to option dictionary of AfsCommand
#                 Added method to determine if credential is available
#                 with system/configuration used
#                 (requests from CLT)
#
# 06/12/2006 KH:  Changed method timeleftInHMS(), to improve parsing
#                 of string returned by "tokens" command
#
# 15/02/2007 KH:  Changed method isAvailable(), to declare credential
#                 available only if ~/gangadir is on AFS, and the
#                 tokens command is found
#
# 28/02/2007 CLT: Replaced AfsCommand.options with dictionaries
#                 init_parameters, destroy_parameters, info_parameters,
#                 each providing independent options
#                 Added AfsCommand.currentOpts dictionary, to add
#                 flexibility and assist in option construction (as opposed
#                 to direct string manipulation)
#                 Added AfsToken.buildOpts(), to consolidate the option
#                 building functionality from create(), destroy() and info()
#
# 23/05/2007 KH:  Changed method isAvailable(), to also declare credential
#                 available if workspace, defined via ['FileWorkspace']topdir
#                 is on AFS, and the tokens command is found
#
# 24/05/2007 KH:  Changed method isAvailable(), to also declare credential
#                 available if local repository is used, and its location,
#                 defined via ['DefaultJobRepository']local_root
#                 is on AFS, and the tokens command is found
#
# 11/06/2007 KH:  Changed logic for checking that tokens command is found
#                 in method isAvailable()
#
# 25/09/2007 KH:  Additional argument (middleware) added to __init__ method
#                 of AfsToken, for compatibility with multi-proxy handling
#
# 08/12/2007 KH:  Changes to take into account ICommandSet being made
#                 a component class
#
# 17/12/2007 KH:  Made changes for handling of AfsTokenCommand as
#                 component class
#
# 28/02/2008 KH:  Include year information when calculating time to token
#                 expiry in timeleftInHMS() - avoid problems with leap years
#
# 28/02/2008 KH:  In isAvailable(), assume AFS is available if cell
#                 is defined
#
# 25/06/2008 KH: Remove separate checks on location of local workspace
#                and local repository, which in Ganga 5 are under directory
#                defined via ['Configuration']gangadir
#
# 02/07/2008 KH: Update to use requiresAfsToken() function of
#                Ganga.Runtime.Repository_runtime to determine
#                whether Ganga repository is on AFS
#
# 18/03/2009 MWS: Added the 'log' option to isValid()
#
#
# 18/03/2009 MWS: Added the 'force_check' argument to timeleft()
#                 and timeleftInMHS()
#

"""Module defining class for creating, querying and renewing AFS token"""
__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "05 November 2009"
__version__ = "1.16"

import os
import time

from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Credentials.ICredential import ICommandSet, ICredential, registerCommandSet
from Ganga.GPIDev.Schema import SimpleItem
from Ganga.Runtime import Repository_runtime
from Ganga.Utility.logging import getLogger

logger = getLogger()


class AfsCommand(ICommandSet):

    """
    Class used to define shell commands and options for working with AFS token
    """

    _schema = ICommandSet._schema.inherit_copy()
    _schema['init']._meta['defvalue'] = "kinit"
    _schema['info']._meta['defvalue'] = "tokens"
    _schema['destroy']._meta['defvalue'] = "unlog"
    _schema['init_parameters']._meta['defvalue'] = {"pipe": "-pipe", "valid": "-l",
                                                    "username": "-principal", "cell": "-cell"}
    _schema['destroy_parameters']._meta['defvalue'] = {"cell": "-cell"}

    _name = "AfsCommand"
    _hidden = 1
    _enable_config = 1

    def __init__(self):
        super(AfsCommand, self).__init__()
        self.currentOpts = {}
        self.infoOpts = {}
        self.destroyOpts = {}
        return

registerCommandSet(AfsCommand)


class AfsToken (ICredential):

    """
    Class for working with AFS token
    """

    _schema = ICredential._schema.inherit_copy()
    _schema.datadict["cell"] = SimpleItem(
        defvalue="", doc="AFS cell with which token is used [empty string implies local cell]")
    _schema.datadict["username"] = SimpleItem(defvalue="",
                                              doc="AFS username with which token is used [defaults to login id]")
    _name = "AfsToken"
    _hidden = 1
    _enable_config = 1

    def __init__(self, middleware=""):
        super(AfsToken, self).__init__()
        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
        if "ICommandSet" == self.command._name or isType(self.command, GangaList):
            self.command = AfsCommand()
        if not self.username:
            if "USERNAME" in os.environ:
                self.username = os.environ["USERNAME"]
        return

    # Populate the self.command.currentOpts dictionary with
    # AfsToken specific options.
    def buildOpts(self, command, clear=True):
        if command == self.command.init:
            if clear:
                self.command.currentOpts.clear()
            if self.username:
                self.command.currentOpts[
                    self.command.init_parameters['username']] = self.username
            if self.cell:
                self.command.currentOpts[
                    self.command.init_parameters['cell']] = self.cell
            if self.validityAtCreation:
                self.command.currentOpts[
                    self.command.init_parameters['valid']] = self.validityAtCreation
        elif command == self.command.destroy:
            if clear:
                self.command.destroyOpts.clear()
            if self.cell:
                self.command.destroyOpts[
                    self.command.destroy_parameters['cell']] = self.cell
        elif command == self.command.info:
            if clear:
                self.command.infoOpts.clear()

    def create(self, validity="", maxTry=0, minValidity="", check=False):
        self.buildOpts(self.command.init)
        return ICredential.create(self, validity, maxTry, minValidity, check)

    def destroy(self, allowed_exit=[0]):
        self.buildOpts(self.command.destroy)
        return ICredential.destroy(self, allowed_exit)

    def isAvailable(self):

        if self.cell:
            available = True
        else:
            available = False

        if not available:
            available = Repository_runtime.requiresAfsToken()

        if available:
            infoCommand = self.command.info.split()[0]
            available = False
            try:
                pathList = os.environ["PATH"].split(os.pathsep)
            except KeyError:
                pathList = []
            for searchDir in pathList:
                try:
                    fileList = os.listdir(searchDir)
                except OSError:
                    fileList = []
                if infoCommand in fileList:
                    available = True
                    break
            if available:
                logger.debug("Command '%s' found in directory '%s'" %
                             (infoCommand, searchDir))
            else:
                logger.debug("Unable to find command '%s'" % infoCommand)

        if available:
            timeleft = self.timeleft()
            if not timeleft:
                available = False

        return available

    def isValid(self, validity="", log=False, force_check=False):
        return ICredential.isValid(self, validity, log, force_check)

    def location(self):
        """
        Dummy method - returns empty string
        """
        return ""

    def renew(self, validity="", maxTry=0, minValidity="", check=True):
        return ICredential.renew(self, validity, maxTry, minValidity, check)

    def timeleft(self, units="hh:mm:ss", force_check=False):
        return ICredential.timeleft(self, units, force_check=force_check)

    def timeleftInHMS(self, force_check=False):

        localTuple = time.localtime()
        status, output, message = self.shell.cmd1(self.command.info)

        timeRemaining = "00:00:00"

        if status:
            if (1 + output.lower().find("command not found")):
                logger.warning("Command '" + self.command.info + "' not found")
                logger.warning("Unable to obtain information on AFS tokens")
                timeRemaining = ""

        if timeRemaining:
            timeString = ""
            lineList = output.split("\n")
            timeRemaining = "-1"
            for line in lineList:
                if (1 + line.lower().find("tokens for")):
                    elementList = line.rstrip("]").split("[")[1].split()
                    if self.cell:
                        afsString = "".join([" afs@", self.cell, " "])
                    else:
                        afsString = "afs@"
                    if not (1 + line.find(afsString)):
                        elementList = []
                    if len(elementList) > 1:
                        elementList.append(str(localTuple[0]))
                        timeString = " ".join(elementList[1:])
                        timeTuple = time.strptime(timeString, "%b %d %H:%M %Y")
                        timeList = list(timeTuple)
                        if localTuple[1] > timeTuple[1]:
                            timeList[0] = 1 + localTuple[0]
                        timeList[8] = localTuple[8]
                        timeTuple = tuple(timeList)
                        timeLeft = int(time.mktime(timeTuple) - time.time())
                        hours = timeLeft / (60 * 60)
                        minutes = (timeLeft - hours * 60 * 60) / 60
                        seconds = timeLeft - hours * 60 * 60 - minutes * 60
                        minString = str(minutes)
                        if len(minString) < 2:
                            minString = "0" + minString
                        secString = str(seconds)
                        if len(secString) < 2:
                            secString = "0" + secString
                        timeRemaining = "%s:%s:%s" % \
                            (str(hours), minString, secString)
                if (timeRemaining != "-1"):
                    break

        return timeRemaining

    # Add documentation strings from base class
    for method in \
            [create, destroy, isAvailable, isValid, renew, timeleft, timeleftInHMS]:
        if hasattr(ICredential, method.__name__):
            baseMethod = getattr(ICredential, method.__name__)
            setattr(method, "__doc__",
                    baseMethod.__doc__.replace("credential", "AFS token"))
