##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GridProxy.py,v 1.5 2009/03/25 15:43:35 karl Exp $
##########################################################################
#
# File: GridProxy.py
# Author: K. Harrison
# Created: 060519
#
# 06/07/2006 KH:  Changed to Ganga.Utility.Shell for shell commands
#                 Added voms support
#
# 02/08/2006 KH:  Modified GridProxy class to create one instance of
#                 VomsCommand and GridCommand
#
# 07/08/2006 KH:  Added isValid() method
#
# 09/08/2006 KH:  Use shell defined via Ganga.Lib.LCG.GridShell.getShell()
#
# 25/08/2006 KH:  Declare GridProxy class as hidden
#
# 06/09/2006 KH:  Argument minValidity added to methods create() and renew()
#
# 25/09/2006 KH:  Changed method isValid(), so that default validity is
#                 value of self.minValidity
#
# 13/11/2006 KH:  Added method info() for obtaining proxy information,
#                 and changed location() to use this method
#
# 23/11/2006 KH:  Added "pipe" keyword to option dictionaries of GridCommand
#                 and VomsCommand
#                 Added method to determine if credential is available
#                 with system/configuration used
#                 (requests from CLT)
#
# 28/02/2007 CLT: Replaced VomsCommand.options and GridCommand.options
#                 with dictionaries init_parameters, destroy_parameters,
#                 info_parameters, each providing independent options
#                 Added VomsCommand.currentOpts and GridCommand.currentOpts
#                 dictionaries, to add flexibility and assist in option
#                 construction (as opposed to direct string manipulation)
#                 Added GridProxy.buildOpts(), to consolidate the option
#                 building functionality from create(), destroy() and info()
#
# 02/03/2007 KH : Added method to determine user's identity
#                 (request from DL)
#
# 25/04/2007 KH : Modified GridProxy.identity method to be able to deal
#                 with new-style CERN certificates, with ambiguous CN definition
#
# 08/06/2007 KH : Added method GridProxy.voname, to allow name of
#                 virtual organisation to be determined from proxy
#
# 25/09/2007 KH:  Changes for compatibility with multi-proxy handling
#                 => "middleware" argument introduced
#
# 08/12/2007 KH:  Changes to take into account ICommandSet being made
#                 a component class
#
# 17/12/2007 KH:  Made changes for handling of GridCommand and VomsCommand as
#                 component classes
#
# 15/01/2008 KH:  Set initial null value for infoCommand in GridProxy.voname()
#
# 18/01/2008 KH : Modified GridProxy.identity method to disregard
#                 values of CN=proxy
#
# 27/02/2008 KH : Setup shell in GridProxy constructor, if middleware is defined
#
# 30/01/2009 KH : Added possibility to request that GridProxy.identity()
#                 returns string with non-alphanumeric characters stripped out
#
# 25/03/2009 KH : Correction to GridProxy.voname() to check that one-word
#                 VO name is returned
#
# 18/03/2009 MWS: Added the 'log' option to isValid
#                 Added method to retrieve the full identity as a dictionary
#                 Require consistency between VO in proxy and in configuration
#
# 15/10/2009 MWS: Added cache for proxy information
#
#
# 09/11/2009 MWS: Added check that proxy is valid before updating cache
#                 (addToProxyCache() method)
#
# 12/11/2009 MWS: Added additional checks that cached information
#                 is consistent with available proxy

"""Module defining class for creating, querying and renewing Grid proxy"""
__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "12 November 2009"
__version__ = "1.21"

import os
import re
import time

from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Credentials.ICredential import ICommandSet, ICredential, registerCommandSet
from Ganga.GPIDev.Schema import SimpleItem
from Ganga.Utility.logging import getLogger
from Ganga.Utility.GridShell import getShell

logger = getLogger()

allowed_exit_range = range(1000)


class GridCommand(ICommandSet):

    """
    Class used to define shell commands and options for working with Grid proxy
    """

    _schema = ICommandSet._schema.inherit_copy()
    _schema['init']._meta['defvalue'] = "grid-proxy-init"
    _schema['info']._meta['defvalue'] = "grid-proxy-info"
    _schema['destroy']._meta['defvalue'] = "grid-proxy-destroy"
    _schema['init_parameters']._meta['defvalue'] = {"pipe": "-pwstdin", "valid": "-valid"}
    _schema['destroy_parameters']._meta['defvalue'] = {}
    _schema['info_parameters']._meta['defvalue'] = {}

    _name = "GridCommand"
    _hidden = 1
    _enable_config = 1

    def __init__(self):
        super(GridCommand, self).__init__()

        self.currentOpts = {}
        self.infoOpts = {}
        self.destroyOpts = {}


class VomsCommand(ICommandSet):

    """
    Class used to define shell commands and options for working with Grid proxy,
    using VOMS extensions
    """

    _schema = ICommandSet._schema.inherit_copy()

    _schema['init']._meta['defvalue'] = "voms-proxy-init"
    _schema['info']._meta['defvalue'] = "voms-proxy-info"
    _schema['destroy']._meta['defvalue'] = "voms-proxy-destroy"
    _schema['init_parameters']._meta['defvalue'] = {"pipe": "-pwstdin", "valid": "-valid", "voms": "-voms"}
    _schema['destroy_parameters']._meta['defvalue'] = {}
    _schema['info_parameters']._meta['defvalue'] = {"vo": "-vo"}

    _name = "VomsCommand"
    _hidden = 1
    _enable_config = 1

    def __init__(self):
        super(VomsCommand, self).__init__()

        self.currentOpts = {}
        self.infoOpts = {}
        self.destroyOpts = {}

for commandSet in [GridCommand, VomsCommand]:
    registerCommandSet(commandSet)

# global proxy info cache
_infoCache = {}


class GridProxy(ICredential):

    """
    Class for working with Grid proxy
    """

    _schema = ICredential._schema.inherit_copy()
    _schema.datadict["voms"] = SimpleItem(defvalue="", doc="Virtual organisation managment system information")
    _schema.datadict["init_opts"] = SimpleItem(defvalue="", doc="String of options to be passed to command for proxy creation")
    _schema.datadict["info_refresh_time"] = SimpleItem(defvalue="00:15", doc="Refresh time of proxy info cache")
    _schema.datadict["maxTry"] = SimpleItem(defvalue=5, doc="Number of password attempts allowed when creating credential")
    _name = "GridProxy"
    _hidden = 1
    _enable_config = 1
    _exportmethods = ["create", "destroy", "identity", "info", "isAvailable",
                      "isValid", "location", "renew", "timeleft", "voname", "fullIdentity"]

    def __init__(self):
        super(GridProxy, self).__init__()
        self.shell = getShell()
        self.chooseCommandSet()
        return

    def chooseCommandSet(self):
        """
        Choose command set to be used for proxy-related commands

        No arguments other than self

        If self.voms has a null value then the GridCommand set of commands
        is used.  Otherwise the VomsCommand set of commands is used.

        Return value: None
        """
        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
        if self.voms:
            # Make sure we don't overwrite a VomsCommand if it's already there
            if not isinstance(self.command, VomsCommand):
                self.command = VomsCommand()
        else:
            self.command = GridCommand()

    # Populate the self.command.currentOpts dictionary with
    # GridProxy specific options.
    def buildOpts(self, command, clear=True):
        if command == self.command.init:
            if clear:
                self.command.currentOpts.clear()
            if "voms" in self.command.init_parameters:
                if self.voms:
                    self.command.currentOpts\
                        [self.command.init_parameters['voms']] = self.voms
            if "valid" in self.command.init_parameters:
                if self.validityAtCreation:
                    self.command.currentOpts\
                        [ self.command.init_parameters[ 'valid' ] ] \
                        = self.validityAtCreation
            if self.init_opts:
                self.command.currentOpts[''] = self.init_opts
        elif command == self.command.destroy:
            if clear:
                self.command.destroyOpts.clear()
        elif command == self.command.info:
            if clear:
                self.command.infoOpts.clear()

    def create(self, validity="", maxTry=0, minValidity="", check=False):
        self.chooseCommandSet()
        self.buildOpts(self.command.init)
        status = ICredential.create(self, validity, maxTry, minValidity, check)
        return status

    def destroy(self, allowed_exit=[0, 1]):
        self.chooseCommandSet()
        self.buildOpts(self.command.destroy)
        return ICredential.destroy(self, allowed_exit)

    def isAvailable(self):
        if self.shell:
            return True
        else:
            return False

    def isValid(self, validity="", log=False, force_check=False):

        # Do parent check
        if not ICredential.isValid(self, validity, log, force_check):
            return False

        # check vo names
        if self.voname() != self.voms:
            if log:
                logger.warning("Grid Proxy not valid. Certificate VO '%s' does not match requested '%s'"
                               % (self.voname(), self.voms))
            return False

        return True

    def location(self):

        proxyPath = self.info("-path").strip()

        if not os.path.exists(proxyPath):
            proxyPath = ""

        return proxyPath

    def fullIdentity(self, safe=False):
        """
        Return the users full identity as a dictionary

        Argument:
           safe - logical flag
                  =>  False : return identity exactly as obtained from proxy
                  =>  True  : return identity after stripping out
                              non-alphanumeric characters

        Return value: Dictionary of the various labels in the users DN
        """

        ele_dict = {}

        subjectList = self.info(opt="-identity").split("/")

        for subjectElement in subjectList:
            element = subjectElement.strip()
            if element.find("=") == -1:
                continue

            field, val = element.split("=")
            if safe:
                val = re.sub("[^a-zA-Z0-9]", "", val)
            ele_dict[field] = val

        return ele_dict

    def identity(self, safe=False):
        """
        Return user's identify

        Argument:
           safe - logical flag
                  =>  False : return identity exactly as obtained from proxy
                  =>  True  : return identity after stripping out
                              non-alphanumeric characters

        => The identity is determined from the user proxy if possible,
           or otherwise from the user's top-level directory

        Return value: String specifying user identity
        """

        cn = os.path.basename(os.path.expanduser("~"))
        try:
            subjectList = self.info(opt="-identity").split("/")
            subjectList.reverse()
            for subjectElement in subjectList:
                element = subjectElement.strip()
                try:
                    cn = element.split("CN=")[1].strip()
                    if cn != "proxy":
                        break
                except IndexError:
                    pass
        except Exception as err:
            logger.debug("Err: %s" % err)
            pass

        id = "".join(cn.split())
        if safe:
            id = re.sub("[^a-zA-Z0-9]", "", id)

        return id

    def info(self, opt="", force_check=False):
        """
        Obtain proxy information

        Arguments other than self:
           opt         - String of options to be used when querying
                         proxy information
                       => Help on valid options can be obtained using:
                          info( opt = "-help" )
           force_check - Force credential check, rather than relying on cache

        Return value: Output from result of querying proxy
        """

        # use cached version of this command call if possible
        output = self.getProxyCacheValue(opt)

        if (force_check) or (output == ""):
            self.chooseCommandSet()
            infoCommand = " ".join([self.command.info, opt])
            logger.debug("Executing info Command: %s" % infoCommand)
            status, output, message = self.shell.cmd1(cmd=infoCommand, allowed_exit=allowed_exit_range)

            self.addToProxyCache(status, output, opt)

        if not output:
            output = ""

        return str("%s" % output)

    def renew(self, validity="", maxTry=0, minValidity="", check=True):
        self.chooseCommandSet()
        if self.voms:
            if not self.voname():
                check = False
        return ICredential.renew(self, validity, maxTry, minValidity, check)

    def timeleft(self, units="hh:mm:ss", force_check=False):
        return ICredential.timeleft(self, units, force_check)

    def timeleftInHMS(self, force_check=False):
        global _infoCache

        output = self.getProxyCacheValue("timeleftInHMS")
        status = 0

        if (force_check) or (output == ""):
            # should really use the 'info' method
            self.chooseCommandSet()
            infoList = [self.command.info]
            # Append option value pairs
            for optName, optVal in self.command.infoOpts.iteritems():
                infoList.append("%s %s" % (optName, optVal))
            logger.debug("Executing timeHMS Command: %s" % " ".join(infoList))
            status, output, message = self.shell.cmd1\
                (cmd=" ".join(infoList), allowed_exit=allowed_exit_range)

            self.addToProxyCache(status, output, "timeleftInHMS")

        timeRemaining = "00:00:00"

        if status:
            if (1 + output.lower().find("command not found")):
                logger.warning("Command '" + self.command.info + "' not found")
                logger.warning("Unable to obtain information on Grid proxy")
                timeRemaining = ""
                if "timeleftInHMS" in _infoCache:
                    del _infoCache["timeleftInHMS"]

        if timeRemaining:
            lineList = output.split("\n")
            for line in lineList:
                if (1 + line.find("Couldn't find a valid proxy")):
                    timeRemaining = "-1"
                    if 'timeleftInHMS' in _infoCache:
                        del _infoCache['timeleftInHMS']
                    break
                elif (1 + line.find("timeleft")):
                    elementList = line.split()
                    timeRemaining = elementList[2]
                    break

        return timeRemaining

    def voname(self, force_check=False):
        """
        Obtain name of virtual organisation from proxy

        Argument other than self:
           force_check - Force credential check, rather than relying on cache

        Return value: Name of virtual organisation where this can be determined
        (voms proxy), or empty string otherwise (globus proxy)
        """
        global _infoCache
        output = self.getProxyCacheValue("voname")

        if (force_check) or (output == ""):
            self.chooseCommandSet()
            infoCommand = ""

            if "vo" in self.command.info_parameters:
                if self.command.info:
                    infoCommand = " ".join\
                        ([self.command.info,
                          self.command.info_parameters["vo"]])
            else:
                infoCommand = self.command.info

            if infoCommand:
                logger.debug("Executing voname Command: %s" % infoCommand)
                status, output, message = self.shell.cmd1(cmd=infoCommand,
                                                          allowed_exit=allowed_exit_range, capture_stderr=True)

                self.addToProxyCache(status, output, "voname")

            else:
                output = ""

        if not output:
            output = ""
            if "voname" in _infoCache:
                del _infoCache["voname"]

        output = output.strip()

        for error in ["VOMS extension not found", "unrecognized option"]:
            if output.find(error) != -1:
                output = ""
                if 'voname' in _infoCache:
                    del _infoCache['voname']
                break

        # Check for reasonable output (single-word VO)
        if len(output.split()) != 1:
            output = self.voms

        return output

    def getProxyCacheValue(self, opt):
        """
        Check the proxy cache for the required key. Make sure the proxy
        file is older than the last check.

        opt - the key to check for
        """

        global _infoCache

        info_refresh = self.timeInSeconds(self.info_refresh_time)
        output = ''
        path = ''

        # check when the grid proxy was created
        if '-path' not in _infoCache or (_infoCache['-path'][1] < (time.time() - info_refresh)):
            self.chooseCommandSet()
            infoCommand = " ".join([self.command.info, '-path'])
            logger.debug("Executing cache Command: %s" % infoCommand)
            status, output, message = self.shell.cmd1\
                (cmd=infoCommand, allowed_exit=allowed_exit_range)

            if not status:
                path = output
                self.addToProxyCache(status, output, '-path')

        else:
            path = _infoCache['-path'][0]

        path = path.strip()
        if not os.path.exists(path):
            # blank the cache as the proxy isn't there
            _infoCache = {}
            return ''

        # we're OK to use the cache
        if opt in _infoCache and\
            ( _infoCache[ opt ][ 1 ] > ( time.time() - info_refresh ) ) and\
                (_infoCache[opt][1] > os.path.getmtime(path)):
            logger.debug("Returning Cached Value %s" % opt)
            output = _infoCache[opt][0]
        else:
            output = ""

        return output

    def addToProxyCache(self, status, output, opt):
        """
        Test the result of grid proxy call
        and add to the cache if all OK

        status - the status output
        output - the output text
        opt - opt to add
        """

        if (not status) and (output):
            error = False
            for line in output.split('\n'):
                if (1 + line.find("Couldn't find a valid proxy")):
                    error = True

            if not error:
                _infoCache[opt] = [output, time.time()]

        return None

    # Add documentation strings from base class
    for method in [create, destroy, isAvailable, isValid, location,
                   renew, timeleft, timeleftInHMS]:
        if hasattr(ICredential, method.__name__):
            baseMethod = getattr(ICredential, method.__name__)
            setattr(method, "__doc__",
                    baseMethod.__doc__.replace("credential", "Grid Proxy"))
