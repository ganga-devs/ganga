from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

from Ganga.Utility.Shell import Shell

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from .ICredentialInfo import ICredentialInfo
from .ICredentialRequirement import ICredentialRequirement
from .exceptions import CredentialsError, CredentialRenewalError

import os

import subprocess
from getpass import getpass

def _convertHmsToSeconds(hmsString):
    """
    Take a string like '10:32:36' and convert it to ``37956``
    If it gets something like '8:43' then it assumes that it is is hours and minutes
    If it only receives '7' then it assumes hours
    """
    timeList = hmsString.split(":")
    totalTime = 0
    if len( timeList ) >= 1:
        totalTime = totalTime + int( timeList[ 0 ] ) * 60 * 60
    if len( timeList ) >= 2:
        totalTime = totalTime + int( timeList[ 1 ] ) * 60
    if len( timeList ) >= 3:
        totalTime = totalTime + int (timeList[ 2 ] )
    return totalTime

class VomsProxyInfo(ICredentialInfo):
    """
    A wrapper around a voms proxy file
    """
    
    def __init__(self, requirements, check_file = False, create = False):
        self.shell = Shell()
        
        super(VomsProxyInfo, self).__init__(requirements, check_file, create)

    def renew(self):
        """
        Renew the grid proxy.
        
        Really this function creates a brand new proxy file
        
        Raises:
            CredentialRenewalError: If the renewal process returns a non-zero value
        """
        vomsCommand = ""
        logger.info("require " + self.initialRequirements.vo)
        if self.initialRequirements.vo:
            vomsCommand = "-voms %s" % self.initialRequirements.vo
            if self.initialRequirements.group or self.initialRequirements.role:
                vomsCommand += ":"
                if self.initialRequirements.group:
                    vomsCommand += "/%s" % self.initialRequirements.group
                if self.initialRequirements.role:
                    vomsCommand += "/%s" % self.initialRequirements.role
        logger.info(vomsCommand)
        command = 'voms-proxy-init -pwstdin -out %s %s' % (self.location, vomsCommand)
        logger.info(command)
        # This next section used to use "status, output, message = self.shell.cmd1(command)" but that had no way to pass in the password
        process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutdata, stderrdata = process.communicate(getpass('Grid password: '))
        if process.returncode == 0:
            logger.info('Grid proxy {path} renewed. Valid for {time}'.format(path=self.location, time=self.timeLeft()))
        else:
            raise CredentialRenewalError(stderrdata)
    
    def destroy(self):
        status, output, message = self.shell.cmd1("voms-proxy-destroy -file %s" % self.location, allowed_exit = [0, 1])
        
        if self.location:
            os.remove(self.location)
    
    def info(self):
        status, output, message = self.shell.cmd1('voms-proxy-info -all -file %s' % self.location)
        return output
    
    @property
    def identity(self):
        status, output, message = self.shell.cmd1('voms-proxy-info -file %s -identity' % self.location)
        return output.strip()
    
    @property
    def vo(self):
        status, output, message = self.shell.cmd1('voms-proxy-info -file %s -vo' % self.location)
        if status != 0:
            return None
        return output.split(":")[0].strip()
       
    @property
    def role(self):
        status, output, message = self.shell.cmd1('voms-proxy-info -file %s -vo' % self.location)
        if status != 0:
            return None #No VO
        voList = output.split(":")
        if len(voList) <= 1:
            return None #No command after VO
        return voList[1].split("/")[-1].split("=")[-1].strip()
    
    @property
    def group(self):
        status, output, message = self.shell.cmd1('voms-proxy-info -file %s -vo' % self.location)
        if status != 0:
            return None #No VO
        voList = output.split(":")
        if len(voList) <= 1:
            return None #No command after VO
        #TODO Make this support multiple groups and subgroups
        groupRoleList = voList[1].split("/")
        if len(groupRoleList) <= 2:
            return None #No group specified in command
        return groupRoleList[-1].strip()
    
    def __eq__(self, other):
        return self.location == other.location
    
    def __ne__(self, other):
        return not self == other
    
    def timeLeftInSeconds(self):
        status, output, message = self.shell.cmd1('voms-proxy-info -file %s -timeleft' % self.location)
        if status != 0:
            return 0
        return int(output)

class VomsProxy(ICredentialRequirement):
    """
    An object specifying the requirements of a VOMS proxy file
    """
    _schema = ICredentialRequirement._schema.inherit_copy()
    _schema.datadict["identity"] = SimpleItem(defvalue = None, typelist = ['str', 'None'], doc = "Identity for the proxy")
    _schema.datadict["vo"] = SimpleItem(defvalue = None, typelist = ['str', 'None'], doc = "Virtual Organisation for the proxy")
    _schema.datadict["role"] = SimpleItem(defvalue = None, typelist = ['str', 'None'], doc = "Role that the proxy must have")
    _schema.datadict["group"] = SimpleItem(defvalue = None, typelist = ['str', 'None'], doc = "Group for the proxy - either 'group' or 'group/subgroup'")
                                                                                
    _category = "credentials2"
    _name = "VomsProxy"
    
    _infoClass = VomsProxyInfo
    
    _defaults = {'vo': 'lhcb',
                 'role': None,
                 'group': None} #TODO move this to the config file
    
    def encoded(self):
        return ':'.join(requirement for requirement in (self.identity, self.vo, self.role, self.group) if requirement) #filter out the empties
    
    def isEmpty(self):
        return not (self.identity or self.vo or self.role or self.group)
    
    def setDefaultsFromConfig(self):
        defaults = self._defaults #TODO get these from config file
        
        for param in self._schema.datadict.keys():
            if param in defaults:
                setattr(self, param, defaults[param])
    
    def defaultLocation(self):
        return os.getenv("X509_USER_PROXY") or "/tmp/x509up_u"+str(os.getuid())
