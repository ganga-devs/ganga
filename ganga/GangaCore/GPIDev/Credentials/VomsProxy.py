

import os
import subprocess
import tempfile
from datetime import datetime, timedelta

import GangaCore.Utility.logging
from GangaCore.GPIDev.Schema import SimpleItem
from GangaCore.Utility import GridShell
from GangaCore.Utility.Config import getConfig

from GangaCore.GPIDev.Adapters.ICredentialInfo import ICredentialInfo, cache, retry_command
from GangaCore.GPIDev.Adapters.ICredentialRequirement import ICredentialRequirement
from GangaCore.Core.exceptions import CredentialRenewalError, InvalidCredentialError

logger = GangaCore.Utility.logging.getLogger()


class VomsProxyInfo(ICredentialInfo):
    """
    A wrapper around a voms proxy file
    """

    def __init__(self, requirements, check_file=False, create=False):
        """
        Args:
            requirements (ICredentialRequirement): An object specifying the requirements
            check_file (bool): Raise an exception if the file does not exist
            create (bool): Create the credential file
        """

        self._shell = None

        super(VomsProxyInfo, self).__init__(requirements, check_file, create)

    @retry_command
    def create(self):
        """
        Creates the grid proxy.

        Raises:
            CredentialRenewalError: If the renewal process returns a non-zero value
        """
        voms_command = ''
        logger.debug('require ' + str(self.initial_requirements.vo))
        if self.initial_requirements.vo:
            voms_command = '-voms %s' % self.initial_requirements.vo
            if self.initial_requirements.group or self.initial_requirements.role:
                voms_command += ':'
                if self.initial_requirements.group:
                    voms_command += '/%s' % self.initial_requirements.group
                if self.initial_requirements.role:
                    voms_command += '/%s' % self.initial_requirements.role
        logger.debug(voms_command)
        command = 'voms-proxy-init -out "%s" %s' % (self.location, voms_command)
        logger.debug(command)
        if not self.shell:
            raise CredentialRenewalError('Failed to create VOMS proxy due to configuration problem')
        try:
            self.shell.check_call(command)
        except subprocess.CalledProcessError:
            raise CredentialRenewalError('Failed to create VOMS proxy')
        else:
            logger.debug('Grid proxy %s created. Valid for %s', self.location, self.time_left())

    @property
    def shell(self):
        """
        This creates a grid shell instance which is used by the VomsProxy only when needed
        """
        if self._shell is None:
            self._shell = GridShell.getShell()
        return self._shell

    def destroy(self):
        """
        This destroys the voms proxy on disk
        """
        self.shell.cmd1('voms-proxy-destroy -file "%s"' % self.location, allowed_exit=[0, 1])

        if os.path.isfile(self.location):
            os.remove(self.location)

    @cache
    def info(self):
        """
        This returns the full proxy info with all information from a given proxy on disk
        """
        status, output, message = self.shell.cmd1('voms-proxy-info -all -file "%s"' % self.location)
        return output

    @property
    @cache
    def identity(self):
        """
        This returns the identity associated with the voms proxy on disk
        """
        status, output, message = self.shell.cmd1('voms-proxy-info -file "%s" -identity' % self.location)
        return output.strip()

    @property
    @cache
    def vo(self):
        """
        This returns the vo associated with a voms proxy on disk
        """
        status, output, message = self.shell.cmd1('voms-proxy-info -file "%s" -vo' % self.location)
        if status != 0:
            return None
        return output.split(':')[0].strip()

    @property
    @cache
    def role(self):
        """
        This returns the role associated with a voms proxy on disk
        """
        status, output, message = self.shell.cmd1('voms-proxy-info -file "%s" -vo' % self.location)
        if status != 0:
            return None  # No VO
        vo_list = output.split(':')
        if len(vo_list) <= 1:
            return None  # No command after VO
        return vo_list[1].split('/')[-1].split('=')[-1].strip()

    @property
    @cache
    def group(self):
        """
        This returns the group associated with a voms proxy on disk
        """
        status, output, message = self.shell.cmd1('voms-proxy-info -file "%s" -vo' % self.location)
        if status != 0:
            return None  # No VO
        vo_list = output.split(':')
        if len(vo_list) <= 1:
            return None  # No command after VO
        # TODO Make this support multiple groups and subgroups
        group_role_list = vo_list[1].split('/')
        if len(group_role_list) <= 2:
            return None  # No group specified in command
        return group_role_list[-1].strip()

    @cache
    def expiry_time(self):
        """
        This returns the time that a proxy will expire at in seconds
        """
        status, output, message = self.shell.cmd1('voms-proxy-info -file "%s" -timeleft' % self.location)
        if status != 0:
            return datetime.now()
        return datetime.now() + timedelta(seconds=int(output))

    def default_location(self):
        """
        This returns the default_location of a voms proxy on disk based only on the location and uid
        """
        return os.getenv('X509_USER_PROXY') or os.path.join(tempfile.gettempdir(), 'x509up_u'+str(os.getuid()))


class VomsProxy(ICredentialRequirement):
    """
    An object specifying the requirements of a VOMS proxy file
    """
    _schema = ICredentialRequirement._schema.inherit_copy()
    _schema.datadict['identity'] = SimpleItem(defvalue=None, typelist=[str, None], doc='Identity for the proxy')
    _schema.datadict['vo'] = SimpleItem(defvalue=None, typelist=[str, None], doc='Virtual Organisation for the proxy. Defaults to LGC/VirtualOrganisation')
    _schema.datadict['role'] = SimpleItem(defvalue=None, typelist=[str, None], doc='Role that the proxy must have')
    _schema.datadict['group'] = SimpleItem(defvalue=None, typelist=[str, None], doc='Group for the proxy - either "group" or "group/subgroup"')

    _category = 'CredentialRequirement'

    info_class = VomsProxyInfo

    def __init__(self, **kwargs):
        """
        Construct a voms proxy requirement and assign the default vo from the config if none has been provided
        """
        super(VomsProxy, self).__init__(**kwargs)
        if 'vo' not in kwargs and getConfig('LCG')['VirtualOrganisation']:
            self.vo = getConfig('LCG')['VirtualOrganisation']

    def encoded(self):
        """
        This returns the additional encoding of the identity, vo, role and group which are to be encoded into the voms file location
        """
        return ':'.join(requirement for requirement in [self.identity, self.vo, self.role, self.group] if requirement)  # filter out the empties

