from __future__ import absolute_import

import os
import re
from datetime import datetime, timedelta

import Ganga.Utility.logging
from Ganga.GPIDev.Schema import SimpleItem
from Ganga.Utility.Shell import Shell

from Ganga.GPIDev.Credentials2.ICredentialInfo import ICredentialInfo, cache
from Ganga.GPIDev.Credentials2.ICredentialRequirement import ICredentialRequirement
from Ganga.GPIDev.Credentials2.exceptions import CredentialRenewalError

from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv

logger = Ganga.Utility.logging.getLogger()


class DiracProxyInfo(ICredentialInfo):
    """
    A wrapper around a DIRAC proxy file
    """

    def __init__(self, requirements, check_file=False, create=False):
        self.shell = Shell()
        self.shell.env.update(getDiracEnv())

        super(DiracProxyInfo, self).__init__(requirements, check_file, create)

    def create(self):
        """
        Creates the grid proxy.

        Raises:
            CredentialRenewalError: If the renewal process returns a non-zero value
        """
        group_command = ''
        logger.debug('require ' + self.initial_requirements.group)
        if self.initial_requirements.group:
            group_command = '--group %s --VOMS' % self.initial_requirements.group
        command = 'dirac-proxy-init --pwstdin --out %s %s' % (self.location, group_command)
        logger.debug(command)
        self.shell.env['X509_USER_PROXY'] = self.location
        rc = self.shell.system(command)
        if rc == 0:
            logger.debug('Grid proxy {path} created. Valid for {time}'.format(path=self.location, time=self.time_left()))
        else:
            raise CredentialRenewalError('Failed to create DIRAC proxy')

    def destroy(self):
        if os.path.isfile(self.location):
            os.remove(self.location)

    @cache
    def info(self):
        self.shell.env['X509_USER_PROXY'] = self.location
        status, output, message = self.shell.cmd1('dirac-proxy-info --file %s' % self.location)
        return output

    def _from_info(self, label):
        # type: (str) -> str
        line = re.search(r'^{0}\s*: (.*)$'.format(label), self.info(), re.MULTILINE)
        return line.group(1)

    @property
    @cache
    def identity(self):
        return self._from_info('identity')

    @property
    @cache
    def group(self):
        return self._from_info('DIRAC group')

    @cache
    def expiry_time(self):
        status, output, message = self.shell.cmd1('voms-proxy-info -file %s -timeleft' % self.location)
        if status != 0:
            return datetime.now()
        return datetime.now() + timedelta(seconds=int(output))

    def default_location(self):
        return os.getenv('X509_USER_PROXY') or '/tmp/x509up_u' + str(os.getuid())


class DiracProxy(ICredentialRequirement):
    """
    An object specifying the requirements of a DIRAC proxy file
    """
    _schema = ICredentialRequirement._schema.inherit_copy()
    _schema.datadict['group'] = SimpleItem(defvalue=None, typelist=[str, None], doc='Group for the proxy')

    _category = 'CredentialRequirement'
    _name = 'DiracProxy'

    info_class = DiracProxyInfo

    def __init__(self, **kwargs):
        super(DiracProxy, self).__init__(**kwargs)
        #if 'vo' not in kwargs and getConfig('LCG')['VirtualOrganisation']:
        #    self.vo = getConfig('LCG')['VirtualOrganisation']

    def encoded(self):
        return ':'.join(requirement for requirement in [self.group] if requirement)  # filter out the empties
