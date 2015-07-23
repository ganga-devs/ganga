from Ganga.GPIDev.Schema import SimpleItem
from Ganga.Utility.Shell import Shell

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from .ICredentialInfo import ICredentialInfo
from .ICredentialRequirement import ICredentialRequirement
from .exceptions import CredentialRenewalError

import os

import subprocess
import datetime
from getpass import getpass

import re
info_pattern = re.compile(r"^User's \(AFS ID \d*\) tokens for (?P<id>\w*@\S*) \[Expires (?P<expires>.*)\]$", re.MULTILINE)


class AfsTokenInfo(ICredentialInfo):
    """
    A wrapper around an AFS token

    For now it is very CERN-specific (or at least only follows the CERN use-case)
    """

    def __init__(self, requirements, check_file=False, create=False):
        self.shell = Shell()

        super(AfsTokenInfo, self).__init__(requirements, check_file, create)

    def create(self):
        """
        Creates a new AFS token

        Raises:
            CredentialRenewalError: If the renewal process returns a non-zero value
        """

        command = 'kinit'

        process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutdata, stderrdata = process.communicate(getpass('Kerberos password: '))

        if process.returncode == 0:
            logger.info('AFS token {path} created. Valid for {time}'.format(path=self.location, time=self.time_left()))
        else:
            raise CredentialRenewalError(stderrdata)

    def renew(self):
        """
        Renews the AFS token

        Raises:
            CredentialRenewalError: If the renewal process returns a non-zero value
        """

        status, output, message = self.shell.cmd1('kinit -R')

        if status != 0:
            logger.debug('kinit -R failed, creating as new')
            self.create()

    def destroy(self):
        status, output, message = self.shell.cmd1('unlog')

        if self.location:
            os.remove(self.location)

    def info(self):
        status, output, message = self.shell.cmd1('tokens')
        return output

    def time_left(self):
        info = self.info()
        matches = re.finditer(info_pattern, info)

        if not matches:
            return datetime.timedelta()

        expires = [match.group('expires') for match in matches if match.group('id') == 'afs@cern.ch'][0]
        expires = datetime.datetime.strptime(expires, '%b %d %H:%M')
        now = datetime.datetime.now()
        expires = expires.replace(year=now.year)

        # If the expiration date is in the past then assume it should be in the future
        if expires < now:
            expires = expires.replace(year=now.year+1)

        return expires - now


class AfsToken(ICredentialRequirement):
    """
    An object specifying the requirements of an AFS token
    """
    _schema = ICredentialRequirement._schema.inherit_copy()

    _category = "CredentialRequirement"
    _name = "AfsToken"

    _infoClass = AfsTokenInfo

    def encoded(self):
        return ''

    def is_empty(self):
        return True

    def set_defaults_from_config(self):
        pass

    def default_location(self):
        KRB5CCNAME = os.getenv("KRB5CCNAME", '')
        if KRB5CCNAME.startswith('FILE:'):
            KRB5CCNAME = KRB5CCNAME[5:]
        return KRB5CCNAME or "/tmp/krb5cc_{uid}".format(uid=os.getuid())
