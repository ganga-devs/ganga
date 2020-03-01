

import datetime
import os
import re
import subprocess
import sys
from getpass import getpass
from glob import glob

import GangaCore.Utility.logging
from GangaCore.Utility.Shell import Shell

from GangaCore.GPIDev.Adapters.ICredentialInfo import ICredentialInfo, cache, retry_command
from GangaCore.GPIDev.Adapters.ICredentialRequirement import ICredentialRequirement
from GangaCore.Core.exceptions import CredentialRenewalError

logger = GangaCore.Utility.logging.getLogger()


class AfsTokenInfo(ICredentialInfo):
    """
    A wrapper around an AFS token

    For now it is very CERN-specific (or at least only follows the CERN use-case)
    """

    should_warn = False

    info_pattern = re.compile(r"^User's \(AFS ID \d*\) tokens for (?P<id>\w*@\S*) \[Expires (?P<expires>.*)\]$", re.MULTILINE)

    __slots__=('shell', 'cache', 'initial_requirements')

    def __init__(self, requirements, check_file=False, create=False):
        """
        Args:
            requirements (ICredentialRequirement): An object specifying the requirements
            check_file (bool): Raise an exception if the file does not exist
            create (bool): Create the credential file
        """
        self.shell = Shell()

        super(AfsTokenInfo, self).__init__(requirements, check_file, create)

    @retry_command
    def create(self):
        """
        Creates a new AFS token

        Raises:
            CredentialRenewalError: If the renewal process returns a non-zero value
        """

        command = 'kinit'

        process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutdata, stderrdata = process.communicate(getpass('Kerberos password: ').encode())

        if process.returncode == 0:
            logger.info('AFS token %s created. Valid for %s', self.location, self.time_left())
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
        """
        This removes the kerberos token from disk
        """
        self.shell.cmd1('unlog')

        if self.location:
            os.remove(self.location)

    @property
    @cache
    def info(self):
        """
        This returns a summary of the token infor on disk
        """
        status, output, message = self.shell.cmd1('tokens')
        return output

    @cache
    def expiry_time(self):
        """
        This calculates the number of seconds left for the kerberos token on disk
        """
        info = self.info
        matches = re.finditer(AfsTokenInfo.info_pattern, info)

        if not matches:
            return datetime.timedelta()

        all_tokens = [match.group('expires') for match in matches]

        if len(all_tokens) > 1:
            if AfsTokenInfo.should_warn:
                logger.warning("Found multiple AFS tokens, taking soonest expiring one for safety")
                logger.warning("Tokens found for: %s".format(" ".join([match.group('id') for match in matches])))
                AfsTokenInfo.should_warn = False

        soonest = None

        for expires in all_tokens:
            now = datetime.datetime.now()
            #Add the current year as the token info doesn't include it but we need to know if it is a leap year
            expires = '%s %s' % (expires, now.year)
            expires = datetime.datetime.strptime(expires, '%b %d %H:%M %Y')

            # If the expiration date is in the past then assume it should be in the future
            if expires < now:
                expires = expires.replace(year=now.year+1)

            if not soonest or expires < soonest:
                soonest = expires

        return soonest

    def default_location(self):
        """
        This returns the default location of a kerberos token on disk as determined from the uid
        """
        krb_env_var = os.getenv('KRB5CCNAME', '')
        if krb_env_var.startswith('FILE:'):
            krb_env_var = krb_env_var[5:]

        # If file already exists
        if os.path.exists(krb_env_var):
            return krb_env_var

        # Lets try to find it if we can't get it from the env
        default_name_prefix = '/tmp/krb5cc_{uid}'.format(uid=os.getuid())
        matches = glob(default_name_prefix+'*')  # Check for partial matches on disk
        if len(matches) == 1:  # If one then use it
            filename_guess = matches[0]
        else: # Otherwise use the default
            filename_guess = default_name_prefix
        return filename_guess


class AfsToken(ICredentialRequirement):
    """
    An object specifying the requirements of an AFS token
    """
    _schema = ICredentialRequirement._schema.inherit_copy()

    _category = 'CredentialRequirement'

    info_class = AfsTokenInfo

    def encoded(self):
        """
        Ther kerberos token doesn't encode any additional information into the token location
        """
        return ''

