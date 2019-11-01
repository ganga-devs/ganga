
import pytest
import functools

from .GangaUnitTest import load_config_files, clear_config
from GangaCore.GPIDev.Credentials.CredentialStore import credential_store
from GangaCore.Core.exceptions import CredentialsError
from GangaCore.Utility.Config import getConfig

external = pytest.mark.externals    

class skipif_config(object):
    """
    Class used to skip a test when it cannot be run due to a conflicting env config
    """

    def __init__(self, section, item, value, reason):
        """
        This is the constructor which takes the flags required by the wrapper
        Args:
            section(str): This is the name of the section in getConfig
            item(str): This is the name of the item under getConfig
            value(anything): This is the value we require to run the test
            reason(str): This is the string which will be returned if we skip the test explaining why
        """
        self.section = section
        self.item = item
        self.value = value
        self.reason = reason

    def __call__(self, function):
        """
        This is the actual function call which is done at the level of decorator testing
        Args:
            function (method, class): This is the test method or class we want to skip if the conditions aren't met
        """
        # Load config
        load_config_files()
        # test if the config parameter is what we require
        test = getConfig(self.section)[self.item] == self.value
        clear_config()
        if test:
            # Mark test as skip if it's not what we require
            return pytest.mark.skip(self.reason)(function)

        return function

class requires_cred(object):
    """
    Class used to skip a test is a required credential is not available
    """

    def __init__(self, credential_requirement, reason):
        """
        This is the constructor for taking the credential and skip reason via the arguments to the decorator
        """
        self._cred_req = credential_requirement
        self.reason = reason

    def __call__(self, function):
        """
        This call is called at the detection step of the tests for py.test.
        It wil fail if the credential_requirement is not present in the store and can't be auto-detected
        Args:
            function (method, class): This is the test method or class we want to skip if the conditions aren't met
        """
        load_config_files()
        from GangaCore.GPIDev.Credentials.CredentialStore import credential_store
        try:
            credential_store[self._cred_req]
            return function
        except KeyError:
            try:
                credential_store.create(self._cred_req)
                return function
            except CredentialsError:
                return pytest.mark.skip(self.reason)(function)
        finally:
            clear_config()

