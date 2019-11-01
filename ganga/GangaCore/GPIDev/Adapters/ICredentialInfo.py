

import copy
import os
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from functools import wraps
import time
import threading

import GangaCore.Utility.logging

from GangaCore.Core.exceptions import CredentialsError
from GangaCore.GPIDev.Credentials.CredentialStore import credential_store

from GangaCore.Utility.Config import getConfig

logger = GangaCore.Utility.logging.getLogger()

def cache(method):
    """
    The cache decorator can be applied to any method in an ``ICredentialInfo` subclass.
    It stores the return value of the function in the ``self.cache`` dictionary
    with the key being the name of the function.
    The cache is invalidated if ``os.path.getmtime(self.location)`` returns
    greater than ``self.cache['mtime']``, i.e. if the file has changed on disk

    Not having to call the external commands comes at the cost of calling ``stat()``
    every time a parameter is accessed but this is still a saving of many orders of
    magnitude.

    Args:
        method (function): This is the method which we're wrapping
    """

    @wraps(method)
    def cache_function(self, *args, **kwargs):

        with self.cache_lock:

            if 'mtime' in self.cache:
                time_before = self.cache['mtime']
            else:
                time_before = -1

            if 'ccheck' in self.cache:
                check_time = self.cache['ccheck']
            else:
                check_time = time.time()

            # If the mtime has been changed, clear the cache
            if credential_store.enable_caching and os.path.exists(self.location):
                if (time.time() - check_time > getConfig('Credentials')['AtomicDelay']) or (abs(time.time() - time_before)  > getConfig('Credentials')['AtomicDelay']):
                    mtime = os.path.getmtime(self.location)
                    if mtime > self.cache['mtime']:
                        self.cache = {'mtime': mtime}
            else:
                self.cache = {'mtime': 0}

            time_after = self.cache['mtime']

            if time_before != time_after:
                self.cache.clear()

            self.cache['mtime'] = time_after

            # If entry is missing from cache, repopulate it
            # This will run if the cache was just cleared
            if method.__name__ not in self.cache:
                self.cache[method.__name__] = method(self, *args, **kwargs)

            return self.cache[method.__name__]
    return cache_function

def retry_command(method):
    """
    This method attempts to run the same function which can exit due to a CredetialsError
    Any other exceptions are raised as appropriate. If a command fails it's assumed to have failed due to an invalid user input and is retried upto crednential_store.retry_limit
    Args:
        method (function): This is the method which we're wrapping
    """
    @wraps(method)
    def retry_function(*args, **kwds):
        for _ in range(credential_store.retry_limit):
            try:
                return method(*args, **kwds)
            except CredentialsError:
                # We expect errors of this type
                pass
        return None
    return retry_function

class ICredentialInfo(object, metaclass=ABCMeta):
    """
    The interface for all credential types.
    Each object covers one credential file exactly.
    The credential file is central to the object and all information is gathered from there.

    These are only created by the store and should not be persisted.
    """

    __slots__ = ('cache', 'cache_lock', 'initial_requirements')

    def __init__(self, requirements, check_file=False, create=False):
        # type: (ICredentialRequirement, bool, bool) -> None
        """
        Args:
            requirements (ICredentialRequirement): An object specifying the requirements
            check_file (bool): Raise an exception if the file does not exist
            create (bool): Create the credential file

        Raises:
            IOError: If told to wrap a non-existent file
            CredentialsError: If this object cannot satisfy ``requirements``
        """
        super(ICredentialInfo, self).__init__()

        self.cache = {'mtime': 0}
        self.cache_lock = threading.RLock()

        self.initial_requirements = copy.deepcopy(requirements)  # Store the requirements that the object was created with. Used for creation

        if check_file:
            logger.debug('Trying to wrap %s', self.location)
            if not self.exists():
                raise IOError('Proxy file {path} not found'.format(path=self.location))
            logger.debug('Wrapping existing file %s', self.location)

        if create:
            logger.debug('Making a new one')
            self.create()

        # If the proxy object does not satisfy the requirements then abort the construction
        if not self.check_requirements(requirements):
            raise CredentialsError('Proxy object cannot satisfy its own requirements')

    def __str__(self):
        """ Returns a user-readable string describing the credential and it's validity """
        return '{class_name} at {file_path} : TimeLeft = {time_left}, Valid = {currently_valid}'.format(\
                        class_name=type(self).__name__, file_path=self.location, currently_valid=self.is_valid(), time_left=self.time_left())

    def _repr_pretty_(self, p, cycle):
        """ A wrapper to self.__str__ for the pretty print methods in IPython """
        p.text(str(self))

    @property
    def location(self):
        # type: () -> str
        """
        The location of the file on disk
        """
        location = self.default_location()
        encoded_ext = self.initial_requirements.encoded()
        if encoded_ext and not location.endswith(encoded_ext):
            location += ':' + encoded_ext

        return location

    @abstractmethod
    def default_location(self):
        # type: () -> str
        """
        Returns the default location for the credential file.
        This is the location that most tools will look for the file
        or where the file is created without specifying anything.
        """
        pass

    @abstractmethod
    def create(self):
        # type: () -> None
        """
        Create a new credential file
        """
        pass

    def renew(self):
        # type: () -> None
        """
        Renew an existing credential file
        """
        self.create()

    def is_valid(self):
        # type: () -> bool
        """
        Is the credential valid to be used
        """
        # TODO We should check that there's more than some minimum time left on the proxy
        return self.time_left() > timedelta()

    @abstractmethod
    def expiry_time(self):
        # type: () -> datetime.datetime
        """
        Returns the expiry time
        """
        pass

    def time_left(self):
        # type: () -> datetime.timedelta
        """
        Returns the time left
        """
        time_left = self.expiry_time() - datetime.now()
        return max(time_left, timedelta())

    def check_requirements(self, query):
        # type: (ICredentialRequirement) -> bool
        """
        Args:
            query (ICredentialRequirement): The requirements to check ourself against

        Checks all requirements.

        Returns:
            ``True`` if we meet all requirements
            ``False`` if even one requirement is not met or if the credential is not valid
        """
        if not self.exists():
            logger.debug('Credential does NOT exit')
            return False
        logger.debug('Credential exists, checking it')
        return all(self.check_requirement(query, requirementName) for requirementName in query._schema.datadict)

    def check_requirement(self, query, requirement_name):
        # type: (ICredentialRequirement, str) -> bool
        """
        Args:
            query (ICredentialRequirement):
            requirement_name (str): The requirement attribute to check

        Returns:
            ``True`` if ``self`` matches ``query``'s ``requirement``.
        """
        requirement_value = getattr(query, requirement_name)
        if requirement_value is None:
            # If this requirementName is unspecified then ignore it
            logger.debug('Param \'%s\': is None' % requirement_name)
            return True
        logger.debug('Param \'%s\': Have \t%s Want \t%s', requirement_name, getattr(self, requirement_name), requirement_value)
        return getattr(self, requirement_name) == requirement_value

    def exists(self):
        # type: () -> bool
        """
        Does the credential file exist on disk
        """
        logger.debug('Checking for Credential at: \'%s\'' % self.location)
        return os.path.exists(self.location)

    def __eq__(self, other):
        """
        Test the equality of a crednetial against another by comparing their on-disk location-s
        Args:
            other (ICredentialInfo) This is the object being compared against
        """
        return self.location == other.location

    def __ne__(self, other):
        """
        Returns the inverse of the equality check
        Args:
            other (ICredentialInfo) This is the object being compared against
        """
        return not self == other

    def __hash__(self):
        """
        Returns a hash of the location of the object on disk
        """
        return hash(self.location)

