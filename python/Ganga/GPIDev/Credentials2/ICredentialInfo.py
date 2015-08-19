from .exceptions import CredentialsError

from Ganga.GPIDev.Base.Proxy import stripProxy

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from datetime import datetime, timedelta
import os
import os.path
from abc import ABCMeta, abstractmethod
from functools import wraps


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
    """
    @wraps(method)
    def wrapped_function(self, *args, **kwargs):

        # If the mtime has been changed, clear the cache
        mtime = os.path.getmtime(self.location)  # TODO If file doesn't exist, do the right thing.
        if mtime > self.cache['mtime']:
            self.cache = {'mtime': mtime}

        # If entry is missing from cache, repopulate it
        # This will run if the cache was just cleared
        if method.func_name not in self.cache:
            self.cache[method.func_name] = method(self, *args, **kwargs)

        return self.cache[method.func_name]
    return wrapped_function


class ICredentialInfo(object):
    """
    The interface for all credential types.
    Each object covers one credential file exactly.
    The proxy/token is central to the object and all information is gathered from there.
    Everything is cached internally
    
    These are only created by the store and should not be persisted.
    """
    __metaclass__ = ABCMeta

    def __init__(self, requirements, check_file=False, create=False):
        """
        Args:
            requirements (ICredentialRequirement): An object specifying the requirements
            check_file: Raise an exception if the file does not exist
            create: Create the credential file
        
        Raises:
            IOError: If told to wrap a non-existent file
            CredentialsError: If this object cannot satisfy ``requirements``
        """
        super(ICredentialInfo, self).__init__()

        self.cache = {'mtime': 0}

        requirements = stripProxy(requirements)
        
        if not requirements.location:
            # If we weren't given a location, assume the encoded location
            requirements.location = ':'.join([requirements.default_location(), requirements.encoded()])

        self.initialRequirements = requirements  # Store the requirements that the object was created with. Used for creation
        
        if check_file:
            logger.debug("Trying to wrap {path}".format(path=self.location))
            if not os.path.exists(self.location):
                raise IOError("Proxy file {path} not found".format(path=self.location))
            logger.debug("Wrapping existing file %s", self.location)
            
        if create:
            logger.debug("Making a new one")
            self.create()

        # If the proxy object does not satisfy the requirements then abort the construction
        if not self.check_requirements(requirements):
            raise CredentialsError('Proxy object cannot satisfy its own requirements')
    
    def __str__(self):
        return '{class_name} at {file_path}'.format(class_name=type(self).__name__, file_path=self.location)
    
    @property
    def location(self):
        """
        The location of the file on disk
        """
        return self.initialRequirements.location

    @abstractmethod
    def create(self):
        """
        Create a new credential file
        """
        pass
    
    def renew(self):
        """
        Renew an existing credential file
        """
        self.create()

    def is_valid(self):
        """
        Is the credential valid to be used
        """
        # TODO We should check that there's more than some minimum time left on the proxy
        return self.time_left() > timedelta()

    @abstractmethod
    def expiry_time(self):
        """
        Returns the expiry time as a datetime.datetime
        """
        pass

    def time_left(self):
        """
        Returns the time left as a datetime.timedelta
        """
        time_left = self.expiry_time() - datetime.now()
        return max(time_left, timedelta())
    
    def check_requirements(self, query):
        """
        Args:
            query (ICredentialRequirement): The requirements to check ourself against
        
        Checks all requirements.
        
        Returns:
            ``True`` if we meet all requirements
            ``False`` if even one requirement is not met or if the credential is not valid
        """
        return all(self.check_requirement(query, requirementName) for requirementName in stripProxy(query)._schema.datadict)

    def check_requirement(self, query, requirement_name):
        """
        Args:
            query (ICredentialRequirement): 
            requirement_name (str): The requirement attribute to check
        
        Returns:
            ``True`` if ``self`` matches ``query``'s ``requirement``.
        """
        requirement_value = getattr(query, requirement_name)
        logger.debug('{name}: \t{cred} \t{requested}'.format(name=requirement_name, cred=getattr(self, requirement_name), requested=requirement_value))
        if requirement_value is None:
            # If this requirementName is unspecified then ignore it
            return True
        return getattr(self,requirement_name) == requirement_value

    def __eq__(self, other):
        return self.location == other.location

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.location)
