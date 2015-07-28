from .exceptions import CredentialsError

from Ganga.GPIDev.Base.Proxy import stripProxy

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from datetime import timedelta
import os
from abc import ABCMeta, abstractmethod


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

        self.expiry_time = None  # cache the expiry time for faster validity lookups
    
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
    def time_left(self):
        """
        Returns the time left in a human readable form
        """
        pass
    
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
